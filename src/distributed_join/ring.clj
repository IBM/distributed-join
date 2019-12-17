(ns distributed-join.ring
  (:require [compojure.route :as route]
            [ring.adapter.jetty :as jetty]
            [ring.util.http-response :refer :all]
            [compojure.api.sweet :refer :all]
            [compojure.api.exception :as ex]
            [schema.core :as schema]
            [distributed-join.core :as core]
            [distributed-join.store :as store]
            [distributed-join.http-handling :refer [respond err] :as handling]
            [distributed-join.scheduler :as scheduler]
            [metrics.ring.instrument :refer [instrument]]
            [metrics.core :refer [default-registry]]
            [metrics.ring.expose :refer [render-metrics expose-metrics-as-json]]
            [manifold.deferred :as deferred]
            [muuntaja.core :as muuntaja]
            [clojure.tools.logging :as log]
            [config.core :refer [env]]
            [distributed-join.redis :as redis])
  (:import [dpx_join.store]
           (clojure.lang ExceptionInfo)
           [org.eclipse.jetty.server.handler StatisticsHandler]
           (org.eclipse.jetty.server Server))
  (:gen-class))

(def app
  (api
    {:swagger    {:ui      "/v1/documentation"
                  :spec    "/v1/swagger.json"
                  :options {:jsonEditor true}
                  :data    {:info {:title       "distributed-join"
                                   :version     (:distributed-join-version env)
                                   :description "as fork-join service"}
                            :tags [{:name "fork-join" :description "the core service"}
                                   {:name "debug" :description "only used for debug"}
                                   {:name "monitor" :description "only used to see current status"}]}}
     :formats    (muuntaja/select-formats muuntaja/default-options ["application/json" "application/edn"])

     ; if you need special exception handlers:
     ; :exceptions
     ;  {:handlers
     ;   {Exception (handler-goes-here)
     ;    ::ex/default (ex/with-logging (handler-goes-here))}}
     ;
     ; here is a custom-handler:
     ;
     ; (defn- custom-handler [f type]
     ;   (fn [^Exception e data request]
     ;     (f {:message (.getMessage e) :type type})})

     :middleware [expose-metrics-as-json instrument]}

    (context "/v1" []
      (POST "/join" []
        :responses {202 {:schema String}
                    400 {:schema String :description "Bad request data"}
                    404 {:schema String :description "Barrier id not found"}}
        :body [request store/Join]
        :summary "report a join"
        :tags ["fork-join"]
        (respond (core/join request)))

      (POST "/fork" []
        :responses {201 {:schema String}
                    400 {:description "Bad request data"}}  ;; this is a way to add failure responses
        :body [request store/Fork]
        :summary "create a fork"
        :tags ["fork-join"]
        (respond (core/fork request)))

      (GET "/status" []
        :responses {200 {:schema store/Status}
                    204 {:description "No content found"}
                    400 {:description "Bad request data"}}
        :query-params [barrier-id :- String]
        :summary "get status of in-flight fork"
        :tags ["monitor"]
        (if (> (count barrier-id) (:barrier-id-len-max env))
          (respond (err ::handling/bad-data (str "barrier-id exceeds " (:barrier-id-len-max env) " in length")))
          (respond (core/status barrier-id))))

      (POST "/publish" []
        :responses {201 {:schema store/Publish}
                    400 {:description "Bad request data"}}  ;; this is a way to add failure responses
        :body [body store/Publish]
        :summary "loopback endpoint for testing an endpoint dispatch"
        :tags ["debug"]
        (log/info "Received publish request:" body)
        (created nil body))

      (POST "/publish/timeout" []
        :responses {201 {:schema store/Publish}
                    400 {:description "Bad request data"}}  ;; this is a way to add failure responses
        :body [body store/Publish]
        :summary "loopback endpoint for testing an endpoint dispatch of timeouts"
        :tags ["debug"]
        (log/info "Received publish request for timed out:" body)
        (created nil body)))

    (undocumented
      (route/not-found "Not Found"))))

(defn conf
  [^Server server]
  (let [stats-handler (StatisticsHandler.)
        default-handler (.getHandler server)]
    (.setHandler stats-handler default-handler)
    (.setHandler server stats-handler)
    (.setStopTimeout server (:jetty-stop-timeout env))
    (.setStopAtShutdown server true)))

(defn -main []
  (scheduler/forever core/process-publish-messages (:publish-thread-count env) (:publish-delay-non-found-ms env))
  (scheduler/forever core/recover-unprocessed-publish-messages (:recover-publish-thread-count env) (:recover-publish-delay-non-found-ms env))

  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. #(do (scheduler/shutdown)
                                  (redis/shutdown))))

  (jetty/run-jetty app {:port (:jetty-port env) :configurator conf}))
