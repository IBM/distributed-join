(ns distributed-join.embedded-jetty
  (:require [clojure.test :refer :all]
            [muuntaja.core :as muuntaja]
            [ring.adapter.jetty :as jetty]
            [ring.util.http-response :refer :all]
            [compojure.api.sweet :refer :all]
            [schema.core :as schema :include-macros true]
            [clojure.tools.logging :as log]
            [config.core :refer [env]]
            [distributed-join.store :as store]
            [clojure.string :as str]
            [compojure.route :as route])
  (:import (org.eclipse.jetty.server Server)))

(def ^:private request-stack (atom []))

(defn get-last-request []
  (last @request-stack))

(defn reset-test-data []
  (reset! request-stack []))

(defn- add-request [request-uri request-body]
  (swap! request-stack conj {:request-uri request-uri :request-body request-body}))

(def app
  (api
    {:formats (muuntaja/select-formats muuntaja/default-options ["application/json" "application/edn"])}
    (context "/v1" []
      (POST "/publish" []
        :responses {201 {:schema store/Publish}
                    400 {:description "Bad request data"}}  ;; this is a way to add failure responses
        :body [body store/Publish]
        :summary "loopback endpoint for testing an endpoint dispatch"
        :tags ["debug"]
        (log/info "Received publish request:" body)
        (add-request "/v1/publish" body)
        (created nil body))

      (POST "/publish/timeout" []
        :responses {201 {:schema store/Publish}
                    400 {:description "Bad request data"}}  ;; this is a way to add failure responses
        :body [body store/Publish]
        :summary "loopback endpoint for testing an endpoint dispatch of timeouts"
        :tags ["debug"]
        (log/info "Received publish request for timed out:" body)
        (add-request "/v1/publish/timeout" body)
        (created nil body))

      (POST "/delayed/echo" []
        :responses {201 {:schema schema/Any}
                    400 {:description "Bad request data"}}
        :body [body schema/Any]
        :query-params [delay :- Long]
        :summary "Allows testing by specifying an amount of time to delay (in milliseconds) sending back the body"
        :tags ["debug"]
        (log/info "Received delay of" delay "to echo:" body)
        (Thread/sleep delay)
        (created nil body)))

    (undocumented
      (route/not-found "Not Found"))))

(def ^:private embedded-jetty (jetty/run-jetty app {:port (:embedded-jetty-port env) :join? false}))

(defn- setup-test-with-jetty []
  (log/info "Resetting test data within Embedded Jetty...")
  (reset-test-data)
  embedded-jetty)

(defn jetty-fixture [f]
  (setup-test-with-jetty)
  (f))

(defn verify-last-request-uri [uri]
  (let [last-req (get-last-request)]
    (log/info "The last request was:" last-req "and the uri is:" uri)
    (is (some? last-req))
    (is (str/ends-with? uri (:request-uri last-req)))))
