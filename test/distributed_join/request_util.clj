(ns distributed-join.request-util
  (:require [clojure.test :refer :all]
            [distributed-join.ring :refer :all]
            [distributed-join.error-util :refer :all]
            [clojure.tools.logging :as log]
            [ring.mock.request :as mock]
            [config.core :refer [env]]
            [cheshire.core :as cheshire])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang PersistentArrayMap ExceptionInfo)
           (java.io ByteArrayInputStream)))

(def time-to-wait-beyond-timeout-ms 100)

(defn pause [base-time-ms]
  (Thread/sleep (+ time-to-wait-beyond-timeout-ms base-time-ms)))

(defn log-failure-data [test-fn]
  (try
    (test-fn)
    (catch Exception ex
      (do (log/error "Failure data:" (ex-data-for-http-resp ex))
          (throw ex)))))

(defn log-failure-data-deref [test-fn]
  (log-failure-data #(@test-fn)))

(defn- add-max-wait [desc max-wait]
  (cond-> desc
          (some? max-wait) (assoc :max-wait max-wait)))

(defn gen-fork-req [join-id transaction-id count destination timeout-destination max-wait]
  (add-max-wait {:join-id join-id, :transaction-id transaction-id, :count count, :destination destination, :timeout-destination timeout-destination}
                max-wait))

(defn gen-join-req [barrier-id position message max-wait]
  (add-max-wait {:barrier-id barrier-id, :position position, :message message}
                max-wait))

(defn gen-destination-root-path []
  (str "http://localhost:" (:embedded-jetty-port env)))

(defn get-delay-url-path [delay-ms]
  (str "/v1/delayed/echo?delay=" delay-ms))

(defn gen-fork
  ([join-count max-wait extra-id]
   (gen-fork-req
     (str "a" extra-id)                                     ; join-id
     (str "b" extra-id)                                     ; transaction-id
     join-count                                             ; count
     (str (gen-destination-root-path) "/v1/publish")        ; destination
     (str (gen-destination-root-path) "/v1/publish/timeout") ; timeout destination
     max-wait))
  ([join-count max-wait]
   (gen-fork join-count max-wait (str (System/nanoTime))))
  ([join-count]
   (gen-fork join-count 1 (str (System/nanoTime)))))

(defn gen-fork-different-dest [join-count fork-max-wait destination timeout-destination]
  (cond-> (gen-fork join-count fork-max-wait)
          (some? destination) (assoc :destination (str (gen-destination-root-path) destination))
          (some? timeout-destination) (assoc :timeout-destination (str (gen-destination-root-path) timeout-destination))))

(defn gen-fork-invalid-dest [join-count fork-max-wait destination-invalid? timeout-destination-invalid?]
  (let [destination (when destination-invalid? "/bad/destination")
        timeout-destination (when timeout-destination-invalid? "/bad/timeout/destination")]
    (gen-fork-different-dest join-count fork-max-wait destination timeout-destination)))

(defmulti parse-body type)
(defmethod parse-body String [body]
  (when (seq body)
    (try
      (cheshire/parse-string body true)
      (catch JsonParseException ex
        (log/warn "Unable to parse body as json, so returning the body:" body)
        body))))
(defmethod parse-body ByteArrayInputStream [body]
  (parse-body (slurp body)))

(defmulti success? type)
(defmethod success? PersistentArrayMap [{status :status}]
  (success? status))
(defmethod success? Long [status]
  (and (not= status 204) (<= 200 status 299)))

(defmulti no-data? type)
(defmethod no-data? PersistentArrayMap [{status :status}]
  (no-data? status))
(defmethod no-data? Long [status]
  (= status 204))

(defmulti not-found? type)
(defmethod not-found? PersistentArrayMap [{status :status}]
  (not-found? status))
(defmethod not-found? Long [status]
  (= status 404))

(defmulti failure? type)
(defmethod failure? PersistentArrayMap [{status :status}]
  (failure? status))
(defmethod failure? Long [status]
  (>= status 400))

(defn post-fork
  ([join-id transaction-id count destination timeout-destination]
   (post-fork (gen-fork-req join-id transaction-id count destination timeout-destination nil)))
  ([join-id transaction-id count destination timeout-destination max-wait]
   (post-fork (gen-fork-req join-id transaction-id count destination timeout-destination max-wait)))
  ([fork]
   (log/info "Posting this fork:" fork)
   (let [response (app (-> (mock/request :post "/v1/fork")
                           (mock/content-type "application/json")
                           (mock/body (cheshire/generate-string fork))))
         status (:status response)
         body (parse-body (:body response))]
     {:source     fork,
      :response   response,
      :status     status,
      :body       body,
      :barrier-id (when (success? status)
                    body)})))

(defn post-join
  ([barrier-id position message]
   (post-join (gen-join-req barrier-id position message nil)))
  ([barrier-id position message max-wait]
   (post-join (gen-join-req barrier-id position message max-wait)))
  ([join]
   (log/info "Posting this join:" join)
   (let [response (app (-> (mock/request :post "/v1/join")
                           (mock/content-type "application/json")
                           (mock/body (cheshire/generate-string join))))
         status (:status response)
         body (parse-body (:body response))]
     {:source   join,
      :response response,
      :status   status,
      :body     body})))

(defn get-status [barrier-id]
  (Thread/sleep 200)                                        ; wait for any async processes to finish
  (let [source (if barrier-id                               ; GETs break for null parameters, so forcing the empty string
                 {:barrier-id barrier-id}
                 {})
        response (app (mock/request :get "/v1/status" source))
        status (:status response)
        body (parse-body (:body response))]
    {:source   source,
     :response response,
     :status   status,
     :body     body,
     :metadata (when (:joins body)
                 (dissoc body :joins)),
     :joins    (:joins body)}))
