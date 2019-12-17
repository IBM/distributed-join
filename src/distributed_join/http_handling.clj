(ns distributed-join.http-handling
  (:require [manifold.deferred :as deferred]
            [ring.util.http-response :refer :all]
            [clojure.tools.logging :as log])
  (:import clojure.lang.ExceptionInfo))

(defn succ
  "generates a successful body with a correctly formatted error"
  ([message]
   (succ nil message))
  ([reason message]
   {:reason reason :message message}))

(defn err
  "generates a failed deferred with a correctly formatted error"
  ([reason message executor]
   (deferred/error-deferred (ex-info nil {:reason reason :message message}) executor))
  ([reason message]
   (deferred/error-deferred (ex-info nil {:reason reason :message message}))))

(defn- resp-to-http
  "converts a keyword to its associated http response"
  [resp]
  (let [[reason message] (if (map? resp)
                           [(:reason resp) (:message resp)]
                           [nil resp])]
    (case reason
      nil (ok message)
      ::success (ok message)
      ::new (created nil message)
      ::accepted (accepted message)
      ::no-data (no-content)
      ::bad-data (bad-request message)
      ::not-found (not-found message)
      ::internal-error (internal-server-error message)
      (internal-server-error (str "unrecognized reason: " reason)))))

(defn catch-error
  "takes a deferred and catches the expected ExceptionInfo, extracts info it needs out of the map to create a response."
  [d]
  (deferred/catch d ExceptionInfo
    (fn [^Throwable ei]
      (let [message (.getMessage ei)
            data (ex-data ei)
            data-message (:message data)
            causes (:causes data)]
        ; Only log failures if a reason is not attached since these should be non-user errors (so ignore 404's and 400's).
        (when-not (:reason data)
          (log/error ei (str data-message causes)))
        (cond-> data
                (some? message) (assoc :message message)
                (nil? (:reason data)) (assoc :reason ::internal-error))))))

(defn respond
  "takes a deferred and converts to a proper http response, including handling any error.
  works on the assumption that the deferred contains a map with a `:message` and `:reason` key.
  if the deferred contains anything other than a map, it is assumed that the response is a 200 with the body set to the contents of the deferred.
  possible reasons include:
  nil        -> 200
  ::new      -> 201
  ::accepted -> 202
  ::success  -> 200
  ::no-data  -> 204
  ::bad-data -> 400"
  [d]
  (deferred/chain (catch-error d)
                  resp-to-http))
