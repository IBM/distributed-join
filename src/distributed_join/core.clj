(ns distributed-join.core
  (:require [distributed-join.store :as store]
            [distributed-join.redis :as redis]
            [distributed-join.publish :as publish]
            [distributed-join.http-handling :refer [err succ] :as handling]
            [distributed-join.time-util :refer :all]
            [distributed-join.error-util :refer :all]
            [config.core :refer [env]]
            [manifold.deferred :as deferred]
            [manifold.stream :as stream]
            [manifold.executor :as executor]
            [clojure.tools.logging :as log]
            [schema-tools.core :as st]
            [distributed-join.deferred-util :as du])
  (:import [java.util UUID]
           (java.util.concurrent TimeoutException)))

(defn- get-max-wait [request]
  (or (:max-wait request) (:fork-max-wait env)))

(defn- add-timeout-to-fork [fork timeout-secs]
  (assoc fork :timeout-secs timeout-secs))

(defn fork
  "Generates and returns a Barrier ID. Some implementations may create or reuse resources for this."
  [request]

  (deferred/let-flow [timeout-secs (now-with-offset (get-max-wait request))
                      barrier-id (redis/fork (add-timeout-to-fork request timeout-secs))
                      publish-queue-resp (redis/store-in-publish-queue barrier-id timeout-secs)
                      resp (succ ::handling/new barrier-id)]
    ;; don't remove publish-queue-resp or let-flow macro will drop the code above
    (first [resp publish-queue-resp])))


(defn all-there? [barrier-id metadata join-count]
  (when-let [c (:count metadata)]
    (= join-count c)))

(defn- join-with-max-wait [{:keys [max-wait barrier-id]}]
  (when-let [join-max-wait-date (now-with-offset max-wait)]
    (redis/store-nearest-in-publish-queue barrier-id join-max-wait-date)))

(defn join
  "Receives a single message with barrier id and position\n
   Stores the message using barrier-id and queueName and checks if all the parts have arrived.\n
   If yes, orders them according to count creating a tuple and sends them to the destination"
  [request]
  (deferred/let-flow [join-resp (redis/join request)
                      all-there (all-there? (:barrier-id request) (:metadata join-resp) (:count join-resp))]

    (if all-there
      (deferred/chain
        (redis/store-in-publish-queue (:barrier-id request) (now-milliseconds))
        (fn [_] (succ ::handling/accepted (str "join received for " (:barrier-id request) ", request will be completed."))))
      (deferred/chain
        (join-with-max-wait request)
        (fn [_] (succ ::handling/accepted (str "join received for " (:barrier-id request) ", waiting on more chunks.")))))))

(defn status
  "Gives the current status of the collected elements"
  [barrier-id]
  (deferred/let-flow [metadata (redis/join-metadata barrier-id)]
    (if (some? metadata)
      (deferred/let-flow [joins (redis/all-joins-status barrier-id (:count metadata))
                          fork (st/select-schema metadata store/Fork)]
        (succ (merge fork {:joins joins})))
      (err ::handling/not-found "barrier-id does not exist"))))

"
Scheduled pool will have two schedules

1. gather all barrier id's that have expired up till now:
  a. remove elements from primary fork-timeouts queue,
  b. process them
  c. only after completion update the backup queue.
2. check the unprocessed timeouts queue (with configurable delay):
  a. remove from the backup queue
  b. insert into primary

Step two ensures that elements are processed 'At least once'.

The publish messages follow a similar algorithm.
"

(defn- is-timeout? [metadata]
  (let [fork-timeout (:timeout-secs metadata)]
    ; If current time is greater than the fork-timeout, then it is a timeout
    (> (now-milliseconds) fork-timeout)))

(defn- select-destination-url [metadata]
  (when metadata
    (if (is-timeout? metadata)
      (:timeout-destination metadata)
      (:destination metadata))))

(defn- select-retry-cnt-keyword [timeout?]
  (if timeout?
    :timeout-publish-retry-cnt
    :publish-retry-cnt))

(defn- select-max-retry-cnt-keyword [timeout?]
  (if timeout?
    :max-retry-post-timeout-publish
    :max-retry-post-publish))

(defn- update-to-retry-publish [barrier-id metadata timeout? publish-retry-cnt]
  (let [publish-retry-cnt-keyword (select-retry-cnt-keyword timeout?)
        metadata-with-retry-cnt (assoc metadata publish-retry-cnt-keyword publish-retry-cnt)]
    (deferred/chain (redis/update-metadata metadata-with-retry-cnt)
                    (fn [_] (redis/store-in-publish-queue barrier-id (now-milliseconds))))))

(defn- reset-publish [barrier-id]
  (deferred/let-flow [metadata (redis/join-metadata barrier-id)
                      metadata-retry-dropped (dissoc metadata :timeout-publish-retry-cnt :publish-retry-cnt)
                      resp (update-to-retry-publish barrier-id metadata-retry-dropped false 0)]
    resp))

(defn- complete [barrier-id published? publish-resp]
  (if (or published? (not (instance? Throwable publish-resp)))
    (log/info "Publishing message for" barrier-id "was successful, completing fork-join.")
    (log/warn "Publishing message for" barrier-id "failed, completing fork-join as retries are exhausted."))
  (-> (deferred/chain (redis/complete barrier-id)
                      (fn [_] publish-resp))
      (deferred/catch (fn [ex]
                        (let [data (ex-data ex)]
                          (if (:updated-join-before-complete data)
                            (do (log/warn "Failed to complete - rescheduling publish of" barrier-id "because of:" (.getMessage ex))
                                (reset-publish barrier-id))
                            (throw ex)))))))

(defn- attempt-retry-publish
  ([barrier-id metadata error]
   (if (nil? metadata) ; This is defensive code to avoid a republishing forever (when the data needed is not there anymore)
     (complete barrier-id false (deferred/error-deferred
                                  (IllegalStateException. (str "Metadata for barrier-id " barrier-id " is missing, so retry of publish cannot happen. The fork will be completed as failed."))))
     (let [timeout? (is-timeout? metadata)
           retry-cnt-keyword (select-retry-cnt-keyword timeout?)
           publish-retry-cnt (or (retry-cnt-keyword metadata) 0)
           max-retry-keyword (select-max-retry-cnt-keyword timeout?)
           max-publish-retry (max-retry-keyword env)]
       (if (>= publish-retry-cnt max-publish-retry)
         ; Just log the last failure and then return nil so that it will try to complete.
         (do
           (log/error "Unable to publish barrier-id" barrier-id "no more retries left. Fork:" metadata
                      "and last error was:" (ex-data-for-http-resp error))
           (complete barrier-id false error))
         (do
           (log/warn "Unable to publish barrier-id" barrier-id "after" publish-retry-cnt
                     "retries, will try again. Error was:" (ex-data-for-http-resp error))
           (deferred/chain (update-to-retry-publish barrier-id metadata timeout? (inc publish-retry-cnt))
                           (fn [_] (deferred/success-deferred error))))))))
  ([barrier-id error]
   (deferred/chain (redis/join-metadata barrier-id)
                   #(attempt-retry-publish barrier-id % error))))

(defn- publish-message-to-destination
  [barrier-id metadata destination]
  (if (nil? metadata) ; This is defensive code to avoid a republishing forever (when the data needed is not there anymore)
    (complete barrier-id false (deferred/error-deferred
                                 (IllegalStateException. (str "Metadata for barrier-id " barrier-id " is missing, so publish cannot happen. The fork will be completed as failed."))))
    (-> (deferred/let-flow [count (:count metadata)
                            joins (redis/all-joins barrier-id count)
                            publish-resp (du/ex-to-error-with-action #(publish/post destination
                                                                                    {:barrier-id barrier-id
                                                                                     :messages   (map :message joins)})
                                                                     #(attempt-retry-publish barrier-id metadata %))
                            completed-resp (complete barrier-id true publish-resp)]
          completed-resp)
        (deferred/catch #(attempt-retry-publish barrier-id metadata %)))))


(defn- publish-message [barrier-id]
  (log/info "Processing publish message for barrier id" barrier-id)
  (deferred/let-flow [meta (redis/join-metadata barrier-id)
                      destination (select-destination-url meta)
                      resp (publish-message-to-destination barrier-id meta destination)]
    resp))

(defn process-publish-messages []
  (try
    ; Block this thread until either the publish completes or it exceeds the timeout.
    @(deferred/timeout!
       (deferred/let-flow [publish-message-ids (redis/all-publish-messages)]
         (if (empty? publish-message-ids)
           (log/info "No messages to publish")
           (let [publish-resps (map publish-message publish-message-ids)]
             (apply deferred/zip publish-resps))))
       (:publish-all-complete-timeout-ms env))
    (catch TimeoutException ex (do (log/error "Timeout while processing publish messages, cancelling all publish messages commands")
                                   (throw ex)))))

(defn recover-unprocessed-publish-messages []
  (log/info "Calling recover-unprocessed-publish-messages")
  (deferred/chain (redis/get-unprocessed-publish-messages)
                  #(map (fn [barrier-id]
                          (log/info "Recovering publish queue entry for barrier id:" barrier-id)
                          (attempt-retry-publish barrier-id
                                                 (TimeoutException. (str "Timeout waiting to publish barrier id: " barrier-id " - adding back into publish queue."))))
                        %)
                  #(apply deferred/zip %)))
