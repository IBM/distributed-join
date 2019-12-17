(ns distributed-join.redis
  (:require [clojure.tools.logging :as log]
            [config.core :refer [env]]
            [clojure.string :as str]
            [manifold.deferred :as deferred]
            [distributed-join.store :as store]
            [distributed-join.http-handling :refer [err] :as handling]
            [distributed-join.time-util :refer :all]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.encore :as enc])
  (:import (clojure.lang ExceptionInfo)))

(def server-conn {:pool {}
                  :spec {:uri        (:redis-uri env)
                         :timeout-ms (:redis-req-timeout-ms env)}})

(defn- gather-failures [resp]
  (let [{successes false failures true} (group-by #(instance? Throwable %) resp)]
    (if failures
      (deferred/error-deferred (ex-info "Database failure(s)" {:causes failures}))
      successes)))

(defmacro wcar-async* [single & rest]
  (if (nil? rest)
    `(deferred/future (car/wcar server-conn ~single))
    (let [body (cons single rest)]
      `(deferred/chain (deferred/future (car/wcar server-conn ~@body))
                       gather-failures
                       #(apply deferred/zip %)))))

(defmacro wcar-async-multi* [& body]
  `(deferred/chain (deferred/future (car/wcar server-conn (car/multi) ~@body (car/exec)))
                   (comp gather-failures last)
                   #(apply deferred/zip %)))

(def ^:private publish-queue "publish-queue")
(def ^:private publish-queue-backup "publish-queue-backup")
(def ^:private check-publish-delay-ms (:check-publish-delay-ms env))

(def ^:private join-trans-sep "~")

(defn- make-key
  "Creates the key used for pulling back the join data."
  ([join-id transaction-id]
   (str join-id join-trans-sep transaction-id))
  ([barrier-id]                                             ; TODO IS THIS ACTUALLY NEEDED?
   (str barrier-id)))

(def ^:private suffix-sep ":")

(def ^:private metadata-suffix (str suffix-sep "metadata"))
(defn- make-key-metadata
  "Creates the key used to pull back the metadata for the join."
  ([join-id transaction-id]
   (str (make-key join-id transaction-id) metadata-suffix))
  ([barrier-id]
   (str (make-key barrier-id) metadata-suffix)))

(def ^:private join-ts-suffix (str suffix-sep "join-ts"))
(defn- make-key-join-ts
  "Creates a key to store the latest timestamp for join data."
  ([join-id transaction-id]
   (str (make-key join-id transaction-id) join-ts-suffix))
  ([barrier-id]
   (str (make-key barrier-id) join-ts-suffix)))

(def ^:private join-ts-read-suffix (str suffix-sep "join-ts-read"))
(defn- make-key-join-ts-read
  "Creates the key used cache what the latest join timestamp read was."
  ([join-id transaction-id]
   (str (make-key join-id transaction-id) join-ts-read-suffix))
  ([barrier-id]
   (str (make-key barrier-id) join-ts-read-suffix)))


(defn- as-array
  "Creates an collection 'count' long of join data, setting nil for any missing data. "
  [count joins]
  (when (some? count)
    (map #(get joins (str %)) (range count))))

(defn- result-if-no-error [resp result]
  (deferred/chain resp
                  (fn [_] result)))

;
; TODO Keep an eye on https://github.com/ztellman/manifold/issues/118 to see if we can remove the deferred/chain,
; fake references (a & b) and call to first to pull out what we want.
;
(defn fork [record]
  (let [join-id (:join-id record)
        transaction-id (:transaction-id record)
        key-metadata (make-key-metadata join-id transaction-id)
        key (make-key join-id transaction-id)
        key-join-ts (make-key-join-ts key)
        key-join-ts-read (make-key-join-ts-read key)
        resp (wcar-async-multi*
               (car/set key-metadata record)
               (car/del key)
               (car/del key-join-ts)
               (car/del key-join-ts-read)
               (car/zrem publish-queue key)
               (car/zrem publish-queue-backup key))]
    (result-if-no-error resp key)))

(defn update-metadata [metadata]
  (deferred/let-flow [join-id (:join-id metadata)
                      transaction-id (:transaction-id metadata)
                      key-metadata (make-key-metadata join-id transaction-id)
                      resp (wcar-async* (car/set key-metadata metadata))]
    (result-if-no-error resp key-metadata)))

(defn- catch-lua-ex [ex error-msg error-fn]
  (deferred/catch ex
                  (fn [ex]
                    (let [message (.getMessage ex)]
                      (if (= message error-msg)
                        (error-fn ex)
                        (deferred/error-deferred ex))))))

(def ^:private add-join-and-count-them-failure-msg "Missing Metadata: join cannot be written.")
(defn- add-join-and-count-them
  "Being done in Lua to avoid the race condition between checking for the metadata and writing the join."
  [keys args]
  (car/lua
    (str "
    local key=_:key
    local keyMetadata=_:key-metadata
    local pos=_:pos
    local record=_:record
    local nowTS=_:now-ts
    local keyJoinTS=_:key-join-ts
    local metadataExists=redis.call(\"exists\", keyMetadata)
    if metadataExists == 1 then
      redis.call(\"hset\", key, pos, record)
      redis.call(\"set\", keyJoinTS, nowTS)
      return redis.call(\"hlen\", key)
    else
      return redis.error_reply(\"" add-join-and-count-them-failure-msg "\")
    end
    ") keys args))

(defn join [record]
  (deferred/let-flow [pos (:position record)
                      barrier-id (:barrier-id record)
                      key (make-key barrier-id)
                      key-metadata (make-key-metadata barrier-id)
                      key-join-ts (make-key-join-ts barrier-id)
                      metadata (wcar-async* (car/get key-metadata))] ; TODO GH-87 this call could be moved into the Lua code along with the tests below

    (cond
      (empty? metadata) (err ::handling/not-found "barrier-id does not exist")
      (>= pos (:count metadata)) (err ::handling/bad-data "position is greater than total")
      :else (-> (deferred/chain (wcar-async* (add-join-and-count-them {:key key :key-metadata key-metadata :key-join-ts key-join-ts}
                                                                      {:pos pos :record record :now-ts (now-milliseconds)}))
                                (fn [resp]
                                  {:metadata metadata :count resp}))
                (catch-lua-ex add-join-and-count-them-failure-msg
                              (fn [_] (err ::handling/not-found "barrier-id does not exist")))))))

(def ^:private complete-update-failure-msg "Join Updated: cannot complete as the join was updated after being pulled for publishing.")
(defn- complete-if-no-update
  "
  Uses a timestamp that is internally managed in this code to detect if joins were updated after they were read for
  publishing purposes. If so, it aborts the deletions and throws back an error.
  "
  [keys args]
  (car/lua
    (str "
    local key=_:key
    local keyMetadata=_:key-metadata
    local keyJoinTS=_:key-join-ts
    local keyJoinTSRead=_:key-join-ts-read
    local publishQueueKey=_:queue
    local publishBackupQueueKey=_:backup-queue
    local barrierId=_:barrier-id
    local joinTimestamp=redis.call(\"get\", keyJoinTS)
    local joinReadTimestamp=redis.call(\"get\", keyJoinTSRead)
    if joinTimestamp == joinReadTimestamp then
      redis.call(\"del\", keyMetadata)
      redis.call(\"del\", key)
      redis.call(\"del\", keyJoinTS)
      redis.call(\"del\", keyJoinTSRead)
      redis.call(\"zrem\", publishQueueKey, barrierId)
      return redis.call(\"zrem\", publishBackupQueueKey, barrierId)
    else
      return redis.error_reply(\"" complete-update-failure-msg "\")
    end
    ") keys args))

(defn complete [barrier-id]
  (let [key (make-key barrier-id)
        key-metadata (make-key-metadata barrier-id)
        key-join-ts (make-key-join-ts barrier-id)
        key-join-ts-read (make-key-join-ts-read barrier-id)]
    (log/info "Completing for barrier-id" barrier-id)
    (-> (wcar-async*
          (complete-if-no-update {:key   key :key-metadata key-metadata :key-join-ts key-join-ts :key-join-ts-read key-join-ts-read
                                  :queue publish-queue :backup-queue publish-queue-backup}
                                 {:barrier-id barrier-id}))
        (catch-lua-ex complete-update-failure-msg
                      (fn [ex]
                        (deferred/error-deferred
                          (ex-info "Join was updated before fork could be completed"
                                   {:updated-join-before-complete true} ex)))))))

(defn- kvs->map [kvs]
  (enc/reduce-kvs assoc {} kvs))

(defn- all-joins-with-update-timestamp
  "
  Updates a timestamp (conditionally) of when the joins are read so that this internal value can be compared later during
  completion to see if joins changed after they were read.
  "
  [keys args]
  (car/lua
    "
    local key=_:key
    local keyJoinTS=_:key-join-ts
    local keyJoinTSRead=_:key-join-ts-read
    local updateTimestamp=_:update-timestamp
    if updateTimestamp then
      local joinTimestamp=redis.call(\"get\", keyJoinTS)
      if joinTimestamp then
        redis.call(\"set\", keyJoinTSRead, joinTimestamp)
      else
        redis.call(\"del\", keyJoinTSRead)
      end
    end
    return redis.call(\"hgetall\", key)
    " keys args))

(defn- all-joins-cond-update-timestamp [barrier-id count update-timestamp?]
  (deferred/let-flow [key (make-key barrier-id)
                      key-join-ts (make-key-join-ts barrier-id)
                      key-join-ts-read (make-key-join-ts-read barrier-id)
                      joins (wcar-async* (car/parse kvs->map
                                                    (all-joins-with-update-timestamp {:key key :key-join-ts key-join-ts :key-join-ts-read key-join-ts-read}
                                                                                     {:update-timestamp update-timestamp?})))]
    (as-array count joins)))

(defn all-joins [barrier-id count]
  (all-joins-cond-update-timestamp barrier-id count true))

(defn all-joins-status [barrier-id count]
  (all-joins-cond-update-timestamp barrier-id count false))

(defn join-metadata [barrier-id]
  (deferred/let-flow [key-metadata (make-key-metadata barrier-id)
                      metadata (wcar-async* (car/get key-metadata))]

    (log/debug "In join-metadata with key:" key-metadata " and metadata:" metadata)
    metadata))

(defn store-in-publish-queue [barrier-id publish-at]
  (deferred/chain (wcar-async-multi*
                    (car/zadd publish-queue-backup publish-at barrier-id)
                    (car/zadd publish-queue publish-at barrier-id))
                  (fn [resp] (cons publish-at resp))))

(defn- store-nearest-in-publish-queue-script [keys args]
  (car/lua
    "
    local publishQueueKey=_:queue
    local publishBackupQueueKey=_:backup-queue
    local barrierId=_:barrier-id
    local publishAt=_:publish-at
    local currentPublishDate=redis.call(\"zscore\", publishQueueKey, barrierId)
    if currentPublishDate > publishAt then
      redis.call(\"zadd\", publishBackupQueueKey, publishAt, barrierId)
      return redis.call(\"zadd\", publishQueueKey, publishAt, barrierId)
    else
      return 0
    end
    " keys args))

(defn store-nearest-in-publish-queue [barrier-id publish-at]
  (deferred/chain (wcar-async* (store-nearest-in-publish-queue-script
                                 {:queue publish-queue :backup-queue publish-queue-backup}
                                 {:barrier-id barrier-id :publish-at publish-at}))
                  (fn [resp] (cons publish-at [resp]))))

(defn all-publish-messages []
  (let [now-milliseconds (now-milliseconds)]
    (log/info "Calling all-publish-messages to see if there is anything that is ready to be published...")
    (deferred/let-flow [resp (wcar-async-multi*
                               (car/zrangebyscore publish-queue 0 now-milliseconds)
                               (car/zremrangebyscore publish-queue 0 now-milliseconds))]
      (first resp))))

(defn get-unprocessed-publish-messages []
  (let [max-score (- (now-milliseconds) check-publish-delay-ms)]
    (wcar-async* (car/zrangebyscore publish-queue-backup 0 max-score))))

(defn get-publish-date [barrier-id]
  (wcar-async* (car/parse-int
                 (car/zscore publish-queue barrier-id))))

(defn shutdown []
  (log/info "Shutting down redis code..."))
