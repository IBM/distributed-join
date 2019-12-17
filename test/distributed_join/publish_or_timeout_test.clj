(ns distributed-join.publish-or-timeout-test
  (:require [clojure.test :refer :all]
            [distributed-join.embedded-redis :as er]
            [distributed-join.embedded-jetty :as ej]
            [distributed-join.request-util :refer :all]
            [distributed-join.error-util :refer :all]
            [distributed-join.time-util :refer :all]
            [distributed-join.core :as core]
            [distributed-join.redis :as redis]
            [config.core :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.math.combinatorics :as combo])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each ej/jetty-fixture er/redis-cleanup-fixture)
(use-fixtures :once er/redis-fixture)

(def fork-max-wait-default 1)
(def milli-seconds-in-second 1000)
(def first-join-position 0)

(defn- pause-for-timeout []
  (log/info "Pausing for timeout...")
  (pause (* milli-seconds-in-second fork-max-wait-default)))

(defn- pause-for-publish-reprocess []
  (pause (:check-publish-delay-ms env)))

(defn- process-publish [should-have-messages?]
  (let [resp (core/process-publish-messages)]
    (if should-have-messages?
      (is (not-empty resp))
      (is (empty? resp)))))

(defn- gen-join-msg [barrier-id position]
  (str "Join for barrier-id " barrier-id " and position " position))

(defn- join-req
  ([max-wait barrier-id]
   (gen-join-req barrier-id first-join-position (gen-join-msg barrier-id first-join-position) max-wait))
  ([barrier-id]
   (join-req nil barrier-id)))

(defn- post-fork-with-one-join [fork-req join-req-fn]
  (let [fork-resp (post-fork fork-req)
        barrier-id (:barrier-id fork-resp)
        join-req (join-req-fn barrier-id)
        _ (post-join join-req)]
    [fork-req fork-resp]))

(defn- fork-with-one-join
  ([join-count fork-max-wait join-max-wait]
   (post-fork-with-one-join (gen-fork join-count fork-max-wait)
                            (partial join-req join-max-wait)))
  ([join-count]
   (fork-with-one-join join-count fork-max-wait-default nil)))

(defn- setup-fork-join-with-bad-destination [join-count destination-invalid? timeout-destination-invalid?]
  (let [fork-req (gen-fork-invalid-dest join-count fork-max-wait-default destination-invalid? timeout-destination-invalid?)]
    (post-fork-with-one-join fork-req join-req)))

(defn- lifecycle [{:keys [join-count should-have-messages? pause-fn destination-name-keyword]}]
  (let [[fork-req _] (fork-with-one-join join-count)]
    (when pause-fn
      (pause-fn))
    (process-publish should-have-messages?)
    (when destination-name-keyword
      (ej/verify-last-request-uri (destination-name-keyword fork-req)))))

(deftest process-publish-no-data
  (testing "If there is no data, should be able to process request and will return nil"
    (process-publish false)))

(deftest process-publish-not-all-data-there
  (testing "Add a fork with one join, but do not add all joins - should not publish"
    (lifecycle {:join-count            3
                :should-have-messages? false})))

(deftest process-publish-all-data-there
  (testing "Add a fork with one join which is all that was expected and so it should publish"
    (lifecycle {:join-count               1
                :should-have-messages?    true
                :destination-name-keyword :destination})))

(deftest process-timeout-data-wait-for-timeout
  (testing "Add a fork and join, then wait for it to timeout"
    (lifecycle {:join-count               3
                :should-have-messages?    true
                :destination-name-keyword :timeout-destination
                :pause-fn                 pause-for-timeout})))

(deftest process-timeout-data-all-joins-were-already-processed
  (testing "Add a fork and all joins, wait for it to timeout - should send to timeout url."
    (lifecycle {:join-count               1
                :should-have-messages?    true
                :destination-name-keyword :timeout-destination
                :pause-fn                 pause-for-timeout})))

(deftest process-publish-all-data-there-but-does-not-publish
  (testing "Add a fork with one join, so all data is there, but the publish times out"
    (let [[_ fork-resp] (fork-with-one-join 1)
          barrier-id (:barrier-id fork-resp)
          publish-msg-first-call @(redis/all-publish-messages)
          _ (pause-for-publish-reprocess)
          publish-msg-second-call @(redis/all-publish-messages)
          _ @(core/recover-unprocessed-publish-messages)
          publish-msg-back @(redis/all-publish-messages)]
      (is (= barrier-id (first publish-msg-first-call)))
      (is (empty? publish-msg-second-call))
      (is (= barrier-id (first publish-msg-back))))))

(deftest fail-to-process-timeout-in-time
  (testing "This will remove timeout entries from that queue, not process them and then run the code that adds them back"
    (let [fork-resp (post-fork (gen-fork 3 fork-max-wait-default))
          barrier-id (:barrier-id fork-resp)
          _ (pause-for-timeout)
          publish-msg-first-call @(redis/all-publish-messages)
          _ @(core/recover-unprocessed-publish-messages)
          timeouts-second-call @(redis/all-publish-messages)
          _ (pause-for-publish-reprocess)
          _ @(core/recover-unprocessed-publish-messages)
          timeouts-back @(redis/all-publish-messages)
          metadata @(redis/join-metadata barrier-id)]
      (is (= barrier-id (first publish-msg-first-call)))
      (is (empty? timeouts-second-call))
      (is (= barrier-id (first timeouts-back)))
      (is (= 1 (:timeout-publish-retry-cnt metadata)))
      (is (nil? (:publish-retry-cnt metadata))))))

(defn- test-several-bad-publishes [num-to-test timeout-pause?]
  (when timeout-pause?
    (pause-for-timeout))
  (dotimes [cnt num-to-test]
    (log/info "Publish Request For Retry Test" (inc cnt))
    (doseq [resp (core/process-publish-messages)]
      (let [{:keys [data]} (ex-data-for-http-resp resp)]
        (is (= 404 (:status data)))))))

(defn- validate-all-published []
  (log/info "All retries should have completed")
  (is (nil? (core/process-publish-messages))))

(defn- full-lifecycle-of-retries [join-count timeout-pause? destination-invalid? timeout-destination-invalid?]
  (let [num-to-test 4]
    (setup-fork-join-with-bad-destination join-count destination-invalid? timeout-destination-invalid?)
    (test-several-bad-publishes num-to-test timeout-pause?)
    (validate-all-published)))

(deftest invalid-destinations-will-exhaust-retries
  (testing "Creates a fork with invalid destinations, which will cause it to go through all retries"
    (let [join-count 1
          timeout-pause? false
          destination-invalid? true
          timeout-destination-invalid? false]
      (full-lifecycle-of-retries join-count timeout-pause? destination-invalid? timeout-destination-invalid?))))

(deftest invalid-destinations-will-exhaust-retries-for-timeout-too
  (testing "Creates a fork with invalid destinations (which will cause it to go through all retries),
            waits for it to timeout and then tries to publish it to the timeout url"
    (let [join-count 3
          timeout-pause? true
          destination-invalid? false
          timeout-destination-invalid? true]
      (full-lifecycle-of-retries join-count timeout-pause? destination-invalid? timeout-destination-invalid?))))

(deftest invalid-destinations-will-exhaust-retries-independently
  (testing "Creates a fork with invalid destinations (which will cause it to go through all retries),
            tries to send a few publishes, waits for it to timeout and then tries to publish it to
            the timeout url."
    (let [join-count 1
          num-to-test-before-timeout 2
          num-to-test-after-timeout 4
          destination-invalid? true
          timeout-destination-invalid? true]
      (setup-fork-join-with-bad-destination join-count destination-invalid? timeout-destination-invalid?)
      (test-several-bad-publishes num-to-test-before-timeout false)
      (test-several-bad-publishes num-to-test-after-timeout true)
      (validate-all-published))))

(defn- join-max-wait-test [fork-max-wait join-1-max-wait join-2-max-wait]
  (let [join-count 3
        [_ fork-resp] (fork-with-one-join join-count fork-max-wait join-1-max-wait)
        barrier-id (:barrier-id fork-resp)
        join-2-req (join-req join-2-max-wait barrier-id)
        _ (post-join join-2-req)
        now (now-milliseconds)
        publish-date @(redis/get-publish-date (:barrier-id fork-resp))
        min-offset (min fork-max-wait join-1-max-wait join-2-max-wait)]
    (is (> publish-date now))
    (is (<= publish-date (now-with-offset min-offset)))))

; Produces every possible combination of the numbers listed in partitions of 3 (so: (2,2,2), (2, 2, 3), (2,3,4), etc...)
(def ^:private fork-and-two-join-max-waits (combo/selections [2 3 4] 3))

(deftest fork-and-two-joins-random-max-wait
  (testing "Will post a fork with two joins - all having a random max-wait value. Will test that min always wins"
    (doseq [[fork-max-wait join-1-max-wait join-2-max-wait] fork-and-two-join-max-waits]
      (log/info "Testing fork and two joins with these max wait values:" fork-max-wait "," join-1-max-wait "," join-2-max-wait)
      (join-max-wait-test fork-max-wait join-1-max-wait join-2-max-wait))))

(deftest fail-complete-when-join-arrives-during-publish
  (testing "If a join arrives while a fork-join is being published, fail the complete and republish."
    (let [delay-response-ms 1000
          second-join-delay-ms (/ delay-response-ms 2)
          join-count 1
          max-wait 2
          fork-resp (post-fork (gen-fork-different-dest join-count max-wait (get-delay-url-path delay-response-ms) nil))
          barrier-id (:barrier-id fork-resp)
          join-req (join-req barrier-id)
          two-joins (pvalues (do (post-join join-req)
                                 (core/process-publish-messages))
                             (do (Thread/sleep second-join-delay-ms) ; Pause for previous publish to start then sneak in another join
                                 (post-join join-req)))
          publish-msg-resp1 @(redis/all-publish-messages)
          _ (pause (+ 100 delay-response-ms))               ; Wait for the publish to go and timeout due to the delay api...
          publish-msg-resp2 @(redis/all-publish-messages)]
      (is (empty? publish-msg-resp1))
      (is (= barrier-id (first publish-msg-resp2)))
      (log-failure-data #(doseq [resp two-joins]
                           (log/info "Response from calling join twice with a delayed publish:" resp))))))

(defn- verify-barrier-id-removed-from-queues []
  (let [publish-msg-resp @(redis/all-publish-messages)
        republish-msg-resp @(redis/get-unprocessed-publish-messages)]
    (is (empty? publish-msg-resp))
    (is (empty? republish-msg-resp))))

(deftest no-metadata-for-barrier-id-in-publish-queue
  (testing "Add a fake barrier id to the publish queue and see if it gets removed"
    (let [store-resp @(redis/store-in-publish-queue "fake-barrier-id" (now-milliseconds))]
      (is (thrown-with-msg? IllegalStateException (re-pattern "Metadata for barrier-id")
                            (core/process-publish-messages)))
      (verify-barrier-id-removed-from-queues))))

(deftest no-metadata-for-barrier-id-in-backup-publish-queue
  (testing "Add a fake barrier id to the backup publish queue and see if it gets removed"
    (let [store-resp @(redis/store-in-publish-queue "fake-barrier-id" (now-milliseconds))
          publish-msg-resp @(redis/all-publish-messages)
          _ (pause-for-publish-reprocess)]
      (is (thrown-with-msg? IllegalStateException (re-pattern "Metadata for barrier-id")
                            @(core/recover-unprocessed-publish-messages)))
      (verify-barrier-id-removed-from-queues))))