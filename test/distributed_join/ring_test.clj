(ns distributed-join.ring-test
  (:require [clojure.test :refer :all]
            [distributed-join.request-util :refer :all]
            [distributed-join.embedded-redis :as er]
            [distributed-join.embedded-jetty :as ej]
            [config.core :refer [env]]
            [distributed-join.core :as core]
            [clojure.tools.logging :as log])

  (:import (java.util HashMap)
           (com.fasterxml.jackson.core JsonParseException)))

(use-fixtures :each ej/jetty-fixture)
(use-fixtures :once er/redis-fixture er/redis-cleanup-fixture)

(defn derived-names [test-id]
  [test-id                                                  ; test-id
   (str "a" test-id)                                        ; join-id
   (str "b" test-id)                                        ; transaction-id
   7                                                        ; count -- TESTS require it be at least 4
   (str "http://localhost:" (:embedded-jetty-port env) "/v1/publish")                 ; destination
   (str "http://localhost:" (:embedded-jetty-port env) "/v1/publish/timeout")         ; timeout destination
   (str "msg" test-id)])                                    ; message

(def ^:private one-hundred-numbers "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")

(defn- repeat-100-str [times]
  (apply str (repeat times one-hundred-numbers)))

(defn- gen-large-string [size]
  (let [how-many-100s (quot size 100)
        how-much-is-left (mod size 100)]
    (str (repeat-100-str how-many-100s)
         (subs one-hundred-numbers 0 how-much-is-left))))

(def ^:private large-transaction-id (gen-large-string (+ 1 (:transaction-id-len-max env))))
(def ^:private large-join-id (gen-large-string (+ 1 (:join-id-len-max env))))
(def ^:private large-destination (str "http://" (gen-large-string (:destination-url-len-max env))))
(def ^:private large-timeout-destination (str "http://" (gen-large-string (:timeout-destination-url-len-max env))))

(def ^:private fork-count-max (:fork-count-max env))
(def generic-fork-test-list
  (let [[_ join-id transaction-id cnt destination timeout-destination _] (derived-names "01")]
    [[[nil    nil             cnt         destination timeout-destination] false "Fork Call with Null Join & Transaction IDs"]
     [[nil    transaction-id  cnt         destination timeout-destination] false "Fork Call with Null Join ID"]
     [[join-id nil            cnt         destination timeout-destination] false "Fork Call with Null Transaction ID"]
     [[""     ""              cnt         destination timeout-destination] false "Fork Call with Empty Join & Transaction IDs"]
     [[""     transaction-id  cnt         destination timeout-destination] false "Fork Call with Empty Join ID"]
     [[join-id ""             cnt         destination timeout-destination] false "Fork Call with Empty Transaction ID"]
     [[1       transaction-id cnt         destination timeout-destination] false "Fork Call with Invalid Join ID"]
     [[join-id 2              cnt         destination timeout-destination] false "Fork Call with Invalid Transaction ID"]
     [[1       2              cnt         destination timeout-destination] false "Fork Call with Invalid Join & Transaction IDs"]
     [[join-id transaction-id nil         destination timeout-destination] false "Fork Call with Null Count Parameter"]
     [[join-id transaction-id ""          destination timeout-destination] false "Fork Call with Empty Count Parameter"]
     [[join-id transaction-id (- cnt)     destination timeout-destination] false "Fork Call with Invalid Count Parameter 1"]
     [[join-id transaction-id (+ cnt 0.5) destination timeout-destination] false "Fork Call with Invalid Count Parameter 2"]
     [[join-id transaction-id 0           destination timeout-destination] false "Fork Call with Invalid Count Parameter 3"]
     [[join-id transaction-id "A"         destination timeout-destination] false "Fork Call with Invalid Count Parameter 4"]
     [[join-id transaction-id (inc fork-count-max) destination timeout-destination] false "Fork Call with Invalid Count Parameter 5"]
     [["a~b"   transaction-id cnt         destination timeout-destination] false "Fork Call with Invalid Character in Join ID 1"]
     [["a:b"   transaction-id cnt         destination timeout-destination] false "Fork Call with Invalid Character in Join ID 2"]
     [[join-id "a~b"          cnt         destination timeout-destination] false "Fork Call with Invalid Character in Transaction ID 1"]
     [[join-id "a:b"          cnt         destination timeout-destination] false "Fork Call with Invalid Character in Transaction ID 2"]
     [["a~b"   "a:b"          cnt         destination timeout-destination] false "Fork Call with Invalid Character in Join and Transaction ID 1"]
     [["a:b"   "a~b"          cnt         destination timeout-destination] false "Fork Call with Invalid Character in Join and Transaction ID 2"]
     [[join-id transaction-id cnt         "" ""] false "Blank destination and timeout destination"]
     [[join-id transaction-id cnt         destination ""] false "Blank timeout destination"]
     [[join-id transaction-id cnt         "" timeout-destination] false "Blank destination"]
     [[join-id transaction-id cnt         nil nil] false "Nil destination and timeout destination"]
     [[join-id transaction-id cnt         destination nil] false "Nil timeout destination"]
     [[join-id transaction-id cnt         nil timeout-destination] false "Nil destination"]
     [[join-id transaction-id cnt         "http" "http"] false "No host destination and timeout destination"]
     [[join-id transaction-id cnt         destination "http"] false "No host timeout destination"]
     [[join-id transaction-id cnt         "http" timeout-destination] false "No host destination"]
     [[join-id transaction-id cnt         "http://" "http://"] false "No host destination and timeout destination 2"]
     [[join-id transaction-id cnt         destination "http://"] false "No host timeout destination 2"]
     [[join-id transaction-id cnt         "http://" timeout-destination] false "No host destination 2"]
     [[join-id transaction-id cnt         "file" "file"] false "Bad protocol destination and timeout destination"]
     [[join-id transaction-id cnt         destination "file"] false "Bad protocol timeout destination"]
     [[join-id transaction-id cnt         "file" timeout-destination] false "Bad protocol destination"]
     [[join-id transaction-id cnt         "file://something" "file://something"] false "Bad protocol with file name destination and timeout destination"]
     [[join-id transaction-id cnt         destination "file://something"] false "Bad protocol with file name  timeout destination"]
     [[join-id transaction-id cnt         "file://something" timeout-destination] false "Bad protocol with file name  destination"]
     [[large-join-id transaction-id cnt   destination timeout-destination] false "Join id too large"]
     [[join-id large-transaction-id cnt   destination timeout-destination] false "Transaction id too large"]
     [[join-id transaction-id cnt         large-destination timeout-destination] false "Destination too large"]
     [[join-id transaction-id cnt         destination large-timeout-destination] false "Timeout destination too large"]
     [["&&"    "**"           cnt         destination timeout-destination] true  "Fork Call with Strange Join & Transaction IDs"]
     [["&&"    transaction-id cnt         destination timeout-destination] true  "Fork Call with Strange Join ID"]
     [[join-id "**"           cnt         destination timeout-destination] true  "Fork Call with Strange Transaction ID"]
     [[join-id transaction-id fork-count-max destination timeout-destination] true  "Fork Call with Max Fork Count"]
     [[join-id transaction-id cnt         "http://somewhere" "http://somewhere"] true  "Minimal http destination and timeout destination URL"]
     [[join-id transaction-id cnt         "https://somewhere" "https://somewhere"] true  "Minimal https destination and timeout destination URL"]
     [[join-id transaction-id cnt         destination timeout-destination] true  "Valid Fork Call"]]))

(def ^:private large-barrier-id (gen-large-string (+ 1 (:barrier-id-len-max env))))
(def ^:private large-join-message (gen-large-string (+ 1 (:join-message-len-max env))))

(def generic-join-test-list
  (delay (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "02")
        fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
        barrier-id (:barrier-id fork-resp)
        mid-pos (quot cnt 2)
        last-pos (dec cnt)]
    [[["XXX-XXX"  mid-pos      message] false "Undefined Barrier ID"]
     [[123456789  mid-pos      message] false "Invalid Barrier ID 1"]
     [["a:b"      mid-pos      message] false "Invalid Barrier ID 2"]
     [[nil        mid-pos      message] false "Missing Barrier ID"]
     [[barrier-id nil          message] false "Missing Position"]
     [[large-barrier-id mid-pos message] false "Barrier id too large"]
     [[barrier-id mid-pos large-join-message] false "Message too large"]
     [[barrier-id (- cnt)      message] false "Invalid Position 1"] ;; out of range: negative number
     [[barrier-id 1.5          message] false "Invalid Position 2"] ;; float number
     [[barrier-id cnt          message] false "Invalid Position 3"] ;; out of range: valid number is up to cnt-1
     [[barrier-id "A"          message] false "Invalid Position 4"]
     [[barrier-id mid-pos      nil    ] false "Missing Message"]
     [[barrier-id mid-pos      ""     ] true  "Empty Message"]])))

(def generic-status-test-list
  (delay (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "03")
        fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
        barrier-id (:barrier-id fork-resp)]
    [[["XXX-XXX" ] not-found? "Undefined Barrier ID"]        ; returns 404
     [[123456789 ] not-found? "Invalid Barrier ID"]          ; returns 404
     [[nil       ] failure? "Missing Barrier ID"]            ; returns 400
     [[barrier-id] success? "Valid Status Call"]])))         ; returns 200


(defn joins-init [cnt]
  (vec (repeat cnt nil)))

(defn joins-clean? [vec]
  (every? nil? vec))

(defn generic-fork-test [[[_ _ cnt :as parameters] expect-success? test-name]]
  (testing test-name
    (let [fork-resp (apply post-fork parameters)
          status-resp (when expect-success?
                        (get-status (:barrier-id fork-resp)))]
      (if expect-success?
        (do
          (is (success? fork-resp))
          (is (success? status-resp))
          (is (= (:source fork-resp) (:metadata status-resp)))
          (is (= (joins-init cnt)
                 (:joins status-resp))))
        (is (failure? fork-resp))))))

(defn generic-join-test [[[barrier-id pos :as parameters] expect-success? test-name]]
  (testing test-name
    (let [join-resp (apply post-join parameters)
          status-resp (get-status barrier-id)]
      (log/info "This is the join response:" join-resp)
      (log/info "This is the status response:" status-resp)
      (if expect-success?
        (do
          (is (success? join-resp))
          (is (success? status-resp))
          (is (= (-> (joins-init (count (:joins status-resp)))
                     (assoc pos (:source join-resp)))
                 (:joins status-resp))))
        (do
          (is (failure? join-resp))
          (is (or (failure? status-resp)
                  (joins-clean? (:joins status-resp)))))))))

(defn generic-status-test [[[barrier-id] predicate? test-name]]
  (testing test-name
    (let [status-resp (get-status barrier-id)]
      (log/info "Received this status response:" status-resp)
      (is (predicate? status-resp)))))

(deftest test-input-validation
  (testing "Input Validation:"
    (testing "Fork Interface:"
      (mapv generic-fork-test generic-fork-test-list))
    (testing "Join Interface:"
      (mapv generic-join-test @generic-join-test-list))
    (testing "Status Interface:"
      (mapv generic-status-test @generic-status-test-list))))

(deftest test-fork-functionality
  (testing "POST Fork Functionality:"
    (testing "Repeated Fork Calls with different parameters"
      (let [[_ join-id transaction-id cnt destination timeout-destination _] (derived-names "12")
            fork-resp1 (post-fork join-id transaction-id (inc cnt) destination timeout-destination)
            fork-resp2 (post-fork join-id transaction-id cnt destination timeout-destination)
            barrier-id1 (:barrier-id fork-resp1)
            barrier-id2 (:barrier-id fork-resp2)
            status-resp (get-status barrier-id1)]
        (is (success? fork-resp1))
        (is (success? fork-resp2))
        (is (= barrier-id1 barrier-id2))
        (is (success? status-resp))
        (is (= (:metadata status-resp)
               (:source fork-resp2)))
        (is (= (joins-init cnt)
               (:joins status-resp)))))))

(deftest test-join-functionality
  (testing "POST Join Functionality:"
    (testing "Executing Valid Join Calls (border cases)"
      (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "21")
            fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
            barrier-id (:barrier-id fork-resp)
            pos1 0                                          ; first valid position
            pos2 (quot cnt 2)                               ; around the middle of the valid positions
            pos3 (dec cnt)                                  ; last valid positions
            join-resp1 (post-join barrier-id pos1 (str message pos1))
            join-resp2 (post-join barrier-id pos2 (str message pos2))
            join-resp3 (post-join barrier-id pos3 (str message pos3))
            status-resp (get-status barrier-id)]
        (is (success? fork-resp))
        (is (success? join-resp1))
        (is (success? join-resp2))
        (is (success? join-resp3))
        (is (success? status-resp))
        (is (= (:source fork-resp) (:metadata status-resp)))
        (is (= (-> (joins-init cnt)
                   (assoc pos1 (:source join-resp1))
                   (assoc pos2 (:source join-resp2))
                   (assoc pos3 (:source join-resp3)))
               (:joins status-resp)))))

    (testing "Executing a Call that Overrides an Existing Position"
      (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "22")
            fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
            barrier-id (:barrier-id fork-resp)
            pos (dec cnt)
            join-resp1 (post-join barrier-id pos (str message "1"))
            join-resp2 (post-join barrier-id pos (str message "2"))
            status-resp (get-status barrier-id)]
        (is (success? fork-resp))
        (is (success? join-resp1))
        (is (success? join-resp2))
        (is (success? status-resp))
        (is (= (:source fork-resp) (:metadata status-resp)))
        (is (= (-> (joins-init cnt)
                   (assoc pos (:source join-resp2)))
               (:joins status-resp)))))))

(deftest test-status-functionality
  (testing "GET Status Functionality:"
    (testing "Valid Status Call"
      (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "31")
            fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
            barrier-id (:barrier-id fork-resp)
            pos (quot cnt 2)
            join-resp (post-join barrier-id pos message)
            status-resp (get-status barrier-id)]
        (is (success? fork-resp))
        (is (success? join-resp))
        (is (success? status-resp))
        (is (= (:source fork-resp) (:metadata status-resp)))
        (is (= (-> (joins-init cnt)
                   (assoc pos (:source join-resp)))
               (:joins status-resp)))))

    (testing "Successive Status of Fork until after All Joins Have Been Received"
      (let [[_ join-id transaction-id cnt destination timeout-destination message] (derived-names "32")
            fork-resp (post-fork join-id transaction-id cnt destination timeout-destination)
            barrier-id (:barrier-id fork-resp)
            join-status-resps (mapv (fn [i] [(get-status barrier-id)
                                             (post-join barrier-id i (str message i))])
                                    (shuffle (range cnt)))
            _ (core/process-publish-messages)
            final-status-resp (get-status barrier-id)]
        (is (success? fork-resp))
        (doseq [[status-resp join-resp] join-status-resps]
          (is (success? status-resp))                       ; statuses before all joins have executed
          (is (success? join-resp)))                        ; status of each join request
        (is (not-found? final-status-resp))                   ; status after Fork has fired
        (is (nil? (:metadata final-status-resp)))
        (is (nil? (:joins final-status-resp)))))))

(deftest test-large-barrier-id-with-status
  (testing "Sending a large barrier id in a status request"
    (let [status-resp (get-status large-barrier-id)]
      (is (failure? status-resp)))))