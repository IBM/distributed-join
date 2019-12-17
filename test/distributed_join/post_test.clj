(ns distributed-join.post-test
  (:require [clojure.test :refer :all]
            [config.core :refer [env]]
            [distributed-join.request-util :refer :all]
            [distributed-join.publish :as publish]
            [distributed-join.embedded-jetty :as ej]
            [clojure.tools.logging :as log])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent TimeoutException)))

(use-fixtures :each ej/jetty-fixture)

(defn- calc-response-delay [delay-ms]
  (+ delay-ms (:publish-request-timeout-ms env)))

(defn- url-with-delay [delay-ms]
  (str "http://localhost:" (:embedded-jetty-port env) "/v1/delayed/echo?delay=" delay-ms))

(deftest verify-request-timeout
  (testing "Will send a request that is guaranteed to timeout and test what happens"
    (is (thrown-with-msg? TimeoutException (re-pattern "timed out after 2000 milliseconds")
                          (log-failure-data-deref
                            (publish/post (url-with-delay (calc-response-delay 200))
                                          "Echo text"))))))

(def ^:private bad-url (str "http://localhost:" (:embedded-jetty-port env) "/v1/bad/url"))

(deftest verify-bad-url
  (testing "Will send bad URL's and see the result"
    (is (thrown-with-msg? ExceptionInfo (re-pattern "status: 404")
                          (log-failure-data-deref
                            (publish/post bad-url "Message for bad url"))))))