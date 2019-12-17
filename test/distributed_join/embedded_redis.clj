(ns distributed-join.embedded-redis
  (:require [config.core :refer [env]]
            [clojure.tools.logging :as log]
            [taoensso.carmine :as car :refer (wcar)])
  (:import (redis.embedded RedisServer)))

(defn- setup-test-with-redis [rs]
  (when-not (.isActive rs)
    (let [st (.start rs)]
      (Thread/sleep 1000)                                   ; Give Redis a chance to start up...
      (log/info "Started redis server" rs))))

(defn- start-embedded-redis []
  (let [rs (-> (RedisServer/builder)
               (.port (int (:embedded-redis-port env)))
               (.setting (str "bind " (:embedded-redis-host env)))
               (.build))]
    (setup-test-with-redis rs)
    rs))                                                    ;return rs so that i can stop it later

(def ^:private db (start-embedded-redis))
(def server-conn {:pool {}
                  :spec {:uri        (:redis-uri env)
                         :timeout-ms (:redis-req-timeout-ms env)}})

(defmacro wcar* [& body] `(car/wcar server-conn ~@body))

(defn- clear-db []
  (log/info "Clearing all data from embedded redis.")
  (when (.isActive db)
    (do
      (try
        (wcar* (car/ping))
        (catch Exception ex (log/error ex "Failure calling ping prior to flushing db")))
      (wcar* (car/flushdb)))))

(defn- teardown-test-with-redis []
  (log/info "Stopping redis server")
  (.stop db))

(defn redis-fixture [f]
  (setup-test-with-redis db)
  (f)
  (teardown-test-with-redis))

(defn redis-cleanup-fixture [f]
  (clear-db)
  (f))
