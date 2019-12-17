# Using Redis Pipelines With Lettuce

## References

* https://github.com/lettuce-io/lettuce-core/wiki/Asynchronous-API
* https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing
* https://github.com/lettuce-io/lettuce-core/wiki/Connection-Pooling

## Overview

Lettuce as well as Carmine isolate creation of pipelines to a database connection (in Lettuce's case, that is via a `com.lambdaworks.redis.api.StatefulConnection`). So, if pipelining is used with Lettuce, a connection pool must also be added and a connection per pipeline used. If you try to create a pipeline on a shared connection, whichever thread calls `flush` first will cause all commands that have been sent to the connection to be flushed to the server for processing (which makes flushing of commands non-deterministic).

Below is one approach taken to support pipelines in the code base.

## Implementation

### Dependencies

```clojure
[celtuce "0.1.0" :exclusions [io.netty/*]]
[org.apache.commons/commons-pool2 "2.4.2"]

[manifold "0.1.7-alpha5"]
```

### Setup: Config, Require/Import, and Connection Pool

Config:
```clojure
{
  :redis-host "127.0.0.1"
  :redis-port 6379
  :redis-get-conn-timeout 5000
  :redis-conn-pool-max-idle 1
  :redis-conn-pool-max-active 50
  :redis-req-timeout 2000
}
```

Connection pool defs:
```clojure
(:require [config.core :refer [env]]
          [manifold.deferred :as deferred]
          [celtuce.commands :as redis]
          [celtuce.connector :as conn]
          [celtuce.codec :refer [nippy-codec]]
          [celtuce.impl.server.async])
(:import (java.util.concurrent TimeUnit)
         (com.lambdaworks.redis.api.async RedisAsyncCommands)
  ;           (com.lambdaworks.redis RedisClient RedisURI RedisConnectionPool)
         (com.lambdaworks.redis RedisClient RedisURI)
         (celtuce.connector RedisConnector)))

; If a password needs to be set, set it on the RedisURI
(def ^RedisURI redis-uri (RedisURI. (:redis-host env) (:redis-port env) (:redis-get-conn-timeout env) TimeUnit/MILLISECONDS))
(def redis-client (delay (RedisClient/create redis-uri)))
(def conn-pool (delay (.asyncPool @redis-client (nippy-codec) (:redis-conn-pool-max-idle env) (:redis-conn-pool-max-active env))))
```

### Macros (Custom) and Helper Functions

```clojure
(defn- ^RedisAsyncCommands get-cmds
  ([] (get-cmds false (:redis-req-timeout env)))
  ([req-timeout] (get-cmds false req-timeout))
  ([auto-flush req-timeout]
   (let [cmds (.allocateConnection @conn-pool)]
     (.setTimeout cmds req-timeout TimeUnit/MILLISECONDS)
     (.setAutoFlushCommands cmds auto-flush)
     cmds)))

(defmacro redis-pipeline [cmds-name & commands]
  `(let [~cmds-name (get-cmds)]
     (-> (deferred/let-flow [resp# ~@commands]
           (.close ~cmds-name)
           resp#)
         (deferred/catch Exception #((.close ~cmds-name)
                                      (throw %))))))

(defn- flush-cmds [cmds]
  (.flushCommands cmds))

(defmacro let-flow-flush
  "Macro that allows use of deferred/let-flow calls with statements in a pipeline. It flushes the statements before passing them to deferred/let-flow."
  [cmds bindings & body]
  (let [var-names (flatten (partition 1 2 bindings))
        flow-bindings (vec (mapcat #(repeat 2 %) var-names))]
    `(let ~bindings
       (flush-cmds ~cmds)
       (deferred/let-flow ~flow-bindings
         ~@body))))

(defn zip-flush [cmds queries]
  "Flushes statements in a pipeline before passing them to deferred/zip."
  (flush-cmds cmds)
  (deferred/zip queries))
```

### Sample Usages

```clojure
(defrecord RedisStore []
  IJoinForkStore

  (fork [this record]
    (redis-pipeline cmds
                    (let-flow-flush cmds [join-id (:join-id record)
                                          transaction-id (:transaction-id record)
                                          key (make-key join-id transaction-id)
                                          key-metadata (make-key-metadata join-id transaction-id)
                                          _ (redis/set cmds key-metadata record)
                                          _ (redis/del cmds key)]
                                         key)))

  (join [this record]
    (redis-pipeline cmds
                    (let-flow-flush cmds [pos (:position record)
                                          key (make-key (:barrier-id record))
                                          key-metadata (make-key-metadata (:barrier-id record))
                                          metadata (redis/get cmds key-metadata)]
                                         (cond
                                           (empty? metadata) (err ::handling/bad-data "barrier-id does not exist")
                                           (>= pos (:count metadata)) (err ::handling/bad-data "position is greater than total")
                                           :else (let-flow-flush cmds [_ (redis/hset cmds key pos record)
                                                                       count (redis/hlen cmds key)]
                                                                      {:metadata metadata :count count})))))

  (join-count [this barrier-id]
    (redis-pipeline cmds
                    (let-flow-flush cmds [key (make-key barrier-id)
                                          count (redis/hlen cmds key)]
                                         count)))

  (complete [this barrier-id]
    (redis-pipeline cmds
                    (let [key (make-key barrier-id)
                          key-metadata (make-key-metadata barrier-id)]
                      (zip-flush cmds [(redis/del cmds key)
                                       (redis/del cmds key-metadata)])))))
```
