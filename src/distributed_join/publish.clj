(ns distributed-join.publish
  (:require [aleph.http :as http]
            [config.core :refer [env]]
            [clojure.tools.logging :as log]))

(defn post [url msg]
  (do
    (log/info "Publishing msg" msg "to url" url)
    (http/post url
               {:content-type       :application/edn
                :form-params        msg
                ; :pool - Can create a custom connection pool via http/connection-pool
                :pool-timeout       (:publish-connection-pool-timeout-ms env)
                :connection-timeout (:publish-connection-timeout-ms env)
                :request-timeout    (:publish-request-timeout-ms env)})))
