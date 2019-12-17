(ns distributed-join.deferred-util
  (:require [clojure.test :refer :all]
            [manifold.deferred :as deferred]
            [clojure.tools.logging :as log]))

(defn ex-to-error [body-fn]
  "Catches exceptions thrown from the body and turns into a deferred error"
  (try
    (body-fn)
    (catch Exception ex
      (deferred/error-deferred ex))))

(defn ex-to-error-with-action [body-fn action-fn]
  "Catches exceptions thrown from the body, turns into deferred error and allows user to define action to take on error"
  (try
    (body-fn)
    (catch Exception ex
      (do
        (log/error ex "Exception caught by ex-to-error-with-action")
        (action-fn (deferred/error-deferred ex))))))
