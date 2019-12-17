(ns distributed-join.time-util)

(defn now-milliseconds []
  (System/currentTimeMillis))

(defn now-with-offset
  "Adds number of seconds to current time"
  [seconds]
  (when seconds
    (+ (* 1000 seconds) (now-milliseconds))))

