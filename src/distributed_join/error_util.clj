(ns distributed-join.error-util)

(defn ex-data-for-http-resp [http-failed-resp]
  "
  This will extract the data and body for a failed HTTP response - returns data and body of response.
  If the response does not implement IExceptionInfo, it will return the response for the data and a nil body.
  "
  (let [data (ex-data http-failed-resp)]
    (cond
      (nil? data) {:data http-failed-resp :body nil}
      (nil? (:body data)) {:data data :body nil}
      :else {:data data :body (slurp (:body data))})))
