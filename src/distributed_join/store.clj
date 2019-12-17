(ns distributed-join.store
  (:require [schema.core :as schema :include-macros true]
            [config.core :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:gen-class)
  (:import (java.net URL)))

(defn- validate-ids [invalid-chars max-len id]
  (and (not (clojure.string/blank? id))
       (nil? (re-find invalid-chars id))
       (<= (count id) max-len)))

(def ^:private invalid-chars-barrier-id #":")

(def ^:private join-max-wait (:join-max-wait env))
(def ^:private barrier-id-len-max (:barrier-id-len-max env))
(def ^:private join-message-len-max (:join-message-len-max env))
(schema/defschema Join
  {:barrier-id                     (schema/constrained String
                                                       #(validate-ids invalid-chars-barrier-id barrier-id-len-max %)
                                                       (str "Barrier id must have a value, not exceed " barrier-id-len-max " characters in length and not contain " invalid-chars-barrier-id))
   :position                       (schema/constrained Long #(>= % 0))
   :message                        (schema/constrained String
                                                       #(<= (count %) join-message-len-max)
                                                       (str "Message length cannot exceed " join-message-len-max))
   (schema/optional-key :max-wait) (schema/constrained Long #(and (pos? %1) (< %1 join-max-wait))
                                                       (str "max-wait must be positive and less than or equal to " join-max-wait))})
(def ^:private invalid-chars-join-or-trans #"[~:]")

(def ^:private fork-count-max (:fork-count-max env))
(defn- validate-fork-count [count]
  (and (pos? count)
       (<= count fork-count-max)))

(defn- validate-url [max-len url]
  (and (<= (count url) max-len)
       (try
         (let [parsed-url (URL. url)
               protocol (str/lower-case (.getProtocol parsed-url))
               host (.getHost parsed-url)]
           (and (or (= "http" protocol)
                    (= "https" protocol))
                (not-empty host)))
         (catch Exception ex (do (log/debug ex "Failure parsing" url)
                                 false)))))

(def ^:private fork-max-wait (:fork-max-wait env))
(def ^:private join-id-len-max (:join-id-len-max env))
(def ^:private transaction-id-len-max (:transaction-id-len-max env))
(def ^:private destination-url-len-max (:destination-url-len-max env))
(def ^:private timeout-destination-url-len-max (:timeout-destination-url-len-max env))
(schema/defschema Fork
  {:join-id                                   (schema/constrained String
                                                                  #(validate-ids invalid-chars-join-or-trans join-id-len-max %)
                                                                  (str "Join id must have a value, not exceed " join-id-len-max " characters in length and not contain " invalid-chars-join-or-trans))
   :transaction-id                            (schema/constrained String
                                                                  #(validate-ids invalid-chars-join-or-trans transaction-id-len-max %)
                                                                  (str "Transaction id must have a value, not exceed " transaction-id-len-max " characters in length and not contain " invalid-chars-join-or-trans))
   :count                                     (schema/constrained Long validate-fork-count
                                                                  (str "Count must be positive and less than or equal to " fork-count-max))
   :destination                               (schema/constrained String
                                                                  #(validate-url destination-url-len-max %)
                                                                  (str "Destination does not contain a valid HTTP/HTTPS URL or is longer than " destination-url-len-max))
   (schema/optional-key :timeout-destination) (schema/constrained String
                                                                  #(validate-url timeout-destination-url-len-max %)
                                                                  (str "Timeout destination does not contain a valid HTTP/HTTPS URL or is longer than " timeout-destination-url-len-max))
   (schema/optional-key :max-wait)            (schema/constrained Long #(and (pos? %1) (<= %1 fork-max-wait))
                                                                  (str "max-wait must be positive and less than or equal to " fork-max-wait))})

(schema/defschema Status
  (merge Fork
         {:joins [(schema/maybe Join)]}))

(schema/defschema Publish
  {:barrier-id String
   :messages   [(schema/maybe String)]})
