(defproject distributed-join "0.1.1"
  :description "Fork Join Service"
  :url "https://github.ibm.com/wdp-dist/distributed-join"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [yogthos/config "0.8"]                    ; config lib
                 [ring "1.6.2"]
                 [ring/ring-defaults "0.3.1"]
                 [compojure "1.6.0"]
                 [metosin/compojure-api "2.0.0-alpha7"]
                 [clj-http "3.6.1" :exclusions [commons-logging]]                        ; http client, should probably use it instead of ring with compojure
                 [metrics-clojure "2.9.0" :exclusions [org.slf4j/slf4j-api]]
                 [metrics-clojure-ring "2.9.0"]
                 [manifold "0.1.7-alpha5"]
                 [aleph "0.4.4-alpha4"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [com.taoensso/carmine "2.16.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]]
  :global-vars {*warn-on-reflection* true}

  :profiles {:prod {:resource-paths ["config/prod"]}
             :dev {:resource-paths ["config/dev"]
                   :dependencies [[javax.servlet/servlet-api "2.5"] ;this and ring-mock are for using the lein-ring plugin which i am not using.
                                  [ring/ring-mock "0.3.1"]]}
             :test {:resource-paths ["config/test"]
                    :dependencies [[com.github.kstyrc/embedded-redis "0.6"]
                                   [org.clojure/math.combinatorics "0.1.4"]]}
             :uberjar {:resource-paths ["config/prod"]
                       :aot :all}}

  :plugins [[lein-pprint "1.1.2"]
            [lein-ring "0.12.0"]
            [lein-kibit "0.1.6-beta2"]]


  :ring {:handler distributed-join.ring/app :browser-uri "/v1/documentation" :reload-paths ["src"]}

  :aliases {"crun" ["do" "clean," "run"]
            "trun" ["do" "clean," "trampoline" "run"]
            "dev-server" ["do" "clean," "with-profile" "+dev" "ring" "server-headless"]
            "uber-server" ["do" "clean," "uberjar," "with-profile" "+dev" "ring" "server-headless"]}

  :main distributed-join.ring)
