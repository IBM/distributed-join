;;https://pragprog.com/magazines/2011-07/create-unix-services-with-clojure
(ns distributed-join.scheduler
  (:require [config.core :refer [env]]
            [manifold.deferred :as deferred]
            [clojure.tools.logging :as log]
            [distributed-join.deferred-util :as du])
  (:import (java.util.concurrent ScheduledThreadPoolExecutor TimeUnit Executors ExecutorService)))

(def ^:private all-schedule-pools (atom #{}))
(def ^:private should-continue (atom true))

(defrecord SchedulePool [num-threads schedule-pool])

(defn- create-schedule-pool [num-threads]
  (let [schedule-pool (ScheduledThreadPoolExecutor. num-threads)]
    (swap! all-schedule-pools conj schedule-pool)
    schedule-pool))

(defn- schedule [schedule-pool delay f]
  (.schedule schedule-pool f delay TimeUnit/MILLISECONDS))

(defn- reschedule-work [schedule-pool delay work-fn]
  (if @should-continue
    ; Thread first macro will cause the first deferred to be realized and allow the catch to run for any exceptions.
    (-> (deferred/let-flow [result (du/ex-to-error work-fn)]
          (if @should-continue
            (if (empty? result)
              ; Since there was no result, wait for the delay amount before trying again.
              (schedule schedule-pool delay #(reschedule-work schedule-pool delay work-fn))
              ; There was a result, so immediately look to see if there is more work to do.
              (schedule schedule-pool 0 #(reschedule-work schedule-pool delay work-fn)))
            (log/info "Work will not be rescheduled for" work-fn "as it has been told to stop."))
          result)
        (deferred/catch (fn [ex]
                          (log/error "Scheduled work for" work-fn "failed because of" ex)
                          (if @should-continue
                            (do
                              (log/info "Rescheduling work" work-fn "after failure with a delay of" delay)
                              (schedule schedule-pool delay #(reschedule-work schedule-pool delay work-fn)))
                            (log/info "Work will not be rescheduled for" work-fn "as it has been told to stop.")))))
    (log/info "No further work will be done for " work-fn "as it has been told to stop.")))

(defn forever [work-fn num-threads delay-non-found]
  (let [schedule-pool (create-schedule-pool num-threads)]
    (log/info "Scheduling" num-threads work-fn "on" schedule-pool "with delay" delay-non-found)
    (doall
      ; Add as many workers as there are threads so that all threads are used.
      (repeatedly num-threads
                  #(schedule schedule-pool delay-non-found (fn [] (reschedule-work schedule-pool delay-non-found work-fn)))))))

(defn shutdown []
  (log/info "Shutting down the scheduler...")
  (reset! should-continue false)
  (log/info "Told all tasks to stop rescheduling - should continue is now:" @should-continue)
  (log/info "Shutting down all schedule pools")
  (doseq [pool @all-schedule-pools]
    (log/info "Shutting down pool:" pool)
    (.shutdown pool)
    (.awaitTermination pool (:shutdown-scheduled-work-delay-ms env) (TimeUnit/MILLISECONDS))
    (.shutdownNow pool))
  (log/info "Scheduler shutdown completed."))