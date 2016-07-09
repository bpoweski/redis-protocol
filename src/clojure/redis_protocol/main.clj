(ns redis-protocol.main
  (:require [redis-protocol.core :as redis]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.trace :as t])
  (:import (java.util.concurrent TimeUnit)
           (java.net InetAddress)))


(timbre/refer-timbre)


(def LATENCY_SAMPLE_RATE 10)
(def LATENCY_HISTORY_DEFAULT_INTERVAL 15000)

(defprotocol DurationConversions
  (ns->ms [x])
  (ns->s [x]))

(extend-protocol DurationConversions
  java.lang.Double
  (ns->ms [x] (* 1e-6 x))
  (ns->s [x] (* 1e-9 x))

  java.lang.Long
  (ns->ms [x] (.toMillis TimeUnit/NANOSECONDS x))
  (ns->s [x] (.toSeconds TimeUnit/NANOSECONDS x)))


(def initial-latency-stats
  {:min (Long/MAX_VALUE)
   :max 0
   :tot 0
   :n   0
   :avg 0.0})

(defn update-latency [{:keys [tot n] :as stats} latency]
  (let [n (inc n)]
    (if (= 1 n)
      (assoc stats :n n :min latency :max latency :tot latency :avg (double latency))
      (-> stats
          (assoc :n n)
          (update :min min latency)
          (update :max max latency)
          (update :tot + latency)
          (assoc :avg (/ (double (+ tot latency)) n))))))

(defn latency [{:keys [interval] :as options}]
  (with-open [conn (redis/connect "127.0.0.1" 6379)]
    (loop [history-start (System/nanoTime)
           start         history-start
           stats         initial-latency-stats]
      (let [result  @(redis/send-command conn [:PING])
            latency (- (System/nanoTime) start)
            stats   (update-latency stats latency)]
        (printf "\u001B[0G\u001B[2Kmin: %d, max: %d, avg: %.2f (%d samples)" (ns->ms (:min stats)) (ns->ms (:max stats)) (ns->ms (:avg stats)) (:n stats))
        (flush)
        (let [reset? (and (:latency-history options)
                          (> (ns->s (double (- (System/nanoTime) history-start))) (:interval options)))]
          (when reset?
            (printf " -- %.2f seconds range\n" (ns->s (double (- (System/nanoTime) history-start)))))
          (let [history-start (if reset? (System/nanoTime) history-start)]
            (Thread/sleep 10)
            (recur history-start (System/nanoTime) (if reset? initial-latency-stats stats))))))))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))


(def cli-options
  [["-h" "--hostname HOST" "Remote host"
    :default (InetAddress/getByName "127.0.0.1")
    :default-desc "127.0.0.1"
    :parse-fn #(InetAddress/getByName %)]
   ["-p" "--port PORT" "Server port."
    :default 6379
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-i" "--interval INTERVAL" "When -r is used, waits <interval> seconds per command.  It is possible to specify sub-second times like -i 0.1."
    :default 15
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be a positive number"]]

   [nil "--latency" "Enter a special mode continuously sampling latency."]
   [nil "--latency-history" "Like --latency but tracking latency changes over time.  Change it using -i."]
   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   [nil "--help"]])


(defn usage [options-summary]
  (->> ["redis-protocol.main"
        ""
        "Usage: lein run -m redis-protocol.main [OPTIONS]"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))

(defn -main [& args]
  (timbre/set-level! :error)
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options)
      (exit 0 (usage summary))

      (or (:latency options) (:latency-history options))
      (latency options)

      errors
      (exit 1 (error-msg errors)))))
