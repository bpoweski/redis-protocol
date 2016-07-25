(ns redis-protocol.cli
  (:refer-clojure :exclude [benchmark])
  (:require [redis-protocol.core :as redis]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.trace :as t]
            [redis-protocol.util :as util])
  (:import (java.util.concurrent TimeUnit)
           (java.net InetAddress)))


(timbre/refer-timbre)


(defprotocol DurationConversions
  (ns->ms [x])
  (ns->s [x])
  (s->ms [x]))

(extend-protocol DurationConversions
  java.lang.Double
  (ns->ms [x] (* 1e-6 x))
  (ns->s [x] (* 1e-9 x))
  (s->ms [x] (* x 1000))

  java.lang.Long
  (ns->ms [x] (.toMillis TimeUnit/NANOSECONDS x))
  (ns->s [x] (.toSeconds TimeUnit/NANOSECONDS x))
  (s->ms [x] (.toMillis TimeUnit/SECONDS x)))


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

(defn print-stats [stats]
  (printf "\u001B[0G\u001B[2Kmin: %d, max: %d, avg: %.2f (%d samples)" (ns->ms (:min stats)) (ns->ms (:max stats)) (ns->ms (:avg stats)) (:n stats))
  (flush))

(defn latency [{:keys [interval host port] :as options :or {interval 1.0}}]
  (with-open [conn (redis/connect host port)]
    (loop [history-start (System/nanoTime)
           start         history-start
           stats         initial-latency-stats]
      (let [result  @(redis/send-command conn [:ping])
            latency (- (System/nanoTime) start)
            stats   (doto (update-latency stats latency)
                      (print-stats))]
        (let [reset? (and (:latency-history options)
                          (> (ns->s (double (- (System/nanoTime) history-start))) interval))]
          (when reset?
            (printf " -- %.2f seconds range\n" (ns->s (double (- (System/nanoTime) history-start)))))
          (let [history-start (if reset? (System/nanoTime) history-start)]
            (Thread/sleep 10)
            (recur history-start (System/nanoTime) (if reset? initial-latency-stats stats))))))))

(defmacro with-latency [& body]
  `(let [start# (System/nanoTime)]
     [(do ~@body) (- (System/nanoTime) start#)]))

(defn sub-random-value
  "Generates and replaces random values per the following placeholders.

  __rand_int__      : Random integer between 0 and Integer/MAX_VALUE
  __rand_str[arg]__ : Random string of printable characters using of size arg, where arg is `n(b|kb|mb|gb)`

  examples:

  key:__rand_int__       => key:134318, key:19823, etc
  key:__rand_str[8b]__   => key:af3*s-vn, etc
  key:__rand_str[10kb]__ => key:asdf21-1832....."
  [x]
  (let [f (fn [[_ val-type size unit]]
            (if (= "int" val-type)
              (str (rand-int Integer/MAX_VALUE))
              (String. (util/rand-chars (Long/parseLong size) (keyword unit)))))]
    (str/replace x #"(?i)__rand_(int|str\[(\d+)(b|kb|mb|gb)\])__" f)))

(defn args->command [[command :as arguments]]
  (->> arguments
       (drop 1)
       (map sub-random-value)
       (into [(keyword (str/lower-case command))])))

(defn run [args {time? :time :keys [repeat interval ^java.net.InetAddress host port] :as options :or {time? false}}]
  (spy :debug options)
  (with-open [conn (redis/connect (.getHostAddress host) port)]
    (loop [stats initial-latency-stats
           n     (dec repeat)]
      (let [command   (args->command args)
            [resp ns] (with-latency @(redis/send-command conn command))
            stats     (update-latency stats ns)]
        (when time?
          (print-stats stats))

        (cond
          (instance? Throwable resp) (throw resp)
          (false? time?)             (print (util/cli-format resp)))

        (when (number? interval)
          (Thread/sleep (s->ms interval)))

        (when (pos? n)
          (recur stats (dec n)))))))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(def cli-options
  [["-h" "--host HOST" "Remote host"
    :default (InetAddress/getByName "127.0.0.1")
    :default-desc "127.0.0.1"
    :parse-fn #(InetAddress/getByName %)]
   ["-p" "--port PORT" "Server port."
    :default 6379
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-r" "--repeat REPEAT" "Execute specified command N times."
    :default 1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive number"]]
   ["-i" "--interval INTERVAL" "When -r is used, waits <interval> seconds per command.  It is possible to specify sub-second times like -i 0.1."
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be a positive number"]]

   ["-T" "--time" "Time results of [cmd [arg [arg ..]]]"]

   [nil "--latency" "Enter a special mode continuously sampling latency."]
   [nil "--latency-history" "Like --latency but tracking latency changes over time.  Change it using -i."]
   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   [nil "--help"]])

(defn usage [options-summary]
  (->> ["redis-protocol.cli"
        ""
        "Usage: lein run -m redis-protocol.cli [OPTIONS] [cmd [arg [arg ..]]]"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))

(defn verbosity->level [x]
  (get (reduce-kv assoc {} (vec (rseq timbre/ordered-levels))) (+ x 2) :error))

(defn -main [& args]
  (timbre/set-level! :error)
  (let [{:keys [options arguments errors summary] :as ops} (cli/parse-opts args cli-options)]
    (spy :debug options)
    (timbre/set-level! (verbosity->level (:verbosity options)))
    (cond
      (:help options)
      (exit 0 (usage summary))

      errors
      (exit 1 (error-msg errors))

      (or (:latency options) (:latency-history options))
      (latency options)

      :else
      (run arguments options))))
