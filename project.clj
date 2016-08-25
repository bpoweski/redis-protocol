(defproject redis-protocol "0.1.0-SNAPSHOT"
  :description "A low level Redis Cluster library"
  :url "http://github.com/bpoweski/redis-protocol"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[byte-streams "0.2.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [com.taoensso/tufte "1.0.0-RC2"]
                 [com.taoensso/encore "2.52.1"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.trace "0.7.9"]
                 [org.clojure/tools.cli "0.3.5"]]
  :profiles {:dev {:dependencies [[com.spotify/docker-client "5.0.1"] ;; it's shocking this hasn't cause dependency conflicts yet
                                  [com.taoensso/carmine "2.13.0-RC1"]
                                  [redis.clients/jedis "2.9.0"]
                                  [criterium "0.4.4"]
                                  [org.clojure/tools.namespace "0.2.9"]]}
             :test {:plugins [[venantius/ultra "0.4.1"]]
                    :ultra {:repl  false
                            :tests {:color-scheme :default}}}

             :repl {:dependencies [[perforate "0.3.3"]]}

             :perf {:plugins [[perforate "0.3.3"]]
                    :dependencies [[redis.clients/jedis "2.9.0"]
                                   [com.taoensso/timbre "4.3.1"]]
                    :perforate {:benchmark-paths ["src/benchmarks"]}}}
  :plugins [[lein-ragel "0.1.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java" "target/ragel"]
  :ragel-source-paths ["src/ragel"]
  :javac-options ["-target" "1.7" "-source" "1.7"]
  :prep-tasks ["ragel" "javac"]
  :aliases {"build" ["do" ["clean"] ["ragel"] ["javac"]]
            "perf"  ["with-profile" "+perf" "perforate"]})
