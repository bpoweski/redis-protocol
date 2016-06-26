(defproject redis-protocol "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://github.com/bpoweski/redis-protocol"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[byte-streams "0.2.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [com.taoensso/encore "2.52.1"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.trace "0.7.9"]]
  :profiles {:dev {:dependencies [[com.spotify/docker-client "5.0.1"] ;; it's shocking this hasn't cause dependency conflicts yet
                                  [com.taoensso/carmine "2.13.0-RC1"]
                                  [criterium "0.4.4"]
                                  [org.clojure/tools.namespace "0.2.9"]]}
             :test {:plugins [[venantius/ultra "0.4.1"]]
                    :ultra {:repl  false
                            :tests {:color-scheme :default}}}}
  :aliases {"build" ["do" ["clean"] ["ragel"] ["javac"]]}
  :plugins [[lein-ragel "0.1.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java" "target/ragel"]
  :ragel-source-paths ["src/ragel"]
  :prep-tasks ["ragel" "javac"])
