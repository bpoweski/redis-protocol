(defproject redis-protocol "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[byte-streams "0.2.2"]
                 [com.lmax/disruptor "3.3.4"]
                 [com.taoensso/timbre "4.3.1"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/java.data "0.1.1"]
                 [org.clojure/tools.trace "0.7.9"]]
  :profiles {:dev {:dependencies [;; it's shocking this hasn't cause dependency conflicts yet
                                  [com.spotify/docker-client "5.0.1"]
                                  [com.taoensso/carmine "2.9.2"]]}}
  :plugins [[lein-ragel "0.1.0"]]
  :java-source-paths ["target/ragel"]
  :ragel-source-paths ["src-ragel"]
  :prep-tasks ["ragel" "javac"])
