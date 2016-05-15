(defproject redis-protocol "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [byte-streams "0.2.2"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.trace "0.7.9"]
                 [com.taoensso/timbre "4.3.1"]]
  :plugins [[lein-ragel "0.1.0"]]
  :java-source-paths ["target/ragel"]
  :ragel-source-paths ["src-ragel"]
  :prep-tasks ["ragel" "javac"])
