(ns redis-protocol.parser-test
  (:require [clojure.test :refer :all]
            [redis-protocol.util :as util]
            [redis-protocol.core :as c]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.tools.trace :as t])
  (:import (redis.resp ReplyParser)))


(defn reload
  "Recompiles & reloads the ReplyParser to continue to work within the REPL."
  []
  (let [classname         "ReplyParser"
        ragel-java-source (str "target/ragel/redis/resp/" classname ".java")
        ragel-classfile   "target/classes/redis/resp/ReplyParser.class"
        ragel-result      (sh/sh "ragel" "-J" (str "src/ragel/redis/resp/" classname ".java.rl") "-o" ragel-java-source)
        full-classname    (str "redis.resp." classname)]
    (when (not= 0 (:exit ragel-result))
      (println  "Unsuccessful ragel compilation: " (:err ragel-result))
      (assert false "ragel compilation failed"))
    (let [javac-result (sh/sh "javac" "-Xlint:unchecked" "-g" "-cp" "target/classes" "-d" "target/classes" ragel-java-source)]
      (when (not= 0 (:exit javac-result))
        (println  "Unsuccessful javac compilation: " (:err javac-result))
        (assert false "java compilation failed")))
    (.defineClass (clojure.lang.DynamicClassLoader.)
                  full-classname
                  (with-open [buffer (java.io.ByteArrayOutputStream.)]
                    (io/copy (io/file ragel-classfile) buffer)
                    (.toByteArray buffer))
                  nil)
    (.importClass @#'clojure.core/*ns* (Class/forName full-classname))))

(reload)

(defn parse-str [^String s]
  (let [parser (ReplyParser.)
        state  (.parse parser s)]
    (if (= state (ReplyParser/PARSE_COMPLETE))
      (first (.root parser))
      (throw (ex-info "parse incomplete" {})))))

(deftest reply-parser-test
  (testing "RESP Simple Strings"
    (is (= (redis.resp.SimpleString. "OK") (parse-str "+OK\r\n"))))
  (testing "RESP Errors"
    (is (= (redis.resp.Error. "Error message") (parse-str "-Error message\r\n")))
    (is (= (redis.resp.Error. "ERR wrong number of arguments for 'get' command") (parse-str "-ERR wrong number of arguments for 'get' command\r\n"))))
  (testing "RESP Integers"
    (is (= 1 (parse-str ":1\r\n") 1))
    (is (= 10 (parse-str ":10\r\n")))
    (is (= 100 (parse-str ":100\r\n")))
    (is (= 1000 (parse-str ":1000\r\n")))
    (is (= 10000 (parse-str ":10000\r\n")))
    (is (= 100000 (parse-str ":100000\r\n")))
    (is (= -1 (parse-str ":-1\r\n"))))
  (testing "RESP Bulk Strings"
    (is (= "" (parse-str "$0\r\n\r\n") ""))
    (is (nil? (parse-str "$-1\r\n")))
    (is (util/bytes= "foobar" (parse-str "$6\r\nfoobar\r\n"))))
  (testing "RESP Arrays"
    (is (= [] (parse-str "*0\r\n")))
    (is (nil? (parse-str "*-1\r\n")))
    (is (util/bytes= "foo" (first (parse-str "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))))
    (is (util/bytes= "bar" (second (parse-str "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))))
    (is (= (parse-str "*3\r\n:1\r\n:2\r\n:3\r\n") [1 2 3]))
    (is (= (take 4 (parse-str "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n")) [1 2 3 4]))
    (is (util/bytes= (last (parse-str "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n")) "foobar"))
    (is (= (parse-str "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n") [[1 2 3] [(redis.resp.SimpleString. "Foo") (redis.resp.Error. "Bar")]]))
    (is (util/bytes= "foo" (first (parse-str "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"))))
    (is (nil? (get (parse-str "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n") 1)))
    (is (util/bytes= "bar" (last (parse-str "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")))))
  (testing "Overflowing a response"
    (let [parsers (vec (take 4 (repeatedly #(ReplyParser. 1))))]
      (is (= (ReplyParser/PARSE_OVERFLOW) (.parse (get parsers 0) "*0\r\n*0\r\n*0\r\n*1\r\n$52\r\n:20160705:20160705:T::DBL:CV-DX::2:100:Y:Y:Y:Y:Y:Y:Y\r\n")))
      (is (= [] (first (.root (get parsers 0 )))))
      (is (= (ReplyParser/PARSE_OVERFLOW) (.parse (get parsers 1) (.getOverflow (get parsers 0)))))
      (is (= [] (first (.root (get parsers 1 )))))
      (is (= (ReplyParser/PARSE_OVERFLOW) (.parse (get parsers 2) (.getOverflow (get parsers 1)))))
      (is (= [] (first (.root (get parsers 2)))))
      (is (= (ReplyParser/PARSE_COMPLETE) (.parse (get parsers 3) (.getOverflow (get parsers 2)))))
      (is (util/bytes= ":20160705:20160705:T::DBL:CV-DX::2:100:Y:Y:Y:Y:Y:Y:Y" (ffirst (.root (get parsers 3))))))
    (let [parser (ReplyParser.)]
      (is (= (ReplyParser/PARSE_OVERFLOW) (.parse parser "+OK\r\n+O")))
      (is (util/bytes= (.getOverflow parser) (.getBytes "+O")))))
  (testing "Multiple Replies"
    (is (= ReplyParser/PARSE_COMPLETE (.parse (ReplyParser. 2) "+OK\r\n:1\r\n")))
    (is (= ReplyParser/PARSE_COMPLETE (.parse (ReplyParser. 2) "*0\r\n*0\r\n")))
    (is (= ReplyParser/PARSE_INCOMPLETE (.parse (ReplyParser. 3) "*0\r\n*0\r\n")))
    (is (= ReplyParser/PARSE_OVERFLOW (.parse (ReplyParser. 1) "*0\r\n*0\r\n")))))
