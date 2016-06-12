(ns redis-protocol.parser-test
  (:require [clojure.test :refer :all]
            [redis-protocol.util :as util :refer [parse-str]])
  (:import (redis.resp ReplyParser)))


(deftest reply-parser-test
  (testing "RESP Simple Strings"
    (is (= (parse-str "+OK\r\n") (redis.resp.SimpleString. "OK"))))
  (testing "RESP Errors"
    (is (= (parse-str "-Error message\r\n") (redis.resp.Error. "Error message")))
    (is (= (parse-str "-ERR wrong number of arguments for 'get' command\r\n") (redis.resp.Error. "ERR wrong number of arguments for 'get' command"))))
  (testing "RESP Integers"
    (is (= (parse-str ":1\r\n") 1))
    (is (= (parse-str ":10\r\n") 10))
    (is (= (parse-str ":100\r\n") 100))
    (is (= (parse-str ":1000\r\n") 1000))
    (is (= (parse-str ":10000\r\n") 10000))
    (is (= (parse-str ":100000\r\n") 100000))
    (is (= (parse-str ":-1\r\n") -1)))
  (testing "RESP Bulk Strings"
    (is (= (parse-str "$0\r\n\r\n") ""))
    (is (nil? (parse-str "$-1\r\n")))
    (is (= (parse-str "$6\r\nfoobar\r\n") "foobar")))
  (testing "RESP Arrays"
    (is (= (parse-str "*0\r\n") []))
    (is (nil? (parse-str "*-1\r\n")))
    (is (= (parse-str "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") ["foo" "bar"]))
    (is (= (parse-str "*3\r\n:1\r\n:2\r\n:3\r\n") [1 2 3]))
    (is (= (parse-str "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n") [1 2 3 4 "foobar"]))
    (is (= (parse-str "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n") [[1 2 3] [(redis.resp.SimpleString. "Foo") (redis.resp.Error. "Bar")]]))
    (is (= (parse-str "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n") ["foo" nil "bar"])))
  (testing "Overflowing a response"
    (let [parser (ReplyParser.)]
      (is (= (ReplyParser/PARSE_OVERFLOW) (.parse parser "+OK\r\n+O")))
      (is (util/bytes= (.getOverflow parser) (.getBytes "+O"))))))

(comment
  (loop [parser (ReplyParser.)]
    (let [state-1 (.parse parser "-ERROR\r")
          state-2 (try (.parse parser "\n")
                       (catch Exception err
                         (clojure.stacktrace/print-stack-trace err)))]
      (println state-1)
      (println state-2)))

  (let [parser (ReplyParser.)]
    (doseq [part  (->> "-ERR wrong number of arguments for 'get' command\r\n"
                       vec
                       (partition-all 8)
                       (map #(apply str %)))]
      (println (.parse parser part))))
  )
