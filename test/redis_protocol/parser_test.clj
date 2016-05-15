(ns redis-protocol.parser-test
  (:require  [clojure.test :refer :all]
             [redis-protocol.parser :refer :all])
  (:import (redis.protocol ReplyParser)))


(defn parse-str [s]
  (let [parser (ReplyParser.)
        state  (.parse parser s)]
    (if (= state (ReplyParser/PARSE_COMPLETE))
      (container->reply (.root parser))
      (throw (ex-info "parse incomplete" {})))))

(deftest reply-parser-test
  (testing "RESP Simple Strings"
    (is (= (parse-str "+OK\r\n") "OK")))
  (testing "RESP Errors"
    (is (= (parse-str "-Error message\r\n") "Error message")))
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
    (is (= (parse-str "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n") [[1 2 3] ["Foo" "Bar"]]))
    (is (= (parse-str "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n") ["foo" nil "bar"]))))

(comment
  (loop [parser (ReplyParser.)]
    (let [state-1 (.parse parser "-ERROR\r")
          state-2 (.parse parser "\n")]
      (println state-1)
      (println state-2)))
  )
