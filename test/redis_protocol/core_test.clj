(ns redis-protocol.core-test
  (:require [clojure.test :refer :all]
            [redis-protocol.core :refer :all]
            [redis-protocol.util :as util]
            [byte-streams :as bs]
            [taoensso.timbre :as timbre])
  (:import (redis.resp ReplyParser SimpleString Array)
           (java.net InetSocketAddress)))


(deftest args->str-test
  (testing "SET key value"
    (is (= (args->str ["SET" "key" "value"]) "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))))

(deftest encode-str-test
  (testing "SET"
    (is (= (encode-str "SET") "$3\r\nSET\r\n"))))

(deftest ops->str-test
  (is (= "[---]" (ops->str 0)))
  (is (= "[--C]" (ops->str OP_CONNECT)))
  (is (= "[R--]" (ops->str OP_READ)))
  (is (= "[-W-]" (ops->str OP_WRITE)))
  (is (= "[RW-]" (ops->str (bit-or OP_WRITE OP_READ))))
  (is (= "[RWC]" (ops->str (bit-or OP_CONNECT OP_WRITE OP_READ)))))

(deftest moved-test
  (is (false? (moved? nil)))
  (is (false? (moved? "OK")))
  (is (false? (moved? (redis.resp.Error. "ERR unknown command 'foobar'"))))
  (is (true? (moved? (redis.resp.Error. "MOVED 3999 127.0.0.1:6381")))))

(deftest moved-to-test
  (is (nil? (moved-to nil)))
  (is (nil? (moved-to "OK")))
  (is (= (moved-to (redis.resp.Error. "MOVED 3999 127.0.0.1:6381")) [3999 (InetSocketAddress. "127.0.0.1" 6381)])))

(deftest hash-slot-test
  (is (= 0 (hash-slot (.getBytes ""))))
  (is (= 12739 (hash-slot (.getBytes "123456789"))))
  (is (= 12739 (hash-slot (.getBytes "super-long-key{123456789}"))))
  (is (= 0 (hash-slot (.getBytes "super-long-key{}"))))
  (is (= 4092 (hash-slot (.getBytes "{")))))
