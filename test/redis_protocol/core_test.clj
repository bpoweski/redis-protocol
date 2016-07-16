(ns redis-protocol.core-test
  (:require [clojure.test :refer :all]
            [redis-protocol.core :refer :all]
            [redis-protocol.util :as util]
            [byte-streams :as bs]
            [taoensso.timbre :as timbre])
  (:import (redis.resp ReplyParser SimpleString Array)
           (java.net InetSocketAddress)))


(deftest args->bytes-test
  (testing "SET key value"
    (is (util/bytes= (args->bytes ["SET" "key" "value"]) "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
    (is (util/bytes= (args->bytes [:SET "key" "value"]) "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
    (is (util/bytes= (args->bytes [:LRANGE "key" 0 -1]) "*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n"))
    (is (not (util/bytes= (args->bytes [:SET (util/utf-16le-bytes "key") "value"]) "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")))))

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

(deftest rerouted-to-test
  (is (= (rerouted-to (redis.resp.Error. "MOVED 3999 127.0.0.1:6381")) [3999 (InetSocketAddress. "127.0.0.1" 6381)]))
  (is (= (rerouted-to (redis.resp.Error. "ASK 3999 127.0.0.1:6381")) [3999 (InetSocketAddress. "127.0.0.1" 6381)])))

(deftest hash-slot-test
  (is (= 0 (hash-slot (.getBytes ""))))
  (is (= 12739 (hash-slot (.getBytes "123456789"))))
  (is (= 12739 (hash-slot (.getBytes "super-long-key{123456789}"))))
  (is (= 0 (hash-slot (.getBytes "super-long-key{}"))))
  (is (= 4092 (hash-slot (.getBytes "{")))))

(def commands
  {:get            {:arity  2 :flags #{:fast :readonly} :key-pos [1 1] :step 1}
   :msetnx         {:arity -3 :flags #{:denyoom :write} :key-pos [1 -1] :step 2}
   :mget           {:arity -2 :flags #{:readonly} :key-pos [1 -1] :step 1}
   :cluster        {:arity -2 :flags #{:admin} :key-pos [0 0] :step 0}
   :eval           {:arity -3 :flags #{:movablekeys :noscript} :key-pos [0 0] :step 0}
   :info           {:arity -1 :flags #{:stale :loading} :key-pos [0 0] :step 0}
   :mset           {:arity -3 :flags #{:denyoom :write} :key-pos [1 -1] :step 2}
   :blpop          {:arity -3 :flags #{:write :noscript} :key-pos [1 -2] :step 1}
   :sort           {:arity -2 :flags #{:denyoom :write :movablekeys} :key-pos [1 1] :step 1}
   :asking         {:arity  1 :flags #{:fast} :key-pos [0 0] :step 0}
   :restore-asking {:arity -4 :flags #{:denyoom :write :asking} :key-pos [1 1] :step 1}})

(deftest routable-slot-test
  (is (= 12182 (routable-slot [:get "foo"] commands)))
  (is (= 12182 (routable-slot [:GET "foo"] commands)))
  (is (nil? (routable-slot [:asking :get "foo"] commands)))
  (is (nil? (routable-slot [:restore-asking "foo" "bar"] commands)))
  (is (nil? (routable-slot [:cluster :nodes] commands)))
  (is (nil? (routable-slot [:mget] commands)))
  (is (nil? (routable-slot [:info] commands)))
  (is (= 247 (routable-slot [:blpop "list1:{10}" "list2:{10}" "list3:{10}" 0] commands)))
  (is (nil? (routable-slot [:eval "foo" "bar"] commands)))
  (is (= 247 (routable-slot [:mget "key:{10}:bar"] commands)))
  (is (= 247 (routable-slot [:mget "key:{10}:bar"  "key:{10}:baz"] commands)))
  (is (= 247 (routable-slot [:msetnx "key:{10}:bar" "val" "key:{10}:baz" "val"] commands)))
  (is (nil? (routable-slot [:mget "key:{10}:bar"  "key:{11}:baz"] commands)))
  (is (= 247 (routable-slot [:mset "key:{10}:bar" "x" "key:{10}:baz" "y"] commands)))
  (is (nil? (routable-slot [:mset "key:{10}:bar" "x" "key:{11}:baz" "y"] commands)))
  (is (nil? (routable-slot [:sort "mylist" :LIMIT 0 10] commands)))
  (is (nil? (routable-slot [:sort "mylist"] commands))))

(deftest command->key-mapping-test
  (is (= [:getbit {:arity 3 :flags #{:readonly :fast} :key-pos [1 1] :step 1}]
         (command->key-mapping [(util/ascii-bytes "getbit") 3 ["readonly" "fast"] 1 1 1]))))
