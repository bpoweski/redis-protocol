(ns redis-protocol.util-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [redis-protocol.util :refer :all]
            [clojure.string :as str])
  (:import (redis.resp ReplyParser)
           (java.net InetSocketAddress)))


(deftest crc16-test
  (is (= (crc16 (ascii-bytes "user1000")) (crc16 (ascii-bytes "user1000"))))
  (is (= 0x0 (crc16 (ascii-bytes ""))))
  (is (= 0x31c3 (crc16 (ascii-bytes "123456789"))))
  (is (= 0x31c3 (crc16 (ascii-bytes "{123456789}") 1 9)))
  (is (= 0xa45c (crc16 (ascii-bytes "sfger132515"))))
  (is (= 0x58ce (crc16 (ascii-bytes "hae9Napahngaikeethievubaibogiech"))))
  (is (= 0x4fd6 (crc16 (ascii-bytes "Hello, World!")))))

(deftest ascii-str-test
  (is (= "1234" (ascii-str (ascii-bytes "1234")))))

(deftest to-bytes-test
  (is (bytes= (ascii-bytes "string") (to-bytes "string")))
  (is (bytes= "SET" :SET))
  (is (bytes= "SET" (ascii-bytes "SET"))))

(deftest buffer=-test
  (is (true? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo"))))
  (is (false? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo ")))))

(deftest cli-format-test
  (is (= "(error) MOVED 12182 10.18.10.5:6379\n" (cli-format (redis.resp.Error. "MOVED 12182 10.18.10.5:6379"))))
  (is (= "OK\n" (cli-format (redis.resp.SimpleString. "OK"))))
  (is (= "\"10.18.10.4\"\n" (cli-format (ascii-bytes "10.18.10.4"))))
  (is (= "1) (integer) 10923\n" (cli-format (doto (redis.resp.Array. 1) (.add 10923)))))
  (is (= "(nil)\n" (cli-format nil))))

(deftest parse-cluster-info-test
  (is (= {"cluster_state" "ok" "cluster_slots_assigned" "16384"} (parse-cluster-info "cluster_state:ok\r\ncluster_slots_assigned:16384"))))

(def sample-cluster-nodes
  ["07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected"
   "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922"
   "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383"
   "6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected"
   "824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected"
   "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460"])

(deftest parse-cluster-nodes-test
  (is (= (parse-cluster-node (get sample-cluster-nodes 5))
         {:redis/id           "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"
          :redis/address      (InetSocketAddress. "127.0.0.1" 30001)
          :redis/flags        #{:myself :master}
          :redis/ping-sent    0
          :redis/pong-recv    0
          :redis/config-epoch 1
          :redis/link-state   :connected
          :redis/slots        "0-5460"}))
  (is (= 6 (count (parse-cluster-nodes (str/join "\n" sample-cluster-nodes))))))

(deftest decode-hexdump-test
  (is (bytes= (ascii-bytes "!CnY\r\n") (first (decode-hexdump "    21 43 6E 59 0D 0A                                      !CnY..")) ))
  (is (= 2 (count (decode-hexdump (str/join "\n\n " (repeat 2 "    21 43 6E 59 0D 0A                                      !CnY..")))) )))
