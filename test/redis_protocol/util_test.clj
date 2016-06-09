(ns redis-protocol.util-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [redis-protocol.util :refer :all]))


(deftest crc16-test
  (is (= (crc16 (.getBytes "user1000")) (crc16 (.getBytes "user1000"))))
  (is (= 0x0 (crc16 (.getBytes ""))))
  (is (= 0x31c3 (crc16 (.getBytes "123456789"))))
  (is (= 0x31c3 (crc16 (.getBytes "{123456789}") 1 9)))
  (is (= 0xa45c (crc16 (.getBytes "sfger132515"))))
  (is (= 0x58ce (crc16 (.getBytes "hae9Napahngaikeethievubaibogiech"))))
  (is (= 0x4fd6 (crc16 (.getBytes "Hello, World!")))))

(deftest buffer=-test
  (is (true? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo"))))
  (is (false? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo ")))))
