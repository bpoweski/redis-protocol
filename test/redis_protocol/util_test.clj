(ns redis-protocol.util-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [redis-protocol.util :refer :all]))

(deftest crc16-test
  (is (= (crc16 (.getBytes "user1000")) (crc16 (.getBytes "user1000")))))

(deftest buffer=-test
  (is (true? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo"))))
  (is (false? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo ")))))
