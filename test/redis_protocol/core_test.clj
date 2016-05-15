(ns redis-protocol.core-test
  (:require [clojure.test :refer :all]
            [redis-protocol.core :refer :all]
            [byte-streams :as bs]
            [taoensso.timbre :as timbre]))


(deftest args->str-test
  (testing "SET key value"
    (is (= (args->str ["SET" "key" "value"]) "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))))

(deftest encode-str-test
  (testing "SET"
    (is (= (encode-str "SET") "$3\r\nSET\r\n"))))

(deftest buffer=-test
  (is (true? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo"))))
  (is (false? (buffer= (bs/to-byte-buffer "foo") (bs/to-byte-buffer "foo ")))))

(deftest aconcat-bytes-test
  (is (bytes= (aconcat-bytes (.getBytes "a") (.getBytes "b")) (.getBytes "ab")))
  (is (bytes= (aconcat-bytes (.getBytes "a") (.getBytes "")) (.getBytes "a")))
  (is (bytes= (aconcat-bytes (.getBytes "") (.getBytes "")) (.getBytes ""))))

;; (deftest complete?-test
;;   (is (complete? (bs/to-byte-array "+OK\r\n")))
;;   (is (not (complete? (bs/to-byte-array "+OK\r"))))
;;   (is (not (complete? (bs/to-byte-array "")))))

;; (deftest parse-chunk-test
;;   (testing "RESP Errors"
;;     (is (= (parse-chunk "-ERROR some\r\n")
;;            (->ErrorFrame "ERROR some")))
;;     (is (= (:reply-type (parse-chunk "-ERROR")) \-)))
;;   (testing "RESP Status"
;;     (is (= (parse-chunk "+OK\r\n")
;;            (->StatusFrame "OK")))
;;     (is (= (:reply-type (parse-chunk "+O")) \+))
;;     (is (bytes= (:buffer (parse-chunk "+O")) (bs/to-byte-array "O"))))
;;   (testing "RESP Integers"
;;     (is (= (parse-chunk ":0\r\n") (->IntegerFrame 0))))
;;   (testing "RESP Bulk Strings"
;;     (is (bytes= (:buffer (parse-chunk "$6\r\nfoobar\r\n")) (bs/to-byte-array "foobar"))))
;;   (testing "RESP Arrays"
;;     (is (= (parse-chunk "*-1\r\n") [])))
;;   (testing "RESP Complex"
;;     (let [body "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
;;           ])))

;; (deftest parse-next-chunk-test
;;   (testing "Errors"
;;     (is (= (-> (bs/to-byte-buffer "-ERROR")
;;                (parse-chunk)
;;                (parse-next-chunk (bs/to-byte-buffer " message\r\n")))
;;            (->ErrorFrame "ERROR message")))))
