(ns redis-protocol.integration-test
  (:require [clojure.test :refer :all :exclude [report]]
            [redis-protocol.core :refer :all]
            [taoensso.timbre :as timbre])
  (:import (redis.protocol ReplyParser ReplyParser$SimpleString)))


(timbre/refer-timbre)

(defn without-debug [f]
  (timbre/set-level! :error)
  (f)
  (timbre/set-level! :debug))

(defn call [client & args]
  @(send-command client (vec args)))

(def test-addresses (map #(vector (str "10.18.10." %) 6379) (range 1 7)))

(deftest cluster-tests
  (testing "a simple GET"
    (with-open [client (connect "10.18.10.1" 6379)]
      (is (= (ReplyParser$SimpleString. "OK") (deref (send-command client ["set" "foo" "bar"]) 1000 :timeout)))
      (is (= "bar" (deref (send-command client ["get" "foo"]) 1000 :timeout)))))
  (testing "when a connection is refused"
    (with-open [client (connect "10.18.10.1" 7000)]
      (is (instance? clojure.lang.ExceptionInfo (deref (send-command client ["get" "foo"]) 1000 :timeout))))))

(use-fixtures :each without-debug)
