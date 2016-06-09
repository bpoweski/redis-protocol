(ns redis-protocol.integration-test
  (:require [clojure.test :refer :all :exclude [report]]
            [clojure.java.data :as java]
            [redis-protocol.core :refer :all]
            [redis-protocol.util :as util]
            [taoensso.timbre :as timbre]
            [docker.client :as docker]
            [redis-protocol.core :refer :all]
            [taoensso.carmine :as car]
            [taoensso.carmine.protocol :as proto])
  (:import (java.net InetSocketAddress)))


(timbre/refer-timbre)

(def ^:dynamic *docker-client* nil)

(defn start-cluster [client image-tag]
  (let [img (docker/find-or-pull-image client image-tag)]
    ))

(defn cluster-fixture [f]
  (let [container-id (start-cluster *docker-client* "grokzen/redis-cluster:3.0.6")]
    (f)))

(defn with-docker-client [f]
  (with-open [client (docker/docker-client)]
    (binding [*docker-client* client]
      (f))))

(defn without-debug [f]
  (timbre/set-level! :error)
  (f)
  (timbre/set-level! :debug))

(defn call [client & args]
  @(send-command client (vec args)))

(def test-addresses (map #(vector (str "10.18.10." %) 6379) (range 1 7)))

(defn cluster-meet-nodes [addresses]
  (loop [[meet-host meet-port] (first addresses)
         [[host port] & rest] (drop 1 addresses)]
    (car/wcar {:spec {:host host :port port}}
      (car/cluster-meet meet-host meet-port))
    (when (seq rest)
      (recur [host port] rest))))

(defn cluster-reset [addresses]
  (doseq [[host port] addresses]
    (car/wcar {:spec {:host host :port port}}
      (car/cluster-reset :soft))))

(defn flush-nodes [addresses]
  (doseq [[host port] addresses]
    (car/wcar {:spec {:host host :port port}}
      (car/flushall))))

(defn recreate-cluster [addresses]
  (doto addresses
    (flush-nodes)
    (cluster-meet-nodes)))

(deftest cluster-tests
  (testing "a simple GET"
    (with-open [client (connect "10.18.10.1" 6379)]
      (is (= "OK" (deref (send-command client ["set" "foo" "bar"]) 1000 :timeout)))
      (is (= "bar" (deref (send-command client ["get" "foo"]) 1000 :timeout)))))
  (testing "when a connection is refused"
    (with-open [client (connect "10.18.10.1" 7000)]
      (is (instance? clojure.lang.ExceptionInfo (deref (send-command client ["get" "foo"]) 1000 :timeout))))))

(use-fixtures :each without-debug)
