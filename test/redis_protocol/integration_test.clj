(ns redis-protocol.integration-test
  (:require [clojure.test :refer :all :exclude [report]]
            [clojure.java.data :as java]
            [redis-protocol.core :refer :all]
            [taoensso.timbre :as timbre]
            [docker.client :as docker]
            [redis-protocol.core :as redis]
            [taoensso.carmine :as car]))


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
  @(redis/send-command client (vec args)))

(use-fixtures :each without-debug)

(deftest cluster-tests
  (testing "client connection"
    (with-open [client (redis/connect "172.17.0.2" 7002)]
      (is (= (call client "get" "foo") [nil])))))
