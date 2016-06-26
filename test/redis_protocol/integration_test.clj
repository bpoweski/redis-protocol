(ns redis-protocol.integration-test
  (:require [clojure.test :refer :all :exclude [report]]
            [redis-protocol.core :refer :all]
            [taoensso.timbre :as timbre]
            [taoensso.carmine :as car]
            [redis-protocol.util :as util])
  (:import (redis.resp ReplyParser)
           (java.net InetSocketAddress)))


(timbre/refer-timbre)

(defn without-debug [f]
  (timbre/set-level! :error)
  (f)
  (timbre/set-level! :debug))

(defn call [client & args]
  (deref (send-command client (vec args)) 1000 :timeout))

(def test-addresses (map #(vector (str "10.18.10." %) 6379) (range 1 7)))


;; use carmine for these operations until the client is more functional
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
      (car/cluster-reset :hard))))

(defn flush-nodes [addresses]
  (doseq [[host port] addresses]
    (car/wcar {:spec {:host host :port port}}
      (car/flushall))))

(defn reset-cluster [addresses]
  (doto addresses
    (flush-nodes)
    (cluster-reset)
    (cluster-meet-nodes)))

(defn await-convergence [host port]
  (let [spec  {:spec {:host host :port port}}]
    (loop [n 0]
      (when-not (> n 10)
        (let [cluster-info (util/parse-cluster-info (car/wcar spec (car/cluster-info)))
              nodes        (util/parse-cluster-nodes (car/wcar spec (car/cluster-nodes)))]
          (when-not (and (= (count nodes) 6) (not= "fail" (get cluster-info "cluster_state")))
            (Thread/sleep 500)
            (recur (inc n))))))))

(defn cluster-assign-all [host port]
  (let [spec {:spec {:host host :port port}}]
    (= "OK" (car/wcar spec (apply car/cluster-addslots (range 0 (dec cluster-hash-slots)))))))

(defn recreate-cluster []
  (info "reset-cluster")
  (reset-cluster test-addresses)
  (info "cluster-assign-all")
  (cluster-assign-all "10.18.10.1" 6379)
  (info "waiting for cluster to converge")
  (await-convergence "10.18.10.1" 6379)
  (info "done"))

(defmacro with-empty-cluster [desc & body]
  `(do (flush-nodes test-addresses)
       (testing ~desc
         ~@body)))

(defn to-spec [^InetSocketAddress address]
  {:spec {:host (.getHostAddress (.getAddress address)) :port (.getPort address)}})

(timbre/set-level! :trace)

(deftest cluster-test
  (recreate-cluster)
  (with-empty-cluster "commands during a stable configuration"
    (with-open [client (connect "10.18.10.1" 6379)]
      (is (nil? (call client "get" "foo")))
      (is (= (redis.resp.SimpleString. "OK") (call client "set" "foo" "bar")))
      (is (= "bar" (call client "get" "foo")))))
  (with-empty-cluster "when a key ASK redirection to be found"
    (let [spec          {:spec {:host "10.18.10.1" :port 6379}}
          cluster-nodes (util/parse-cluster-nodes (car/wcar spec (car/cluster-nodes)))
          {:as source-node} (some #(when (= (:redis/address %) (InetSocketAddress. "10.18.10.1" 6379)) %) cluster-nodes)
          {:as destination-node} (some #(when (= (:redis/address %) (InetSocketAddress. "10.18.10.2" 6379)) %) cluster-nodes)]

      (car/wcar spec
        (car/set "foo" "bar"))

      ;; step 1
      (car/wcar (to-spec (:redis/address destination-node))
        (car/cluster-setslot 12182 :importing (:redis/id source-node)))

      (car/wcar (to-spec (:redis/address source-node))
        ;; step 2
        (car/cluster-setslot 12182 :migrating (:redis/id destination-node)))

      ;; step 3 - get keys

      (car/wcar (to-spec (:redis/address source-node))
        (car/migrate "10.18.10.2" 6379 "" 0 5000 :KEYS "foo"))

      (with-open [client (connect "10.18.10.2" 6379)]
        (is (= "bar" (call client "get" "foo"))))

      ;; 4
      ;; (car/wcar (to-spec (:redis/address source-node))
      ;;   (car/cluster-setslot 12182 :NODE (:redis/id destination-node)))

      ;; (car/wcar (to-spec (:redis/address destination-node))
      ;;   (car/cluster-setslot 12182 :NODE (:redis/id destination-node)))
      ))
  )

(deftest invalid-connection-test
  (testing "when a connection is refused"
    (with-open [client (connect "10.18.10.1" 7000)]
      (is (instance? clojure.lang.ExceptionInfo (deref (send-command client ["get" "foo"]) 1000 :timeout))))))

(use-fixtures :each without-debug)
