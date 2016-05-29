(ns redis-protocol.core
  (:require [byte-streams :as bs]
            [redis-protocol.util :as util]
            [clojure.java.io :as io]
            [clojure.tools.trace :as t]
            [taoensso.timbre :as timbre]
            [clojure.core.async :as async])
  (:import (redis.protocol ReplyParser)
           (java.net InetSocketAddress StandardSocketOptions)
           (java.util Arrays ArrayList)
           (java.io Closeable IOException)
           (java.nio ByteBuffer)
           (java.nio.channels AsynchronousSocketChannel CompletionHandler SelectionKey Selector SocketChannel)
           (java.util.concurrent TimeUnit ArrayBlockingQueue BlockingQueue Phaser LinkedTransferQueue ConcurrentLinkedDeque)
           (java.util.concurrent.atomic AtomicLong)))

(timbre/refer-timbre)


;; http://rox-xmlrpc.sourceforge.net/niotut/#The client


;; for now pretend charset encoding is not relevant
(defn encode-str [s]
  (apply str "$" (count s) "\r\n" s "\r\n"))

(defn args->str [args]
  (apply str "*" (count args) "\r\n" (map encode-str args)))

(defn debug-bytes [buf]
  (let [lines (.split (with-out-str (bs/print-bytes buf)) "\n")]
    (doseq [line (take 5 lines)]
      (debug line))
    (if (> (count lines) 5)
      (debug "..."))))

(deftype WriteOperation [^long id ^ByteBuffer payload return]
  Object
  (toString [_]
    (str "WriteOperation<<id: " id ", length: " (.limit payload) ", remaining: " (.remaining payload) ">>")))

(deftype ReadOperation [^long id ^ReplyParser parser return]
  Object
  (toString [_]
    (str "ReadOperation<<id: " id ">>")))

(def ^AtomicLong ^:private op-sequence (AtomicLong.))

(defn write-operation
  ([args ch] (write-operation args (.getAndIncrement op-sequence) ch))
  ([args id ch]
   {:pre [(vector? args)]}
   (->WriteOperation id (ByteBuffer/wrap (.getBytes (args->str args))) ch)))

(defn write->read [^WriteOperation op]
  (->ReadOperation (.id op) (ReplyParser.) (.return op)))

(defrecord RedisClient [seeds selector connections pending-changes]
  Closeable
  (close [this]
    (doseq [[_ connection] @connections]
      (.close connection))
    (.close selector)))

(defrecord DuplexConnection [channel ^InetSocketAddress address read-buffer read-queue write-queue]
  Closeable
  (close [this]
    (debug "closing DuplexConnection:" channel)
    (.close channel))
  (toString [_]
    (str "DuplexConnection<<channel: " channel ">>")))

(defn connection-closed [{:keys [connections] :as client} ^DuplexConnection {:keys [address] :as conn}]
  (.close conn)
  (swap! connections dissoc address))

(def OP_CONNECT (SelectionKey/OP_CONNECT))
(def OP_READ (SelectionKey/OP_READ))
(def OP_WRITE (SelectionKey/OP_WRITE))

(defn send-command
  [{:keys [connections ^Selector selector ^BlockingQueue pending-changes] :as client} args]
  {:pre [(vector? args)]}
  (let [p (promise)
        conn (some val @connections)
        {:keys [^ArrayBlockingQueue write-queue]} conn]
    (.put write-queue (write-operation args p))
    (.put pending-changes [:set-ops conn OP_WRITE])
    (.wakeup selector)
    p))

(defn open-connection [^InetSocketAddress address]
  (let [read-buffer (ByteBuffer/allocateDirect (* 16 1024))
        read-queue  (ConcurrentLinkedDeque.)
        write-queue (ArrayBlockingQueue. 1024)
        channel     (doto (SocketChannel/open)
                      (.configureBlocking false)
                      (.setOption (StandardSocketOptions/TCP_NODELAY) false))]
    ;; disable Nagle's algorithm
    (->DuplexConnection channel address read-buffer read-queue write-queue)))

(defn start-connect [conn]
  (.connect (:channel conn) (:address conn))
  conn)

(defn open [{:keys [^Selector selector connections ^LinkedTransferQueue pending-changes] :as client} ^InetSocketAddress address]
  (let [connection (open-connection address)]
    (start-connect connection)
    (.put pending-changes [:register connection OP_CONNECT])
    (swap! connections assoc address connection)))

(defn incomplete? [^WriteOperation op]
  (.hasRemaining (.payload op)))

(defn complete? [^WriteOperation op]
  (not (incomplete? op)))

(set! *warn-on-reflection* true)

(defn ops->str [x]
  (str "[" (if (bit-test x 0) "R" "-") (if (bit-test x 2) "W" "-") (if (bit-test x 3) "C" "-") "]"))

(defn suggest-op!
  "Sets interested ops."
  ([^SelectionKey k x]
   (.interestOps k (bit-or (.interestOps k) x)))
  ([^SelectionKey k x y] (.interestOps k (bit-or (.interestOps k) x y)))
  ([^SelectionKey k x y z] (.interestOps k (bit-or (.interestOps k) x y z))))

(defn ignore-op!
  "Removes interested ops."
  ([^SelectionKey k x]
   (let [current-ops (.interestOps k)
         new-ops     (bit-and current-ops (bit-not x))]
     (debug "ops:" (ops->str current-ops) "->" (ops->str new-ops))
     (.interestOps k new-ops))))

(defn read! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^ConcurrentLinkedDeque read-queue ^ByteBuffer read-buffer] :as conn} (.attachment k)]
    (.clear read-buffer)
    (loop [n (.read channel read-buffer)]
      (debug "read:" n "bytes")
      (cond
        (= -1 n)  (do (warn "connection closed")
                      (connection-closed client conn))
        (zero? n) (debug "no bytes read")
        :else     (let [_ (.flip read-buffer)
                        ba (byte-array n)
                        _ (.get read-buffer ba)]
                    (loop [^ReadOperation op (.getFirst read-queue)
                           buffer            ba]
                      (let [^ReplyParser parser (.parser op)
                            outcome             (.parse parser buffer)]
                        (condp = outcome
                          ReplyParser/PARSE_INCOMPLETE (do (debug "waiting for more bytes"))
                          ReplyParser/PARSE_COMPLETE   (do (debug "parse is complete")
                                                           (.removeFirst read-queue)
                                                           (debug (.root parser))
                                                           (deliver (.return op) (.root parser)))
                          ReplyParser/PARSE_OVERFLOW   (do (debug "overflow of response")
                                                           (.removeFirst read-queue)
                                                           (debug (.root parser))
                                                           (deliver (.return op) (.root parser))
                                                           (recur (.getFirst read-queue) (.getOverflow parser)))))))))))

(defn write! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^ArrayBlockingQueue write-queue] :as conn} (.attachment k)
        ops (ArrayList.)
        _ (.drainTo write-queue ops)]
    (doseq [^WriteOperation op ops]
      (loop []
        (let [n (.write channel ^ByteBuffer (.payload op))]
          (cond
            (= -1 n)       (do (warn "connection closed"))
            (complete? op) (do (debug "done writing op")
                               (ignore-op! k OP_WRITE)
                               (suggest-op! k OP_READ)
                               (.addLast read-queue (write->read op)))
            :else (recur)))))))

(defn connect! [client ^SelectionKey k]
  (debug "connect!" k)
  (let [^SocketChannel channel (.channel k)
        {:keys [^InetSocketAddress address read-queue write-queue]} (.attachment k)]
    (try
      (.finishConnect channel)
      (suggest-op! k OP_READ)
      (ignore-op! k OP_CONNECT)

      (catch IOException ex
        (error ex "IOException when finishing connect")
        ;; remove channel in a Cluster configuration
        ;; raise loss of connection
        ))))

(defn drain! [^BlockingQueue q ^long max]
  (let [a (ArrayList.)]
    (.drainTo q ^Collection a max)
    a))

(defn io-loop [{:keys [^Selector selector ^BlockingQueue pending-changes] :as client}]
  (loop [step 0]
    (debug "loop:" step)
    (if (and (< step 100) (.isOpen selector))
      (do (doseq [[action ^SocketChannel connection interested-ops] (drain! pending-changes 1024)
                  :let [^SocketChannel channel (:channel connection)]]
            (case action
              :register  (do (debug "registering new channel" (str connection))
                             (.register channel selector interested-ops connection))
              :set-ops   (let [selection-key (.keyFor channel selector)
                               current-ops   (.interestOps selection-key)
                               new-ops       (bit-or current-ops interested-ops)]
                           (debug "ops:" (ops->str current-ops) "->" (ops->str new-ops) "on" (:channel connection))
                           (.interestOps selection-key new-ops))
              :unset-ops (let [selection-key (.keyFor channel selector)
                               current-ops   (.interestOps selection-key)
                               new-ops       (bit-xor current-ops interested-ops)]
                           (debug "ops:" (ops->str current-ops) "->" (ops->str new-ops) "on" (:channel connection))
                           (.interestOps selection-key new-ops))
              (warn "unknown action:" action)))

          (if (> (.select selector 1000) 0)
            (let [selected-keys (.selectedKeys selector)
                  itr           (.iterator selected-keys)]
              (while (.hasNext itr)
                (let [^SelectionKey selected-key (.next itr)
                      _ (.remove itr)]
                  (cond
                    (not (.isValid selected-key)) (debug "key is not valid:" selected-key)
                    (.isReadable selected-key)    (read! client selected-key)
                    (.isWritable selected-key)    (write! client selected-key)
                    (.isConnectable selected-key) (connect! client selected-key))))
              (recur (inc step)))
            (do (recur (inc step)))))
      (warn "Selector is closed"))))

(defn connect [host ^long port]
  (let [selector   (Selector/open)
        seeds      #{(InetSocketAddress. (str host) port)}
        client     (->RedisClient seeds selector (atom {}) (LinkedTransferQueue.))
        _          (open client (first seeds))
        dispatcher (fn []
                     (try
                       (io-loop client)
                       (catch Exception err
                         (error err))))]
    (assoc client :client-thread (doto (Thread. dispatcher (str "RedisClientThread: " host ":" port)) (.start)))))

(comment
  (with-open [client (connect "172.17.0.2" 7000)]
    (let [returns (doseq [_ (range 1)]
                    (send-command client ["set" "bar" "1"]))]
      (Thread/sleep 100000)))

  (with-open [client (connect "127.0.0.1" 6379)]
    (let [r1 (send-command client ["get"])
          r2 (send-command client ["get"])]
      (println @r1)
      (println @r2)))
  )
