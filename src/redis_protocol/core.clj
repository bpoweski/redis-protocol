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

(defrecord RedisClient [seeds selector connections]
  Closeable
  (close [this]
    (doseq [[_ connection] @connections]
      (.close connection))
    (.close selector)))

(defn shutdown! [^SocketChannel channel]
  (warn "shutting down input/output")
  (doto channel
    (.shutdownInput)
    (.shutdownOutput)))

(defrecord DuplexConnection [channel read-buffer read-queue write-queue]
  Closeable
  (close [this]
    (debug "closing DuplexConnection:" channel)
    (.close channel)))

(defn send-command
  [{:keys [connections ^Selector selector] :as client} args]
  {:pre [(vector? args)]}
  (let [p (promise)
        _ (.wakeUp selector)
        conn (t/trace :conn (some val @connections))
        {:keys [^ArrayBlockingQueue write-queue]} conn]
    (.put write-queue (write-operation args p))
    p))

(defn open-connection [^InetSocketAddress address]
  (let [read-buffer (ByteBuffer/allocateDirect (* 16 1024))
        read-queue  (ConcurrentLinkedDeque.)
        write-queue (ArrayBlockingQueue. 1024)]
    (->DuplexConnection (SocketChannel/open) read-buffer read-queue write-queue)))

(defn open [{:keys [^Selector selector connections] :as client} ^InetSocketAddress address]
  (let [connection (open-connection address)]
    (doto (:channel connection)
      (.configureBlocking false)
      (.register selector OP_CONNECT connection)
      (.connect address))
    (swap! connections assoc address connection)))

(defn incomplete? [^WriteOperation op]
  (.hasRemaining (.payload op)))

(defn complete? [^WriteOperation op]
  (not (incomplete? op)))

(set! *warn-on-reflection* true)

(def OP_CONNECT (SelectionKey/OP_CONNECT))
(def OP_READ (SelectionKey/OP_READ))
(def OP_WRITE (SelectionKey/OP_WRITE))

(defn suggest-op!
  "Sets interested ops."
  ([^SelectionKey k x] (.interestOps k (bit-or (.interestOps k)  x)))
  ([^SelectionKey k x y] (.interestOps k (bit-or (.interestOps k) x y)))
  ([^SelectionKey k x y z] (.interestOps k (bit-or (.interestOps k) x y z))))

(defn ignore-op!
  "Removes interested ops."
  ([^SelectionKey k x] (.interestOps k (bit-xor (.interestOps k) x)))
  ([^SelectionKey k x y] (.interestOps k (bit-xor (.interestOps k) x y)))
  ([^SelectionKey k x y z] (.interestOps k (bit-xor (.interestOps k) x y z))))

(defn reconnect! [^SocketChannel channel]
  (warn "attempting to reconnect")
  (shutdown! channel))

(defn read! [client ^SelectionKey k]
  (debug "read!" k)
  (let [^SocketChannel channel (.channel k)
        {:keys [^ConcurrentLinkedDeque read-queue ^ByteBuffer read-buffer]} (.attachment k)]
    (loop [n (.read channel read-buffer)]
      (cond
        (= -1 n)  (do (warn "connection closed")
                      (reconnect! channel))
        (zero? n) (debug "no bytes read")
        :else     (let [_ (.flip read-buffer)
                        ba (byte-array n)
                        _ (.get read-buffer ba)]
                    (loop [^ReadOperation op (.getFirst read-queue)
                           buffer            ba]
                      (let [^ReplyParser parser (.parser op)
                            outcome             (.parse parser buffer)]
                        (condp = outcome
                          ReplyParser/PARSE_INCOMPLETE (debug "waiting for more bytes")
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
        {:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^ArrayBlockingQueue write-queue]} (.attachment k)
        ops (ArrayList.)
        _ (.drainTo write-queue ops)]
    (doseq [^WriteOperation op ops]
      (loop []
        (let [n (.write channel ^ByteBuffer (.payload op))]
          (cond
            (= -1 n)       (do (warn "connection closed")
                               (reconnect! channel))
            (complete? op) (do (debug "done writing op")
                               (ignore-op! k OP_WRITE)
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

(defn dispatch! [{:keys [^Selector selector] :as client}]
  (loop [step 0]
    (if (and (< step 10000) (.isOpen selector))
      (if (> (.select selector 1000) 0)
        (let [selected-keys (.selectedKeys selector)
              itr           (.iterator selected-keys)]

          (while (.hasNext itr)
            (let [^SelectionKey selected-key (.next itr)
                  _ (.remove itr)]
              (cond
                (.isReadable selected-key)    (read! client selected-key)
                (.isWritable selected-key)    (write! client selected-key)
                (.isConnectable selected-key) (connect! client selected-key))))

          (recur (inc step)))
        (do (recur (inc step))))
      (warn "Selector is closed"))))

(defn connect [host ^long port]
  (let [selector   (Selector/open)
        seeds      #{(InetSocketAddress. (str host) port)}
        client     (->RedisClient seeds selector (atom {}))
        _          (open client (first seeds))
        dispatcher (fn []
                     (try
                       (dispatch! client)
                       (catch Exception err
                         (error err))))]
    (assoc client :client-thread (doto (Thread. dispatcher (str "RedisClientThread: " host ":" port))
                                   (.start)))))
(comment
  (with-open [client (connect "127.0.0.1" 6379)]
    (let [returns (for [_ (range 2)]
                    (send-command client ["set" "bar" "1"]))]
      (doseq [ret returns]
        (debug "response:")
        (debug @ret))
      (Thread/sleep 10000)))

  (with-open [client (connect "127.0.0.1" 6379)]
    @(send-command client ["get"]))

  )
