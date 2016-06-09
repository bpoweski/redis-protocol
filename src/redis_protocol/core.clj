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
           (java.nio.channels AsynchronousSocketChannel CompletionHandler SelectionKey Selector SocketChannel CancelledKeyException)
           (java.util.concurrent TimeUnit ArrayBlockingQueue BlockingQueue Phaser LinkedTransferQueue ConcurrentLinkedDeque ScheduledThreadPoolExecutor Future)
           (java.util.concurrent.atomic AtomicLong)))



;; (set! *warn-on-reflection* true)
;; (set! *unchecked-math* :warn-on-boxed)

(timbre/refer-timbre)

(def cluster-hash-slots 16384)



(defn drain
  "Drains items from q into a new collection."
  ([^BlockingQueue q]
   (let [a (ArrayList.)]
     (.drainTo q ^Collection a)
     a))
  ([^BlockingQueue q ^long max]
   (let [a (ArrayList.)]
     (.drainTo q ^Collection a max)
     a)))

;; http://rox-xmlrpc.sourceforge.net/niotut/#The client


;; for now pretend charset encoding is not relevant
(defn encode-str [s]
  (apply str "$" (count s) "\r\n" s "\r\n"))

(defn ^String args->str [args]
  (apply str "*" (count args) "\r\n" (map encode-str args)))

;; naive implementation
(defn args->key [[command & rest]]
  (first rest))

(defn hash-slot
  "Calculates a Redis hash slot.

  A port of the C example in http://redis.io/topics/cluster-spec"
  [^bytes ba]
  (mod (let [len (alength ba)]
         (if (zero? len)
           (util/crc16 ba)
           (let [left-brace  (long \{)
                 right-brace (long \})
                 len         (alength ba)
                 s           (loop [x 0]
                               (when (< x len)
                                 (if (= (aget ba x) left-brace)
                                   x
                                   (recur (inc x)))))]
             (if (nil? s)
               (util/crc16 ba)
               (if-let [e (loop [x (inc s)]
                            (when (< x len)
                              (if (= (aget ba x) right-brace)
                                x
                                (recur (inc x)))))]
                 (util/crc16 ba (inc s) (- e s 1))
                 (util/crc16 ba))))))
       cluster-hash-slots))

(defn debug-bytes [buf]
  (let [lines (.split ^String (with-out-str (bs/print-bytes buf)) "\n")]
    (doseq [line (take 5 lines)]
      (debug line))
    (if (> (count lines) 5)
      (debug "..."))))

(defprotocol Operation
  (return-value [op val]))

(deftype WriteOperation [^long id ^long slot ^ByteBuffer payload prom ^Future fut]
  Object
  (toString [_]
    (str "WriteOperation<<id: " id ", length: " (.limit payload) ", remaining: " (.remaining payload) ">>"))
  Operation
  (return-value [_ val]
    (.cancel ^Future fut true)
    (deliver prom val)))

(deftype ReadOperation [^long id ^ReplyParser parser prom ^Future fut]
  Object
  (toString [_]
    (str "ReadOperation<<id: " id ">>"))
  Operation
  (return-value [_ val]
    (.cancel ^Future fut true)
    (deliver prom val)))

(def ^AtomicLong ^:private op-sequence (AtomicLong.))

(defn write-operation
  ([args prom fut] (write-operation args (.getAndIncrement op-sequence) prom fut))
  ([args id prom fut]
   {:pre [(vector? args)]}
   (let [slot  (if-let [k (args->key args)]
                 (hash-slot (.getBytes k))
                 -1)]
     (->WriteOperation id slot (ByteBuffer/wrap (.getBytes (args->str args))) prom fut))))

(defn write->read [^WriteOperation op]
  (->ReadOperation (.id op) (ReplyParser.) (.prom op) (.fut op)))

(defrecord RedisClient [seeds ^Selector selector connections slot-cache dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor]
  Closeable
  (close [this]
    (.put dispatch-queue [:shutdown])
    (.shutdown scheduled-executor)))

(defrecord SocketConnection [^SocketChannel channel ^InetSocketAddress address read-buffer read-queue write-queue]
  Closeable
  (close [this]
    (debug "closing SocketConnection:" channel)
    (.close channel))
  (toString [_]
    (str "SocketConnection<<channel: " channel ">>")))

(defn cleanup-connection [{:keys [connections] :as client} ^SocketConnection {:keys [address] :as conn}]
  (.close ^Closeable conn)
  (swap! connections dissoc address))

(defn fail-connection [client {:keys [^ArrayBlockingQueue write-queue] :as conn} ex]
  (cleanup-connection client conn)
  (doseq [op (drain write-queue)]
    (return-value op ex)))

(def OP_CONNECT (SelectionKey/OP_CONNECT))
(def OP_READ (SelectionKey/OP_READ))
(def OP_WRITE (SelectionKey/OP_WRITE))

(defn socket-connection [^InetSocketAddress address]
  (let [read-buffer (ByteBuffer/allocateDirect (* 16 1024))
        read-queue  (ConcurrentLinkedDeque.)
        write-queue (ArrayBlockingQueue. 1024)
        channel     (doto (SocketChannel/open)
                      (.configureBlocking false)
                      (.setOption (StandardSocketOptions/TCP_NODELAY) false))]
    ;; disable Nagle's algorithm
    (->SocketConnection channel address read-buffer read-queue write-queue)))

(defn open [{:keys [^LinkedTransferQueue dispatch-queue] :as client} ^InetSocketAddress address]
  (.put dispatch-queue [:connect address])
  client)

(defn incomplete? [^WriteOperation op]
  (.hasRemaining ^ByteBuffer (.payload op)))

(defn complete? [^WriteOperation op]
  (not (incomplete? op)))

(defn ops->str [x]
  (str "[" (if (bit-test x 0) "R" "-") (if (bit-test x 2) "W" "-") (if (bit-test x 3) "C" "-") "]"))

(defn set-op!
  "Sets interested ops."
  [^SelectionKey k x]
  (let [current-ops (.interestOps k)
        new-ops     (bit-or current-ops x)]
    (when (not= current-ops new-ops)
      (debug "adding op" (ops->str current-ops) "->" (ops->str new-ops))
      (.interestOps k new-ops))))

(defn ignore-op!
  "Removes interested ops."
  ([^SelectionKey k x]
   (let [current-ops (.interestOps k)
         new-ops     (bit-and current-ops (bit-not x))]
     (when (not= current-ops new-ops)
       (debug "ignoring op" (ops->str current-ops) "->" (ops->str new-ops)))
     (.interestOps k new-ops))))

(defn read! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^ConcurrentLinkedDeque read-queue ^ByteBuffer read-buffer] :as conn} (.attachment k)]
    (.clear read-buffer)
    (loop [n (.read channel read-buffer)]
      (debug "read:" n "bytes")
      (cond
        (= -1 n)  (do (warn "connection closed")
                      (cleanup-connection client conn))
        (zero? n) (debug "no bytes read")
        :else     (let [_ (.flip read-buffer)
                        ba (byte-array n)
                        _ (.get read-buffer ba)
                        _ (debug-bytes ba)]
                    (loop [^ReadOperation op (.getFirst read-queue)
                           buffer            ba]
                      (let [^ReplyParser parser (.parser op)
                            outcome             (.parse parser buffer)]
                        (debug "op:" op)
                        (condp = outcome
                          ReplyParser/PARSE_INCOMPLETE (do (debug "waiting for more bytes"))
                          ReplyParser/PARSE_COMPLETE   (do (debug "parse is complete")
                                                           (.removeFirst read-queue)
                                                           (debug (.root parser))
                                                           (return-value op (.get (t/trace :root (.root parser)) 0)))
                          ReplyParser/PARSE_OVERFLOW   (do (debug "overflow of response")
                                                           (.removeFirst read-queue)
                                                           (debug (.root parser))
                                                           (return-value op (.get (.root parser) 0))
                                                           (recur (.getFirst read-queue) (.getOverflow parser)))))))))))

(defn maybe-write
  "Attempts to write the entire op to the connection. If the entire op is unable to be written registers OP_WRITE and re-schedules."
  [client {:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^ArrayBlockingQueue write-queue] :as conn} ^WriteOperation op]
  (if (.isConnectionPending channel)
    (.add write-queue op)
    (let [n (.write channel ^ByteBuffer (.payload op))]
      (if (= -1 n)
        (do (warn "connection closed")
            (cleanup-connection client conn)
            (return-value op (ex-info "Connection closed when writing" {:op op})))
        (let [k (.keyFor channel (.selector client))]
          (if (complete? op)
            (do (debug "done writing op")
                (ignore-op! k OP_WRITE)
                (set-op! k OP_READ)
                (.addLast read-queue (write->read op)))
            (do (debug "op is incomplete")
                (set-op! k OP_WRITE)
                (.addFirst write-queue op))))))))

(defn write! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^ArrayBlockingQueue write-queue] :as conn} (.attachment k)
        ops (drain write-queue)]
    (doseq [^WriteOperation op ops]
      (let [n (.write channel ^ByteBuffer (.payload op))]
        (cond
          (= -1 n)       (do (warn "connection closed")
                             (cleanup-connection client conn))
          (complete? op) (do (debug "done writing op")
                             (ignore-op! k OP_WRITE)
                             (set-op! k OP_READ)
                             (.addLast read-queue (write->read op))))))))

(defn connect! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^InetSocketAddress address read-queue write-queue] :as conn} (.attachment k)]
    (try
      (debug "finishing connect on" channel)
      (let [connect-finished? (.finishConnect channel)]
        (when connect-finished?
          (set-op! k OP_READ)
          (ignore-op! k OP_CONNECT)
          (debug "connected, writing queued ops")
          (write! client k)))
      (catch java.net.ConnectException ex
        (warn ex "error connecting to" address)
        (fail-connection client conn (ex-info "error connecting" {} ex))
        (.cancel k))
      (catch IOException ex
        (error ex "IOException when finishing connect")
        (fail-connection client conn ex)
        (.cancel k)))))

(defmulti dispatch
  (fn [_ _ event]
    (first event)))

(defmethod dispatch :default [_ _ [action & event]]
  (warn "unknown action:" action "event:" event))

(defn rand-seed [client]
  (-> client
      :seeds
      seq
      rand-nth))

(def shutdown-exception (ex-info "Shutting down NIO loop" {:shutdown? true}))

(defmethod dispatch :shutdown [connections {:keys [seeds ^Selector selector] :as client} _]
  (throw shutdown-exception))

(defmethod dispatch :write [connections {:keys [seeds ^Selector selector] :as client} [_ ^WriteOperation op]]
  (if-let [conns (not-empty @connections)]
    (let [{:keys [^SocketChannel channel] :as conn} (some val conns)]
      (maybe-write client conn op))
    (let [address (rand-seed client)
          _       (debug "no active connections exist, randomly selecting" address)
          {:keys [^SocketChannel channel] :as conn} (socket-connection address)
          _       (swap! connections assoc address conn)]
      (if (.connect channel address)
        (do (debug address "can be established immediately, registering" channel "with" (ops->str OP_READ))
            (.register channel selector OP_READ conn)
            (maybe-write client conn op))
        (do (debug address "cannot be established immediately, registering" channel "with" (ops->str OP_CONNECT))
            (.register channel selector OP_CONNECT conn)
            (maybe-write client conn op))))))

(defn nio-loop
  "Responsible for managing IO operations of dispatched operations."
  [{:keys [^Selector selector ^BlockingQueue dispatch-queue connections] :as client}]
  (try
    (loop []
      (if (.isOpen selector)
        (do (doseq [event (drain dispatch-queue 1024)]
              (debug "dispatching:" event)
              (try
                (dispatch connections client event)
                (catch Throwable err
                  (if (= err shutdown-exception)
                    (throw err)
                    (error err "caught throwable when dispatching event")))))

            (let [n (.select selector 1)]
              (if (> n 0)
                (let [selected-keys (.selectedKeys selector)
                      itr           (.iterator selected-keys)]
                  (debug "Selector/select yielded" n "key(s)")
                  (while (.hasNext itr)
                    (let [^SelectionKey selected-key (.next itr)]
                      (.remove itr)
                      (if (not (.isValid selected-key))
                        (debug "key is not valid:" selected-key)
                        (do
                          (when (.isReadable selected-key)
                            (read! client selected-key))
                          (when (.isWritable selected-key)
                            (write! client selected-key))
                          (when (.isConnectable selected-key)
                            (connect! client selected-key))))))
                  (recur))
                (recur))))
        (warn "Selector is closed")))
    (catch Throwable ex
      (when (not= shutdown-exception ex)
        (error ex "Throwable caught in nio loop, exiting.")))
    (finally
      (doseq [[_ ^Closeable connection] @connections]
        (.close connection))
      (.close selector))))

(defn send-command
  [{:keys [connections ^Selector selector ^BlockingQueue dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor] :as client} args]
  {:pre [(vector? args)]}
  (let [p           (promise)
        fut         (.schedule scheduled-executor ^Runnable (fn [] (deliver p (ex-data {:timeout? true}))) 2000 (TimeUnit/MILLISECONDS))
        write-queue (write-operation args p fut)]
    (.put dispatch-queue [:write (write-operation args p fut)])
    p))

(defn connect [host ^long port]
  (let [selector (Selector/open)
        seeds    #{(InetSocketAddress. (str host) port)}
        executor (doto (ScheduledThreadPoolExecutor. 2) ;; used to handle timeouts
                   (.setRemoveOnCancelPolicy true))
        client   (->RedisClient seeds selector (atom {}) (atom {}) (LinkedTransferQueue.) executor)
        io-fn    (fn []
                   (try
                     (nio-loop client)
                     (catch Exception err
                       (error err))))]
    (assoc client :io-thread (doto (Thread. io-fn (str "RedisIOThread")) (.start)))))

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

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client ["cluster" "slots"]))

  (with-open [client (connect "10.18.10.2" 7000)]
    @(send-command client ["cluster" "slots"]))

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client ["info"]))
  )
