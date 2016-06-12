(ns redis-protocol.core
  (:require [byte-streams :as bs]
            [redis-protocol.util :as util]
            [clojure.java.io :as io]
            [clojure.tools.trace :as t]
            [taoensso.timbre :as timbre]
            [clojure.core.async :as async]
            [clojure.string :as str])
  (:import (redis.resp ReplyParser)
           (java.net InetSocketAddress StandardSocketOptions)
           (java.util Arrays ArrayList)
           (java.io Closeable IOException)
           (java.nio ByteBuffer)
           (java.nio.channels AsynchronousSocketChannel CompletionHandler SelectionKey Selector SocketChannel CancelledKeyException)
           (java.util.concurrent TimeUnit ArrayBlockingQueue BlockingQueue Phaser LinkedTransferQueue ConcurrentLinkedDeque Executors ScheduledThreadPoolExecutor Future)
           (java.util.concurrent.atomic AtomicLong)))



;; (set! *warn-on-reflection* true)
;; (set! *unchecked-math* :warn-on-boxed)

(timbre/refer-timbre)

(timbre/set-level! :trace)

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

(defn moved? [reply]
  (and (instance? redis.resp.Error reply)
       (.startsWith (.message ^redis.resp.Error reply) "MOVED")))

(defn moved-to [reply]
  (when (moved? reply)
    (let [[_ slot address-port] (str/split (.message ^redis.resp.Error reply) #" " 3)
          [^String address port] (str/split address-port #":" 2)]
      [(Long/parseLong slot) (InetSocketAddress. address (Integer/parseInt port))])))

(defprotocol Operation
  (return-value [op val]))

(deftype WriteOperation [^long id ^long slot ^long redirects ^bytes request ^ByteBuffer payload prom ^Future fut]
  Object
  (toString [_]
    (str "WriteOperation<<id: " id ", length: " (.limit payload) ", remaining: " (.remaining payload) ">>"))
  Operation
  (return-value [_ val]
    (.cancel ^Future fut true)
    (deliver prom val)))

(deftype ReadOperation [^long id ^long redirects ^ReplyParser parser prom ^Future fut ^bytes request]
  Object
  (toString [_]
    (str "ReadOperation<<id: " id ", redirects: " redirects ">>"))
  Operation
  (return-value [_ val]
    (.cancel ^Future fut true)
    (when-not (instance? Throwable val)
      (doseq [line (.split ^String (util/cli-format val) "\n")]
        (trace line)))
    (deliver prom val)))

(def ^AtomicLong ^:private op-sequence (AtomicLong.))

(defn write-operation
  ([args prom fut] (write-operation args (.getAndIncrement op-sequence) prom fut))
  ([args id prom fut]
   {:pre [(vector? args)]}
   (let [slot  (if-let [k (args->key args)]
                 (hash-slot (.getBytes k))
                 -1)]
     (let [request (.getBytes (args->str args))]
       (->WriteOperation id slot 0 request (ByteBuffer/wrap request) prom fut)))))

(defn redispatch-write [^ReadOperation op slot]
  (->WriteOperation (.id op) slot (inc (.redirects op)) (.request op) (ByteBuffer/wrap (.request op)) (.prom op) (.fut op)))

(defn write->read [^WriteOperation op]
  (->ReadOperation (.id op) (.redirects op) (ReplyParser.) (.prom op) (.fut op) (.request op)))

(defrecord RedisClient [seeds ^Selector selector connections slot-cache ^ConcurrentLinkedDeque dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor]
  Closeable
  (close [this]
    (.addLast dispatch-queue [:shutdown])
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

(defn parsed-value [^ReplyParser parser]
  (.get (.root parser) 0))

(defn dispatch-value [{:keys [^ConcurrentLinkedDeque dispatch-queue] :as client} ^ReadOperation op reply]
  (if-let [[slot address] (moved-to reply)]
    (do (debug "-> Redirected to slot" (str "[" slot "]") "located at" address)
        (.addFirst dispatch-queue [:resolve (redispatch-write op slot) address])
        ;;(return-value op reply)
        )
    (return-value op reply)))

(defn read! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^ConcurrentLinkedDeque read-queue ^ByteBuffer read-buffer] :as conn} (.attachment k)]
    (loop [n (.read channel (doto read-buffer
                              (.clear)))]
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
                                                           (dispatch-value client op (parsed-value parser)))
                          ReplyParser/PARSE_OVERFLOW   (do (debug "overflow of response")
                                                           (.removeFirst read-queue)
                                                           (dispatch-value client op (parsed-value parser))
                                                           (recur (.getFirst read-queue) (.getOverflow parser)))))))))))

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

(defn- register! [{:keys [^SocketChannel channel ^InetSocketAddress address] :as conn} ^Selector selector]
  (if (.connect channel address)
    (do (debug address "can be established immediately, registering" channel "with" (ops->str OP_READ))
        (.register channel selector OP_READ conn))
    (do (debug address "cannot be established immediately, registering" channel "with" (ops->str OP_CONNECT))
        (.register channel selector OP_CONNECT conn))))

(defn resolve-connection [{:keys [seeds ^Selector selector connections] :as client} address]
  (or (get @connections address)
      (let [conn (socket-connection address)]
        (register! conn selector)
        (swap! connections assoc address conn)
        conn)))

(defmethod dispatch :resolve [connections {:keys [seeds ^Selector selector ^ConcurrentLinkedDeque dispatch-queue slot-cache] :as client} [_ ^WriteOperation op addr]]
  (let [conns            @connections
        resolved-conn    (if-let [address (or addr (get @slot-cache (.slot op)))]
                           (resolve-connection client address)
                           (or (rand-nth (vals conns))
                               (resolve-connection client (rand-seed client))))]
    (.addFirst dispatch-queue [:write op resolved-conn])))

(defmethod dispatch :write [connections client [_ ^WriteOperation op conn]]
  (let [{:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^ArrayBlockingQueue write-queue]} conn]
    (cond (.isConnectionPending channel)
          (.add write-queue op)

          (.isConnected channel)
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
                      (.addFirst write-queue op))))))

          :else
          (throw (ex-info "Connection is not pending or connected!" {:op op})))))

(defn nio-loop
  "Responsible for managing IO operations of dispatched operations."
  [{:keys [^Selector selector ^ConcurrentLinkedDeque dispatch-queue connections] :as client}]
  (try
    (loop []
      (if (.isOpen selector)
        (do (loop [n 0]
              (when-let [event (.pollFirst dispatch-queue)]
                (debug "dispatching:" event)
                (try
                  (dispatch connections client event)
                  (catch Throwable err
                    (if (= err shutdown-exception)
                      (throw err)
                      (error err "caught throwable when dispatching event"))))
                (recur (inc n))))

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
  [{:keys [connections ^Selector selector ^ConcurrentLinkedDeque dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor] :as client} args]
  {:pre [(vector? args)]}
  (let [p           (promise)
        timeout     (ex-info "Operation timed out" {:timeout? true :args args})
        fut         (.schedule scheduled-executor ^Runnable (fn [] (deliver p timeout)) 2000 (TimeUnit/MILLISECONDS))
        write-queue (write-operation args p fut)]
    (.addLast dispatch-queue [:resolve (write-operation args p fut)])
    p))

(defn connect [host ^long port]
  (let [selector (Selector/open)
        seeds    #{(InetSocketAddress. (str host) port)}
        executor (doto (ScheduledThreadPoolExecutor. 1) ;; used for timeouts, completing & rerouting operations
                   (.setRemoveOnCancelPolicy true))
        client   (->RedisClient seeds selector (atom {}) (atom {}) (ConcurrentLinkedDeque.) executor)
        io-fn    (fn []
                   (try
                     (nio-loop client)
                     (catch Exception err
                       (error err))))]
    (assoc client :io-thread (doto (Thread. io-fn (str "RedisIO")) (.start)))))

(comment
  (with-open [client (connect "172.17.0.2" 7000)]
    (let [returns (doseq [_ (range 1)]
                    (send-command client ["set" "bar" "1"]))]
      (Thread/sleep 100000)))

  (with-open [client (connect "10.18.10.2" 6379)]
    (let [r1 (send-command client ["get" "foo"])
          r2 (send-command client ["get" "bar"])]
      [@r1 @r2]))

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client ["cluster" "slots"]))

  (with-open [client (connect "10.18.10.2" 7000)]
    @(send-command client ["cluster" "slots"]))

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client ["info"])))
