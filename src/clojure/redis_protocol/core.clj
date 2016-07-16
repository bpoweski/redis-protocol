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
           (java.util.concurrent TimeUnit LinkedBlockingDeque BlockingQueue LinkedTransferQueue ConcurrentLinkedDeque Executors ScheduledThreadPoolExecutor Future)
           (java.util.concurrent.atomic AtomicLong)))


(def OP_CONNECT (SelectionKey/OP_CONNECT))
(def OP_READ (SelectionKey/OP_READ))
(def OP_WRITE (SelectionKey/OP_WRITE))

(set! *warn-on-reflection* true)
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

(defn arg-length [^bytes ba]
  (+ 1 (count (str (alength ba))) 2 (alength ba) 2))

(defn ^"[B" resp-prefix [ch x]
  (util/ascii-bytes (str ch x "\r\n")))

(defn args->bytes
  "Encodes a vector of values into the corresponding length encoded Redis format."
  [args]
  {:pre [(vector? args)]}
  (let [prefix       (resp-prefix \* (count args))
        byte-arrays  (map util/to-bytes args)
        total-bytes  (apply + (alength prefix) (map arg-length byte-arrays))
        result       (byte-array total-bytes prefix)]
    (loop [[^bytes ba & more] byte-arrays
           pos         (alength prefix)]
      (let [ba-prefix (resp-prefix \$ (alength ba))]
        (System/arraycopy ba-prefix 0 result pos (alength ba-prefix))
        (System/arraycopy ba 0 result (+ pos (alength ba-prefix)) (alength ba))
        (aset-byte result (+ pos (alength ba-prefix) (alength ba)) \return)
        (aset-byte result (+ pos (alength ba-prefix) (alength ba) 1) \newline)
        (when (seq more)
          (recur more (+ pos (alength ba-prefix) (alength ba) 2)))))
    result))

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

(defn routable-slot
  "Returns the key(s) using command definitions returned by COMMAND."
  [[command & _ :as args] command-defs]
  (when-let [{[start stop] :key-pos :keys [flags step]} (get command-defs command (get command-defs (keyword (str/lower-case (name command)))))]
    (when-not (or (= 0 step) (:movablekeys flags) (:asking flags))
      (let [[slot & more] (->> args
                               (drop start)
                               (drop-last (- -1 stop))
                               (take-nth step)
                               (map #(some-> % util/to-bytes hash-slot)))]
        (when (every? (partial = slot) more)
          slot)))))

(defn trace-bytes [buf msg]
  (trace (let [^String byte-str (with-out-str (bs/print-bytes buf))
               lines (.split byte-str "\n")
               log-msg (str/join "\n" (conj (map str (repeat "    ") (take 10 lines)) msg))]
           log-msg)))

(defn error? [reply]
  (instance? redis.resp.Error reply))

(defn error-prefix? [^redis.resp.Error error prefix]
  (.startsWith (.message error) prefix))

(defn moved? [reply]
  (and (error? reply)
       (error-prefix? reply "MOVED")))

(defn ask? [reply]
  (and (error? reply)
       (error-prefix? reply "ASK")))

(defn reroute? [reply]
  (or (moved? reply)
      (ask? reply)))

(defn rerouted-to
  [reply]
  {:pre [(or (moved? reply) (ask? reply))]}
  (let [[_ slot address-port] (str/split (.message ^redis.resp.Error reply) #" " 3)
        [^String address port] (str/split address-port #":" 2)]
    [(Long/parseLong slot) (InetSocketAddress. address (Integer/parseInt port))]))

(def byte-array-class (Class/forName "[B"))

(defn op? [{:keys [write-buffer request timeout-future action] :as op}]
  (and (instance? ByteBuffer write-buffer)
       (instance? byte-array-class request)
       (instance? Future timeout-future)
       (instance? clojure.lang.IFn action)))

(defn parse [{:keys [^ReplyParser parser] :as op} ^bytes buffer]
  {:pre [(op? op)]}
  (let [outcome (.parse parser buffer)]
    [outcome
     (when (= ReplyParser/PARSE_OVERFLOW outcome)
       (.getOverflow parser))]))

(defn flip [{:keys [asking?] :as op}]
  {:pre [(op? op)]}
  (assoc op :parser (if asking?
                      (ReplyParser. 2)
                      (ReplyParser. 1))))

(defn redirect [{:keys [^bytes request] :as op} x]
  {:pre [(op? op) (integer? x)]}
  (-> op
      (update :redirects inc)
      (assoc :write-buffer (ByteBuffer/wrap request) :slot x)))

(def ^"[B" asking-bytes (args->bytes [:ASKING]))

(defn prefix-asking [^bytes request]
  (doto (ByteBuffer/allocate (+ (alength asking-bytes) (alength request)))
    (.put asking-bytes)
    (.put request)
    (.flip)))

(defn ask [{:keys [request redirects] :as op} slot]
  {:pre [(op? op) (integer? slot)]}
  (assoc op :write-buffer (prefix-asking request) :asking? true :redirects (inc redirects) :slot slot))

(defn finish [{:keys [action ^Future timeout-future] :as op} result]
  {:pre [(op? op)]}
  (.cancel timeout-future true)
  (action result))

(def max-redirects 20)

(defn complete [{:keys [action ^Future timeout-future ^ReplyParser parser asking? redirects] :as op} {:keys [^ConcurrentLinkedDeque dispatch-queue] :as client}]
  (let [reply (.get (.root parser) (if asking? 1 0))]
    (if (and (reroute? reply) (< redirects max-redirects))
      (let [[slot address] (rerouted-to reply)]
        (if (moved? reply)
          (do (debug "-> Redirected to slot" (str "[" slot "]") "located at" address)
              (.addFirst dispatch-queue [:resolve (redirect op slot) address]))
          (do (debug "-> Asking for slot" (str "[" slot "]") "located at" address)
              (.addFirst dispatch-queue [:resolve (ask op slot) address]))))
      (finish op reply))))

(defn write-operation [args action timeout-fut]
  {:pre [(vector? args)]}
  (let [request (args->bytes args)]
    {:redirects      0
     :request        request
     :write-buffer   (ByteBuffer/wrap request)
     :action         action
     :timeout-future timeout-fut}))

(defrecord RedisClient [seeds ^Selector selector connections command-cache slot-cache ^ConcurrentLinkedDeque dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor]
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

(defn fail-connection [client {:keys [^LinkedBlockingDeque write-queue] :as conn} ex]
  (cleanup-connection client conn)
  (doseq [op (drain write-queue)]
    (finish op ex)))

(defn socket-connection [^InetSocketAddress address]
  (let [read-buffer (ByteBuffer/allocateDirect (* 16 1024))
        read-queue  (ConcurrentLinkedDeque.)
        write-queue (LinkedBlockingDeque. 1024)
        channel     (doto (SocketChannel/open)
                      (.configureBlocking false)
                      (.setOption (StandardSocketOptions/TCP_NODELAY) false))]
    (->SocketConnection channel address read-buffer read-queue write-queue)))

(defn incomplete? [op]
  {:pre [(op? op)]}
  (.hasRemaining ^ByteBuffer (:write-buffer op)))

(defn complete? [op]
  (not (incomplete? op)))

(defn ops->str [x]
  (str "[" (if (bit-test x 0) "R" "-") (if (bit-test x 2) "W" "-") (if (bit-test x 3) "C" "-") "]"))

(defn remote-address [^SelectionKey k]
  (.getRemoteAddress ^SocketChannel (.channel k)))

(defn set-op!
  "Sets interested ops."
  [^SelectionKey k x]
  (let [current-ops (.interestOps k)
        new-ops     (bit-or current-ops x)]
    (when (not= current-ops new-ops)
      (debug (remote-address k) "adding op" (ops->str current-ops) "->" (ops->str new-ops))
      (.interestOps k new-ops))))

(defn ignore-op!
  "Removes interested ops."
  ([^SelectionKey k x]
   (let [current-ops (.interestOps k)
         new-ops     (bit-and current-ops (bit-not x))]
     (when (not= current-ops new-ops)
       (debug (remote-address k) "ignoring op" (ops->str current-ops) "->" (ops->str new-ops)))
     (.interestOps k new-ops))))

(defmulti dispatch
  (fn [_ _ event]
    (first event)))

(defmethod dispatch :default [_ _ [action & event]]
  (warn "unknown action:" action "event:" event))

(defn handle-read! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^ConcurrentLinkedDeque read-queue ^ByteBuffer read-buffer] :as conn} (.attachment k)]
    (loop [n (.read channel (doto read-buffer (.clear)))]
      (cond
        (= -1 n)  (do (warn (remote-address k) "connection closed")
                      (cleanup-connection client conn))
        (zero? n) (debug (remote-address k) "no bytes read")
        :else     (let [_  (.flip read-buffer)
                        ba (byte-array n)
                        _  (.get read-buffer ba)
                        _  (trace-bytes ba (str (remote-address k) " read " n " byte(s)"))]
                    (loop [buffer ba]
                      (let [op (.getFirst read-queue)
                            [outcome ^bytes overflow] (parse op buffer)]
                        (cond
                          (= outcome ReplyParser/PARSE_INCOMPLETE)
                          (debug (remote-address k) "waiting for more byte(s)")

                          (= (ReplyParser/PARSE_COMPLETE) outcome)
                          (do (debug (remote-address k) "parse is complete")
                              (.removeFirst read-queue)
                              (complete op client))

                          (= ReplyParser/PARSE_OVERFLOW outcome)
                          (do (debug (remote-address k) "completed parsing op, but more byte(s) remain")
                              (when overflow
                                (warn (remote-address k) "buffer has an additional" (alength overflow) "byte(s)")
                                (trace-bytes overflow (str (remote-address k) " remaining byte(s) in receive buffer")))
                              (.removeFirst read-queue)
                              (complete op client)
                              (recur overflow))

                          :else
                          (do (error (remote-address k) "parse error" outcome)
                              (fail-connection client conn (ex-info "Error parsing Redis Reply" {})))))))))))

(defn handle-write! [client ^SelectionKey k]
  (let [{:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^LinkedBlockingDeque write-queue] :as conn} (.attachment k)]
    (loop []
      (if-let [op (.pollFirst write-queue)]
        (let [n (.write channel ^ByteBuffer (:write-buffer op))
              _ (trace (remote-address k) "wrote" n "byte(s)" (:write-buffer op))]
          (cond
            (= -1 n)         (do (warn (remote-address k) "connection closed")
                                 (cleanup-connection client conn))
            (incomplete? op) (do (debug (remote-address k) "op is incomplete, adding back to write queue" (:write-buffer op))
                                 (set-op! k OP_WRITE)
                                 (.addFirst write-queue op))
            :else            (do (trace-bytes (:request op) (str (remote-address k) " done writing op " (:write-buffer op)))
                                 (set-op! k OP_READ)
                                 (.addLast read-queue (flip op))
                                 (recur))))
        (do (trace (remote-address k) "write queue is empty")
            (ignore-op! k OP_WRITE))))))

(defn handle-connect! [client ^SelectionKey k]
  (let [^SocketChannel channel (.channel k)
        {:keys [^InetSocketAddress address read-queue write-queue] :as conn} (.attachment k)]
    (try
      (debug (remote-address k) "finishing connect")
      (let [connect-finished? (.finishConnect channel)]
        (when connect-finished?
          (set-op! k OP_READ)
          (ignore-op! k OP_CONNECT)
          (debug (remote-address k) "connected, writing queued ops")
          (handle-write! client k)))
      (catch java.net.ConnectException ex
        (warn ex "error connecting to" address)
        (fail-connection client conn (ex-info "error connecting" {} ex))
        (.cancel k))
      (catch IOException ex
        (error ex "IOException when finishing connect")
        (fail-connection client conn ex)
        (.cancel k)))))

(defn rand-seed [client]
  (-> client
      :seeds
      seq
      rand-nth))

(def shutdown-exception (ex-info "Shutting down NIO loop" {:shutdown? true}))

(defmethod dispatch :shutdown [connections {:keys [seeds ^Selector selector] :as client} _]
  (throw shutdown-exception))

(defn open-connection [{:keys [seeds ^Selector selector connections] :as client} address]
  (let [{:keys [^SocketChannel channel ^InetSocketAddress address] :as conn} (socket-connection address)]
    (if (.connect channel address)
      (do (debug address "connection has been established immediately, registering" (ops->str OP_READ))
          (.register channel selector OP_READ conn))
      (do (debug address "connection cannot be established immediately, registering" (ops->str OP_CONNECT))
          (.register channel selector OP_CONNECT conn)))
    (swap! connections assoc address conn)
    conn))

(defn resolve-connection [{:keys [seeds ^Selector selector connections] :as client} address]
  (or (get @connections address)
      (open-connection client address)))

(defmethod dispatch :resolve [connections {:keys [seeds ^Selector selector ^ConcurrentLinkedDeque dispatch-queue slot-cache command-cache] :as client} [_ op addr]]
  (let [conns         @connections
        resolved-conn (if-let [address (or addr (get @slot-cache (:slot op)))]
                        (resolve-connection client address)
                        (or (rand-nth (vals conns))
                            (resolve-connection client (rand-seed client))))]
    (.addFirst dispatch-queue [:write op resolved-conn])))

(defmethod dispatch :write [connections {:keys [^Selector selector] :as client} [_ op conn]]
  (let [{:keys [^SocketChannel channel ^ConcurrentLinkedDeque read-queue ^LinkedBlockingDeque write-queue]} conn
        address (.getRemoteAddress channel)]
    (cond (.isConnectionPending channel)
          (.add write-queue op)

          (.isConnected channel)
          (let [n (.write channel ^ByteBuffer (:write-buffer op))]
            (if (= -1 n)
              (do (warn address "connection closed")
                  (cleanup-connection client conn)
                  (finish op (ex-info "Connection closed when writing" {})))
              (let [k (.keyFor channel selector)]
                (if (complete? op)
                  (do (trace-bytes (:request op) (str address " done writing op " (:write-buffer op)))
                      (.addLast read-queue (flip op))
                      (ignore-op! k OP_WRITE)
                      (set-op! k OP_READ))
                  (do (debug address "op is incomplete" (:write-buffer op))
                      (.addFirst write-queue op)
                      (set-op! k OP_WRITE))))))

          :else
          (throw (ex-info "Connection is not pending or connected!" {:op op})))))

(defn nio-loop
  "Responsible for managing IO operations of dispatched operations."
  [{:keys [^Selector selector ^ConcurrentLinkedDeque dispatch-queue connections] :as client}]
  (try
    (loop []
      (if (.isOpen selector)
        (do (loop [n 0]
              (if-let [event (.pollFirst dispatch-queue)]
                (do (debug "dispatching:" event)
                    (try
                      (dispatch connections client event)
                      (catch Throwable err
                        (if (= err shutdown-exception)
                          (throw err)
                          (error err "caught throwable when dispatching event"))))
                    (recur (inc n)))
                (trace "dispatch queue is empty after" n "dispatched events")))

            (let [n (.select selector 50)]
              (if (> n 0)
                (let [selected-keys (.selectedKeys selector)
                      itr           (.iterator selected-keys)]
                  (trace "Selector/select yields" n "key(s)")
                  (while (.hasNext itr)
                    (let [^SelectionKey selected-key (.next itr)]
                      (trace (remote-address selected-key) "SelectionKey is ready:" (ops->str (.readyOps selected-key)))
                      (.remove itr)
                      (if (not (.isValid selected-key))
                        (debug (remote-address selected-key) "SelectionKey is not valid")
                        (do
                          (when (.isReadable selected-key)
                            (handle-read! client selected-key))
                          (when (and (.isValid selected-key) (.isWritable selected-key))
                            (handle-write! client selected-key))
                          (when (and (.isValid selected-key) (.isConnectable selected-key))
                            (handle-connect! client selected-key))))))
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
  "Sends a command vector to Redis using client"
  ([client args]
   (send-command client args nil))
  ([client args address]
   (send-command client args address (promise)))
  ([{:keys [^ConcurrentLinkedDeque dispatch-queue ^ScheduledThreadPoolExecutor scheduled-executor ^Selector selector command-cache] :as client} args address f]
   {:pre [(vector? args)]}
   (let [timeout (ex-info "Operation timed out" {:timeout? true :args args})
         fut     (.schedule scheduled-executor ^Runnable (fn [] (f timeout)) 10000 (TimeUnit/MILLISECONDS))
         op      (write-operation args f fut)
         op      (if-let [slot (routable-slot args @command-cache)]
                   (assoc op :slot slot)
                   op)]
     (.addLast dispatch-queue (if (instance? InetSocketAddress address) [:resolve op address] [:resolve op]))
     (.wakeup selector)
     f)))

(defn connect
  ([{:keys [seeds]}]
   (let [selector (Selector/open)
         executor (doto (ScheduledThreadPoolExecutor. 1) ;; used for timeouts
                    (.setRemoveOnCancelPolicy true))
         client   (->RedisClient seeds selector (atom {}) (atom {}) (atom {}) (ConcurrentLinkedDeque.) executor)
         io-fn    (fn []
                    (try
                      (nio-loop client)
                      (catch Exception err
                        (error err))))]
     (assoc client :io-thread (doto (Thread. io-fn (str "RedisProtocol-IO")) (.start)))))
  ([host port] (connect {:seeds #{(InetSocketAddress. (str host) ^long port)}})))

(defn connect-all
  "Discovers and connects to each cluster node."
  [conn]
  (let [cluster-nodes-reply @(send-command conn [:cluster :nodes])]
    (when (instance? byte-array-class cluster-nodes-reply)
      (let [cluster-nodes (util/parse-cluster-nodes (util/ascii-str cluster-nodes-reply))]
        (doseq [node cluster-nodes]
          (debug "resolving:" (:redis/address node))
          (resolve-connection conn (:redis/address node)))

        (doseq [node cluster-nodes]
          (debug "pinging" (:redis/address node))
          @(send-command conn [:ping] (:redis/address node))))))
  conn)

(defn resp->persistent
  "Recursively converts redis.resp.Array results into persistent data structures."
  [x]
  (cond (instance? redis.resp.Array x)        (mapv resp->persistent x)
        (instance? redis.resp.SimpleString x) (.message ^redis.resp.SimpleString x)
        :else x))

(defn command->key-mapping [[command-name arity flags key-pos-first key-pos-last step & _]]
  [(keyword (util/ascii-str command-name))
   {:arity   arity
    :flags   (set (map keyword flags))
    :key-pos [key-pos-first key-pos-last]
    :step    step}])

(defn update-command-cache
  "Updates the cached mappings between commands and key positions using the Redis command COMMAND.
  See http://redis.io/commands/command."
  [{:keys [command-cache] :as conn}]
  (->> @(send-command conn [:command])
       resp->persistent
       (map command->key-mapping)
       (reduce conj {})
       (swap! command-cache merge)))

(comment
  (with-open [client (connect "172.17.0.2" 7000)]
    (let [returns (doseq [_ (range 1)]
                    (send-command client ["set" "bar" "1"]))]
      (Thread/sleep 100000)))

  (with-open [client (connect "10.18.10.2" 6379)]
    (let [r1 (send-command client ["get" "foo"])
          r2 (send-command client ["get" "bar"])]
      [@r1 @r2]))

  (with-open [client (connect "10.18.10.1" 6379)]
    (connect-all client))

  (def commands
    (with-open [client (connect "10.18.10.2" 6379)]
      (resp->persistent @(send-command client [:command]))))

  (with-open [client (connect "10.18.10.2" 6379)]
    (def command-cache (update-command-cache client))
    client)

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client [:cluster :nodes]))

  (with-open [client (connect "10.18.10.2" 7000)]
    @(send-command client [:cluster :slots]))

  (with-open [client (connect "10.18.10.2" 6379)]
    @(send-command client [:info]))
  )
