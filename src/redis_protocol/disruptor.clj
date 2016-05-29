(ns redis-protocol.disruptor
  (:require [taoensso.timbre :as timbre])
  (:import (com.lmax.disruptor EventFactory RingBuffer EventHandler)
           (com.lmax.disruptor.dsl Disruptor)
           (java.util.concurrent ExecutorService Executors)
           (java.nio.channels AsynchronousSocketChannel)
           (com.lmax.disruptor.util DaemonThreadFactory)))

(timbre/refer-timbre)

;; SocketChannel
;; Socket channels are safe for use by multiple concurrent threads. They support concurrent reading and writing, though at most one thread may be reading and
;; at most one thread may be writing at any given time. The connect and finishConnect methods are mutually synchronized against each other, and an attempt to
;; initiate a read or write operation while an invocation of one of these methods is in progress will block until that invocation is complete.

;; SelectionKey
;; Selection keys are safe for use by multiple concurrent threads.


;; Request Ring
;; [SET foo 1] -> WriteCommandRing
;;   -> ChannelWriter
;;     - assigns a Connection/SocketChannel to each command
;;     - writes command to channel
;;     - zero bytes written, then writes journals an OP_WRITE change ops to
;;     - journals a read op change for the given socket in the IO ring

;; IO Selector
;; .select()
;; for each k
;;   publish into SelectionKey Ring


;; IO SelectionKey Ring
;;


;; (defn io-loop [{:keys [^Selector selector ^BlockingQueue pending-changes] :as client}]
;;   (loop [step 0]
;;     (debug "loop:" step)
;;     (if (and (< step 100) (.isOpen selector))
;;       (do (doseq [[action ^SocketChannel connection interested-ops] (drain! pending-changes 1024)
;;                   :let [^SocketChannel channel (:channel connection)]]
;;             (case action
;;               :register  (do (debug "registering new channel" (str connection))
;;                              (.register channel selector interested-ops connection))
;;               :set-ops   (let [selection-key (.keyFor channel selector)
;;                                current-ops   (.interestOps selection-key)
;;                                new-ops       (bit-or current-ops interested-ops)]
;;                            (debug "ops:" (ops->str current-ops) "->" (ops->str new-ops) "on" (:channel connection))
;;                            (.interestOps selection-key new-ops))
;;               :unset-ops (let [selection-key (.keyFor channel selector)
;;                                current-ops   (.interestOps selection-key)
;;                                new-ops       (bit-xor current-ops interested-ops)]
;;                            (debug "ops:" (ops->str current-ops) "->" (ops->str new-ops) "on" (:channel connection))
;;                            (.interestOps selection-key new-ops))
;;               (warn "unknown action:" action)))

;;           (if (> (.select selector 1000) 0)
;;             (let [selected-keys (.selectedKeys selector)
;;                   itr           (.iterator selected-keys)]
;;               (while (.hasNext itr)
;;                 (let [^SelectionKey selected-key (.next itr)
;;                       _ (.remove itr)]
;;                   (cond
;;                     (not (.isValid selected-key)) (debug "key is not valid:" selected-key)
;;                     (.isReadable selected-key)    (read! client selected-key)
;;                     (.isWritable selected-key)    (write! client selected-key)
;;                     (.isConnectable selected-key) (connect! client selected-key))))
;;               (recur (inc step)))
;;             (do (recur (inc step)))))
;;       (warn "Selector is closed"))))


(defprotocol MutateEvent
  (set-op [this op])
  (get-op [this]))

(deftype SendEvent [^:unsynchronized-mutable op]
  MutateEvent
  (set-op [this x] (set! op  x))
  (get-op [_] op))

(defn channel-write-handler [^AsynchronousSocketChannel channel]
  (reify EventHandler
    (onEvent [_ entry seqn end?]
      (debug "name:" (Thread/currentThread) "on-event" seqn end? (get-op entry)))))

(defrecord DisruptorClient [request])

(defn disruptor! []
  (let [es         (Executors/newCachedThreadPool)
        disruptor (Disruptor.
                   (reify EventFactory
                     (newInstance [_]
                       (SendEvent. nil)))
                   64
                   es)
        handler    (reify EventHandler
                     (onEvent [_ entry seqn end?]
                       (debug "name:" (Thread/currentThread) "on-event" seqn end? (get-op entry))))
        _          (doto disruptor
                     (.handleEventsWith (into-array EventHandler [handler]))
                     (.start))
        ring       (.getRingBuffer disruptor)]
    (try
      (dotimes [n 1000]
        (Thread/sleep (rand-int 50))
        (let [nseq  (.next ring)
              entry (.get ring nseq)]
          ;;(set-op entry (write-operation ["get"] (async/chan 1)))
          (.publish ring nseq)))
      (finally
        (.shutdown disruptor)
        (.shutdownNow es)))))



;; (defn read!
;;   "Reads the operation from a AsynchronousSocketChannel"
;;   [^AsynchronousSocketChannel channel ^ByteBuffer buffer ^ReadOperation op ^LinkedTransferQueue ready-queue]
;;   (let [handler (reify CompletionHandler
;;                   (completed [_ n _]
;;                     (try
;;                       (let [_       (.flip buffer)
;;                             ba      (byte-array n)
;;                             _       (.get buffer ba)
;;                             outcome (.parse (.parser op) ba)]
;;                         (condp = outcome
;;                           ReplyParser/PARSE_INCOMPLETE (read! channel buffer op ready-queue)
;;                           ReplyParser/PARSE_COMPLETE   (.put ready-queue [:complete op])))
;;                       (catch Exception err
;;                         (error err "caught error when reading"))))
;;                   (failed [_ throwable _]
;;                     (.put ready-queue [:failed op throwable])))]
;;     (.clear buffer)
;;     (try (.read channel buffer 2000 (TimeUnit/MILLISECONDS) op handler)
;;          (catch Exception err
;;            (error err "Caught Error when reading")))))

;; (defn write!
;;   "Writes the operation to the AsynchronousSocketChannel."
;;   [^AsynchronousSocketChannel channel ^WriteOperation op ^LinkedTransferQueue ready-queue]
;;   (let [handler (reify CompletionHandler
;;                   (completed [_ n _]
;;                     (try
;;                       (debug "completed" n)
;;                       (if (complete? op)
;;                         (do (debug op "is complete")
;;                             (.put ready-queue [:proceed (write->read op)]))
;;                         (do (debug op "is not complete")
;;                             (write! channel op ready-queue)))
;;                       (catch Exception err
;;                         (error err "In CompletionHandler"))))
;;                   (failed [_ throwable _]
;;                     (debug op "failed")
;;                     (.put ready-queue [:failed op throwable])))]
;;     (.write channel (.payload op) 2000 (TimeUnit/MILLISECONDS) op handler)))
