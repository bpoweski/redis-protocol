(ns redis-protocol.disruptor
  (:require [taoensso.timbre :as timbre])
  (:import (com.lmax.disruptor EventFactory RingBuffer EventHandler)
           (com.lmax.disruptor.dsl Disruptor)
           (java.util.concurrent ExecutorService Executors)
           (java.nio.channels AsynchronousSocketChannel)
           (com.lmax.disruptor.util DaemonThreadFactory)))

(timbre/refer-timbre)




(defprotocol MutateEvent
  (set-op [this op])
  (get-op [this]))

(deftype SendEvent [^:unsynchronized-mutable op ]
  MutateEvent
  (set-op [this x] (set! op  x))
  (get-op [_] op))

(defn channel-write-handler [^AsynchronousSocketChannel channel]
  (reify EventHandler
    (onEvent [_ entry seqn end?]
      (debug "name:" (Thread/currentThread) "on-event" seqn end? (get-op entry)))))

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
