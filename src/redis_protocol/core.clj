(ns redis-protocol.core
  (:require [byte-streams :as bs]
            [clojure.java.io :as io]
            [clojure.tools.trace :as t]
            [taoensso.timbre :as timbre]
            [clojure.core.async :as async])
  (:import (java.net InetSocketAddress StandardSocketOptions)
           (java.util Arrays)
           (java.nio ByteBuffer)
           (java.nio.channels AsynchronousSocketChannel CompletionHandler)))

(timbre/refer-timbre)

;; http://www.ibm.com/developerworks/library/j-nio2-1/


;; for now pretend charset encoding is not relevant
(defn encode-str [s]
  (apply str "$" (count s) "\r\n" s "\r\n"))

(defn args->str [args]
  (apply str "*" (count args) "\r\n" (map encode-str args)))

;; #define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
;; #define PROTO_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
;; #define PROTO_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
;; #define PROTO_MBULK_BIG_ARG     (1024*32)
;; #define LONG_STR_SIZE      21          /* Bytes needed for long -> str */
;; https://webtide.com/on-jdk-7-asynchronous-io/
(def proto-reply-chunk-bytes (* 16 1024))

(defn bytes= [x y]
  (java.util.Arrays/equals x y))

(defn buffer= [^ByteBuffer x ^ByteBuffer y]
  (zero? (.compareTo x y)))

(defn debug-bytes [buf]
  (let [lines (.split (with-out-str (bs/print-bytes buf)) "\n")]
    (doseq [line (take 5 lines)]
      (debug line))
    (if (> (count lines) 5)
      (debug "..."))))

(defn aconcat-bytes [^bytes a ^bytes b]
  (let [ba (Arrays/copyOf a (+ (alength a) (alength b)))
        _ (System/arraycopy b 0 ba (alength a) (alength b))]
    ba))

;; (defn run [command]
;;   (with-open [client (AsynchronousSocketChannel/open)]
;;     @(.connect client (InetSocketAddress. "127.0.0.1" 6379))
;;     (let [req (bs/convert (args->str command) ByteBuffer)
;;           _   @(.write client req)]
;;       (loop [reply  nil
;;              buffer (ByteBuffer/allocateDirect 8)]
;;         (let [bytes-read @(.read client buffer)
;;               buffer     (doto buffer
;;                            (.limit bytes-read)
;;                            (.rewind))
;;               reply      (if (nil? reply)
;;                            (parse-chunk buffer)
;;                            (parse-next-chunk reply buffer))]
;;           (if (complete? reply)
;;             (do (.clear buffer)
;;                 reply)
;;             (recur reply (.compact buffer))))))))
