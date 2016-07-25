(ns redis-protocol.parser-bench
  (:require [perforate.core :refer :all]
            [redis-protocol.util :as util]
            [redis-protocol.core :as c]
            [taoensso.carmine.protocol :as carmine]
            [byte-streams :as bs]
            [clojure.java.io :as io])
  (:import (redis.resp ReplyParser)
           (java.io DataInputStream ByteArrayInputStream BufferedInputStream)
           (redis.clients.jedis Protocol)
           (redis.clients.util RedisInputStream)))


(set! *warn-on-reflection* true)

(def buffer-size-16k (* 16 1024))

(defmacro defparsecase [name desc setup]
  `(do
     (defgoal ~name ~desc
       :setup
       (fn []
         ~setup))

     (defcase ~name :reply-parser
       [bis# byteseq#]
       (loop [parser# (ReplyParser.)
              byteseq# byteseq#]
         (when-let [^"[B" ba# (first byteseq#)]
           (.parse parser# ba#)
           (recur parser# (next byteseq#)))))

     (defcase ~name :carmine
       [^"[B" ba# byteseq#]
       (carmine/get-unparsed-reply (DataInputStream. (BufferedInputStream. (ByteArrayInputStream. ba#) buffer-size-16k)) {}))

     (defcase ~name :jedis
       [^"[B" ba# byteseq#]
       (Protocol/read (RedisInputStream. (BufferedInputStream. (ByteArrayInputStream. ba#) buffer-size-16k))))))

(defparsecase parse-complete-16-kb-str "Parsing an 16kb RESP bulk string"
  (let [^bytes ba (c/args->bytes [(String. (util/rand-chars 16 :kb))])]
    [ba
     (doall (bs/to-byte-arrays ba {:chunk-size buffer-size-16k}))]))

(defparsecase parse-complete-1-mb-str "Parsing an 1mb byte RESP bulk string"
  (let [^bytes ba (c/args->bytes [(String. (util/rand-chars 1 :mb))])]
    [ba
     (doall (bs/to-byte-arrays ba {:chunk-size buffer-size-16k}))]))
