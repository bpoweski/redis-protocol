(ns redis-protocol.util
  (:require [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.nio ByteBuffer)
           (redis.resp ReplyParser)
           (java.net InetSocketAddress)))


(def ^"[J" ^:private xmodem-crc16-lookup
  (long-array
   [0x0000 0x1021 0x2042 0x3063 0x4084 0x50a5 0x60c6 0x70e7
    0x8108 0x9129 0xa14a 0xb16b 0xc18c 0xd1ad 0xe1ce 0xf1ef
    0x1231 0x0210 0x3273 0x2252 0x52b5 0x4294 0x72f7 0x62d6
    0x9339 0x8318 0xb37b 0xa35a 0xd3bd 0xc39c 0xf3ff 0xe3de
    0x2462 0x3443 0x0420 0x1401 0x64e6 0x74c7 0x44a4 0x5485
    0xa56a 0xb54b 0x8528 0x9509 0xe5ee 0xf5cf 0xc5ac 0xd58d
    0x3653 0x2672 0x1611 0x0630 0x76d7 0x66f6 0x5695 0x46b4
    0xb75b 0xa77a 0x9719 0x8738 0xf7df 0xe7fe 0xd79d 0xc7bc
    0x48c4 0x58e5 0x6886 0x78a7 0x0840 0x1861 0x2802 0x3823
    0xc9cc 0xd9ed 0xe98e 0xf9af 0x8948 0x9969 0xa90a 0xb92b
    0x5af5 0x4ad4 0x7ab7 0x6a96 0x1a71 0x0a50 0x3a33 0x2a12
    0xdbfd 0xcbdc 0xfbbf 0xeb9e 0x9b79 0x8b58 0xbb3b 0xab1a
    0x6ca6 0x7c87 0x4ce4 0x5cc5 0x2c22 0x3c03 0x0c60 0x1c41
    0xedae 0xfd8f 0xcdec 0xddcd 0xad2a 0xbd0b 0x8d68 0x9d49
    0x7e97 0x6eb6 0x5ed5 0x4ef4 0x3e13 0x2e32 0x1e51 0x0e70
    0xff9f 0xefbe 0xdfdd 0xcffc 0xbf1b 0xaf3a 0x9f59 0x8f78
    0x9188 0x81a9 0xb1ca 0xa1eb 0xd10c 0xc12d 0xf14e 0xe16f
    0x1080 0x00a1 0x30c2 0x20e3 0x5004 0x4025 0x7046 0x6067
    0x83b9 0x9398 0xa3fb 0xb3da 0xc33d 0xd31c 0xe37f 0xf35e
    0x02b1 0x1290 0x22f3 0x32d2 0x4235 0x5214 0x6277 0x7256
    0xb5ea 0xa5cb 0x95a8 0x8589 0xf56e 0xe54f 0xd52c 0xc50d
    0x34e2 0x24c3 0x14a0 0x0481 0x7466 0x6447 0x5424 0x4405
    0xa7db 0xb7fa 0x8799 0x97b8 0xe75f 0xf77e 0xc71d 0xd73c
    0x26d3 0x36f2 0x0691 0x16b0 0x6657 0x7676 0x4615 0x5634
    0xd94c 0xc96d 0xf90e 0xe92f 0x99c8 0x89e9 0xb98a 0xa9ab
    0x5844 0x4865 0x7806 0x6827 0x18c0 0x08e1 0x3882 0x28a3
    0xcb7d 0xdb5c 0xeb3f 0xfb1e 0x8bf9 0x9bd8 0xabbb 0xbb9a
    0x4a75 0x5a54 0x6a37 0x7a16 0x0af1 0x1ad0 0x2ab3 0x3a92
    0xfd2e 0xed0f 0xdd6c 0xcd4d 0xbdaa 0xad8b 0x9de8 0x8dc9
    0x7c26 0x6c07 0x5c64 0x4c45 0x3ca2 0x2c83 0x1ce0 0x0cc1
    0xef1f 0xff3e 0xcf5d 0xdf7c 0xaf9b 0xbfba 0x8fd9 0x9ff8
    0x6e17 0x7e36 0x4e55 0x5e74 0x2e93 0x3eb2 0x0ed1 0x1ef0]))

(defn crc16
  "XMODEM (also known as ZMODEM or CRC-16/ACORN) hash function."
  ([^bytes ba] (crc16 ba 0 (alength ba)))
  ([^bytes ba start len]
   (let [stop (+ start len)]
     (loop [n   start
            crc 0]
       (if (>= n stop)
         crc
         (recur (inc n)
                (bit-xor (bit-and (bit-shift-left crc 8) 0xffff)
                         (aget xmodem-crc16-lookup
                               (-> (bit-shift-right crc 8)
                                   (bit-xor (aget ba n))
                                   (bit-and 0xff))))))))))


(def utf8-charset (java.nio.charset.Charset/forName "UTF-8"))

(defprotocol ByteSource
  (to-bytes [this]))

(extend-protocol ByteSource
  String
  (to-bytes [this] (.getBytes this utf8-charset))

  clojure.lang.Keyword
  (to-bytes [kw] (to-bytes (name kw)))

  java.lang.Long
  (to-bytes [l] (to-bytes (Long/toString l))))

;; Refer: [Google Group Discussion](https://groups.google.com/forum/#!topic/clojure/cioMCdArsKw)
(extend-type (Class/forName "[B")
  ByteSource
  (to-bytes ([this] this)))

(defn bytes= [x y]
  (java.util.Arrays/equals (to-bytes x) (to-bytes y)))

(defn buffer= [^ByteBuffer x ^ByteBuffer y]
  (zero? (.compareTo x y)))

(defn parse-str [s]
  (let [parser (ReplyParser.)
        state  (.parse parser s)]
    (if (= state (ReplyParser/PARSE_COMPLETE))
      (first (.root parser))
      (throw (ex-info "parse incomplete" {})))))

(defn ascii-bytes [s]
  (.getBytes s (java.nio.charset.Charset/forName "ASCII")))

(defn utf-16le-bytes [s]
  (.getBytes s (java.nio.charset.Charset/forName "UTF-16LE")))

(def ^:dynamic *cli-print-indent* 0)

(defmulti cli-print
  "Formats a parser reply similar to the redis-cli output."
  (fn [x _]
    (class x)))

(defmethod cli-print redis.resp.Error [error ^java.io.PrintWriter w]
  (.println w (str "(error) " (.message error))))

(defmethod cli-print redis.resp.SimpleString [info ^java.io.PrintWriter w]
  (.println w (str (.message info))))

(defmethod cli-print String [s ^java.io.PrintWriter w]
  (.print w \")
  (.print w s)
  (.println w \"))

(defmethod cli-print Long [x ^java.io.PrintWriter w]
  (.print w "(integer) ")
  (.println w x))

(defmethod cli-print nil [x ^java.io.PrintWriter w]
  (.println w "(nil)"))

(defmethod cli-print redis.resp.Array [^redis.resp.Array array ^java.io.PrintWriter w]
  (let [n        (.length array)
        n-digits (count (str n))]
    (doseq [[i v] (map-indexed vector array)]
      (when (pos? i)
        (.print w (str/join (repeat *cli-print-indent* " "))))
      (.print w (format (str "%" n-digits "d) ") (inc i)))
      (binding [*cli-print-indent* (+ *cli-print-indent* n-digits 2)]
        (cli-print v w)))))

(defn cli-format [reply]
  (with-open [str-writer (java.io.StringWriter.)
              print-writer (java.io.PrintWriter. str-writer)]
    (cli-print reply print-writer)
    (.toString (.getBuffer str-writer))))

(defn parse-cluster-info [s]
  (->> #"\r\n"
       (str/split s)
       (map #(str/split % #":"))
       (into {})))

(defn parse-cluster-node [s]
  (let [[id ip-port flags master ping-sent pong-recv config-epoch link-state slots] (str/split s #" " 9)
        [ip port] (str/split ip-port #":")]
    {:redis/id           id
     :redis/address      (InetSocketAddress. ip (Integer/parseInt port))
     :redis/flags        (->> (str/split flags #",")
                              (map keyword)
                              (into #{}))
     :redis/slots        slots
     :redis/ping-sent    (Long/parseLong ping-sent)
     :redis/pong-recv    (Long/parseLong pong-recv)
     :redis/config-epoch (Long/parseLong config-epoch)
     :redis/link-state   (keyword link-state)}))

(defn parse-cluster-nodes [s]
  (->> (java.io.StringReader. s)
       io/reader
       line-seq
       (map parse-cluster-node)))
