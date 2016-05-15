(ns redis-protocol.parser
  (:import (redis.protocol ReplyParser ReplyParser$ArrayContainer)))


(defn container->reply [^ReplyParser$ArrayContainer container]
  (first container))
