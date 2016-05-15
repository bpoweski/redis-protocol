#!/bin/bash

CLASS_NAME=ReplyParser
JAVA_FILE=target/ragel/redis/protocol/$CLASS_NAME.java

rm -f $JAVA_FILE && rm -rf target/classes/redis

mkdir -p target/ragel/redis/protocol

ragel -J src-ragel/redis/protocol/$CLASS_NAME.java.rl -o $JAVA_FILE
# ragel -Vp -Sresp_array_header src-ragel/redis/protocol/$CLASS_NAME.java.rl -o /tmp/resp.dot
# dot /tmp/resp.dot -Tpng -o /tmp/resp.png

javac -Xlint:unchecked -g -d target/classes $JAVA_FILE

if [[ $DEBUG -eq "1" ]]; then
  jdb -classpath target/classes redis.protocol.$CLASS_NAME
else
  java -cp target/classes redis.protocol.$CLASS_NAME
fi
