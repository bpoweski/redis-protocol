#!/bin/bash

SLOT_COUNT=1000

while true
do
    SELECTED_NODE=$(ruby docker/redis-trib.rb check 10.18.10.4:6379  | grep "M:" | gshuf  | head -n 1)

    NODE_ID=$(echo $SELECTED_NODE | cut -f2 -d' ')
    HOST_PORT=$(echo $SELECTED_NODE | cut -f3 -d' ')

    echo "resharding ${SLOT_COUNT} to ${HOST_PORT}"
    ruby docker/redis-trib.rb reshard  --yes --slots $SLOT_COUNT --to $NODE_ID --from all $HOST_PORT > /dev/null
    echo "done"
    sleep 2
done
