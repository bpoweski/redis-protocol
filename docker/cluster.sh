#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

eval $(docker-machine env)

export NODES=$(seq 1 6)
export IMAGE=redis:3.2

stop-nodes() {
    for id in $(docker network inspect dmr | jq -r '.[].Containers | to_entries | .[].key')
    do
        echo "stopping container: ${id}"
        docker stop $id
        docker wait $id > /dev/null
        docker rm $id
    done
}

create-volumes() {
    rm -rf $DIR/volumes/*

    mkdir -p $DIR/var

    echo "creating volumes..."
    for node in $NODES
    do
        mkdir -p $DIR/volumes/node-${node}
        cat << EOF > $DIR/volumes/node-${node}/config.conf
port 6379
cluster-enabled yes
cluster-config-file /data/nodes.conf
cluster-node-timeout 5000
cluster-require-full-coverage no
dir /data
EOF

    done
}

start-nodes() {
    echo "creating containers..."
    for node in $NODES
    do
        docker run -d --net dmr --ip 10.18.10.${node} -v "$DIR/volumes/node-${node}:/redis-config" $IMAGE redis-server /redis-config/config.conf
    done
}

create-cluster () {
    # echo "creating cluster"
    HOSTS=$(echo $NODES | xargs -n 1 printf "10.18.10.%s:6379 ")
    sh -c "echo yes | ruby $DIR/redis-trib.rb create $HOSTS"
}

setup-network () {
    sudo -E docker-machine-router
}

case "$1" in
    boot)
        setup-network
        start-nodes
        create-cluster
        ;;

    start-nodes)
        start-nodes
        ;;

    stop-nodes)
        stop-nodes
        ;;

    restart)
        stop-nodes
        create-volumes
        start-nodes
        ;;
    setup)
        setup-network
        ;;

    create-cluster)
        create-cluster
        ;;

    *)
        echo "Primary Commands:"
        echo
        echo "$0 start-nodes"
        echo "$0 stop-nodes"
        echo "$0 restart"
        echo "$0 setup"
        ;;
esac
