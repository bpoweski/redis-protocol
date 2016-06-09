#!/bin/bash

## assumes the local docker network is routeable
## see docker-machine-router

eval $(docker-machine env)


nodes=$(seq 1 6)

for id in $(docker network inspect dmr | jq -r '.[].Containers | to_entries | .[].key')
do
    echo "stopping container: ${id}"
    docker stop $id
    docker wait $id > /dev/null
    docker rm $id
done


export IMAGE=redis:3.2

rm -rf docker/volumes/*

echo "creating volumes..."
for x in $nodes
do
    mkdir -p docker/volumes/node-${x}
    cat << EOF > docker/volumes/node-${x}/config.conf
port 6379
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
dir /data
EOF

done

echo "creating containers..."
for node in $nodes
do
    docker run -d --net dmr --ip 10.18.10.${node} -v "`pwd`/docker/volumes/node-${node}:/redis-config" $IMAGE /redis-config/config.conf
done

# echo "creating cluster"
hosts=$(echo $nodes | xargs -n 1 printf "10.18.10.%s:6379 ")
sh -c "echo yes | ruby docker/redis-trib.rb create $hosts"


# for port in $(seq 7000 7006)
# do
# ##    docker run --net dmr --ip 10.18.10.1 $IMAGE
# done
