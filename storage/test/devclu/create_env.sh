#!/bin/bash -x
NUM_NODES=$1
PORT_BASE=50000
IP=127.0.0.1
HOSTS=()
CLUSTER_HOSTS="["

for i in $(seq 1 $NUM_NODES)
do
    PORT=$((PORT_BASE + i))
    HOSTS+=($IP:$PORT)
    if [[ $i -eq $NUM_NODES ]];
    then
        CLUSTER_HOSTS+="\"$IP:$PORT\"]"
    else
        CLUSTER_HOSTS+="\"$IP:$PORT\", "
    fi
done

for i in $(seq 1 $NUM_NODES)
do
    mkdir -p ./nodes/node$i
    cp ../../../bin/naiv_storage_server ./nodes/node$i/naiv_storage_server
    IDENTITY_UUID=$(uuidgen) IDENTITY_HOST=${HOSTS[i-1]} LOG_FILE_NAME=./naiv_storage_server.log CLUSTER_HOSTS=$CLUSTER_HOSTS ./gen_server_config.sh > ./nodes/node$i/config.toml
done

PIDS=()
trap exit_script SIGINT SIGTERM
exit_script() {
    for pid in "${PIDS[@]}"
    do
        kill $pid
    done

    exit
}

for i in $(seq 1 $NUM_NODES)
do
    pushd .
    cd ./nodes/node$i
    nohup ./naiv_storage_server > ./stdout.log 2>&1 &
    PIDS+=($!)
    popd
done

while true
do
	sleep 1
done