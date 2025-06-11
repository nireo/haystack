#!/bin/bash

set -e

echo "=== setting up test environment ==="

DIRECTORY_PORT_1=8001
DIRECTORY_PORT_2=8002
DIRECTORY_PORT_3=8003
RAFT_PORT_1=7001
RAFT_PORT_2=7002
RAFT_PORT_3=7003

STORE_PORT_1=9001
STORE_PORT_2=9002
STORE_PORT_3=9003

cleanup() {
    echo "[LOADTEST] cleaning up processes..."
    pkill -f "haystack-directory" 2>/dev/null || true
    pkill -f "haystack-store" 2>/dev/null || true
    sleep 2
}

trap cleanup EXIT

cleanup

echo "[LOADTEST] creating data directories..."
mkdir -p data/directory1 data/directory2 data/directory3
mkdir -p data/store1 data/store2 data/store3

echo "[LOADTEST] starting directory cluster..."

echo "[LOADTEST] starting directory node 1 (bootstrap)..."
./haystack-directory \
    -node-id=dir1 \
    -raft-addr=127.0.0.1:$RAFT_PORT_1 \
    -http-addr=127.0.0.1:$DIRECTORY_PORT_1 \
    -data-dir=./data/directory1 \
    -bootstrap=true \
    -replication-factor=3 \
    -max-lv-size=1073741824 &

sleep 3

echo "[LOADTEST] starting directory node 2..."
./haystack-directory \
    -node-id=dir2 \
    -raft-addr=127.0.0.1:$RAFT_PORT_2 \
    -http-addr=127.0.0.1:$DIRECTORY_PORT_2 \
    -data-dir=./data/directory2 \
    -existingAddr=127.0.0.1:$DIRECTORY_PORT_1 \
    -replication-factor=3 \
    -max-lv-size=1073741824 &

sleep 2

echo "[LOADTEST] starting directory node 3..."
./haystack-directory \
    -node-id=dir3 \
    -raft-addr=127.0.0.1:$RAFT_PORT_3 \
    -http-addr=127.0.0.1:$DIRECTORY_PORT_3 \
    -data-dir=./data/directory3 \
    -existingAddr=127.0.0.1:$DIRECTORY_PORT_1 \
    -replication-factor=3 \
    -max-lv-size=1073741824 &

sleep 3

echo "[LOADTEST] Starting store nodes..."

echo "[LOADTEST] starting store node 1..."
./haystack-store \
    -port=$STORE_PORT_1 \
    -dir=./data/store1 &

sleep 1

echo "[LOADTEST] starting store node 2..."
./haystack-store \
    -port=$STORE_PORT_2 \
    -dir=./data/store2 &

sleep 1

echo "[LOADTEST] Starting store node 3..."
./haystack-store \
    -port=$STORE_PORT_3 \
    -dir=./data/store3 &

sleep 2

echo "[LOADTEST] registering stores with directory cluster..."

curl -X POST http://127.0.0.1:$DIRECTORY_PORT_1/api/v1/stores \
    -H "Content-Type: application/json" \
    -d '{"store_id":"store1","address":"127.0.0.1:'$STORE_PORT_1'"}' || true

sleep 1

curl -X POST http://127.0.0.1:$DIRECTORY_PORT_1/api/v1/stores \
    -H "Content-Type: application/json" \
    -d '{"store_id":"store2","address":"127.0.0.1:'$STORE_PORT_2'"}' || true

sleep 1

curl -X POST http://127.0.0.1:$DIRECTORY_PORT_1/api/v1/stores \
    -H "Content-Type: application/json" \
    -d '{"store_id":"store3","address":"127.0.0.1:'$STORE_PORT_3'"}' || true

sleep 2

echo "=== cluster setup complete ==="
echo "directory nodes running on ports: $DIRECTORY_PORT_1, $DIRECTORY_PORT_2, $DIRECTORY_PORT_3"
echo "store nodes running on ports: $STORE_PORT_1, $STORE_PORT_2, $STORE_PORT_3"
echo ""
echo "cluster status:"
curl -s http://127.0.0.1:$DIRECTORY_PORT_1/api/v1/status | python3 -m json.tool || echo "Could not get status"
echo ""
echo "Press Ctrl+C to stop all services"

while true; do
    sleep 1
done
