#!/bin/bash
# Runs the cluster, waits for clients to finish, SIGTERMs servers,
# then checks whether all replicas reached the same state (consensus).

set -e
cd "$(dirname "$0")"
RUST_LOG=${RUST_LOG:-info}
BIN_DIR="../target/debug"

mkdir -p logs

echo "==> Starting servers..."
SERVER_CONFIG_FILE=server-1-config.toml CLUSTER_CONFIG_FILE=cluster-config.toml \
  RUST_LOG=$RUST_LOG "$BIN_DIR/server" > logs/server-1-run.log 2>&1 &
S1=$!
SERVER_CONFIG_FILE=server-2-config.toml CLUSTER_CONFIG_FILE=cluster-config.toml \
  RUST_LOG=$RUST_LOG "$BIN_DIR/server" > logs/server-2-run.log 2>&1 &
S2=$!
SERVER_CONFIG_FILE=server-3-config.toml CLUSTER_CONFIG_FILE=cluster-config.toml \
  RUST_LOG=$RUST_LOG "$BIN_DIR/server" > logs/server-3-run.log 2>&1 &
S3=$!

echo "    server PIDs: $S1 $S2 $S3"
echo "==> Waiting 4s for servers to be ready..."
sleep 4

echo "==> Starting clients..."
CONFIG_FILE=client-1-config.toml RUST_LOG=$RUST_LOG "$BIN_DIR/client" > logs/client-1-run.log 2>&1 &
C1=$!
CONFIG_FILE=client-2-config.toml RUST_LOG=$RUST_LOG "$BIN_DIR/client" > logs/client-2-run.log 2>&1
wait $C1
echo "==> Clients finished."

echo "==> Sending SIGTERM to servers (triggers save_output)..."
kill -TERM $S1 $S2 $S3
wait $S1 $S2 $S3 2>/dev/null
echo "==> Servers stopped."

echo ""
echo "==> Checking consensus..."
python3 "$(dirname "$0")/check_consensus.py"
