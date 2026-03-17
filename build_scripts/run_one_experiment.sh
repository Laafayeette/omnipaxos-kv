#!/bin/bash
# run_one_experiment.sh  —  Run one clock-quality experiment manually.
# Usage: ./run_one_experiment.sh <uncertainty> <sync_ms> <max_drift> <label>
#
# Starts 3 servers + 2 clients, waits for clients to finish, SIGTERMs servers,
# then runs check_consensus.py.
# drift_us_per_s is a MAX bound — each node derives its own drift from its pid.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$SCRIPT_DIR/../target/debug"
LOGS_DIR="$SCRIPT_DIR/logs"
CLUSTER_CONFIG="$SCRIPT_DIR/cluster-config.toml"

UNCERTAINTY=$1
SYNC_MS=$2
MAX_DRIFT=$3
LABEL=$4

echo ""
echo "============================================================"
echo "  Experiment: $LABEL"
echo "  uncertainty=±${UNCERTAINTY}µs  sync=${SYNC_MS}ms  max_drift=${MAX_DRIFT}µs/s"
echo "============================================================"

# Kill leftover processes
pkill -f "target/debug/server" 2>/dev/null || true
pkill -f "target/debug/client" 2>/dev/null || true
sleep 0.5

# Clean logs
mkdir -p "$LOGS_DIR"
rm -f "$LOGS_DIR"/*.json "$LOGS_DIR"/*.csv

# Write server configs
for i in 1 2 3; do
    NC=0
    if [ "$i" -le 2 ]; then NC=1; fi
    PORT=$((8000 + i))
    cat > "$SCRIPT_DIR/server-${i}-bench.toml" <<TOML
location = "bench-${i}"
server_id = ${i}
num_clients = ${NC}
listen_address = "127.0.0.1"
listen_port = ${PORT}
output_filepath = "./logs/server-${i}.json"

[clock]
drift_us_per_s   = ${MAX_DRIFT}
uncertainty      = ${UNCERTAINTY}
sync_interval_ms = ${SYNC_MS}
TOML
done

# Write client configs
for CID in 1 2; do
    PORT=$((8000 + CID))
    cat > "$SCRIPT_DIR/client-${CID}-bench.toml" <<TOML
location = "bench-client-${CID}"
server_id = ${CID}
server_address = "127.0.0.1:${PORT}"
summary_filepath = "./logs/client-${CID}.json"
output_filepath  = "./logs/client-${CID}.csv"

[[requests]]
duration_sec     = 1
requests_per_sec = 10
read_ratio       = 0.25

[[requests]]
duration_sec     = 10
requests_per_sec = 50
read_ratio       = 0.25
TOML
done

echo "  Config files written."

# Start servers in background
PIDS=()
for i in 1 2 3; do
    RUST_LOG=error \
    SERVER_CONFIG_FILE="$SCRIPT_DIR/server-${i}-bench.toml" \
    CLUSTER_CONFIG_FILE="$CLUSTER_CONFIG" \
    "$BIN_DIR/server" &
    PIDS+=($!)
done
echo "  Servers started: PIDs ${PIDS[*]}"

# Brief wait for TCP listeners to come up
sleep 1

# Start clients (they will finish on their own)
CLIENT_PIDS=()
for CID in 1 2; do
    RUST_LOG=error \
    CONFIG_FILE="$SCRIPT_DIR/client-${CID}-bench.toml" \
    "$BIN_DIR/client" &
    CLIENT_PIDS+=($!)
done
echo "  Clients started: PIDs ${CLIENT_PIDS[*]}"
echo "  Waiting for clients to finish (~11s)..."

# Wait for clients to finish
for pid in "${CLIENT_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
echo "  Clients finished."

# Give servers a moment, then SIGTERM them so save_output fires
sleep 1
for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
done
# Wait for servers to exit
for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
echo "  Servers stopped."

# Check consensus
echo ""
echo "  --- Consensus Check ---"
python3 "$SCRIPT_DIR/check_consensus.py"
echo ""
