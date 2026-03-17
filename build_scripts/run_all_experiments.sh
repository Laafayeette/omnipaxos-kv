#!/bin/bash
# run_all_experiments.sh — Run all 3 clock quality experiments sequentially
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$SCRIPT_DIR/../target/debug"
LOGS_DIR="$SCRIPT_DIR/logs"
CLUSTER_CONFIG="$SCRIPT_DIR/cluster-config.toml"
RESULTS_DIR="$SCRIPT_DIR/results"

mkdir -p "$RESULTS_DIR"

run_experiment() {
    local LABEL="$1"
    local UNCERTAINTY="$2"
    local SYNC_MS="$3"
    local DRIFT="$4"
    local TAG="$5"

    echo ""
    echo "============================================================"
    echo "  $LABEL"
    echo "  uncertainty=±${UNCERTAINTY}µs  sync=${SYNC_MS}ms  max_drift=${DRIFT}µs/s"
    echo "============================================================"

    # Kill leftovers
    pkill -f "target/debug/server" 2>/dev/null || true
    pkill -f "target/debug/client" 2>/dev/null || true
    sleep 1

    # Clean logs
    mkdir -p "$LOGS_DIR"
    rm -f "$LOGS_DIR"/server-*.json "$LOGS_DIR"/server-*.log
    rm -f "$LOGS_DIR"/client-*.json "$LOGS_DIR"/client-*.csv "$LOGS_DIR"/client-*.log

    # Write server configs
    for i in 1 2 3; do
        NC=0
        if [ "$i" -le 2 ]; then NC=1; fi
        PORT=$((8000 + i))
        printf 'location = "bench-%s"\nserver_id = %s\nnum_clients = %s\nlisten_address = "127.0.0.1"\nlisten_port = %s\noutput_filepath = "./logs/server-%s.json"\n\n[clock]\ndrift_us_per_s   = %s\nuncertainty      = %s\nsync_interval_ms = %s\n' \
            "$i" "$i" "$NC" "$PORT" "$i" "$DRIFT" "$UNCERTAINTY" "$SYNC_MS" \
            > "$SCRIPT_DIR/server-${i}-bench.toml"
    done

    # Write client configs
    for CID in 1 2; do
        PORT=$((8000 + CID))
        printf 'location = "bench-client-%s"\nserver_id = %s\nserver_address = "127.0.0.1:%s"\nsummary_filepath = "./logs/client-%s.json"\noutput_filepath  = "./logs/client-%s.csv"\n\n[[requests]]\nduration_sec     = 1\nrequests_per_sec = 10\nread_ratio       = 0.25\n\n[[requests]]\nduration_sec     = 10\nrequests_per_sec = 50\nread_ratio       = 0.25\n' \
            "$CID" "$CID" "$PORT" "$CID" "$CID" \
            > "$SCRIPT_DIR/client-${CID}-bench.toml"
    done

    echo "  Configs written."

    # Start servers
    local SPIDS=()
    for i in 1 2 3; do
        RUST_LOG=error \
        SERVER_CONFIG_FILE="$SCRIPT_DIR/server-${i}-bench.toml" \
        CLUSTER_CONFIG_FILE="$CLUSTER_CONFIG" \
        "$BIN_DIR/server" > "$LOGS_DIR/server-${i}.log" 2>&1 &
        SPIDS+=($!)
    done
    echo "  Servers started: ${SPIDS[*]}"

    # Wait for TCP listeners
    sleep 2

    # Start clients
    local CPIDS=()
    for CID in 1 2; do
        RUST_LOG=error \
        CONFIG_FILE="$SCRIPT_DIR/client-${CID}-bench.toml" \
        "$BIN_DIR/client" > "$LOGS_DIR/client-${CID}.log" 2>&1 &
        CPIDS+=($!)
    done
    echo "  Clients started: ${CPIDS[*]}"
    echo "  Waiting for clients (~11s)..."

    # Wait for clients
    for pid in "${CPIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "  Clients finished."

    # SIGTERM servers
    sleep 1
    for pid in "${SPIDS[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${SPIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "  Servers stopped."

    # Check consensus
    echo ""
    echo "  --- Consensus Check ($LABEL) ---"
    python3 "$SCRIPT_DIR/check_consensus.py"
    echo ""

    # Save results
    for f in "$LOGS_DIR"/server-*.json "$LOGS_DIR"/client-*.csv "$LOGS_DIR"/client-*.json; do
        [ -f "$f" ] && cp "$f" "$RESULTS_DIR/$(basename "${f%.*}")-${TAG}.${f##*.}"
    done
    echo "  Results saved to $RESULTS_DIR with tag=$TAG"
}

# ── Run all 3 experiments ──────────────────────────────────
run_experiment "HIGH QUALITY (±10µs, 1ms sync)"     10   1   1.0   high
run_experiment "MEDIUM QUALITY (±100µs, 10ms sync)" 100  10  10.0  medium
run_experiment "LOW QUALITY (±1000µs, 100ms sync)"  1000 100 50.0  low

echo ""
echo "============================================================"
echo "  ALL EXPERIMENTS COMPLETE"
echo "============================================================"
echo ""
echo "Results in: $RESULTS_DIR"
ls -la "$RESULTS_DIR"
