#!/usr/bin/env python3
"""
benchmark_clock.py  ·  Clock-Assisted Consensus Benchmark
==========================================================

Tests OmniPaxos with 3 clock-quality configurations and measures:
  • consensus latency  (mean, p50, p99 in ms)
  • throughput         (completed ops / second)
  • fast-path ratio    (% of requests committed via 1-RTT fast path)
  • consensus safety   (all replicas hold identical synced logs)

Usage (run from build_scripts/):
    python3 benchmark_clock.py           # uses pre-built binary
    python3 benchmark_clock.py --build   # builds first

Requirements:
    - Rust toolchain with `cargo` on $PATH
    - Python 3.9+
"""

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean

# ──────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────
SCRIPT_DIR     = Path(__file__).parent.resolve()
OMI_DIR        = SCRIPT_DIR.parent
BIN_DIR        = OMI_DIR / "target" / "debug"
LOGS_DIR       = SCRIPT_DIR / "logs"
SERVER_BIN     = BIN_DIR / "server"
CLIENT_BIN     = BIN_DIR / "client"
CLUSTER_CONFIG = SCRIPT_DIR / "cluster-config.toml"

NUM_SERVERS      = 3
SERVER_BASE_PORT = 8001          # server i listens on 8000 + i

# ──────────────────────────────────────────────────────────────────
# Clock quality configurations (Section 2.1 of the assignment)
# ──────────────────────────────────────────────────────────────────
# drift_us_per_s is a MAX bound — each node derives its own drift in
# [-max, +max] from a hash of its pid, so different nodes diverge.
CLOCK_CONFIGS = [
    {
        "name":             "high_quality",
        "label":            "High Quality   (±10 µs,    1 ms sync)",
        "uncertainty":      10,
        "sync_interval_ms": 1,
        "drift_us_per_s":   1.0,
    },
    {
        "name":             "medium_quality",
        "label":            "Medium Quality (±100 µs,  10 ms sync)",
        "uncertainty":      100,
        "sync_interval_ms": 10,
        "drift_us_per_s":   10.0,
    },
    {
        "name":             "low_quality",
        "label":            "Low Quality    (±1000 µs, 100 ms sync)",
        "uncertainty":      1000,
        "sync_interval_ms": 100,
        "drift_us_per_s":   50.0,
    },
]

# ──────────────────────────────────────────────────────────────────
# Benchmark workload knobs
# ──────────────────────────────────────────────────────────────────
WARMUP_DURATION_SEC    = 1
WARMUP_REQUESTS_PER_S  = 10
WARMUP_READ_RATIO      = 0.25

BENCH_DURATION_SEC     = 10    # seconds of sustained load per run
BENCH_REQUESTS_PER_S   = 20    # ops / second per client  (×2 clients = 40 ops/s)
BENCH_READ_RATIO       = 0.25  # fraction of ops that are reads

SERVER_STARTUP_WAIT_S  = 5     # wait for TCP listeners + leader election

# ──────────────────────────────────────────────────────────────────
# TOML template generators
# ──────────────────────────────────────────────────────────────────

def _server_toml(server_id: int, clock: dict) -> str:
    """Return the full TOML content for one server node."""
    num_clients = 1 if server_id in (1, 2) else 0
    port        = SERVER_BASE_PORT + server_id - 1
    return (
        f'location = "bench-{server_id}"\n'
        f"server_id = {server_id}\n"
        f"num_clients = {num_clients}\n"
        f'listen_address = "127.0.0.1"\n'
        f"listen_port = {port}\n"
        f'output_filepath = "./logs/server-{server_id}.json"\n'
        f"\n"
        f"[clock]\n"
        f"drift_us_per_s   = {clock['drift_us_per_s']}\n"
        f"uncertainty      = {clock['uncertainty']}\n"
        f"sync_interval_ms = {clock['sync_interval_ms']}\n"
    )


def _client_toml(client_id: int, server_id: int) -> str:
    """Return the full TOML content for one benchmark client."""
    port = SERVER_BASE_PORT + server_id - 1
    return (
        f'location = "bench-client-{client_id}"\n'
        f"server_id = {server_id}\n"
        f'server_address = "127.0.0.1:{port}"\n'
        f'summary_filepath = "./logs/client-{client_id}.json"\n'
        f'output_filepath  = "./logs/client-{client_id}.csv"\n'
        f"\n"
        f"# warm-up phase\n"
        f"[[requests]]\n"
        f"duration_sec     = {WARMUP_DURATION_SEC}\n"
        f"requests_per_sec = {WARMUP_REQUESTS_PER_S}\n"
        f"read_ratio       = {WARMUP_READ_RATIO}\n"
        f"\n"
        f"# sustained benchmark phase\n"
        f"[[requests]]\n"
        f"duration_sec     = {BENCH_DURATION_SEC}\n"
        f"requests_per_sec = {BENCH_REQUESTS_PER_S}\n"
        f"read_ratio       = {BENCH_READ_RATIO}\n"
    )


# ──────────────────────────────────────────────────────────────────
# Build
# ──────────────────────────────────────────────────────────────────

def build_binaries() -> None:
    print("==> Building Rust binaries (cargo build) …")
    result = subprocess.run(["cargo", "build"], cwd=OMI_DIR)
    if result.returncode != 0:
        sys.exit("Build failed — aborting benchmark.")
    print("==> Build complete.\n")


def ensure_built() -> None:
    if not SERVER_BIN.exists() or not CLIENT_BIN.exists():
        print("==> Binaries not found — building …")
        build_binaries()


# ──────────────────────────────────────────────────────────────────
# Experiment runner
# ──────────────────────────────────────────────────────────────────

def _write_configs(clock: dict) -> None:
    """Write server-{i}-bench.toml and client-{j}-bench.toml files."""
    for i in range(1, NUM_SERVERS + 1):
        path = SCRIPT_DIR / f"server-{i}-bench.toml"
        path.write_text(_server_toml(i, clock))
    for cid, sid in [(1, 1), (2, 2)]:
        path = SCRIPT_DIR / f"client-{cid}-bench.toml"
        path.write_text(_client_toml(cid, sid))


def _clear_logs() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    for f in LOGS_DIR.iterdir():
        try:
            f.unlink()
        except OSError:
            pass


def _kill_all(procs: list) -> None:
    """Send SIGTERM then SIGKILL to a list of Popen objects."""
    for p in procs:
        try:
            p.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
    time.sleep(1)
    for p in procs:
        try:
            p.kill()
        except ProcessLookupError:
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except (subprocess.TimeoutExpired, ChildProcessError):
            pass


def run_experiment(clock: dict) -> dict:
    """
    Bring up the cluster with the given clock config, run clients,
    tear down, and return the raw metrics dict.
    """
    _clear_logs()
    _write_configs(clock)

    env = {**os.environ, "RUST_LOG": "error"}

    # ── Start servers ────────────────────────────────────────────
    server_procs: list[subprocess.Popen] = []
    client_procs: list[subprocess.Popen] = []

    try:
        for i in range(1, NUM_SERVERS + 1):
            proc = subprocess.Popen(
                [str(SERVER_BIN)],
                env={
                    **env,
                    "SERVER_CONFIG_FILE":  str(SCRIPT_DIR / f"server-{i}-bench.toml"),
                    "CLUSTER_CONFIG_FILE": str(CLUSTER_CONFIG),
                },
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            server_procs.append(proc)

        print(f"    Servers started (PIDs {[p.pid for p in server_procs]}).")
        print(f"    Waiting {SERVER_STARTUP_WAIT_S}s for TCP listeners …")
        time.sleep(SERVER_STARTUP_WAIT_S)

        # ── Start clients ────────────────────────────────────────────
        for cid in (1, 2):
            proc = subprocess.Popen(
                [str(CLIENT_BIN)],
                env={
                    **env,
                    "CONFIG_FILE": str(SCRIPT_DIR / f"client-{cid}-bench.toml"),
                },
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            client_procs.append(proc)

        # Wait for all clients to finish their workload (with safety timeout)
        client_timeout = WARMUP_DURATION_SEC + BENCH_DURATION_SEC + 30  # generous timeout
        for proc in client_procs:
            try:
                proc.wait(timeout=client_timeout)
            except subprocess.TimeoutExpired:
                print(f"    WARNING: client {proc.pid} timed out after {client_timeout}s — killing")
                proc.kill()
                proc.wait()
        print("    Clients finished.")

        # ── Stop servers (SIGTERM → save_output fires) ───────────────
        for proc in server_procs:
            try:
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
        for proc in server_procs:
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    except (KeyboardInterrupt, SystemExit):
        print("\n    Interrupted — killing all child processes …")
        _kill_all(client_procs + server_procs)
        raise

    return _collect_metrics()


# ──────────────────────────────────────────────────────────────────
# Metrics collection
# ──────────────────────────────────────────────────────────────────

def _collect_metrics() -> dict:
    """Parse server JSON logs and client CSV logs; compute metrics."""

    # ── Server JSON logs ─────────────────────────────────────────
    server_snapshots = []
    for i in range(1, NUM_SERVERS + 1):
        path = LOGS_DIR / f"server-{i}.json"
        if path.exists():
            with open(path) as fh:
                server_snapshots.append(json.load(fh))

    total_fast = sum(d.get("fast_path_count", 0) for d in server_snapshots)
    total_slow = sum(d.get("slow_path_count", 0) for d in server_snapshots)
    total_ops  = total_fast + total_slow
    fast_ratio = (total_fast / total_ops * 100.0) if total_ops > 0 else 0.0

    per_server_fast = [d.get("fast_path_count", 0) for d in server_snapshots]
    per_server_slow = [d.get("slow_path_count", 0) for d in server_snapshots]

    # Consensus safety: all replicas hold the identical synced log (request ordering)
    synced_logs = [d.get("synced_log", []) for d in server_snapshots]
    # Compare only the request part (results differ: leader has values, followers have null)
    synced_request_logs = [
        [entry.get("request") for entry in log] for log in synced_logs
    ]
    if len(synced_request_logs) == NUM_SERVERS and len(synced_request_logs[0]) > 0:
        consensus_ok = all(lg == synced_request_logs[0] for lg in synced_request_logs)
    elif len(synced_request_logs) < NUM_SERVERS:
        consensus_ok = False   # some servers didn't write output
    else:
        consensus_ok = None    # no writes yet (reads-only run)

    synced_lens = [d.get("synced_log_len", 0) for d in server_snapshots]

    # ── Client CSV logs ──────────────────────────────────────────
    all_latencies: list[float] = []
    total_completed = 0
    earliest_start  = None
    latest_end      = None

    for cid in (1, 2):
        path = LOGS_DIR / f"client-{cid}.csv"
        if not path.exists():
            continue
        with open(path, newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                req_time  = int(row["request_time"])
                resp_raw  = row.get("response_time", "").strip()
                if not resp_raw:
                    continue
                resp_time = int(resp_raw)
                lat_ms    = resp_time - req_time
                if lat_ms < 0:
                    continue
                all_latencies.append(lat_ms)
                total_completed += 1
                if earliest_start is None or req_time < earliest_start:
                    earliest_start = req_time
                if latest_end is None or resp_time > latest_end:
                    latest_end = resp_time

    # Wall-clock window (ms → s)
    if (earliest_start is not None and latest_end is not None
            and latest_end > earliest_start):
        elapsed_s = (latest_end - earliest_start) / 1000.0
    else:
        elapsed_s = 1.0

    throughput = total_completed / elapsed_s

    if all_latencies:
        all_latencies.sort()
        n        = len(all_latencies)
        lat_mean = mean(all_latencies)
        lat_p50  = all_latencies[max(0, int(n * 0.50) - 1)]
        lat_p99  = all_latencies[max(0, min(int(n * 0.99), n - 1))]
    else:
        lat_mean = lat_p50 = lat_p99 = float("nan")

    return {
        "total_fast":           total_fast,
        "total_slow":           total_slow,
        "total_ops":            total_ops,
        "fast_ratio":           fast_ratio,
        "per_server_fast":      per_server_fast,
        "per_server_slow":      per_server_slow,
        "consensus_ok":         consensus_ok,
        "synced_lens":          synced_lens,
        "lat_mean_ms":          lat_mean,
        "lat_p50_ms":           lat_p50,
        "lat_p99_ms":           lat_p99,
        "throughput_ops_per_s": throughput,
        "total_completed":      total_completed,
    }


# ──────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────

def _fmt(val, fmt=".1f") -> str:
    try:
        if val != val:   # NaN check
            return "N/A"
        return format(val, fmt)
    except (TypeError, ValueError):
        return str(val)


def print_results(results: list[dict]) -> None:
    sep = "=" * 100
    print()
    print(sep)
    print(f"{'CLOCK-ASSISTED CONSENSUS BENCHMARK  —  RESULTS':^100}")
    print(sep)
    header = (
        f"  {'Configuration':<40}  {'Fast%':>6}  {'Fast':>6}  {'Slow':>6}  "
        f"{'Mean ms':>8}  {'P50 ms':>7}  {'P99 ms':>7}  {'Tput op/s':>10}  {'Consensus':>10}"
    )
    print(header)
    print("-" * 100)
    for r in results:
        lbl  = r["clock"]["label"]
        m    = r["metrics"]
        csok = {True: "PASS", False: "FAIL", None: "empty log"}[m["consensus_ok"]]
        print(
            f"  {lbl:<40}  {_fmt(m['fast_ratio']):>5}%  "
            f"{m['total_fast']:>6}  {m['total_slow']:>6}  "
            f"{_fmt(m['lat_mean_ms']):>8}  {_fmt(m['lat_p50_ms']):>7}  {_fmt(m['lat_p99_ms']):>7}  "
            f"{_fmt(m['throughput_ops_per_s']):>10}  {csok:>10}"
        )
        # per-server detail
        for idx, (f_cnt, s_cnt) in enumerate(
            zip(m["per_server_fast"], m["per_server_slow"]), start=1
        ):
            tot = f_cnt + s_cnt
            pct = f"{f_cnt / tot * 100:.1f}%" if tot > 0 else "  n/a"
            print(f"      server {idx}: fast={f_cnt:4d}  slow={s_cnt:4d}  fast%={pct}")
    print(sep)
    print()

    # Hypothesis check
    if len(results) >= 2:
        first_fast = results[0]["metrics"]["fast_ratio"]
        last_fast  = results[-1]["metrics"]["fast_ratio"]
        if first_fast > last_fast:
            print("HYPOTHESIS SUPPORTED: higher clock quality → higher fast-path ratio.")
        else:
            print("NOTE: fast-path ratio did not decrease with lower clock quality.")
            print("      On localhost, OWDs are near-zero so deadlines are almost always")
            print("      met regardless of uncertainty. Try adding artificial delay.")
        print()


def save_csv(results: list[dict]) -> None:
    out_path = LOGS_DIR / "benchmark_results.csv"
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "config_name", "uncertainty_us", "sync_interval_ms",
            "max_drift_us_per_s",
            "fast_path_count", "slow_path_count", "fast_ratio_pct",
            "lat_mean_ms", "lat_p50_ms", "lat_p99_ms",
            "throughput_ops_per_s", "total_completed", "consensus_ok",
        ])
        for r in results:
            c = r["clock"]
            m = r["metrics"]
            writer.writerow([
                c["name"],
                c["uncertainty"],
                c["sync_interval_ms"],
                c["drift_us_per_s"],
                m["total_fast"],
                m["total_slow"],
                _fmt(m["fast_ratio"], ".2f"),
                _fmt(m["lat_mean_ms"], ".3f"),
                _fmt(m["lat_p50_ms"], ".3f"),
                _fmt(m["lat_p99_ms"], ".3f"),
                _fmt(m["throughput_ops_per_s"], ".2f"),
                m["total_completed"],
                m["consensus_ok"],
            ])
    print(f"Results saved → {out_path}")


# ──────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="OmniPaxos clock-quality benchmark"
    )
    parser.add_argument(
        "--build", action="store_true",
        help="Run `cargo build` before starting experiments."
    )
    parser.add_argument(
        "--skip", nargs="*", default=[],
        metavar="NAME",
        help="Clock config names to skip (e.g. --skip low_quality)."
    )
    parser.add_argument(
        "--no-csv", action="store_true",
        help="Do not write benchmark_results.csv."
    )
    args = parser.parse_args()

    if args.build:
        build_binaries()
    else:
        ensure_built()

    results = []
    for clock in CLOCK_CONFIGS:
        if clock["name"] in (args.skip or []):
            print(f"==> Skipping: {clock['label']}")
            continue

        banner = f"  Running: {clock['label']}  "
        print(f"\n{'='*60}")
        print(banner)
        print(f"{'='*60}")

        metrics = run_experiment(clock)
        results.append({"clock": clock, "metrics": metrics})

        m = metrics
        csok = {True: "PASS", False: "FAIL", None: "empty log"}[m["consensus_ok"]]
        print(
            f"    fast={m['total_fast']:4d}  slow={m['total_slow']:4d}  "
            f"fast%={_fmt(m['fast_ratio'])}%  "
            f"lat_mean={_fmt(m['lat_mean_ms'])} ms  "
            f"tput={_fmt(m['throughput_ops_per_s'])} op/s  "
            f"consensus={csok}"
        )

    if results:
        print_results(results)
        if not args.no_csv:
            save_csv(results)


if __name__ == "__main__":
    main()
