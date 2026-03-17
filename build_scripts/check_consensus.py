#!/usr/bin/env python3
"""
check_consensus.py
==================
Verify that all replicas reached the same state and report fast/slow-path
counts for the most recently completed run.

Exit codes:
  0  — consensus reached (or log is empty / read-only)
  1  — divergence detected
  2  — one or more log files are missing
"""
import json, sys, os

script_dir = os.path.dirname(os.path.abspath(__file__))
files = [os.path.join(script_dir, f'logs/server-{i}.json') for i in range(1, 4)]

# ── Load server output files ──────────────────────────────────────
data = []
missing = []
for f in files:
    if not os.path.exists(f):
        missing.append(f)
    else:
        with open(f) as fh:
            data.append(json.load(fh))

if missing:
    print("ERROR: the following server log files were not found:")
    for m in missing:
        print(f"  {m}")
    print("Run the cluster first (e.g. ./run-and-check-consensus.sh).")
    sys.exit(2)

# ── Extract fields ────────────────────────────────────────────────
lens = [d.get('synced_log_len', 0) for d in data]
logs = [d.get('synced_log', [])    for d in data]
fast = [d.get('fast_path_count', 0) for d in data]
slow = [d.get('slow_path_count', 0) for d in data]

# ── Clock config (if present) ─────────────────────────────────────
clocks = [d.get('clock', {}) for d in data]
clock0 = clocks[0] if clocks else {}
if clock0:
    unc  = clock0.get('uncertainty', '?')
    sync = clock0.get('sync_interval_ms', '?')
    drift = clock0.get('drift_us_per_s', '?')
    print(f"Clock config:  uncertainty=±{unc} µs  sync_interval={sync} ms  drift={drift} µs/s")
    print()

# ── Synced-log summary ────────────────────────────────────────────
print(f"Synced-log lengths : {lens}")
print(f"Synced-log match   : {logs[0] == logs[1] == logs[2]}")
print()

# ── Fast / slow path breakdown ────────────────────────────────────
total_fast = sum(fast)
total_slow = sum(slow)
total_ops  = total_fast + total_slow
fast_ratio = (total_fast / total_ops * 100.0) if total_ops > 0 else 0.0

print("Fast / slow path counts  (tallied on coordinator/proxy nodes only):")
for i, (f_cnt, s_cnt) in enumerate(zip(fast, slow), start=1):
    tot = f_cnt + s_cnt
    pct = f"{f_cnt / tot * 100:.1f}%" if tot > 0 else "n/a"
    print(f"  Server {i}: fast={f_cnt:5d}  slow={s_cnt:5d}  total={tot:5d}  fast%={pct}")
print(
    f"  Cluster : fast={total_fast:5d}  slow={total_slow:5d}  "
    f"total={total_ops:5d}  fast%={fast_ratio:.1f}%"
)
print()

# ── Consensus safety check ────────────────────────────────────────
if logs[0] != logs[1] or logs[1] != logs[2]:
    print("!!! DIVERGENCE DETECTED in synced_log:")
    n = max(len(l) for l in logs)
    for i in range(n):
        entries = [logs[j][i] if i < len(logs[j]) else 'MISSING' for j in range(3)]
        if not all(e == entries[0] for e in entries):
            print(f"  index {i}: {entries}")
    sys.exit(1)
else:
    if all(l == 0 for l in lens):
        print("  (synced_log is empty — no write requests were decided yet)")
    else:
        print("==> CONSENSUS REACHED: all replicas have identical synced logs.")
