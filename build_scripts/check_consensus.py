#!/usr/bin/env python3
"""Compare server output files to verify all replicas reached the same state."""
import json, sys, os

script_dir = os.path.dirname(os.path.abspath(__file__))
files = [os.path.join(script_dir, f'logs/server-{i}.json') for i in range(1, 4)]

data = []
for f in files:
    with open(f) as fh:
        data.append(json.load(fh))

lens  = [d.get('synced_log_len', 'N/A') for d in data]
logs  = [d.get('synced_log', [])         for d in data]
fast  = [d.get('fast_path_count', 'N/A') for d in data]
slow  = [d.get('slow_path_count', 'N/A') for d in data]

print(f"synced_log lengths:  {lens}")
print(f"synced_log match:    {logs[0] == logs[1] == logs[2]}")
print(f"fast_path_count:     {fast}  (only counted on proxy nodes)")
print(f"slow_path_count:     {slow}  (only counted on proxy nodes)")

if logs[0] != logs[1] or logs[1] != logs[2]:
    print("\n!!! DIVERGENCE DETECTED in synced_log:")
    n = max(len(l) for l in logs)
    for i in range(n):
        entries = [logs[j][i] if i < len(logs[j]) else 'MISSING' for j in range(3)]
        if not all(e == entries[0] for e in entries):
            print(f"  index {i}: {entries}")
    sys.exit(1)
else:
    if all(l == 0 for l in lens):
        print("\n  (synced_log is empty — no write requests were decided yet)")
    else:
        print("\n==> CONSENSUS REACHED: all replicas have identical synced logs.")
