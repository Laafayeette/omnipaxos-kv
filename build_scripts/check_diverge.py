#!/usr/bin/env python3
import json

logs = []
for i in range(1, 4):
    with open(f'logs/server-{i}.json') as f:
        logs.append(json.load(f))

for i, lg in enumerate(logs, 1):
    sl = lg.get('synced_log', [])
    print(f'Server {i}: synced_log len={len(sl)}')

# Compare requests only
rlogs = [[e.get('request') for e in lg.get('synced_log', [])] for lg in logs]

lengths_match = all(len(r) == len(rlogs[0]) for r in rlogs)

if not lengths_match:
    for i, r in enumerate(rlogs, 1):
        print(f'  Server {i}: len={len(r)}')

min_len = min(len(r) for r in rlogs)
diverge_count = 0
for idx in range(min_len):
    if not all(rlogs[s][idx] == rlogs[0][idx] for s in range(len(rlogs))):
        diverge_count += 1
        if diverge_count <= 10:
            print(f'DIVERGE at index {idx}:')
            for s in range(len(rlogs)):
                print(f'  Server {s+1}: {rlogs[s][idx]}')

print(f'Total divergences in first {min_len} entries: {diverge_count}')

# Check if they're just reordered (same set)
sets = [set(json.dumps(e, sort_keys=True) for e in r) for r in rlogs]
print(f'Same set of requests (ignoring order): {all(s == sets[0] for s in sets)}')
print(f'Set sizes: {[len(s) for s in sets]}')
