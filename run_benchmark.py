import json
import subprocess
import time
import os
import signal

# The 3 Configurations from your benchmark requirements
CONFIGS = [
    {"name": "High Quality", "uncertainty": 10, "sync_interval_ms": 1},
    {"name": "Medium Quality", "uncertainty": 100, "sync_interval_ms": 10},
    {"name": "Low Quality", "uncertainty": 1000, "sync_interval_ms": 100},
]

NUM_NODES = 3

def create_configs(uncertainty, sync_interval_ms):
    """Generates cluster and server configuration files dynamically."""
    cluster_config = {
        "nodes": [1, 2, 3],
        "node_addrs": ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"],
        "initial_leader": 1,
        "initial_flexible_quorum": None
    }
    with open("cluster.json", "w") as f:
        json.dump(cluster_config, f)

    for i in range(1, NUM_NODES + 1):
        server_config = {
            "location": f"Node{i}",
            "server_id": i,
            "listen_address": "127.0.0.1",
            "listen_port": 8000 + i,
            "num_clients": 1 if i == 1 else 0, # Only connect the client to Node 1
            "output_filepath": f"output_node_{i}.json",
            "drift_us_per_s": 0.0,
            "uncertainty": uncertainty,
            "sync_interval_ms": sync_interval_ms
        }
        with open(f"server_{i}.json", "w") as f:
            json.dump(server_config, f)

def run_benchmark(config):
    print(f"\n{'='*50}")
    print(f"Running Benchmark: {config['name']} (±{config['uncertainty']}μs)")
    print(f"{'='*50}")

    create_configs(config['uncertainty'], config['sync_interval_ms'])

    server_processes = []

    # 1. Start the cluster nodes
    for i in range(1, NUM_NODES + 1):
        env = os.environ.copy()
        env["CLUSTER_CONFIG_FILE"] = "cluster.json"
        env["SERVER_CONFIG_FILE"] = f"server_{i}.json"

        # Spawning the server binary
        p = subprocess.Popen(["cargo", "run", "--release", "--bin", "server"], env=env)
        server_processes.append(p)

    print("Waiting 5 seconds for cluster to initialize and elect a leader...")
    time.sleep(5)

    # 2. Start the Load Client
    print("Starting load generation...")
    # This calls the client code we will set up below
    subprocess.run(["cargo", "run", "--release", "--bin", "load_client"])

    # 3. Graceful Shutdown (Triggers the servers to dump their stats to JSON)
    print("Shutting down cluster to flush stats...")
    for p in server_processes:
        p.send_signal(signal.SIGTERM)
        p.wait()

    # 4. Parse Server Metrics from Node 1
    try:
        with open("output_node_1.json", "r") as f:
            stats = json.load(f)

        fast_path = stats.get("fast_path_count", 0)
        slow_path = stats.get("slow_path_count", 0)
        total_requests = fast_path + slow_path

        fast_path_ratio = (fast_path / total_requests) * 100 if total_requests > 0 else 0

        print(f"\n--- Server Output Metrics ---")
        print(f"Total Consensus Requests Processed: {total_requests}")
        print(f"Fast-Path Ratio: {fast_path_ratio:.2f}% ({fast_path} fast / {slow_path} slow)")

    except FileNotFoundError:
        print("Error: output_node_1.json not found. Check if the server panicked or failed to shut down.")

if __name__ == "__main__":
    # Compile everything once before starting the timer-sensitive tests
    print("Compiling project in release mode...")
    subprocess.run(["cargo", "build", "--release"])

    for config in CONFIGS:
        run_benchmark(config)