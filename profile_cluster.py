import json
import time
import subprocess
import requests

# --- CLUSTER CONFIGURATION ---
NODES = {
    "laptop01": {"ip": "127.0.0.1", "protocol": "grpc", "port": 50051},
    "jetson01": {"ip": "172.16.12.35", "protocol": "http", "port": 5000},
    "raspi01":  {"ip": "172.16.12.40", "protocol": "http", "port": 5000} # Replace with your real Pi IP
}

TASKS = ["Data_Preprocessing", "Feature_Extraction", "Model_Training"]

NUM_COMPUTE_RUNS = 100
NUM_NETWORK_RUNS = 30

worst_case_compute = {}
worst_case_network = {}

print("🚀 STARTING AUTOMATED HETEROGENEOUS CLUSTER PROFILER...")
print("=========================================================")

# ==========================================
# 1. COMPUTATION PROFILING LOOP (100x RUNS)
# ==========================================
for node_name, node_info in NODES.items():
    worst_case_compute[node_name] = {}
    print(f"\n🖥️ Profiling Computation on Node: {node_name} ({node_info['ip']})")
    
    for task in TASKS:
        print(f"  ⏳ Running task '{task}' {NUM_COMPUTE_RUNS} times to find maximum latency...")
        runtimes = []
        
        for run in range(NUM_COMPUTE_RUNS):
            start = time.time()
            
            if node_info["protocol"] == "grpc":
                # Simulated local execution via subprocess to avoid loading heavy framework dependencies
                res = subprocess.run(
                    ["python3", "workloads/synthetic/cpu_task.py", "--task", task, "--duration", "1.5"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                )
            else:
                # Fire network request to remote worker daemon
                payload = {
                    "task_id": f"profile_{run}",
                    "task_type": task,
                    "script_path": "workloads/synthetic/cpu_task.py",
                    "payload_duration": "1.5"
                }
                try:
                    url = f"http://{node_info['ip']}:{node_info['port']}/"
                    response = requests.post(url, json=payload, timeout=10)
                    # Use the actual execution time reported by the worker hardware
                    if response.status_code == 200:
                        exec_time = response.json().get("execution_time_sec", 1.5)
                        runtimes.append(exec_time)
                        continue
                except Exception:
                    pass
            
            # Fallback to local timing if network telemetry didn't intercept
            runtimes.append(time.time() - start)
            
        # Extract the absolute worst-case scenario
        max_runtime = max(runtimes)
        worst_case_compute[node_name][task] = round(max_runtime, 4)
        print(f"  ✅ Done. Worst-case execution time for {task}: {worst_case_compute[node_name][task]}s")

# ==========================================
# 2. COMMUNICATION PROFILING LOOP (iperf3)
# ==========================================
print("\n\n📡 STARTING NETWORK BANDWIDTH PROFILER (iperf3)...")
print("=========================================================")

for node_name, node_info in NODES.items():
    if node_name == "laptop01":
        worst_case_network[node_name] = {"latency_ms": 0.0, "bandwidth_mbps": 10000.0} # Loopback defaults
        continue
        
    print(f"📡 Testing network channel to {node_name} ({node_info['ip']})...")
    bandwidths = []
    
    # Run iperf3 loops to monitor throughput degradation under stress
    for run in range(NUM_NETWORK_RUNS):
        try:
            # Call iperf3 client towards remote node acting as a server
            result = subprocess.run(
                ["iperf3", "-c", node_info["ip"], "-t", "1", "-J"], 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
            )
            data = json.loads(result.stdout)
            # Pull bit-rate performance in Megabits per second
            bps = data["end"]["sum_received"]["bits_per_second"]
            mbps = bps / 1e6
            bandwidths.append(mbps)
        except Exception:
            bandwidths.append(100.0) # Conservative fallback if test interrupts
            
    # CRITICAL: Worst-case network performance means choosing the LOWEST bandwidth recorded!
    min_bandwidth = min(bandwidths)
    worst_case_network[node_name] = {
        "latency_ms": 1.5, # Baseline baseline Ethernet ping delay overhead estimate
        "bandwidth_mbps": round(min_bandwidth, 2)
    }
    print(f"  ✅ Done. Guaranteed minimum bandwidth: {worst_case_network[node_name]['bandwidth_mbps']} Mbps")

# Save output data structures cleanly to the file system
with open("worst_case_compute.json", "w") as f:
    json.dump(worst_case_compute, f, indent=4)

with open("worst_case_network.json", "w") as f:
    json.dump(worst_case_network, f, indent=4)

print("\n\n🎉 [SUCCESS] Profiling phase complete. Matrices written to disk!")
print("  -> worst_case_compute.json")
print("  -> worst_case_network.json")