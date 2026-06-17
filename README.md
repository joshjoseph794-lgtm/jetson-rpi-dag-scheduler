# Heterogeneous Edge Cluster Scheduling Engine

A high-performance, predictable framework for orchestrating Directed Acyclic Graph (DAG) task pipelines across heterogeneous edge computing topologies. The platform implements classic list-scheduling heuristics (**HEFT**, **PEFT**) along with greedy baselines (**Min-Min**, **Round-Robin**, **Random Allocation**), evaluating scheduling efficiency via structural simulation and live gRPC task dispatch.

---

## 🏗️ System Architecture

The ecosystem splits cleanly into a math-driven scheduling/simulation plane and a live gRPC task execution plane:

* **Orchestration & Simulation**: Evaluates DAG execution constraints using empirical performance profiles, computing precise **Makespan**, **Scheduling Length Ratio (SLR)**, **Parallel Speedup**, and **Cluster Utilization**.
* **Edge Worker Daemon**: A low-overhead gRPC infrastructure server running on target nodes (e.g., Laptop, Jetson, Raspberry Pi) featuring a high-precision, millisecond-accurate **Time-Triggered Clock Gate** for synchronized task releases.

---

## 📂 Project Directory Structure

```text
root_directory/
├── configs/
│   ├── experiment_matrix.json   # Defines algorithm execution suites
│   ├── workers.json             # Maps cluster nodes, IPs, and protocols
│   ├── task_registry.json       # Maps canonical task types to script assets
│   ├── worst_case_compute.json  # (Generated) Hardware runtime cost matrix (W)
│   └── worst_case_network.json  # (Generated) Network bandwidth/latency mapping
├── dags/
│   └── vision_pipeline.json     # Fork-join Diamond DAG layout (Capture->Resize/DNN->Log)
├── runtime/
│   ├── __init__.py
│   ├── messages.proto           # Protobuf binary schema definitions
│   ├── dispatcher_grpc.py       # Production gRPC low-latency task dispatcher
│   └── worker.py                # High-performance edge worker daemon
├── scheduler/
│   ├── __init__.py
│   ├── heft.py                  # Heterogeneous Earliest Finish Time algorithm
│   ├── peft.py                  # Predict Earliest Finish Time algorithm
│   ├── baseline.py              # Round-Robin, Random, and Min-Min algorithms
│   ├── cost_model.py            # Interfaces with empirical profiles
│   └── metrics.py               # SLR, Speedup, and Cluster Utilization math
├── workloads/
│   └── synthetic/
│       └── cpu_task.py          # Deterministic, volatile-safe synthetic task engine
├── requirements.txt             # Project library dependencies
├── profile_cluster.py           # Cluster hardware & network profiling engine
├── run_benchmarks.py            # Multi-iteration statistical simulation runner
├── plot_results.py              # Publication-quality visualization generator
├── evaluate.py                  # Lightweight standalone pipeline validation script
└── remote_worker.py             # (Legacy Backup) Isolated HTTP worker utility