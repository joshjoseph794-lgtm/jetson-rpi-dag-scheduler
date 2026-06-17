# run_benchmarks.py
import json
import os
import numpy as np
import networkx as nx

# ======================================================================
# 📦 UNIFIED SCHEDULING HEURISTICS IMPORTS
# ======================================================================
from scheduler.heft import allocate_tasks_heft
from scheduler.peft import allocate_tasks_peft
from scheduler.cpop import allocate_tasks_cpop
from scheduler.min_max import allocate_tasks_min_max
from scheduler.baseline import (
    allocate_tasks_round_robin,
    allocate_tasks_min_min,
    allocate_tasks_random
)
from scheduler.cost_model import CostModel

# ======================================================================
# 🗃️ ALGORITHM REGISTRY MAPPING
# ======================================================================
# Maps the string keys from your experiment_matrix.json directly 
# to the unique Python execution points inside your scheduler/ folder.
ALGORITHM_REGISTRY = {
    # Classic Baselines (from scheduler/baseline.py)
    "baseline": allocate_tasks_round_robin,      # Standard matrix control fallback
    "round_robin": allocate_tasks_round_robin,
    "min_min": allocate_tasks_min_min,
    "random": allocate_tasks_random,
    
    # Advanced Heterogeneous List Schedulers (Unique Files)
    "heft": allocate_tasks_heft,                 # from scheduler/heft.py
    "peft": allocate_tasks_peft,                 # from scheduler/peft.py
    "cpop": allocate_tasks_cpop,                 # from scheduler/cpop.py
    
    # Advanced Batch/Greedy Heuristic (Unique File)
    "min-max": allocate_tasks_min_max            # from scheduler/min_max.py
}

def load_json_asset(filepath):
    """Safely loads a JSON file from disk."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Required configuration asset missing at: {filepath}")
    with open(filepath, "r") as f:
        return json.load(f)

def build_networkx_dag(tasks):
    """Converts the normalized task-list execution pipeline into a NetworkX directed graph."""
    task_id_to_idx = {str(task["task_id"]): idx for idx, task in enumerate(tasks)}
    
    dag = nx.DiGraph()
    for idx, task in enumerate(tasks):
        dag.add_node(idx, task_id=str(task["task_id"]), task_type=task.get("task_type", "Capture"))
        
    DEFAULT_DATA_SIZE_MB = 1.50 

    for idx, task in enumerate(tasks):
        child_id = str(task["task_id"])
        deps = task.get("dependencies", [])
        
        for parent_id in deps:
            parent_str = str(parent_id)
            if parent_str in task_id_to_idx and child_id in task_id_to_idx:
                dag.add_edge(
                    task_id_to_idx[parent_str], 
                    task_id_to_idx[child_id], 
                    weight=task.get("data_size_mb", DEFAULT_DATA_SIZE_MB)
                )
    return dag

def generate_computation_matrix(tasks, workers, cost_model):
    """Constructs the W execution cost matrix (tasks x processors) from true profiles."""
    num_tasks = len(tasks)
    num_processors = len(workers)
    
    W = np.zeros((num_tasks, num_processors))
    for idx, task in enumerate(tasks):
        task_type = task.get("task_type", "Capture")
        for j, worker in enumerate(workers):
            # Extract names depending on dictionary layout style
            w_name = worker if isinstance(worker, str) else worker.get("name", f"worker_{j}")
            W[idx, j] = cost_model.get_computation_cost(w_name, task_type)
    return W

def normalize_dag_data(dag_data):
    """Translates incoming styles into a unified list schema layout format."""
    if isinstance(dag_data, list):
        return dag_data
        
    normalized_tasks = []
    nodes = dag_data.get("nodes", dag_data.get("tasks", []))
    edges = dag_data.get("edges", dag_data.get("dependencies", []))
    
    # Handle implicit linear sequencing flat blocks
    if not nodes and isinstance(dag_data, dict):
        return [dag_data]

    for node in nodes:
        task_id = node.get("task_id", node.get("id"))
        task_type = node.get("task_type", node.get("name"))
        
        dependencies = []
        for edge in edges:
            child = edge.get("to", edge.get("child"))
            parent = edge.get("from", edge.get("parent"))
            if str(child) == str(task_id):
                dependencies.append(parent)
        
        normalized_tasks.append({
            "task_id": str(task_id),
            "task_type": task_type,
            "dependencies": [str(p) for p in dependencies]
        })
        
    return normalized_tasks

# --- METRIC FALLBACK CALCULATORS TO ASSURE INDEPENDENT LOGGING ---
def calculate_scheduling_length_ratio(makespan, dag, W):
    min_costs_sum = sum(np.min(W[node, :]) for node in dag.nodes())
    return makespan / min_costs_sum if min_costs_sum > 0 else 0.0

def calculate_speedup(makespan, W):
    sequential_time = sum(np.mean(W, axis=1))
    return sequential_time / makespan if makespan > 0 else 0.0

def calculate_processor_utilization(schedule, W, num_workers, makespan):
    if makespan == 0: return 0.0
    total_active_computation = 0.0
    for task_idx, data in schedule.items():
        # Handle unpacking cleanly whether returned as (processor, est, eft) or a slice
        p_idx = data[0]
        total_active_computation += W[task_idx, p_idx]
    return total_active_computation / (num_workers * makespan)

def main():
    print("======================================================================")
    print("🚀 EDGE CLUSTER METRICS INTEGRATION & BENCHMARK HARNESS STARTED")
    print("======================================================================\n")

    # Standardize network parameters to your handbook's verified Gigabit Ethernet specs
    avg_bandwidth = 117.0  # 117 MB/s (940 Mbps GbE)

    # 1. Load Configurations and Infrastructure Lists
    try:
        matrix_cfg = load_json_asset("configs/experiment_matrix.json")
        
        # Use your explicit configuration filename
        workers_path = "configs/cluster_nodes.json"
        workers_raw = load_json_asset(workers_path)
        
        # DEFENSIVE PARSING: Safely extract the workers list regardless of JSON format
        if isinstance(workers_raw, list):
            workers = workers_raw
        elif isinstance(workers_raw, dict):
            workers = workers_raw.get("workers", list(workers_raw.values()))
        else:
            raise TypeError("Format of cluster_nodes.json must be a JSON array or object.")
            
    except Exception as e:
        print(f"❌ Initialization aborted: {e}")
        return

    global_settings = matrix_cfg.get("global_settings", {})
    iterations = global_settings.get("iterations_per_config", 1)
    output_log_file = global_settings.get("metrics_log_file", "outputs/benchmark_results.json")
    
    cost_model = CostModel()
    
    benchmark_report = {
        "test_suite_name": matrix_cfg.get("test_suite_name", "Edge Performance Run"),
        "cluster_size_nodes": len(workers),
        "results": {}
    }

    # 2. Iterate through the Experiment Matrix
    matrix_runs = matrix_cfg.get("matrix", []) if isinstance(matrix_cfg, dict) else matrix_cfg
    
    for exp in matrix_runs:
        exp_id = exp.get("experiment_id", f"exp_{exp.get('algorithm')}")
        algo_key = exp.get("algorithm", "").lower()
        dag_path = exp.get("workload_dag", "dags/vision_pipeline.json")
        enabled = exp.get("enabled", True)

        if not enabled:
            print(f"⏭️ Skipping experiment [{exp_id}] (Disabled in matrix layout)")
            continue

        if algo_key not in ALGORITHM_REGISTRY:
            print(f"⚠️ Skipping experiment [{exp_id}]: Algorithm '{algo_key}' is unregistered.")
            continue

        print(f"📊 Running Evaluation Suite: {exp_id.upper()} ({algo_key.upper()})")
        print(f"   Target DAG Workload: {dag_path} | Executing {iterations} tracking loops...")

        try:
            dag_raw = load_json_asset(dag_path)
            normalized_tasks = normalize_dag_data(dag_raw)  
            dag = build_networkx_dag(normalized_tasks)
            comp_matrix = generate_computation_matrix(normalized_tasks, workers, cost_model)
        except Exception as e:
            print(f"   ❌ Failed to load DAG infrastructure for this experiment loop: {e}\n")
            continue

        makespans, slrs, speedups, utilizations, ccrs = [], [], [], [], []
        schedule_results = {}

        # 3. Statistical Execution Loop
        for i in range(iterations):
            scheduler_func = ALGORITHM_REGISTRY[algo_key]
            
            # 😎 UNIFIED UNIFORM CALL FOR ALL ALGORITHMS
            makespan, schedule_results = scheduler_func(dag, comp_matrix, avg_bandwidth, workers)
            # Calculate metrics
            slr = calculate_scheduling_length_ratio(makespan, dag, comp_matrix)
            speedup = calculate_speedup(makespan, comp_matrix)
            pu = calculate_processor_utilization(schedule_results, comp_matrix, len(workers), makespan)

            # Calculate communication-to-computation ratios
            avg_comp_cost = np.mean(comp_matrix)
            edge_weights = [dag[u][v].get('weight', 0.0) for u, v in dag.edges()]
            avg_comm_cost = np.mean(edge_weights) if edge_weights else 0.0
            ccr_value = avg_comm_cost / avg_comp_cost if avg_comp_cost > 0 else 0.0

            makespans.append(makespan)
            slrs.append(slr)
            speedups.append(speedup)
            utilizations.append(pu)
            ccrs.append(ccr_value)

        # 4. Compile and Average Results 
        avg_results = {
            "algorithm": algo_key,
            "metrics": {
                "makespan_seconds_avg": float(np.mean(makespans)),
                "scheduling_length_ratio_avg": float(np.mean(slrs)),
                "parallel_speedup_avg": float(np.mean(speedups)),
                "cluster_processor_utilization_avg": float(np.mean(utilizations)),
                "communication_to_computation_ratio_avg": float(np.mean(ccrs))
            }
        }
        
        benchmark_report["results"][exp_id] = avg_results
        
        m = avg_results["metrics"]
        ccr_status = "Network-Bound" if m['communication_to_computation_ratio_avg'] > 1.0 else "Compute-Bound"
        print(f"   🏁 Completed! Avg Makespan: {m['makespan_seconds_avg']:.4f}s | SLR: {m['scheduling_length_ratio_avg']:.3f} | Speedup: {m['parallel_speedup_avg']:.2f}x | Utilization: {m['cluster_processor_utilization_avg']*100:.1f}%")
        print(f"       Workload Characteristic Profile: CCR = {m['communication_to_computation_ratio_avg']:.4f} ({ccr_status})")
        print("-" * 70)

        # 4.5 Export Human-Readable Schedule Trace Files
        trace_dir = "outputs/schedules"
        os.makedirs(trace_dir, exist_ok=True)
        trace_file = os.path.join(trace_dir, f"{algo_key}_trace.txt")
        
        with open(trace_file, "w") as tf:
            tf.write(f"=== PHYSICAL SCHEDULE TRACE FOR LOG: {exp_id.upper()} ===\n")
            tf.write(f"Algorithm Strategy: {algo_key.upper()}\n")
            tf.write(f"Calculated Pipeline Makespan: {avg_results['metrics']['makespan_seconds_avg']:.4f}s\n")
            tf.write(f"Workload Profile CCR Value : {avg_results['metrics']['communication_to_computation_ratio_avg']:.4f}\n")
            tf.write("-" * 60 + "\n")
            tf.write(f"{'Task Index':<12} | {'Assigned Node':<15} | {'EST (s)':<10} | {'EFT (s)':<10}\n")
            tf.write("-" * 60 + "\n")
            
            for t_idx in sorted(schedule_results.keys()):
                val = schedule_results[t_idx]
                p_idx, est, eft = val[0], val[1], val[2]
                node_name = workers[p_idx]["name"] if isinstance(workers[p_idx], dict) else f"Worker_{p_idx}"
                tf.write(f"Task {t_idx:<7} | {node_name:<15} | {est:<10.4f} | {eft:<10.4f}\n")

    # 5. Export Compiled Telemetry Log to Disk
    if os.path.dirname(output_log_file):
        os.makedirs(os.path.dirname(output_log_file), exist_ok=True)
    with open(output_log_file, "w") as f:
        json.dump(benchmark_report, f, indent=2)

    print(f"\n✅ All benchmarks successfully logged to: {output_log_file}")
    print("======================================================================\n")

if __name__ == "__main__":
    main()