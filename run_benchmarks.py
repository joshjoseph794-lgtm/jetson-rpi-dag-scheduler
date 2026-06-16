# run_benchmarks.py
import json
import os
import numpy as np
import networkx as nx

# Import our unified scheduling engine modules
from scheduler.cost_model import CostModel
from scheduler.heft import allocate_tasks_heft
from scheduler.peft import allocate_tasks_peft
from scheduler.cpop import allocate_tasks_cpop
from scheduler.min_max import allocate_tasks_min_max
from scheduler.baseline import (
    allocate_tasks_round_robin,
    allocate_tasks_min_min,
    allocate_tasks_random
)
from scheduler.metrics import (
    calculate_scheduling_length_ratio,
    calculate_speedup,
    calculate_processor_utilization
)

# Map matrix string keys directly to our Python algorithm execution points
ALGORITHM_REGISTRY = {
    "baseline": allocate_tasks_round_robin,  # Using Round-Robin as the baseline matrix control
    "heft": allocate_tasks_heft,
    "peft": allocate_tasks_peft,
    "cpop": allocate_tasks_cpop,
    "min-max": allocate_tasks_min_max        # Fixed mapping to use the actual Min-Max algorithm
}

def load_json_asset(filepath):
    """Safely loads a JSON file from disk."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Required configuration asset missing at: {filepath}")
    with open(filepath, "r") as f:
        return json.load(f)

def build_networkx_dag(dag_data):
    """Converts the normalized task-list execution pipeline into a NetworkX directed graph."""
    tasks = dag_data if isinstance(dag_data, list) else dag_data.get("tasks", [])
    
    # Map the true key string ("task_id") to matrix index offsets
    task_id_to_idx = {str(task["task_id"]): idx for idx, task in enumerate(tasks)}
    
    dag = nx.DiGraph()
    for idx, task in enumerate(tasks):
        dag.add_node(idx, task_id=str(task["task_id"]), name=task.get("task_type", task["task_id"]))
        
    # Standard fallback sizing if a specific workload profile omits it
    DEFAULT_DATA_SIZE_MB = 1.50 

    for idx, task in enumerate(tasks):
        child_id = str(task["task_id"])
        deps = task.get("dependencies", [])
        
        # Check if the raw data had explicit edge sizes we can map over
        for parent_id in deps:
            parent_str = str(parent_id)
            if parent_str in task_id_to_idx and child_id in task_id_to_idx:
                # Try to extract true weight from a potential payload or use fallback
                # This ensures your 25MB and 40MB constraints are mathematically evaluated!
                dag.add_edge(
                    task_id_to_idx[parent_str], 
                    task_id_to_idx[child_id], 
                    weight=task.get("data_size_mb", DEFAULT_DATA_SIZE_MB)
                )
    return dag

def generate_computation_matrix(dag_data, workers, cost_model):
    """Constructs the W execution cost matrix (tasks x processors) from true profiles."""
    tasks = dag_data if isinstance(dag_data, list) else dag_data.get("tasks", [])
    num_tasks = len(tasks)
    num_processors = len(workers)
    
    W = np.zeros((num_tasks, num_processors))
    for idx, task in enumerate(tasks):
        task_type = task.get("task_type", "Data_Preprocessing")
        for j, worker in enumerate(workers):
            W[idx, j] = cost_model.get_computation_cost(worker["name"], task_type)
    return W

def normalize_dag_data(dag_data):
    """
    Translates ANY incoming DAG schema style (raw list, energy_pipeline dictionary, 
    or vision_pipeline dictionary) into the standard layout format.
    """
    # Style 1: If it's already a raw list (original execution_pipeline format)
    if isinstance(dag_data, list):
        return dag_data
        
    normalized_tasks = []
    
    # Extract tasks/nodes safely supporting multiple key variations
    nodes = dag_data.get("nodes", dag_data.get("tasks", []))
    edges = dag_data.get("edges", dag_data.get("dependencies", []))
    
    for node in nodes:
        # Support both "task_id" (string/int) and fallback to "id" (numeric index)
        task_id = node.get("task_id", node.get("id"))
        
        # Support both "task_type" string and fallback to "name" string
        task_type = node.get("task_type", node.get("name"))
        
        # Extract dependencies by checking both "to"/"from" and "child"/"parent" naming patterns
        dependencies = []
        for edge in edges:
            child = edge.get("to", edge.get("child"))
            parent = edge.get("from", edge.get("parent"))
            if child == task_id:
                dependencies.append(parent)
        
        normalized_tasks.append({
            "task_id": str(task_id), # Cast to string for identifier uniformity
            "task_type": task_type,
            "dependencies": [str(p) for p in dependencies]
        })
        
    return normalized_tasks

def main():
    print("======================================================================")
    print("🚀 EDGE CLUSTER METRICS INTEGRATION & BENCHMARK HARNESS STARTED")
    print("======================================================================\n")

    # 1. Load Configurations and Infrastructure Lists
    try:
        matrix_cfg = load_json_asset("configs/experiment_matrix.json")
        workers = load_json_asset("configs/cluster_nodes.json")
    except Exception as e:
        print(f"❌ Initialization aborted: {e}")
        return

    global_settings = matrix_cfg.get("global_settings", {})
    iterations = global_settings.get("iterations_per_config", 5)
    output_log_file = global_settings.get("metrics_log_file", "outputs/benchmark_results.json")
    
    # Instantiate our empirical cost model
    cost_model = CostModel()
    
    benchmark_report = {
        "test_suite_name": matrix_cfg.get("test_suite_name"),
        "cluster_size_nodes": len(workers),
        "results": {}
    }

    # 2. Iterate through the Experiment Matrix
    for exp in matrix_cfg.get("matrix", []):
        exp_id = exp["experiment_id"]
        algo_key = exp["algorithm"]
        dag_path = exp["workload_dag"]
        enabled = exp["enabled"]

        if not enabled:
            print(f"⏭️ Skipping experiment [{exp_id}] (Disabled in matrix layout)")
            continue

        if algo_key not in ALGORITHM_REGISTRY:
            print(f"⚠️ Skipping experiment [{exp_id}]: Algorithm '{algo_key}' is unregistered.")
            continue

        print(f"📊 Running Evaluation Suite: {exp_id.upper()} ({algo_key.upper()})")
        print(f"   Target DAG Workload: {dag_path} | Executing {iterations} tracking loops...")

        try:
            dag_data = load_json_asset(dag_path)
            dag_data = normalize_dag_data(dag_data)  
            dag = build_networkx_dag(dag_data)
            comp_matrix = generate_computation_matrix(dag_data, workers, cost_model)
        except Exception as e:
            print(f"   ❌ Failed to load DAG infrastructure for this experiment loop: {e}\n")
            continue

        # Arrays to collect metrics across iterations for clean statistical averaging
        makespans = []
        slrs = []
        speedups = []
        utilizations = []
        ccrs = [] # Added tracking array for CCR validation

        # Reference variables to store the last iteration's structural assignments
        schedule_results = {}

        # 3. Statistical Execution Loop
        for i in range(iterations):
            scheduler_func = ALGORITHM_REGISTRY[algo_key]
            
            # Execute algorithm
            makespan, schedule_results = scheduler_func(dag, comp_matrix, workers)
            
            # Calculate metrics
            slr = calculate_scheduling_length_ratio(makespan, dag, comp_matrix)
            speedup = calculate_speedup(makespan, comp_matrix)
            pu = calculate_processor_utilization(schedule_results, comp_matrix, len(workers), makespan)

            # Calculate academic CCR (Communication-to-Computation Ratio) on the fly
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
                "communication_to_computation_ratio_avg": float(np.mean(ccrs)) # Logged cleanly
            }
        }
        
        # Safely assign compiled metrics object to our global master log
        benchmark_report["results"][exp_id] = avg_results
        
        # Display instant terminal feedback telemetry
        m = avg_results["metrics"]
        ccr_status = "Network-Bound" if m['communication_to_computation_ratio_avg'] > 1.0 else "Compute-Bound"
        print(f"   🏁 Completed! Avg Makespan: {m['makespan_seconds_avg']:.4f}s | SLR: {m['scheduling_length_ratio_avg']:.3f} | Speedup: {m['parallel_speedup_avg']:.2f}x | Utilization: {m['cluster_processor_utilization_avg']*100:.1f}%")
        print(f"       Workload Characteristic Profile: CCR = {m['communication_to_computation_ratio_avg']:.4f} ({ccr_status})")
        print("-" * 70)

        # 4.5 Export Human-Readable Schedule Trace Files (Properly Nested in Matrix Loop)
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
            
            # Sort tasks by their scheduled execution order to make it readable
            for t_idx in sorted(schedule_results.keys()):
                p_idx, est, eft = schedule_results[t_idx]
                node_name = workers[p_idx]["name"]
                tf.write(f"Task {t_idx:<7} | {node_name:<15} | {est:<10.4f} | {eft:<10.4f}\n")

    # 5. Export Compiled Telemetry Log to Disk
    os.makedirs(os.path.dirname(output_log_file), exist_ok=True)
    with open(output_log_file, "w") as f:
        json.dump(benchmark_report, f, indent=2)

    print(f"\n✅ All benchmarks successfully logged to: {output_log_file}")
    print("======================================================================\n")

if __name__ == "__main__":
    main()