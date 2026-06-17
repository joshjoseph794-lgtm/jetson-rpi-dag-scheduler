# evaluate.py
import numpy as np
import json
import os

# =====================================================================
# 1. ROBUST MATHEMATICAL PERFORMANCE METRICS
# =====================================================================
def calculate_scheduling_length_ratio(makespan, dag, comp_matrix):
    """
    SLR divides the scheduled makespan by the lower bound execution 
    cost of tasks along the critical path. Handles dictionary matrix formats cleanly.
    """
    min_costs_sum = 0.0
    # Map nodes to string identifiers to safely scan your dictionary matrix layout
    for node in dag.nodes():
        node_id = str(node)
        task_type = dag.nodes[node].get("task_type", node_id)
        
        # Pull performance metrics across all active cluster worker profiles
        costs = []
        for worker in comp_matrix:
            if task_type in comp_matrix[worker]:
                costs.append(comp_matrix[worker][task_type])
            elif node_id in comp_matrix[worker]:
                costs.append(comp_matrix[worker][node_id])
                
        if costs:
            min_costs_sum += min(costs)
            
    if min_costs_sum == 0:
        return 0.0
    return makespan / min_costs_sum

def calculate_speedup(makespan, comp_matrix):
    """
    Speedup evaluates processing efficiency gains relative to isolated serial execution.
    Calculates the sum of average sequential costs across all unique tasks.
    """
    sequential_time = 0.0
    # Average task costs over your heterogeneous nodes
    for worker in comp_matrix:
        worker_costs = list(comp_matrix[worker].values())
        if worker_costs:
            sequential_time += sum(worker_costs) / len(comp_matrix)
            
    if makespan == 0:
        return 0.0
    return sequential_time / makespan

# =====================================================================
# 2. CONFIG-DRIVEN BENCHMARK EXECUTION PIPELINE
# =====================================================================
if __name__ == "__main__":
    print("====== EDGE PERFORMANCE BENCHMARKING ENGINE ======")
    
    # Standardize network parameters to your handbook's verified Gigabit Ethernet specs
    avg_bandwidth = 117.0  # 117 MB/s (940 Mbps GbE)
    
    # Import the layout loader and all algorithms from your scheduler package
    from scheduler.heft import load_cluster_infrastructure, calculate_upward_ranks, allocate_tasks_heft
    from scheduler.baseline import allocate_tasks_round_robin, allocate_tasks_min_min, allocate_tasks_random
    from scheduler.peft import calculate_oct_matrix, allocate_tasks_peft

    # Dynamically track down the designated active execution DAG file
    target_dag_file = "dags/vision_pipeline.json"  # Safe Fallback Default
    matrix_path = "configs/experiment_matrix.json" if os.path.exists("configs/experiment_matrix.json") else "config/experiment_matrix.json"
    
    if os.path.exists(matrix_path):
        try:
            with open(matrix_path, "r") as f:
                matrix_data = json.load(f)
                if isinstance(matrix_data, list) and len(matrix_data) > 0:
                    target_dag_file = matrix_data[0].get("workload_dag", target_dag_file)
                elif isinstance(matrix_data, dict):
                    target_dag_file = matrix_data.get("workload_dag", target_dag_file)
            print(f"🎯 [MATRIX] Benchmark targeting file layout: {target_dag_file}")
        except Exception as e:
            print(f"⚠️ [MATRIX] Reading error. Reverting to default: {e}")
    else:
        print(f"⚠️ [MATRIX] Configuration missing at {matrix_path}. Using default.")

    # Target the unified workers setup manifest 
    workers_config = "configs/workers.json" if os.path.exists("configs/workers.json") else "config/workers.json"

    # Parse production manifests cleanly using your custom infrastructure loader
    dag, W, worker_list, names_map = load_cluster_infrastructure(
        dag_file=target_dag_file,
        workers_file=workers_config
    )
    
    print(f"Graph Layout: {len(dag.nodes())} Tasks loaded from manifest.")
    print("Executing all policies across identical network topologies...\n")
    
    # Execute Competitors
    rr_makespan, _ = allocate_tasks_round_robin(dag, W, avg_bandwidth)
    rand_makespan, _ = allocate_tasks_random(dag, W, avg_bandwidth)
    minmin_makespan, _ = allocate_tasks_min_min(dag, W, avg_bandwidth)
    
    # Execute HEFT
    ranks = calculate_upward_ranks(dag, W, avg_bandwidth)
    heft_makespan, _ = allocate_tasks_heft(dag, W, ranks, avg_bandwidth)
    
    # Execute PEFT
    oct_table = calculate_oct_matrix(dag, W, avg_bandwidth)
    peft_makespan, _ = allocate_tasks_peft(dag, W, oct_table, avg_bandwidth)
    
    # --- Print Comparative Results Summary Table for Your Paper ---
    print("----------------------------------------------------------------------------")
    print(f"| {'Scheduling Policy':<23} | {'Makespan (s)':<12} | {'SLR':<10} | {'Speedup':<10} |")
    print("----------------------------------------------------------------------------")
    
    for name, makespan in [
        ("1. Naive Round-Robin", rr_makespan),
        ("2. Random Allocation", rand_makespan),
        ("3. Min-Min Heuristic", minmin_makespan),
        ("4. HEFT (Optimized)", heft_makespan),
        ("5. PEFT (Predictive)", peft_makespan)
    ]:
        slr = calculate_scheduling_length_ratio(makespan, dag, W)
        speedup = calculate_speedup(makespan, W)
        print(f"| {name:<23} | {makespan:<12.2f} | {slr:<10.2f} | {speedup:<10.2f} |")
        
    print("----------------------------------------------------------------------------")