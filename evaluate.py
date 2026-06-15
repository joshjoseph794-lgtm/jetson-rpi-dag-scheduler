# evaluate.py
import numpy as np

# 1. Keep your existing metric functions exactly as they are
def calculate_scheduling_length_ratio(makespan, dag, comp_matrix):
    """
    SLR divides the scheduled makespan by the lower bound execution 
    cost of tasks along the critical path.
    """
    min_costs_sum = sum(np.min(comp_matrix[node, :]) for node in dag.nodes())
    if min_costs_sum == 0:
        return 0
    return makespan / min_costs_sum

def calculate_speedup(makespan, comp_matrix):
    """Speedup evaluates processing efficiency gains relative to isolated serial execution."""
    sequential_time = sum(np.mean(comp_matrix, axis=1))
    return sequential_time / makespan

# =====================================================================
# 2. Permanent Config-Driven Benchmark Execution
# =====================================================================
if __name__ == "__main__":
    print("====== EDGE PERFORMANCE BENCHMARKING ENGINE ======")
    
    # Standardize network parameters to your handbook's verified Gigabit Ethernet specs
    avg_bandwidth = 117.0  # 117 MB/s (940 Mbps GbE)
    
    # Import the layout loader and all algorithms from your scheduler package
    from scheduler.heft import load_cluster_infrastructure, calculate_upward_ranks, allocate_tasks_heft
    from scheduler.baseline import allocate_tasks_round_robin, allocate_tasks_min_min, allocate_tasks_random
    from scheduler.peft import calculate_oct_matrix, allocate_tasks_peft

    # Parse production JSON manifests
    # Change line 35 in evaluate.py to target your project layout explicitly:
   # Update these paths on line 35 of evaluate.py
    dag, W, worker_list, names_map = load_cluster_infrastructure(
        dag_file="dags/vision_pipeline.json",   # 👈 Pointing inside your dags folder
        workers_file="configs/workers.json"
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