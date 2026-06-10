import networkx as nx
import numpy as np
import random

def generate_random_dag(num_tasks=10):
    """Generates a random Directed Acyclic Graph (DAG) for benchmarking."""
    dag = nx.DiGraph()
    random.seed(101) # Locked seed for exact baseline comparison
    
    for i in range(num_tasks):
        dag.add_node(i, name=f"Task_{i}")
        
    # Generate dependencies (forward edges only to guarantee no loops)
    for i in range(num_tasks):
        for j in range(i + 1, num_tasks):
            if random.random() < 0.35: 
                data_size = random.randint(15, 75) # Data size in MB
                dag.add_edge(i, j, weight=data_size)
                
    # Safeguard against orphan/disconnected nodes
    for node in list(dag.nodes()):
        if dag.in_degree(node) == 0 and node != 0:
            dag.add_edge(0, node, weight=20)
            
    return dag

def generate_random_costs(num_tasks):
    """Generates heterogeneous compute costs for [Laptop, Jetson]."""
    np.random.seed(101)
    laptop_costs = np.random.uniform(2.0, 8.0, size=(num_tasks, 1))
    jetson_costs = np.random.uniform(1.5, 12.0, size=(num_tasks, 1))
    return np.hstack((laptop_costs, jetson_costs))

def calculate_runtime_stats(dag, schedule_results):
    """Helper to safely retrieve the max end time (makespan) across assignments."""
    return max([times[2] for times in schedule_results.values()])

def calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth):
    """Calculates Earliest Start Time and Earliest Finish Time for a task on a processor."""
    avail_time_processor = processor_free_time[proc]
    max_data_arrival_time = 0.0
    
    for parent in dag.predecessors(task):
        parent_proc, _, parent_end_time = schedule_results[parent]
        if parent_proc != proc:
            network_delay = dag[parent][task]['weight'] / avg_bandwidth
            data_arrival = parent_end_time + network_delay
        else:
            data_arrival = parent_end_time # Local memory transfer = 0s lag
        max_data_arrival_time = max(max_data_arrival_time, data_arrival)
        
    est = max(avail_time_processor, max_data_arrival_time)
    return est

# ==========================================
# 1. HEFT SCHEDULER IMPLEMENTATION
# ==========================================
def calculate_upward_ranks(dag, comp_matrix, avg_bandwidth):
    avg_w = np.mean(comp_matrix, axis=1)
    ranks = {}
    for node in reversed(list(nx.topological_sort(dag))):
        successors = list(dag.successors(node))
        if not successors:
            ranks[node] = avg_w[node]
        else:
            max_successor_cost = max([(dag[node][succ]['weight'] / avg_bandwidth) + ranks[succ] for succ in successors])
            ranks[node] = avg_w[node] + max_successor_cost
    return ranks

def allocate_tasks_heft(dag, comp_matrix, task_priorities, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    sorted_tasks = [task[0] for task in sorted(task_priorities.items(), key=lambda x: x[1], reverse=True)]
    
    for task in sorted_tasks:
        best_processor = -1
        best_eft = float('inf')
        
        for proc in range(num_processors):
            est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
            eft = est + comp_matrix[task][proc]
            
            if eft < best_eft:
                best_eft = eft
                best_processor = proc
                
        est = calculate_est_eft(dag, task, best_processor, processor_free_time, schedule_results, avg_bandwidth)
        schedule_results[task] = (best_processor, est, best_eft)
        processor_free_time[best_processor] = best_eft
        
    return calculate_runtime_stats(dag, schedule_results)

# ==========================================
# 2. ROUND-ROBIN SCHEDULER
# ==========================================
def allocate_tasks_round_robin(dag, comp_matrix, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # Alternates assignments sequentially down the topological sort list
    for idx, task in enumerate(nx.topological_sort(dag)):
        proc = idx % num_processors
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(dag, schedule_results)

# ==========================================
# 3. MIN-MIN SCHEDULER
# ==========================================
def allocate_tasks_min_min(dag, comp_matrix, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Step A: Identify tasks that are 'ready' (all dependencies are satisfied)
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        
        # If no tasks are ready but tasks remain, there is a graph integrity error (fallback to unscheduled list)
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        task_min_options = []
        
        # Step B: Find the absolute best processor pairing for EACH ready task
        for task in ready_tasks:
            best_proc_for_task = -1
            best_eft_for_task = float('inf')
            
            for proc in range(num_processors):
                est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
                eft = est + comp_matrix[task][proc]
                if eft < best_eft_for_task:
                    best_eft_for_task = eft
                    best_proc_for_task = proc
            
            task_min_options.append((task, best_proc_for_task, best_eft_for_task))
            
        # Step C: Out of all minimum pairs, select the task with the SMALLEST overall finish time (Min-Min rule)
        chosen_task, chosen_proc, chosen_eft = min(task_min_options, key=lambda x: x[2])
        
        # Step D: Commit allocation
        chosen_est = calculate_est_eft(dag, chosen_task, chosen_proc, processor_free_time, schedule_results, avg_bandwidth)
        schedule_results[chosen_task] = (chosen_proc, chosen_est, chosen_eft)
        processor_free_time[chosen_proc] = chosen_eft
        unscheduled.remove(chosen_task)
        
    return calculate_runtime_stats(dag, schedule_results)

# ==========================================
# 4. RANDOM SCHEDULER
# ==========================================
def allocate_tasks_random(dag, comp_matrix, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    random.seed(101) # Fix seed for replication
    for task in nx.topological_sort(dag):
        proc = random.randint(0, num_processors - 1)
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(dag, schedule_results)


if __name__ == "__main__":
    print("====== EDGE PERFORMANCE BENCHMARKING ENGINE ======")
    num_tasks = 10
    bandwidth = 8.0 # Simulated throughput in MB/s
    
    graph = generate_random_dag(num_tasks)
    cost_matrix = generate_random_costs(num_tasks)
    
    print(f"Graph Layout: {num_tasks} Tasks to deploy over [Lenovo Laptop, Jetson Nano].")
    print("Executing all policies across identical network topologies...\n")
    
    # Execute Competitors
    rr_makespan = allocate_tasks_round_robin(graph, cost_matrix, bandwidth)
    rand_makespan = allocate_tasks_random(graph, cost_matrix, bandwidth)
    minmin_makespan = allocate_tasks_min_min(graph, cost_matrix, bandwidth)
    
    # Execute HEFT
    ranks = calculate_upward_ranks(graph, cost_matrix, bandwidth)
    heft_makespan = allocate_tasks_heft(graph, cost_matrix, ranks, bandwidth)
    
    # Summary Table Output
    print("---------------------------------------------------------")
    print(f"| {'Scheduling Policy':<25} | {'Makespan (Seconds)':<20} |")
    print("---------------------------------------------------------")
    print(f"| {'1. Naive Round-Robin':<25} | {rr_makespan:<20.2f} |")
    print(f"| {'2. Random Allocation':<25} | {rand_makespan:<20.2f} |")
    print(f"| {'3. Min-Min Heuristic':<25} | {minmin_makespan:<20.2f} |")
    print(f"| {'4. HEFT (Optimized)':<25} | {heft_makespan:<20.2f} |")
    print("---------------------------------------------------------")
    
    gain_over_rr = ((rr_makespan - heft_makespan) / rr_makespan) * 100
    gain_over_minmin = ((minmin_makespan - heft_makespan) / minmin_makespan) * 100
    
    print(f"\n💡 HEFT optimization cut down processing time by {gain_over_rr:.1f}% compared to Round-Robin.")
    print(f"💡 HEFT out-performed the greedy Min-Min heuristic by {gain_over_minmin:.1f}%.")