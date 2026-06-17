# scheduler/cpop.py
import networkx as nx
import numpy as np
from scheduler.cost_model import CostModel
from scheduler.heft import calculate_upward_ranks, calculate_est_eft

def calculate_downward_ranks(dag, comp_matrix, workers):
    """
    Calculates the downward rank (rank_d) of each node in the DAG.
    Formula: rank_d(i) = max_{j in pred(i)} (rank_d(j) + w_avg(j) + c_avg(j,i))
    """
    cost_engine = CostModel()
    avg_w = np.mean(comp_matrix, axis=1)
    ranks_d = {node: 0.0 for node in dag.nodes()}
    
    # Process tasks forward using standard topological sort order
    for node in list(nx.topological_sort(dag)):
        predecessors = list(dag.predecessors(node))
        if predecessors:
            pred_costs = []
            for pred in predecessors:
                edge_data_mb = dag[pred][node]['weight']
                
                # Calculate average transmission delay between all possible hardware paths
                link_latencies = []
                for w1 in workers:
                    for w2 in workers:
                        link_latencies.append(
                            cost_engine.get_communication_cost(w1["name"], w2["name"], edge_data_mb)
                        )
                avg_comm_delay = np.mean(link_latencies) if link_latencies else 0.0
                pred_costs.append(ranks_d[pred] + avg_w[pred] + avg_comm_delay)
                
            ranks_d[node] = max(pred_costs)
            
    return ranks_d

# --- ADDED AVG_BANDWIDTH TO MATCH THE BENCHMARK SIGNATURE CONTRACT ---
def allocate_tasks_cpop(dag, comp_matrix, avg_bandwidth, workers):
    """
    Main entry point for Critical Path On a Processor (CPOP) Scheduling Algorithm.
    Guarantees parent dependencies are allocated before children.
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # 1. Calculate Priorities using Upward and Downward Ranks
    rank_u = calculate_upward_ranks(dag, comp_matrix, workers)
    rank_d = calculate_downward_ranks(dag, comp_matrix, workers)
    
    # Priority value for CPOP is rank_u + rank_d
    total_priority = {i: rank_u[i] + rank_d[i] for i in dag.nodes()}
    
    # 2. Identify the Critical Path (CP)
    entry_nodes = [n for n in dag.nodes() if dag.in_degree(n) == 0]
    cp_value = max([total_priority[n] for n in entry_nodes]) if entry_nodes else 0.0
    critical_path_nodes = set([n for n, priority in total_priority.items() if abs(priority - cp_value) < 1e-5])
    
    # 3. Designate the Critical Path Processor (The absolute fastest single node)
    node_total_execution_times = np.sum(comp_matrix, axis=0)
    critical_path_processor_idx = int(np.argmin(node_total_execution_times))
    
    # 4. Use an unscheduled set to preserve dependency execution flow
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Filter tasks whose parent dependencies have already been successfully allocated
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        
        if not ready_tasks:
            # Fallback safeguard for disconnected components
            ready_tasks = list(unscheduled)
            
        # Select the READY task possessing the highest total priority
        task = max(ready_tasks, key=lambda x: total_priority[x])
        
        # Strategy A: If the task is on the Critical Path, lock it to the fastest processor
        if task in critical_path_nodes:
            best_processor = critical_path_processor_idx
        
        # Strategy B: If it's a sub-critical task, distribute it using standard EFT minimization
        else:
            best_processor = -1
            best_eft = float('inf')
            
            for proc in range(num_processors):
                est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
                eft = est + comp_matrix[task][proc]
                
                if eft < best_eft:
                    best_eft = eft
                    best_processor = proc
                    
        # Allocate slot and lock execution intervals safely
        est = calculate_est_eft(dag, task, best_processor, processor_free_time, schedule_results, workers)
        eft = est + comp_matrix[task][best_processor]
        
        schedule_results[task] = (best_processor, est, eft)
        processor_free_time[best_processor] = eft
        
        # Pop from tracking set
        unscheduled.remove(task)
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results