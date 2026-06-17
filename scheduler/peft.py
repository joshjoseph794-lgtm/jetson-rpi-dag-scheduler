# scheduler/peft.py
import networkx as nx
import numpy as np
from scheduler.cost_model import CostModel
from scheduler.heft import calculate_est_eft

def calculate_oct_matrix(dag, comp_matrix, workers):
    """
    Computes the Optimistic Predictor Cost Table (OCT) matrix.
    OCT is a task-by-processor matrix that looks ahead to predict the minimum 
    remaining processing costs down the path of the DAG.
    """
    cost_engine = CostModel()
    num_tasks = comp_matrix.shape[0]
    num_processors = comp_matrix.shape[1]
    oct_table = np.zeros((num_tasks, num_processors))
    
    # Traverse backward from exit nodes to entry nodes using reversed topological sort
    sorted_nodes = list(reversed(list(nx.topological_sort(dag))))
    
    for node in sorted_nodes:
        successors = list(dag.successors(node))
        if not successors:
            oct_table[node, :] = 0.0
        else:
            for p_w in range(num_processors):
                source_worker_name = workers[p_w]["name"]
                succ_costs = []
                
                for succ in successors:
                    min_val = float('inf')
                    edge_weight_mb = dag[node][succ]['weight']
                    
                    for p_m in range(num_processors):
                        target_worker_name = workers[p_m]["name"]
                        
                        # Calculate communication overhead between processors using node names
                        c_delay = cost_engine.get_communication_cost(
                            source_worker_name, target_worker_name, edge_weight_mb
                        )
                        val = oct_table[succ, p_m] + comp_matrix[succ, p_m] + c_delay
                        
                        if val < min_val:
                            min_val = val
                    succ_costs.append(min_val)
                    
                oct_table[node, p_w] = max(succ_costs) if succ_costs else 0.0
    return oct_table

# --- ADDED AVG_BANDWIDTH TO MATCH THE BENCHMARK SIGNATURE CONTRACT ---
def allocate_tasks_peft(dag, comp_matrix, avg_bandwidth, workers):
    """
    Main entry point for Predict Earliest Finish Time (PEFT) scheduling algorithm.
    Returns:
       makespan (float): The total execution length boundary.
       schedule_results (dict): Mapping of task_idx -> (processor_idx, est, eft)
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # 1. Compute the OCT table look-ahead window
    oct_table = calculate_oct_matrix(dag, comp_matrix, workers)
    
    # 2. Compute rank_oct priority values: rank_oct(i) = structural average computation + average OCT row costs
    avg_w = np.mean(comp_matrix, axis=1)
    rank_oct = {i: avg_w[i] + np.mean(oct_table[i, :]) for i in dag.nodes()}
    
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Step A: Filter out tasks whose dependency parents have already been pinned
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        # Step B: Pick the task from the ready queue boasting the HIGHEST rank_oct value
        task = max(ready_tasks, key=lambda x: rank_oct[x])
        
        best_processor = -1
        best_val = float('inf')
        best_est = float('inf')
        
        # Step C: Evaluate target nodes using the look-ahead prediction modifier
        for proc in range(num_processors):
            est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
            eft = est + comp_matrix[task][proc]
            
            # The heart of PEFT: Choose processor based on Earliest Finish Time + predicted downstream impact
            evaluation_value = eft + oct_table[task][proc]
            
            if evaluation_value < best_val:
                best_val = evaluation_value
                best_processor = proc
                best_est = est
                
        actual_eft = best_est + comp_matrix[task][best_processor]
        
        # Step D: Commit allocation slot
        schedule_results[task] = (best_processor, best_est, actual_eft)
        processor_free_time[best_processor] = actual_eft
        unscheduled.remove(task)
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results