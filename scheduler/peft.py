# scheduler/peft.py
import networkx as nx
import numpy as np
import os
from scheduler.heft import load_cluster_infrastructure, calculate_est_eft, calculate_runtime_stats
from scheduler.cost_model import CostModel

def calculate_oct_matrix(dag, comp_matrix, workers):
    """Computes the Optimistic Predictor Cost Table (OCT) using asymmetric network profiles."""
    cost_engine = CostModel()
    num_tasks = comp_matrix.shape[0]
    num_processors = comp_matrix.shape[1]
    oct_table = np.zeros((num_tasks, num_processors))
    
    sorted_nodes = list(reversed(list(nx.topological_sort(dag))))
    
    for node in sorted_nodes:
        successors = list(dag.successors(node))
        if not successors:
            oct_table[node, :] = 0.0
        else:
            for p_w in range(num_processors):
                source_worker_id = workers[p_w]["id"]
                succ_costs = []
                
                for succ in successors:
                    min_val = float('inf')
                    edge_weight_mb = dag[node][succ]['weight']
                    
                    for p_m in range(num_processors):
                        target_worker_id = workers[p_m]["id"]
                        
                        # Fetch true asymmetric network cost from profiles
                        c_delay = cost_engine.get_communication_cost(source_worker_id, target_worker_id, edge_weight_mb)
                        val = oct_table[succ, p_m] + comp_matrix[succ, p_m] + c_delay
                        
                        if val < min_val:
                            min_val = val
                    succ_costs.append(min_val)
                    
                oct_table[node, p_w] = max(succ_costs) if succ_costs else 0.0
    return oct_table

def allocate_tasks_peft(dag, comp_matrix, oct_table, workers):
    """Allocates tasks using the PEFT algorithm matching updated infrastructure models."""
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # Calculate rank_oct to prioritize tasks
    avg_w = np.mean(comp_matrix, axis=1)
    rank_oct = {i: avg_w[i] + np.mean(oct_table[i, :]) for i in dag.nodes()}
    
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Step A: Find tasks whose parent dependencies have already been allocated
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        
        # Fallback safeguard in case of disconnected graph structural issues
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        # Step B: Select the task from the ready queue with the HIGHEST rank_oct value
        task = max(ready_tasks, key=lambda x: rank_oct[x])
        
        best_processor = -1
        best_val = float('inf')
        best_est = float('inf')
        
        # Step C: Evaluate processor options using the OCT value prediction modifier
        for proc in range(num_processors):
            # Interfacing seamlessly with updated heft.py helper definitions
            est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
            eft = est + comp_matrix[task][proc]
            evaluation_value = eft + oct_table[task][proc] # Core PEFT heuristic constraint
            
            if evaluation_value < best_val:
                best_val = evaluation_value
                best_processor = proc
                best_est = est
                
        actual_eft = best_est + comp_matrix[task][best_processor]
        
        # Step D: Commit scheduling bounds securely
        schedule_results[task] = (best_processor, best_est, actual_eft)
        processor_free_time[best_processor] = actual_eft
        unscheduled.remove(task)
        
    return calculate_runtime_stats(schedule_results), schedule_results

if __name__ == "__main__":
    print("====== PEFT SCHEDULER ENGINE LOCAL TESTING ======")
    configs_ready = os.path.exists("configs/workers.json") and os.path.exists("dags/vision_pipeline.json")
    
    if not configs_ready:
        print("[Notice] Workspace configuration profiles missing. Skipping execution test trace.")
    else:
        graph, cost_matrix, worker_list, names_map, idx_to_id = load_cluster_infrastructure()
        oct_matrix = calculate_oct_matrix(graph, cost_matrix, worker_list)
        peft_makespan, peft_schedule = allocate_tasks_peft(graph, cost_matrix, oct_matrix, worker_list)
        
        print(f"\n✔ PEFT Optimization Matrix complete for {len(peft_schedule)} nodes.")
        print(f"-> PEFT Optimized Makespan: {peft_makespan:.2f}s")