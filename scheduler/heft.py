# scheduler/heft.py
import networkx as nx
import numpy as np
from scheduler.cost_model import CostModel

def calculate_upward_ranks(dag, comp_matrix, workers):
    """
    Calculates the upward rank rank_u of each node in the DAG.
    Formula: rank_u(i) = w_avg(i) + max_{j in succ(i)} (c_avg(i,j) + rank_u(j))
    """
    cost_engine = CostModel()
    avg_w = np.mean(comp_matrix, axis=1)
    ranks = {}
    
    # Process tasks backward from exit nodes to entry nodes using reversed topological sort
    for node in reversed(list(nx.topological_sort(dag))):
        successors = list(dag.successors(node))
        if not successors:
            ranks[node] = avg_w[node]
        else:
            communication_costs = []
            for succ in successors:
                edge_data_mb = dag[node][succ]['weight']
                
                # Calculate average transmission time across all possible node paths
                link_latencies = []
                for w1 in workers:
                    for w2 in workers:
                        link_latencies.append(
                            cost_engine.get_communication_cost(w1["name"], w2["name"], edge_data_mb)
                        )
                avg_comm_delay = np.mean(link_latencies) if link_latencies else 0.0
                communication_costs.append(avg_comm_delay + ranks[succ])
                
            ranks[node] = avg_w[node] + max(communication_costs)
    return ranks

def calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers):
    """
    Calculates the Earliest Start Time (EST) and Earliest Finish Time (EFT) 
    for a task on a specific target processor.
    """
    cost_engine = CostModel()
    avail_time_processor = processor_free_time[proc]
    max_data_arrival_time = 0.0
    
    target_worker_name = workers[proc]["name"]
    
    # Scan all dependencies to check when data will arrive over the network link
    for parent in dag.predecessors(task):
        parent_proc_idx, _, parent_end_time = schedule_results[parent]
        source_worker_name = workers[parent_proc_idx]["name"]
        
        network_delay = cost_engine.get_communication_cost(
            source_worker_name, target_worker_name, dag[parent][task]['weight']
        )
        data_arrival = parent_end_time + network_delay
        max_data_arrival_time = max(max_data_arrival_time, data_arrival)
        
    return max(avail_time_processor, max_data_arrival_time)

def allocate_tasks_heft(dag, comp_matrix, workers):
    """
    Main entry point for HEFT Scheduling Heuristic.
    Returns:
       makespan (float): The total execution length boundary.
       schedule_results (dict): Mapping of task_idx -> (processor_idx, est, eft)
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # 1. Prioritize tasks by sorting their upward ranks in descending order
    task_priorities = calculate_upward_ranks(dag, comp_matrix, workers)
    sorted_tasks = [task[0] for task in sorted(task_priorities.items(), key=lambda x: x[1], reverse=True)]
    
    # 2. Assign each task to the processor that minimizes its Earliest Finish Time
    for task in sorted_tasks:
        best_processor = -1
        best_eft = float('inf')
        
        for proc in range(num_processors):
            est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
            eft = est + comp_matrix[task][proc]
            
            if eft < best_eft:
                best_eft = eft
                best_processor = proc
                
        # Lock in the optimal scheduling decision slot
        est = calculate_est_eft(dag, task, best_processor, processor_free_time, schedule_results, workers)
        schedule_results[task] = (best_processor, est, best_eft)
        processor_free_time[best_processor] = best_eft
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results