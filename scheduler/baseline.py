# scheduler/baseline.py
import networkx as nx
import numpy as np
from scheduler.heft import load_cluster_infrastructure, calculate_est_eft, calculate_runtime_stats

def allocate_tasks_round_robin(dag, comp_matrix, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for idx, task in enumerate(nx.topological_sort(dag)):
        proc = idx % num_processors
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(dag, schedule_results), schedule_results

def allocate_tasks_min_min(dag, comp_matrix, avg_bandwidth):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        task_min_options = []
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
            
        chosen_task, chosen_proc, chosen_eft = min(task_min_options, key=lambda x: x[2])
        chosen_est = calculate_est_eft(dag, chosen_task, chosen_proc, processor_free_time, schedule_results, avg_bandwidth)
        schedule_results[chosen_task] = (chosen_proc, chosen_est, chosen_eft)
        processor_free_time[chosen_proc] = chosen_eft
        unscheduled.remove(chosen_task)
        
    return calculate_runtime_stats(dag, schedule_results), schedule_results

def allocate_tasks_random(dag, comp_matrix, avg_bandwidth, seed=101):
    import random
    random.seed(seed)
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for task in nx.topological_sort(dag):
        proc = random.randint(0, num_processors - 1)
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, avg_bandwidth)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(dag, schedule_results), schedule_results