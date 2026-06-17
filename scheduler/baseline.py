# scheduler/baseline.py
import networkx as nx
import numpy as np
import random
from scheduler.heft import calculate_est_eft

def allocate_tasks_round_robin(dag, comp_matrix, avg_bandwidth, workers):
    """
    Allocates tasks sequentially to processors in a circular, round-robin loop.
    Enforces a strict topological order to respect parent dependencies.
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for idx, task in enumerate(nx.topological_sort(dag)):
        proc = idx % num_processors
        
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results

def allocate_tasks_min_min(dag, comp_matrix, avg_bandwidth, workers):
    """
    Min-Min Heuristic: Out of all ready tasks, finds their minimum EFT across 
    all processors, then schedules the task with the absolute minimum overall EFT.
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Filter for tasks whose parent constraints have been entirely met
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        task_min_options = []
        for task in ready_tasks:
            best_proc_for_task = -1
            best_eft_for_task = float('inf')
            
            for proc in range(num_processors):
                est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
                eft = est + comp_matrix[task][proc]
                if eft < best_eft_for_task:
                    best_eft_for_task = eft
                    best_proc_for_task = proc
            
            task_min_options.append((task, best_proc_for_task, best_eft_for_task))
            
        # Select the task possessing the minimum overall EFT
        chosen_task, chosen_proc, chosen_eft = min(task_min_options, key=lambda x: x[2])
        chosen_est = calculate_est_eft(dag, chosen_task, chosen_proc, processor_free_time, schedule_results, workers)
        
        schedule_results[chosen_task] = (chosen_proc, chosen_est, chosen_eft)
        processor_free_time[chosen_proc] = chosen_eft
        unscheduled.remove(chosen_task)
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results

def allocate_tasks_random(dag, comp_matrix, avg_bandwidth, workers, seed=101):
    """
    Allocates ready tasks to a completely randomized cluster node processor.
    Provides a baseline to demonstrate the performance benefits of smart heuristics.
    """
    random.seed(seed)
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for task in nx.topological_sort(dag):
        proc = random.randint(0, num_processors - 1)
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results