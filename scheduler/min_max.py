# scheduler/min_max.py
import networkx as nx
import numpy as np
from scheduler.heft import calculate_est_eft

# --- ADDED AVG_BANDWIDTH TO MATCH THE BENCHMARK SIGNATURE CONTRACT ---
def allocate_tasks_min_max(dag, comp_matrix, avg_bandwidth, workers):
    """
    Main entry point for the Min-Max Heuristic Scheduling Algorithm.
    Returns:
       makespan (float): The total execution length boundary.
       schedule_results (dict): Mapping of task_idx -> (processor_idx, est, eft)
    """
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    # Track the set of all tasks left to plan
    unscheduled = set(dag.nodes())
    
    while unscheduled:
        # Step 1: Isolate tasks whose parent dependencies have been entirely planned
        ready_tasks = [t for t in unscheduled if all(parent in schedule_results for parent in dag.predecessors(t))]
        
        # Safe fallback in case of disconnected nodes or graph anomalies
        if not ready_tasks:
            ready_tasks = list(unscheduled)
            
        task_execution_choices = []
        
        # Step 2: For each ready task, find its best allocation slot and overall variance
        for task in ready_tasks:
            best_proc_for_task = -1
            best_eft_for_task = float('inf')
            worst_eft_for_task = float('-inf')
            
            # Map EFT across all available processors in our cluster
            for proc in range(num_processors):
                est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
                eft = est + comp_matrix[task][proc]
                
                if eft < best_eft_for_task:
                    best_eft_for_task = eft
                    best_proc_for_task = proc
                    
                if eft > worst_eft_for_task:
                    worst_eft_for_task = eft
            
            # Calculate the execution variance (delta) for this specific task
            eft_variance = worst_eft_for_task - best_eft_for_task
            
            task_execution_choices.append({
                "task": task,
                "processor": best_proc_for_task,
                "eft": best_eft_for_task,
                "variance": eft_variance
            })
            
        # Step 3: Select the task facing the HIGHEST variance penalty
        # This prevents long, heavy tasks from being starved of your fastest node (the laptop)
        chosen_selection = max(task_execution_choices, key=lambda x: x["variance"])
        
        chosen_task = chosen_selection["task"]
        chosen_proc = chosen_selection["processor"]
        
        # Step 4: Re-calculate and lock in the final timing bounds securely
        final_est = calculate_est_eft(dag, chosen_task, chosen_proc, processor_free_time, schedule_results, workers)
        final_eft = final_est + comp_matrix[chosen_task][chosen_proc]
        
        schedule_results[chosen_task] = (chosen_proc, final_est, final_eft)
        processor_free_time[chosen_proc] = final_eft
        
        # Remove the planned task from the pool and loop again
        unscheduled.remove(chosen_task)
        
    makespan = max([times[2] for times in schedule_results.values()]) if schedule_results else 0.0
    return makespan, schedule_results