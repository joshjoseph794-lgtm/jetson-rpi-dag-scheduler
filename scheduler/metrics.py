# scheduler/metrics.py
import numpy as np

def calculate_scheduling_length_ratio(makespan, dag, comp_matrix):
    """
    SLR (Scheduling Length Ratio) measures how close the schedule is to the theoretical ideal.
    Formula: Makespan / Sum of minimum possible execution costs of critical path tasks.
    A lower SLR indicates a more optimized schedule.
    """
    # Mathematical lower bound: Summing the absolute minimum execution costs available for each task
    min_costs_sum = sum(np.min(comp_matrix[node, :]) for node in dag.nodes())
    if min_costs_sum == 0:
        return 0.0
    return float(makespan / min_costs_sum)

def calculate_speedup(makespan, comp_matrix):
    """
    Speedup evaluates processing efficiency gains relative to a single sequential processor.
    Formula: Total sequential execution time on the fastest single node / Scheduled Makespan.
    A higher speedup value proves the cluster is distributing work efficiently.
    """
    # 1. Find the total execution time if everything ran sequentially on each individual node
    node_sequential_times = np.sum(comp_matrix, axis=0)
    
    # 2. Baseline against the FASTEST single node (minimum sequential time)
    fastest_single_node_time = np.min(node_sequential_times)
    
    if makespan == 0:
        return 0.0
    return float(fastest_single_node_time / makespan)

def calculate_processor_utilization(schedule_results, comp_matrix, num_processors, makespan):
    """
    Calculates the average Processor Utilization (PU) across the cluster.
    Formula: (Sum of active execution times on all processors) / (Number of Processors * Makespan)
    Returns a percentage (0.0 to 1.0) representing how much time nodes spent working vs idling.
    """
    if makespan == 0:
        return 0.0
        
    total_active_compute_time = 0.0
    
    # Accumulate the actual time processors spent calculating (excluding network waiting times)
    for task_idx, allocation in schedule_results.items():
        proc_idx = allocation[0]
        actual_compute_cost = comp_matrix[task_idx][proc_idx]
        total_active_compute_time += actual_compute_cost
        
    total_cluster_available_time = num_processors * makespan
    return float(total_active_compute_time / total_cluster_available_time)