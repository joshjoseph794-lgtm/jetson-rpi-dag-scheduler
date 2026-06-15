# scheduler/metrics.py
import numpy as np

def calculate_scheduling_length_ratio(makespan, dag, comp_matrix):
    """
    SLR divides the scheduled makespan by the lower bound execution 
    cost of tasks along the critical path.
    """
    # Summing the absolute minimum execution costs available for each task
    min_costs_sum = sum(np.min(comp_matrix[node, :]) for node in dag.nodes())
    if min_costs_sum == 0:
        return 0
    return makespan / min_costs_sum

def calculate_speedup(makespan, comp_matrix):
    """Speedup evaluates processing efficiency gains relative to isolated serial execution."""
    sequential_time = sum(np.mean(comp_matrix, axis=1))
    return sequential_time / makespan