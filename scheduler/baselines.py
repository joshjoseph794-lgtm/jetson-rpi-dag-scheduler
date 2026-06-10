import random
import numpy as np
import networkx as nx

def schedule_round_robin(dag, comp_matrix):
    """
    Assigns tasks to processors in a strict alternating order (0, 1, 0, 1...)
    following the topological order of the DAG to respect dependencies.
    """
    num_tasks, num_processors = comp_matrix.shape
    topo_order = list(nx.topological_sort(dag))
    
    # Track when each processor becomes free
    processor_free_time = np.zeros(num_processors)
    # Track when each task finishes execution
    task_completion_time = {}
    
    allocations = {}
    
    for idx, task in enumerate(topo_order):
        # Round-robin target selection
        p_id = idx % num_processors
        
        # Determine when parent data arrives
        ready_time = 0
        for parent in dag.predecessors(task):
            parent_p = allocations[parent]
            edge_data = dag[parent][task].get('weight', 0)
            
            # If parent was on a different processor, add communication delay
            comm_delay = edge_data if parent_p != p_id else 0
            ready_time = max(ready_time, task_completion_time[parent] + comm_delay)
            
        # Task starts when both processor is free and data has arrived
        start_time = max(processor_free_time[p_id], ready_time)
        execution_cost = comp_matrix[task][p_id]
        end_time = start_time + execution_cost
        
        # Log allocation data
        allocations[task] = p_id
        task_completion_time[task] = end_time
        processor_free_time[p_id] = end_time
        
    makespan = max(task_completion_time.values())
    return allocations, makespan

def schedule_random(dag, comp_matrix, seed=42):
    """
    Randomly assigns valid tasks to processors.
    """
    random.seed(seed)
    num_tasks, num_processors = comp_matrix.shape
    topo_order = list(nx.topological_sort(dag))
    
    processor_free_time = np.zeros(num_processors)
    task_completion_time = {}
    allocations = {}
    
    for task in topo_order:
        p_id = random.randint(0, num_processors - 1)
        
        ready_time = 0
        for parent in dag.predecessors(task):
            parent_p = allocations[parent]
            edge_data = dag[parent][task].get('weight', 0)
            comm_delay = edge_data if parent_p != p_id else 0
            ready_time = max(ready_time, task_completion_time[parent] + comm_delay)
            
        start_time = max(processor_free_time[p_id], ready_time)
        end_time = start_time + comp_matrix[task][p_id]
        
        allocations[task] = p_id
        task_completion_time[task] = end_time
        processor_free_time[p_id] = end_time
        
    makespan = max(task_completion_time.values())
    return allocations, makespan