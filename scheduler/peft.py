import numpy as np
import networkx as nx

def calculate_oct(dag, comp_matrix):
    """
    Computes the Optimistic Cost Table (OCT) for PEFT.
    OCT is a matrix of size (num_tasks x num_processors).
    """
    num_tasks, num_processors = comp_matrix.shape
    oct_table = np.zeros((num_tasks, num_processors))
    
    # Reverse topological sort to calculate from exit nodes upwards
    nodes_reversed = list(nx.topological_sort(dag))[::-1]
    
    for node in nodes_reversed:
        children = list(dag.successors(node))
        if not children:
            # Exit node has an OCT value of 0
            oct_table[node] = 0
            continue
            
        for p in range(num_processors):
            child_min_costs = []
            for child in children:
                # Find minimum potential cost for the child across all processors
                min_val = float('inf')
                edge_weight = dag[node][child].get('weight', 0)
                
                for pk in range(num_processors):
                    comm_cost = edge_weight if p != pk else 0
                    val = oct_table[child][pk] + comp_matrix[child][pk] + comm_cost
                    if val < min_val:
                        min_val = val
                child_min_costs.append(min_val)
                
            oct_table[node][p] = max(child_min_costs) if child_min_costs else 0
            
    return oct_table

def schedule_peft(dag, comp_matrix):
    """
    Executes the Predictive Earliest Finish Time (PEFT) scheduling routine.
    """
    num_tasks, num_processors = comp_matrix.shape
    oct_table = calculate_oct(dag, comp_matrix)
    
    # Task prioritizations rank is the average of its OCT row values
    peft_ranks = {node: np.mean(oct_table[node]) for node in dag.nodes()}
    sorted_tasks = sorted(dag.nodes(), key=lambda x: peft_ranks[x], reverse=True)
    
    processor_free_time = np.zeros(num_processors)
    task_completion_time = {}
    allocations = {}
    
    for task in sorted_tasks:
        best_p = -1
        best_oft = float('inf')
        best_end_time = float('inf')
        
        for p_id in range(num_processors):
            ready_time = 0
            for parent in dag.predecessors(task):
                parent_p = allocations[parent]
                edge_data = dag[parent][task].get('weight', 0)
                comm_delay = edge_data if parent_p != p_id else 0
                ready_time = max(ready_time, task_completion_time[parent] + comm_delay)
                
            start_time = max(processor_free_time[p_id], ready_time)
            end_time = start_time + comp_matrix[task][p_id]
            
            # PEFT Lookahead Objective Function: Optimistic Predicted Finish Time (OFT)
            oft = end_time + oct_table[task][p_id]
            
            if oft < best_oft:
                best_oft = oft
                best_p = p_id
                best_end_time = end_time
                
        allocations[task] = best_p
        task_completion_time[task] = best_end_time
        processor_free_time[best_p] = best_end_time
        
    makespan = max(task_completion_time.values())
    return allocations, makespan