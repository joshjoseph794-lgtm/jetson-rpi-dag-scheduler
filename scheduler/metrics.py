import numpy as np
import networkx as nx

def calculate_critical_path_lower_bound(dag, comp_matrix):
    """
    Computes the absolute lower bound of the DAG's execution time.
    This is the longest path through the graph assuming every task 
    runs on its fastest possible processor with zero network latency.
    """
    # Find the minimum computation cost for each task
    min_costs = {node: np.min(comp_matrix[node]) for node in dag.nodes()}
    
    # Store the maximum path length leading to each node
    longest_path_to_node = {}
    
    # Process nodes in topological order to find the longest path
    for node in nx.topological_sort(dag):
        parents = list(dag.predecessors(node))
        if not parents:
            longest_path_to_node[node] = min_costs[node]
        else:
            longest_path_to_node[node] = min_costs[node] + max(longest_path_to_node[p] for p in parents)
            
    return max(longest_path_to_node.values()) if longest_path_to_node else 0.0

def calculate_slr(makespan, dag, comp_matrix):
    """
    Calculates the Schedule Length Ratio (SLR).
    SLR = Makespan / Critical_Path_Lower_Bound
    """
    cp_lower_bound = calculate_critical_path_lower_bound(dag, comp_matrix)
    if cp_lower_bound == 0:
        return 0.0
    return makespan / cp_lower_bound

def calculate_speedup(makespan, comp_matrix):
    """
    Calculates the System Speedup.
    Speedup = Sum of sequential execution costs on a single best-case core / Makespan
    """
    # Sum of the minimum computation costs for all tasks
    total_sequential_cost = np.sum(np.min(comp_matrix, axis=1))
    if makespan == 0:
        return 0.0
    return total_sequential_cost / makespan