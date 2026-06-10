import json
import os
import numpy as np
import networkx as nx

def load_dag_manifest(file_path):
    """
    Parses a JSON pipeline manifest, returns a NetworkX DiGraph, 
    the computation matrix, and the calculated scientific CCR value.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Manifest file not found at: {file_path}")
        
    with open(file_path, 'r') as f:
        data = json.load(f)
        
    num_tasks = len(data['tasks'])
    num_processors = data['num_processors']
    
    # 1. Construct the computation cost matrix (Tasks x Processors)
    comp_matrix = np.zeros((num_tasks, num_processors))
    for task in data['tasks']:
        comp_matrix[task['id']] = task['costs']
        
    # 2. Build the NetworkX Directed Acyclic Graph (DAG)
    dag = nx.DiGraph()
    for task in data['tasks']:
        dag.add_node(task['id'], name=task['name'])
        
    for edge in data['edges']:
        dag.add_edge(
            edge['source'], 
            edge['target'], 
            weight=edge['communication_cost']
        )
        
    # 3. Calculate Academic Metrics: CCR
    avg_comp_cost = np.mean(comp_matrix)
    
    edge_weights = [edge['communication_cost'] for edge in data['edges']]
    avg_comm_cost = np.mean(edge_weights) if edge_weights else 0.0
    
    # CCR Formula validation
    ccr = avg_comm_cost / avg_comp_cost if avg_comp_cost > 0 else 0.0
    
    return dag, comp_matrix, ccr

if __name__ == "__main__":
    # Test execution block to verify the parser works perfectly locally
    sample_path = os.path.join(os.path.dirname(__file__), 'vision_pipeline.json')
    try:
        graph, matrix, ccr_value = load_dag_manifest(sample_path)
        print("✅ DAG Manifest Parsed Successfully!")
        print(f"🔹 Total Pipeline Tasks: {graph.number_of_nodes()}")
        print(f"🔹 Total Data Dependencies (Edges): {graph.number_of_edges()}")
        print(f"🔹 Computation Matrix Shape: {matrix.shape}")
        print(f"🔹 Calculated Pipeline CCR: {ccr_value:.4f}")
        
        if ccr_value > 1.0:
            print("   👉 Status: Communication-intensive workload (Network Bound)")
        else:
            print("   👉 Status: Computation-intensive workload (CPU/GPU Bound)")
            
    except Exception as e:
        print(f"❌ Parsing failed: {e}")