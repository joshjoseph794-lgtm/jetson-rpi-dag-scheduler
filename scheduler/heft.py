# scheduler/heft.py
import networkx as nx
import numpy as np
import json
import os
import sys
from scheduler.cost_model import CostModel

class SilenceStdout:
    """Context manager to cleanly silence cost engine warning logs during planning."""
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')
    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def load_cluster_infrastructure(dag_file="dags/vision_pipeline.json", workers_file="configs/workers.json"):
    """
    Parses JSON manifests to dynamically build the NetworkX DAG 
    and uses the CostModel engine to generate the formal heterogeneous 
    Computation Cost Matrix (W).
    """
    cost_engine = CostModel()

    # --- 1. PARSE WORKER INVENTORY ---
    with open(workers_file, "r") as f:
        worker_data = json.load(f)
    workers = worker_data["workers"] 
    num_processors = len(workers)

    # --- 2. PARSE DAG MANIFEST ---
    with open(dag_file, "r") as f:
        dag_data = json.load(f)
    
    tasks = dag_data.get("tasks", dag_data.get("nodes", []))
    dependencies = dag_data.get("dependencies", dag_data.get("edges", []))
    num_tasks = len(tasks)

    # --- 3. CONSTRUCT THE HETEROGENEOUS COMPUTATION MATRIX W ---
    W = np.zeros((num_tasks, num_processors))
    
    task_id_to_idx = {task["id"]: idx for idx, task in enumerate(tasks)}
    task_idx_to_id = {idx: task["id"] for idx, task in enumerate(tasks)}
    task_metadata = {idx: task.get("name", task["id"]) for idx, task in enumerate(tasks)}

    with SilenceStdout():
        for idx, task in enumerate(tasks):
            task_type = task.get("task_type", "vision_task")
            
            if "base_execution_cost_ms" in task:
                base_cost = task["base_execution_cost_ms"] / 1000.0
            else:
                base_cost = task.get("base_computation_cost", 10.0)
            
            for j, worker in enumerate(workers):
                worker_profile_id = worker["id"]
                W[idx, j] = cost_engine.get_computation_cost(worker_profile_id, task_type, base_cost)

    # --- 4. BUILD THE PHYSICAL NETWORKX DAG GRAPH ---
    dag = nx.DiGraph()
    for idx, task in enumerate(tasks):
        dag.add_node(idx, task_id=task["id"], name=task_metadata[idx])
        
    for edge in dependencies:
        parent_id = edge.get("parent", edge.get("from"))
        child_id = edge.get("child", edge.get("from"))
        data_size = float(edge.get("data_size_mb", 0.0))
        
        if parent_id in task_id_to_idx and child_id in task_id_to_idx:
            dag.add_edge(
                task_id_to_idx[parent_id], 
                task_id_to_idx[child_id], 
                weight=data_size
            )
        
    return dag, W, workers, task_metadata, task_idx_to_id

def calculate_upward_ranks(dag, comp_matrix, workers):
    """Calculates upward rank using true average profiling latencies."""
    cost_engine = CostModel()
    avg_w = np.mean(comp_matrix, axis=1)
    ranks = {}
    
    with SilenceStdout():
        for node in reversed(list(nx.topological_sort(dag))):
            successors = list(dag.successors(node))
            if not successors:
                ranks[node] = avg_w[node]
            else:
                communication_costs = []
                for succ in successors:
                    edge_data_mb = dag[node][succ]['weight']
                    
                    link_latencies = []
                    for w1 in workers:
                        for w2 in workers:
                            link_latencies.append(cost_engine.get_communication_cost(w1["id"], w2["id"], edge_data_mb))
                    avg_comm_delay = np.mean(link_latencies) if link_latencies else 0.0
                    
                    communication_costs.append(avg_comm_delay + ranks[succ])
                    
                ranks[node] = avg_w[node] + max(communication_costs)
    return ranks

def calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers):
    """Calculates EST using heterogeneous cost model data transmission latencies."""
    cost_engine = CostModel()
    avail_time_processor = processor_free_time[proc]
    max_data_arrival_time = 0.0
    
    target_worker_id = workers[proc]["id"]
    
    with SilenceStdout():
        for parent in dag.predecessors(task):
            parent_proc_idx, _, parent_end_time = schedule_results[parent]
            source_worker_id = workers[parent_proc_idx]["id"]
            
            network_delay = cost_engine.get_communication_cost(source_worker_id, target_worker_id, dag[parent][task]['weight'])
            data_arrival = parent_end_time + network_delay
            max_data_arrival_time = max(max_data_arrival_time, data_arrival)
        
    return max(avail_time_processor, max_data_arrival_time)

def calculate_runtime_stats(schedule_results):
    """Retrieves the overall scheduling makespan duration boundary."""
    if not schedule_results:
        return 0.0
    return max([times[2] for times in schedule_results.values()])

# ==========================================
# ALGORITHM SCHEDULING STRATEGIES
# ==========================================

def allocate_tasks_heft(dag, comp_matrix, task_priorities, workers):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    sorted_tasks = [task[0] for task in sorted(task_priorities.items(), key=lambda x: x[1], reverse=True)]
    
    for task in sorted_tasks:
        best_processor = -1
        best_eft = float('inf')
        
        for proc in range(num_processors):
            est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
            eft = est + comp_matrix[task][proc]
            if eft < best_eft:
                best_eft = eft
                best_processor = proc
                
        est = calculate_est_eft(dag, task, best_processor, processor_free_time, schedule_results, workers)
        schedule_results[task] = (best_processor, est, best_eft)
        processor_free_time[best_processor] = best_eft
        
    return calculate_runtime_stats(schedule_results), schedule_results

def allocate_tasks_round_robin(dag, comp_matrix, workers):
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for idx, task in enumerate(nx.topological_sort(dag)):
        proc = idx % num_processors
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(schedule_results), schedule_results

def allocate_tasks_min_min(dag, comp_matrix, workers):
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
                est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
                eft = est + comp_matrix[task][proc]
                if eft < best_eft_for_task:
                    best_eft_for_task = eft
                    best_proc_for_task = proc
            
            task_min_options.append((task, best_proc_for_task, best_eft_for_task))
            
        chosen_task, chosen_proc, chosen_eft = min(task_min_options, key=lambda x: x[2])
        chosen_est = calculate_est_eft(dag, chosen_task, chosen_proc, processor_free_time, schedule_results, workers)
        schedule_results[chosen_task] = (chosen_proc, chosen_est, chosen_eft)
        processor_free_time[chosen_proc] = chosen_eft
        unscheduled.remove(chosen_task)
        
    return calculate_runtime_stats(schedule_results), schedule_results

def allocate_tasks_random(dag, comp_matrix, workers):
    import random
    num_processors = comp_matrix.shape[1]
    processor_free_time = np.zeros(num_processors)
    schedule_results = {}
    
    for task in nx.topological_sort(dag):
        proc = random.randint(0, num_processors - 1)
        est = calculate_est_eft(dag, task, proc, processor_free_time, schedule_results, workers)
        eft = est + comp_matrix[task][proc]
        
        schedule_results[task] = (proc, est, eft)
        processor_free_time[proc] = eft
        
    return calculate_runtime_stats(schedule_results), schedule_results

if __name__ == "__main__":
    print("====== SCHEDULER ENGINE LOCAL INTEGRATION TESTING ======")
    configs_ready = os.path.exists("configs/workers.json") and os.path.exists("dags/vision_pipeline.json")
    
    if not configs_ready:
        print("[Notice] Real workspace paths missing. Skipping local execution run trace test.")
    else:
        graph, cost_matrix, worker_list, names_map, idx_to_id = load_cluster_infrastructure()
        rr_makespan, _ = allocate_tasks_round_robin(graph, cost_matrix, worker_list)
        ranks = calculate_upward_ranks(graph, cost_matrix, worker_list)
        heft_makespan, heft_schedule = allocate_tasks_heft(graph, cost_matrix, ranks, worker_list)
        
        print(f"\n✔ Complete mapping generated successfully for {len(heft_schedule)} tasks.")
        print(f"-> HEFT Makespan: {heft_makespan:.2f}s | Baseline RR: {rr_makespan:.2f}s")