# runtime/coordinator.py
import json
import time
import grpc
import concurrent.futures
import networkx as nx
import os

# Import your newly compiled gRPC stubs
from runtime import messages_pb2
from runtime import messages_pb2_grpc

# ==========================================
# 1. INGEST AUTOMATED WORST-CASE PROFILES
# ==========================================
try:
    with open("worst_case_compute.json", "r") as f:
        COMPUTE_PROFILES = json.load(f)
    with open("worst_case_network.json", "r") as f:
        NETWORK_PROFILES = json.load(f)
    print("📊 [COORDINATOR] Successfully ingested worst-case cluster profiles from disk.")
except FileNotFoundError as e:
    print(f"❌ [COORDINATOR] Critical Error: Profiler files missing. Run profile_cluster.py first. Details: {e}")
    exit(1)

# Node networking mapping details - pointing to the unified path
try:
    with open("config/cluster_nodes.json", "r") as f:
        nodes_list = json.load(f)
except FileNotFoundError:
    with open("configs/cluster_nodes.json", "r") as f:
        nodes_list = json.load(f)

# Reconstruct the NODE_CHANNELS dictionary on the fly
NODE_CHANNELS = {
    node["name"]: {
        "target": f"{node['ip']}:{node['port']}", 
        "protocol": node["protocol"]
    }
    for node in nodes_list
}

# Define a representation for our Directed Acyclic Graph (DAG) Tasks
class DAGTask:
    def __init__(self, task_id, task_type, script_path, data_size_mb, dependencies=None):
        self.id = str(task_id)  # Uniform string identifiers
        self.type = task_type
        self.script_path = script_path
        self.data_size_mb = float(data_size_mb)
        self.dependencies = dependencies if dependencies else []
        self.rank = 0.0
        self.assigned_node = None
        self.heft_start_offset = 0.0
        self.heft_finish_offset = 0.0

# ==========================================
# 2. UNIVERSAL DATAG SCHEMA NORMALIZATION
# ==========================================
def normalize_coordinator_dag(dag_data, registry):
    """
    Translates any incoming DAG schema variety into a uniform dictionary 
    object list that the coordinator can cleanly process.
    """
    if isinstance(dag_data, list):
        # Raw legacy layout format support
        return dag_data
        
    normalized = []
    nodes = dag_data.get("nodes", dag_data.get("tasks", []))
    edges = dag_data.get("edges", dag_data.get("dependencies", []))
    
    for node in nodes:
        task_id = str(node.get("task_id", node.get("id")))
        task_type = node.get("task_type", node.get("name"))
        
        # Pull edge data dependencies targeting this node
        dependencies = []
        data_size = None
        
        for edge in edges:
            child = str(edge.get("to", edge.get("child")))
            parent = str(edge.get("from", edge.get("parent")))
            if child == task_id:
                dependencies.append(parent)
                # Keep tracking data sizes if explicitly mapped on the edge metadata
                if "data_size_mb" in edge:
                    data_size = float(edge["data_size_mb"])
                    
        # Fallback to registry default data size if edge metadata doesn't declare it
        if data_size is None:
            data_size = float(registry.get(task_type, {}).get("default_data_size_mb", 1.5))
            
        normalized.append({
            "task_id": task_id,
            "task_type": task_type,
            "dependencies": dependencies,
            "data_size_mb": data_size
        })
    return normalized

# ==========================================
# 3. THE CORE HEFT ALGORITHM ENGINE
# ==========================================
def calculate_heft_schedule(tasks):
    g = nx.DiGraph()
    task_map = {t.id: t for t in tasks}
    
    for t in tasks:
        g.add_node(t.id)
        for dep in t.dependencies:
            g.add_edge(dep.id, t.id)
            
    topo_order = list(nx.topological_sort(g))
    
    for t_id in reversed(topo_order):
        task = task_map[t_id]
        avg_compute = sum(COMPUTE_PROFILES[node][task.type] for node in COMPUTE_PROFILES) / len(COMPUTE_PROFILES)
        
        successors = list(g.successors(t_id))
        max_successor_cost = 0.0
        
        for succ_id in successors:
            succ = task_map[succ_id]
            avg_comm = sum(
                ((task.data_size_mb * 8) / NETWORK_PROFILES[node]["bandwidth_mbps"]) + (NETWORK_PROFILES[node]["latency_ms"] / 1000.0)
                for node in NODE_CHANNELS
            ) / len(NODE_CHANNELS)
            
            max_successor_cost = max(max_successor_cost, avg_comm + succ.rank)
            
        task.rank = avg_compute + max_successor_cost
    
    scheduled_tasks = sorted(tasks, key=lambda t: t.rank, reverse=True)
    
    node_available_time = {node: 0.0 for node in NODE_CHANNELS}
    task_finish_times = {}

    print("\n🗓️ [HEFT ENGINE] Calculating optimal schedule matrix...")
    
    for task in scheduled_tasks:
        best_node = None
        earliest_finish_time = float('inf')
        optimal_start_time = 0.0
        
        for node in NODE_CHANNELS:
            comp_cost = COMPUTE_PROFILES[node][task.type]
            max_dependency_ready_time = 0.0
            
            for dep in task.dependencies:
                dep_finish = task_finish_times.get(dep.id, 0.0)
                parent_node = dep.assigned_node
                
                if parent_node is None:
                    comm_delay = 0.0
                elif parent_node != node:
                    bw = NETWORK_PROFILES[parent_node]["bandwidth_mbps"]
                    latency = NETWORK_PROFILES[parent_node]["latency_ms"] / 1000.0
                    comm_delay = ((dep.data_size_mb * 8) / bw) + latency
                else:
                    comm_delay = 0.0 
                    
                transfer_ready = dep_finish + comm_delay
                max_dependency_ready_time = max(max_dependency_ready_time, transfer_ready)
            
            ready_to_start = max(node_available_time[node], max_dependency_ready_time)
            predicted_finish = ready_to_start + comp_cost
            
            if predicted_finish < earliest_finish_time:
                earliest_finish_time = predicted_finish
                optimal_start_time = ready_to_start
                best_node = node
                
        task.assigned_node = best_node
        task.heft_start_offset = optimal_start_time
        task.heft_finish_offset = earliest_finish_time
        
        node_available_time[best_node] = earliest_finish_time
        task_finish_times[task.id] = earliest_finish_time
        
        print(f"  📌 Task {task.id} -> {best_node} | Start Offset: {task.heft_start_offset:.2f}s | Finish Offset: {task.heft_finish_offset:.2f}s")
        
    return scheduled_tasks

# ==========================================
# 4. DISPATCH EXECUTOR & TIME-TRIGGER
# ==========================================
def dispatch_task(task, dag_start_epoch):
    target_trigger_epoch = dag_start_epoch + task.heft_start_offset
    node_config = NODE_CHANNELS[task.assigned_node]
    
    print(f"🚀 [DISPATCHER] Deploying {task.id} to {task.assigned_node}. Target Start Epoch: {target_trigger_epoch:.4f}")
    
    if node_config["protocol"] == "grpc":
        try:
            with grpc.insecure_channel(node_config["target"]) as channel:
                stub = messages_pb2_grpc.TaskDispatcherStub(channel)
                request = messages_pb2.TaskRequest(
                    task_id=task.id,
                    task_type=task.type,
                    script_path=task.script_path,
                    data_size_mb=task.data_size_mb,
                    scheduled_start_time=str(target_trigger_epoch)
                )
                response = stub.ExecuteTask(request, timeout=60)
                print(f"✅ [DISPATCHER] gRPC Node Response for {task.id}: {response.status}")
        except Exception as e:
            print(f"❌ [DISPATCHER] gRPC Link Critical Failure on {task.id}: {e}")
            
    elif node_config["protocol"] == "http":
        import requests
        payload = {
            "task_id": task.id,
            "task_type": task.type,
            "script_path": task.script_path,
            "data_size_mb": task.data_size_mb,
            "scheduled_start_time": str(target_trigger_epoch)
        }
        try:
            url = f"http://{node_config['target']}/"
            res = requests.post(url, json=payload, timeout=60)
            if res.status_code == 200:
                print(f"✅ [DISPATCHER] HTTP Node Response for {task.id}: {res.json().get('status')}")
            else:
                print(f"❌ [DISPATCHER] HTTP Error Code received from Node on {task.id}: {res.status_code}")
        except Exception as e:
            print(f"❌ [DISPATCHER] gRPC Link Critical Failure on {task.id}: {e}")

# ==========================================
# 5. DYNAMIC MAIN EXECUTION PIPELINE
# ==========================================
if __name__ == "__main__":
    print("=========================================================")
    print("🏁 INITIALIZING STATIC TIME-TRIGGERED HEFT COORDINATOR")
    print("=========================================================")
    
    # Dynamically locate the target DAG active in the experiment matrix config
    target_dag_file = "execution_pipeline.json" # Fallback Default
    try:
        matrix_path = "configs/experiment_matrix.json" if os.path.exists("configs/experiment_matrix.json") else "config/experiment_matrix.json"
        with open(matrix_path, "r") as f:
            matrix_data = json.load(f)
            # Support both object config or array blocks
            if isinstance(matrix_data, list) and len(matrix_data) > 0:
                target_dag_file = matrix_data[0].get("workload_dag", target_dag_file)
            elif isinstance(matrix_data, dict):
                target_dag_file = matrix_data.get("workload_dag", target_dag_file)
        print(f"🎯 [MATRIX] Targeting Active Execution File: {target_dag_file}")
    except Exception:
        print(f"⚠️ [MATRIX] Could not load matrix setup. Defaulting to: {target_dag_file}")

    try:
        with open("task_registry.json", "r") as f:
            registry = json.load(f)
        with open(target_dag_file, "r") as f:
            pipeline_raw = json.load(f)
        print("📁 [CONFIG] Successfully parsed task registry and active pipeline files.")
    except Exception as e:
        print(f"❌ [CONFIG] Critical Error loading configuration JSON files: {e}")
        exit(1)

    # Normalize our target structural dataset
    normalized_pipeline = normalize_coordinator_dag(pipeline_raw, registry)

    task_lookup = {}
    dag_topology = []

    # Map uniform DAG objects
    for block in normalized_pipeline:
        t_id = block["task_id"]
        t_type = block["task_type"]
        data_size = block["data_size_mb"]
        
        if t_type not in registry:
            print(f"❌ [CONFIG] Error: Task type '{t_type}' requested by {t_id} is missing from task_registry.json!")
            exit(1)
            
        script_path = registry[t_type]["script_path"]
        
        task_obj = DAGTask(task_id=t_id, task_type=t_type, script_path=script_path, data_size_mb=data_size)
        task_lookup[t_id] = task_obj
        dag_topology.append(task_obj)

    # Link up parent references
    for block in normalized_pipeline:
        t_id = block["task_id"]
        deps = block.get("dependencies", [])
        
        for dep_id in deps:
            dep_str = str(dep_id)
            if dep_str in task_lookup:
                task_lookup[t_id].dependencies.append(task_lookup[dep_str])
            else:
                print(f"❌ [CONFIG] Dependency Error: Task {t_id} references an unknown parent '{dep_id}'")
                exit(1)
                
    optimized_schedule = calculate_heft_schedule(dag_topology)
    
    print("\n🔒 [SYNCHRONIZATION LOCK] Establishing Global System Base-Clock...")
    global_dag_start_epoch = time.time() + 3.0
    print(f"⏰ Master Time-Zero Epoch designated as: {global_dag_start_epoch:.4f}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for task in optimized_schedule:
            executor.submit(dispatch_task, task, global_dag_start_epoch)
            
    print("\n🎉 [SUCCESS] All dynamically configured tasks dispatched cleanly.")