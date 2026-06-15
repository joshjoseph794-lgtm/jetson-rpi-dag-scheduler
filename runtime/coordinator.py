import json
import time
import grpc
import concurrent.futures

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

# Node networking mapping details
NODE_CHANNELS = {
    "laptop01": {"target": "127.0.0.1:50051", "protocol": "grpc"},
    "jetson01": {"target": "172.16.12.35:5000", "protocol": "http"},
    "raspi01":  {"target": "172.16.12.40:5001", "protocol": "http"} # Custom isolated port
}

# Define a representation for our Directed Acyclic Graph (DAG) Tasks
class DAGTask:
    def __init__(self, task_id, task_type, script_path, data_size_mb, dependencies=None):
        self.id = task_id
        self.type = task_type
        self.script_path = script_path
        self.data_size_mb = data_size_mb
        self.dependencies = dependencies if dependencies else []
        self.rank = 0.0
        self.assigned_node = None
        self.heft_start_offset = 0.0
        self.heft_finish_offset = 0.0

# ==========================================
# 2. THE CORE HEFT ALGORITHM ENGINE (FIXED)
# ==========================================
def calculate_heft_schedule(tasks):
    # Step 2A: Compute TRUE HEFT upward ranks by traversing backward
    for task in reversed(tasks):
        avg_compute = sum(COMPUTE_PROFILES[node][task.type] for node in COMPUTE_PROFILES) / len(COMPUTE_PROFILES)
        
        # Find tasks that list THIS task as a dependency (its downstream successors)
        successors = [t for t in tasks if task in t.dependencies]
        
        max_successor_cost = 0.0
        for succ in successors:
            # Calculate average network transfer cost across all cluster nodes
            avg_comm = sum(
                ((task.data_size_mb * 8) / NETWORK_PROFILES[node]["bandwidth_mbps"]) + (NETWORK_PROFILES[node]["latency_ms"] / 1000.0)
                for node in NODE_CHANNELS
            ) / len(NODE_CHANNELS)
            
            max_successor_cost = max(max_successor_cost, avg_comm + succ.rank)
            
        task.rank = avg_compute + max_successor_cost
    
    # Sort tasks in descending order of upward rank (Guarantees parents are scheduled before children!)
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
# 3. DISPATCH EXECUTOR & TIME-TRIGGER INJECTION
# ==========================================
def dispatch_task(task, dag_start_epoch):
    # Convert the relative HEFT offset into a rock-solid physical real-world timestamp
    target_trigger_epoch = dag_start_epoch + task.heft_start_offset
    node_config = NODE_CHANNELS[task.assigned_node]
    
    print(f"🚀 [DISPATCHER] Deploying {task.id} to {task.assigned_node}. Target Start Epoch: {target_trigger_epoch:.4f}")
    
    if node_config["protocol"] == "grpc":
        # Handle gRPC communications for local/laptop tasks
        try:
            with grpc.insecure_channel(node_config["target"]) as channel:
                stub = messages_pb2_grpc.TaskDispatcherStub(channel)
                request = messages_pb2.TaskRequest(
                    task_id=task.id,
                    task_type=task.type,
                    script_path=task.script_path,
                    data_size_mb=task.data_size_mb,
                    scheduled_start_time=str(target_trigger_epoch) # Attached timestamp
                )
                response = stub.ExecuteTask(request, timeout=60)
                print(f"✅ [DISPATCHER] gRPC Node Response for {task.id}: {response.status}")
        except Exception as e:
            print(f"❌ [DISPATCHER] gRPC Link Critical Failure on {task.id}: {e}")
            
    elif node_config["protocol"] == "http":
        # Handle native HTTP communications for edge nodes (Jetson / RPi)
        import requests
        payload = {
            "task_id": task.id,
            "task_type": task.type,
            "script_path": task.script_path,
            "data_size_mb": task.data_size_mb,
            "scheduled_start_time": str(target_trigger_epoch) # Attached timestamp
        }
        try:
            url = f"http://{node_config['target']}/"
            res = requests.post(url, json=payload, timeout=60)
            if res.status_code == 200:
                print(f"✅ [DISPATCHER] HTTP Node Response for {task.id}: {res.json().get('status')}")
            else:
                print(f"❌ [DISPATCHER] HTTP Error Code received from Node on {task.id}: {res.status_code}")
        except Exception as e:
            print(f"❌ [DISPATCHER] HTTP Link Critical Failure on {task.id}: {e}")

# ==========================================
# 4. DYNAMIC MAIN EXECUTION PIPELINE (CONSTANT)
# ==========================================
if __name__ == "__main__":
    print("=========================================================")
    print("🏁 INITIALIZING STATIC TIME-TRIGGERED HEFT COORDINATOR")
    print("=========================================================")
    
    # 1. Load the Task Registry and Active Pipeline configurations from disk
    try:
        with open("task_registry.json", "r") as f:
            registry = json.load(f)
        with open("execution_pipeline.json", "r") as f:
            pipeline_definition = json.load(f)
        print("📁 [CONFIG] Successfully parsed task registry and active pipeline pipeline files.")
    except Exception as e:
        print(f"❌ [CONFIG] Critical Error loading configuration JSON files: {e}")
        exit(1)

    # 2. Reconstruct the DAG Topology dynamically
    task_lookup = {}
    dag_topology = []

    # First Pass: Instantiate all DAGTask objects based on the registry settings
    for block in pipeline_definition:
        t_id = block["task_id"]
        t_type = block["task_type"]
        
        if t_type not in registry:
            print(f"❌ [CONFIG] Error: Task type '{t_type}' requested by {t_id} is missing from task_registry.json!")
            exit(1)
            
        # Extract default attributes from registry mapping
        script_path = registry[t_type]["script_path"]
        data_size = registry[t_type]["default_data_size_mb"]
        
        # Instantiate object and register it into our temporary lookup tracking dictionary
        task_obj = DAGTask(task_id=t_id, task_type=t_type, script_path=script_path, data_size_mb=data_size)
        task_lookup[t_id] = task_obj
        dag_topology.append(task_obj)

    # Second Pass: Link dependencies across objects dynamically
    for block in pipeline_definition:
        t_id = block["task_id"]
        deps = block.get("dependencies", [])
        
        for dep_id in deps:
            if dep_id in task_lookup:
                # Append the true object pointer to the dependency array
                task_lookup[t_id].dependencies.append(task_lookup[dep_id])
            else:
                print(f"❌ [CONFIG] Dependency Error: Task {t_id} references an unknown parent '{dep_id}'")
                exit(1)
                
    # 3. Calculate scheduling offsets using HEFT Engine
    optimized_schedule = calculate_heft_schedule(dag_topology)
    
    print("\n🔒 [SYNCHRONIZATION LOCK] Establishing Global System Base-Clock...")
    global_dag_start_epoch = time.time() + 3.0
    print(f"⏰ Master Time-Zero Epoch designated as: {global_dag_start_epoch:.4f}")
    
    # 4. Dispatch tasks concurrently over the wire
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for task in optimized_schedule:
            executor.submit(dispatch_task, task, global_dag_start_epoch)
            
    print("\n🎉 [SUCCESS] All dynamically configured tasks dispatched cleanly.")