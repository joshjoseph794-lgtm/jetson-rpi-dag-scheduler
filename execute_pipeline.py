# execute_pipeline.py
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# Establish runtime paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

from runtime.dispatcher_grpc import GRPCDispatcher

# ======================================================================
# 1. LOAD TRUE DEPLOYMENT TOPOLOGY
# ======================================================================
def load_cluster_nodes():
    nodes_path = os.path.join(BASE_DIR, "configs", "cluster_nodes.json")
    with open(nodes_path, "r") as f:
        return json.load(f)

def load_dag_structure():
    dag_path = os.path.join(BASE_DIR, "execution_pipeline.json")
    with open(dag_path, "r") as f:
        return json.load(f)

# ======================================================================
# 2. DEFINE THE LIVE DYNAMIC RUNNER
# ======================================================================
def execute_live_cluster():
    print("======================================================================")
    
    # Load physical infrastructure mappings from configurations
    try:
        nodes = load_cluster_nodes()
        tasks = load_dag_structure()
    except Exception as e:
        print(f"❌ Structural load failed: {e}")
        return

    # Create mapping of node name -> network targets
    cluster_topology = {}
    for node in nodes:
        cluster_topology[node["name"]] = {"ip": node["ip"], "port": 50051}

    # ======================================================================
    # 3. YOUR PRODUCTION RUNTIME SCHEDULE MAP
    # (Directly mapping your DAG tasks to your actual production scripts)
    # ======================================================================
    PRODUCTION_WORKLOADS = {
        "T1": {"node": "laptop",       "script": "workloads/synthetic/cpu_task.py"},
        "T2": {"node": "jetson",       "script": "workloads/synthetic/cpu_task.py"},
        "T3": {"node": "laptop",       "script": "workloads/synthetic/cpu_task.py"},
        "T4": {"node": "raspberry_pi", "script": "workloads/synthetic/cpu_task.py"}
    }

    dispatcher = GRPCDispatcher(timeout=130) # Must match your profiler's 120s limit safely
    
    completed_tasks = set()
    running_tasks = set()
    executor = ThreadPoolExecutor(max_workers=len(cluster_topology))
    
    def dispatch_real_worker_subprocess(task_id, task_type, target_node, script_path):
        node_net = cluster_topology[target_node]
        print(f"🔥 [DISPATCH] Sending real Subprocess -> {task_id} ({task_type}) onto {target_node.upper()}")
        
        try:
            # This triggers your worker's runtime/worker.py which imports profilers/exec_profiler.py
            response = dispatcher.dispatch_task(
                worker_ip=node_net["ip"],
                port=node_net["port"],
                task_id=task_id,
                task_type=task_type,
                script_path=script_path,
                data_size_mb=2.25,  # Pass true runtime data size constraints
                scheduled_start_time=str(time.time())
            )
            return task_id, response
        except Exception as e:
            return task_id, {"status": "FAILED", "error_message": str(e)}

    # ======================================================================
    # 4. DEPENDENCY-DRIVEN PARALLEL EXECUTION ENGINE
    # ======================================================================
    while len(completed_tasks) < len(tasks):
        futures = []
        
        for task in tasks:
            t_id = task["task_id"]
            if t_id in completed_tasks or t_id in running_tasks:
                continue
                
            # Verify if preceding structural branches are successfully finalized
            dependencies = task.get("dependencies", [])
            if all(parent in completed_tasks for parent in dependencies):
                running_tasks.add(t_id)
                workload_info = PRODUCTION_WORKLOADS[t_id]
                
                f = executor.submit(
                    dispatch_real_worker_subprocess,
                    t_id,
                    task["task_type"],
                    workload_info["node"],
                    workload_info["script"]
                )
                futures.append(f)
                
        if futures:
            for future in futures:
                task_id, response_payload = future.result()
                running_tasks.remove(task_id)
                
                # Check the exact return message from exec_profiler via gRPC
                if response_payload and response_payload.get("status") == "SUCCESS":
                    exec_time = response_payload.get("execution_time_sec", 0.0)
                    print(f"✅ [SUCCESS] Task {task_id} exited cleanly. True hardware time: {exec_time:.4f}s")
                    completed_tasks.add(task_id)
                else:
                    err = response_payload.get("error_message", "Unknown Subprocess Error")
                    print(f"❌ [CRITICAL BREAKAGE] {task_id} broke on worker hardware node!")
                    print(f"Reason from Remote Profiler stderr: {err}")
                    return
                    
        time.sleep(0.05)

    print("======================================================================")
    print("🎉 PIPELINE COMPLETE: All true remote subprocesses completed successfully!")

if __name__ == "__main__":
    execute_live_cluster()