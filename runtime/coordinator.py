import time
import sys
import socket
import json

# GLOBAL CONFIGURATION SWITCH
SIMULATION_MODE = False  # 🚀 TAKE IT LIVE!

# Hardware Node Configurations
NODES = {
    0: {"name": "Lenovo Laptop CPU", "ip": "127.0.0.1"},
    1: {"name": "Jetson Nano GPU", "ip": "172.16.12.35"}  # 👈 Your live Jetson IP
}
PORT = 5000  # Standardized port for agent communication

def execute_task_on_node(task_id, task_name, node_id, duration):
    node_info = NODES[node_id]
    print(f"\n[LAUNCHER] Sending Task {task_id} ({task_name}) to {node_info['name']}...")
    
    # Bundle data into a structured telemetry packet
    payload = {
        "id": task_id,
        "name": task_name,
        "execution_cost": int(duration * 1000)  # Convert seconds to milliseconds
    }

    if SIMULATION_MODE:
        # --- SIMULATION MODE ---
        print(f"[EMU-LINK] Emulating execution on Node {node_id} locally...")
        start_time = time.time()
        
        # Emulate the work inside your local pipeline
        time.sleep(duration)
        elapsed = time.time() - start_time
        
        print(f"[SUCCESS] Task {task_id} completed emulation in {elapsed:.2f} seconds.")
        return True
    
    else:
        # --- LIVE HARDWARE MODE (SOCKET STREAMING) ---
        target_ip = node_info["ip"]
        print(f"[HARDWARE-LINK] Connecting via Socket to {target_ip}:{PORT}...")
        start_time = time.time()
        
        try:
            # Establish a rapid TCP handshake connection channel
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)  # 5-second connection safety rail
            sock.connect((target_ip, PORT))
            
            # Send task info down the wire
            sock.sendall(json.dumps(payload).encode('utf-8'))
            
            # Await confirmation packet from agent.py running on target node
            response = sock.recv(4096).decode('utf-8')
            telemetry = json.loads(response)
            
            elapsed = time.time() - start_time
            print(f"[SUCCESS] Physical Node {node_id} completed task in {elapsed:.2f}s (Agent profile: {telemetry.get('actual_runtime_ms')}ms).")
            return True
            
        except Exception as e:
            print(f"❌ [CRITICAL ERROR] Network dispatch connection failure to Node {node_id}!", file=sys.stderr)
            print(f"Error Details: {e}", file=sys.stderr)
            return False

if __name__ == "__main__":
    print("--- Testing Integrated Socket Cluster Architecture ---")
    print(f"Current Environment Status: SIMULATION_MODE = {SIMULATION_MODE}")
    
    # Test layout
    execute_task_on_node(task_id=0, task_name="Data_Preprocessing", node_id=0, duration=2.0)
    execute_task_on_node(task_id=2, task_name="Model_Training", node_id=1, duration=3.0)