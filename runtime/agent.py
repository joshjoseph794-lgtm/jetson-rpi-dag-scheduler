import socket
import json
import time

def handle_task(task_data):
    """
    Simulates the actual workload execution on the edge device.
    In your full pipeline, this will trigger your actual OpenCV/Inference code.
    """
    task_id = task_data.get("id")
    task_name = task_data.get("name")
    simulated_cost = task_data.get("execution_cost", 1)
    
    print(f"📥 [AGENT] Received Task {task_id}: {task_name}")
    print(f"⏳ [AGENT] Executing workload for {simulated_cost}ms...")
    
    # Simulate processing time
    time.sleep(simulated_cost / 1000.0) 
    
    print(f"✅ [AGENT] Task {task_id} completed successfully.")
    return {"status": "SUCCESS", "task_id": task_id, "actual_runtime_ms": simulated_cost}

def start_agent(port=5000):
    # Create a standard TCP Socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Bind to all available network interfaces on this device
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    
    print(f"🖥️ [AGENT] Edge node listener active on port {port}...")
    print("🚀 Waiting for task dispatches from cluster coordinator...")
    
    try:
        while True:
            client_socket, client_address = server_socket.accept()
            print(f"\n🔗 Connection established from coordinator: {client_address}")
            
            # Receive incoming task data packet
            packet = client_socket.recv(4096).decode('utf-8')
            if not packet:
                continue
                
            try:
                task_data = json.loads(packet)
                # Execute workload
                result = handle_task(task_data)
                # Return telemetry confirmation back to laptop
                client_socket.send(json.dumps(result).encode('utf-8'))
            except json.JSONDecodeError:
                print("❌ Received malformed data packet.")
            finally:
                client_socket.close()
                
    except KeyboardInterrupt:
        print("\n🛑 Shutting down edge node listener gracefully.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_agent()