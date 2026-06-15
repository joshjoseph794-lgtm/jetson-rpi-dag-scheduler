# remote_worker.py
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import time
import subprocess
import sys

class TaskHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        payload = json.loads(post_data.decode('utf-8'))
        
        task_id = payload.get("task_id", "unknown")
        task_type = payload.get("task_type", "unknown")
        script_path = payload.get("script_path", "")
        allocated_duration = str(payload.get("payload_duration", "1.5"))
        
        # --- PHASE 4 UPGRADE: EXTRACT SCHEDULED START TIME ---
        scheduled_start_time_str = payload.get("scheduled_start_time", "0.0")
        scheduled_start_time = float(scheduled_start_time_str)
        
        print(f"\n📥 [WORKER] HTTP POST Received - Task {task_id} ({task_type})")
        
        # --- THE TIME-TRIGGERED CLOCK GATE ---
        if scheduled_start_time > 0.0:
            current_time = time.time()
            if scheduled_start_time > current_time:
                wait_time = scheduled_start_time - current_time
                print(f"⏳ [CLOCK GATE] Holding task {task_id}. Sleeping for {wait_time:.4f}s until target epoch...")
                
                # High-precision millisecond loop to guarantee synchronized start time
                while time.time() < scheduled_start_time:
                    time.sleep(0.001)
                print(f"🚀 [CLOCK GATE] Target time reached! Releasing task {task_id} for execution.")
            else:
                print(f"⚠️ [CLOCK GATE] Task arrived late by {current_time - scheduled_start_time:.4f}s. Executing immediately.")
        
        execution_time = 1.50
        status_flag = "SUCCESS"
        error_msg = ""
        
        try:
            print(f"⏳ [WORKER] Spawning standalone execution sub-process...")
            start_time = time.time()
            
            # Execute the workload script natively on the node
            result = subprocess.run(
                ["python3", script_path, "--task", task_type, "--duration", allocated_duration],
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                universal_newlines=True,
                timeout=45
            )
            execution_time = time.time() - start_time
            
            if result.returncode != 0:
                status_flag = "FAILED"
                error_msg = result.stderr
                print(f"❌ [WORKER] Script failed: {error_msg}")
            else:
                print(f"✅ [WORKER] Script completed successfully in {execution_time:.4f}s.")
                
        except Exception as e:
            status_flag = "FAILED"
            error_msg = str(e)
            print(f"⚠️ [WORKER] Runtime exception: {error_msg}")

        response_data = {
            "task_id": str(task_id),
            "status": status_flag,
            "execution_time_sec": float(execution_time),
            "error_message": str(error_msg)
        }
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response_data).encode('utf-8'))

def run(port=5000):
    server_address = ('', port)
    httpd = HTTPServer(server_address, TaskHandler)
    print(f"🖥️ [HTTP DAEMON] Production-Isolated Worker active on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down HTTP daemon.")
        httpd.server_close()

if __name__ == '__main__':
    # --- DYNAMIC PORT UPGRADE ---
    # Allows passing port via CLI argument: `python3 remote_worker.py 5001`
    # Defaults to port 5000 if no arguments are provided
    target_port = 5000
    if len(sys.argv) > 1:
        try:
            target_port = int(sys.argv[1])
        except ValueError:
            print("⚠️ Invalid port argument, falling back to default port 5000.")
            
    run(port=target_port)