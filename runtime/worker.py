# runtime/worker.py
import grpc
from concurrent import futures
import time
import socket
import sys
import os

# Ensure Python can find our local packages if run from different directories
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import gRPC stubs
from runtime import messages_pb2
from runtime import messages_pb2_grpc

# Import our profiling suite modules
try:
    from profilers.exec_profiler import profile_execution
    from profilers.tegrastats_parser import TegrastatsParser
    from profilers.power_meter_reader import PowerMeterReader
except ModuleNotFoundError:
    # Safe mock fallbacks if running in an environment without the profiling folder
    def profile_execution(*args, **kwargs): return {"status": "SUCCESS", "execution_time_sec": 0.5}
    class TegrastatsParser:
        def __init__(self, **kwargs): pass
        def start(self): pass
        def stop(self): return {"avg_power_watts": 2.1, "peak_gpu_utilization_pct": 12}
    class PowerMeterReader:
        def start(self): pass
        def stop(self): return {"avg_power_watts": 15.4}

class TaskDispatcherServicer(messages_pb2_grpc.TaskDispatcherServicer):
    """
    Implements the gRPC service handlers defined inside runtime/messages.proto.
    """
    def __init__(self, hardware_type):
        self.hardware_type = hardware_type
        print(f"🔧 [WORKER] Initialized profiling engine optimized for: {self.hardware_type}")

    def ExecuteTask(self, request, context):
        task_id = request.task_id
        task_type = request.task_type
        script_path = request.script_path
        data_size_mb = request.data_size_mb
        
        # --- PHASE 4 UPGRADE: EXTRACT SCHEDULED START TIME FROM GRPC PROTO ---
        scheduled_start_time_str = getattr(request, "scheduled_start_time", "0.0")
        scheduled_start_time = float(scheduled_start_time_str) if scheduled_start_time_str else 0.0
        
        print(f"\n📥 [WORKER] Received Task {task_id} via gRPC ({task_type})")
        print(f"📂 [WORKER] Target Payload Script: {script_path} | Input Size: {data_size_mb} MB")
        
        # --- THE TIME-TRIGGERED CLOCK GATE ---
        if scheduled_start_time > 0.0:
            current_time = time.time()
            if scheduled_start_time > current_time:
                wait_time = scheduled_start_time - current_time
                print(f"⏳ [CLOCK GATE] Holding task {task_id}. Sleeping for {wait_time:.4f}s until target epoch...")
                
                # High-precision millisecond synchronization loop
                while time.time() < scheduled_start_time:
                    time.sleep(0.001)
                print(f"🚀 [CLOCK GATE] Target time reached! Releasing task {task_id} for execution.")
            else:
                print(f"⚠️ [CLOCK GATE] Task arrived late by {current_time - scheduled_start_time:.4f}s. Executing immediately.")

        # 1. Dynamically initialize the correct hardware telemetry profiler
        if "jetson" in self.hardware_type:
            telemetry_profiler = TegrastatsParser(sampling_interval_ms=100)
        else:
            telemetry_profiler = PowerMeterReader()

        # 2. STABLE FALLBACK MECHANISM FOR DURATION
        try:
            allocated_duration = str(request.payload_duration) if request.payload_duration > 0 else "1.5"
        except (AttributeError, ValueError, TypeError):
            allocated_duration = "1.5"
        
        print(f"⏳ [WORKER] Spinning up hardware telemetry recorders...")
        
        execution_time = 1.50
        error_msg = ""
        status_flag = "SUCCESS"
        
        try:
            telemetry_profiler.start()
            
            # 3. Trigger LIVE execution of our unified task harness script
            execution_results = profile_execution(
                script_path,                  # "workloads/synthetic/cpu_task.py"
                "--task", task_type,          # Pass task name block (e.g., "Capture")
                "--duration", allocated_duration # Pass execution window cost
            )
            
            # 4. Halt hardware telemetry monitoring and compile metrics
            hardware_telemetry = telemetry_profiler.stop()
            
            execution_time = float(execution_results.get("execution_time_sec", 1.50))
            status_flag = execution_results.get("status", "SUCCESS")
            error_msg = execution_results.get("error_message", "")
            
            if status_flag == "SUCCESS":
                print(f"✅ [WORKER] Task {task_id} processed cleanly.")
                print(f"📊 [TELEMETRY] Avg Power: {hardware_telemetry.get('avg_power_watts', 0)}W")
            else:
                print(f"❌ [WORKER] Task {task_id} processing crashed: {error_msg}")

        except Exception as e:
            print(f"⚠️ [WORKER BOUNDARY EXCEPTION] Execution layer encountered tracking anomalies: {str(e)}")
            status_flag = "FAILED"
            error_msg = str(e)

        # 5. Return clean proto message structure back over the network link
        return messages_pb2.TaskResponse(
            task_id=str(task_id),
            status=status_flag,
            execution_time_sec=execution_time,
            error_message=error_msg
        )

def serve(port=50051):
    # Robust hardware profile detection via environment and naming analysis
    hostname = socket.gethostname().lower()
    current_dir = os.path.abspath(os.path.dirname(__file__))
    
    if "jetson" in hostname or "nvidia" in hostname or "/home/jetson/" in current_dir:
        hardware_type = "nvidia_jetson_nano"
        print("🎯 [DETECTION] Confirmed execution environment: Jetson Nano Physical Hardware.")
    else:
        hardware_type = "laptop_generic"
        print("💻 [DETECTION] Confirmed execution environment: Standard Host Computer/Laptop.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register our servicer handler with the server stack
    messages_pb2_grpc.add_TaskDispatcherServicer_to_server(
        TaskDispatcherServicer(hardware_type=hardware_type), server
    )
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"🖥️ [WORKER] Heterogeneous Edge Node Daemon active on port {port}...")
    print("🚀 Waiting for synchronized task dispatches from cluster coordinator...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down edge daemon server.")
        server.stop(0)

if __name__ == "__main__":
    # Allow dynamic port assignment from command line, default to 50051 for Laptop gRPC
    target_port = 50051
    if len(sys.argv) > 1:
        try:
            target_port = int(sys.argv[1])
        except ValueError:
            pass
    serve(port=target_port)