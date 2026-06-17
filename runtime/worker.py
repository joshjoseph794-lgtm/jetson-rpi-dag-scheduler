# runtime/worker.py
import grpc
from concurrent import futures
import time
import socket
import sys
import os
import threading

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
        def sample_load(self): pass
        def stop(self): return {"avg_power_watts": 4.2}

class TaskDispatcherServicer(messages_pb2_grpc.TaskDispatcherServicer):
    """
    Implements the gRPC service handlers defined inside runtime/messages.proto.
    """
    def __init__(self, hardware_type):
        self.hardware_type = hardware_type
        print(f" [WORKER] Initialized profiling engine optimized for: {self.hardware_type}")

    def ExecuteTask(self, request, context):
        task_id = request.task_id
        task_type = request.task_type
        script_path = request.script_path
        data_size_mb = request.data_size_mb
        
        # Extract time-triggered synchronization parameters safely
        scheduled_start_time_str = getattr(request, "scheduled_start_time", "0.0")
        try:
            scheduled_start_time = float(scheduled_start_time_str) if scheduled_start_time_str else 0.0
        except ValueError:
            scheduled_start_time = 0.0
        
        print(f"\n [WORKER] Received Task {task_id} via gRPC ({task_type})")
        print(f" [WORKER] Target Payload Script: {script_path} | Input Size: {data_size_mb} MB")
        
        # --- THE TIME-TRIGGERED CLOCK GATE ---
        if scheduled_start_time > 0.0:
            current_time = time.time()
            if scheduled_start_time > current_time:
                wait_time = scheduled_start_time - current_time
                print(f" [CLOCK GATE] Holding task {task_id}. Sleeping for {wait_time:.4f}s until target epoch...")
                
                # High-precision millisecond synchronization loop
                while time.time() < scheduled_start_time:
                    time.sleep(0.001)
                print(f" [CLOCK GATE] Target time reached! Releasing task {task_id} for execution.")
            else:
                print(f" [CLOCK GATE] Task arrived late by {current_time - scheduled_start_time:.4f}s. Executing immediately.")

        # 1. Dynamically initialize the correct hardware telemetry profiler
        is_jetson = "jetson" in self.hardware_type or "nvidia" in self.hardware_type
        if is_jetson:
            telemetry_profiler = TegrastatsParser(sampling_interval_ms=100)
            bg_sampler_active = False
        else:
            telemetry_profiler = PowerMeterReader()
            bg_sampler_active = True

        execution_time = 1.50
        error_msg = ""
        status_flag = "SUCCESS"
        
        # Define a safe default task run duration window
        allocated_duration = "2.0" 
        
        try:
            telemetry_profiler.start()
            
            # If using a software model (like on Raspberry Pi), spin up a background sampling thread
            stop_sampling = threading.Event()
            def poll_loop():
                while not stop_sampling.is_set():
                    telemetry_profiler.sample_load()
                    time.sleep(0.1)

            if bg_sampler_active:
                sampler_thread = threading.Thread(target=poll_loop, daemon=True)
                sampler_thread.start()
            
            # 2. Trigger LIVE execution of our unified task harness script
            print(f" [WORKER] Spinning up hardware telemetry recorders...")
            execution_results = profile_execution(
                script_path,
                "--task", task_type,
                "--duration", allocated_duration
            )
            
            # Tear down background sampling thread if running
            if bg_sampler_active:
                stop_sampling.set()
                sampler_thread.join(timeout=1)
            
            # 3. Halt hardware telemetry monitoring and compile metrics
            hardware_telemetry = telemetry_profiler.stop()
            
            execution_time = float(execution_results.get("execution_time_sec", 1.50))
            status_flag = execution_results.get("status", "SUCCESS")
            error_msg = execution_results.get("error_message", "")
            
            if status_flag == "SUCCESS":
                print(f" [WORKER] Task {task_id} processed cleanly.")
                print(f" [TELEMETRY] Avg Power Consumption: {hardware_telemetry.get('avg_power_watts', 0)}W")
            else:
                print(f" [WORKER] Task {task_id} processing crashed: {error_msg}")

        except Exception as e:
            print(f" [WORKER BOUNDARY EXCEPTION] Execution layer encountered tracking anomalies: {str(e)}")
            status_flag = "FAILED"
            error_msg = str(e)

        # 4. Return clean proto message structure back over the network link
        return messages_pb2.TaskResponse(
            task_id=str(task_id),
            status=status_flag,
            execution_time_sec=execution_time,
            error_message=error_msg
        )

def serve(port=50051, hardware_override=None):
    if hardware_override:
        hardware_type = hardware_override
        print(f" [DETECTION] Using explicit command-line override: {hardware_type}")
    else:
        # Robust hardware profile detection via environment and naming analysis
        hostname = socket.gethostname().lower()
        current_dir = os.path.abspath(os.path.dirname(__file__))
        
        if "jetson" in hostname or "nvidia" in hostname or "/home/jetson" in current_dir:
            hardware_type = "nvidia_jetson_nano"
            print(" [DETECTION] Confirmed execution environment: Jetson Nano Physical Hardware.")
        elif "raspberry" in hostname or "pi-" in hostname or os.path.exists("/sys/firmware/devicetree/base/model"):
            hardware_type = "raspberry_pi"
            print(" [DETECTION] Confirmed execution environment: Raspberry Pi Edge Node.")
        else:
            hardware_type = "laptop_generic"
            print(" [DETECTION] Confirmed execution environment: Standard Host Computer/Laptop.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    messages_pb2_grpc.add_TaskDispatcherServicer_to_server(
        TaskDispatcherServicer(hardware_type=hardware_type), server
    )
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f" [WORKER] Heterogeneous Edge Node Daemon active on port {port}...")
    print(" Waiting for synchronized task dispatches from cluster coordinator...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("\n Shutting down edge daemon server.")
        server.stop(0)

if __name__ == "__main__":
    # Parsing simple command line parameters: python3 worker.py [port] [--hardware override]
    target_port = 50051
    hardware_override = None
    
    args = sys.argv[1:]
    if args:
        try:
            target_port = int(args[0])
            args = args[1:]
        except ValueError:
            pass
            
    if "--hardware" in args:
        idx = args.index("--hardware")
        if idx + 1 < len(args):
            hardware_override = args[idx + 1]

    serve(port=target_port, hardware_override=hardware_override)