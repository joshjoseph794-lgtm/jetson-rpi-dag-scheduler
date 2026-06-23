import grpc
import time
import sys
import os

# Ensure the root directory is in the path so generated files can find each other smoothly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    import runtime.messages_pb2 as messages_pb2
    import runtime.messages_pb2_grpc as messages_pb2_grpc
except ModuleNotFoundError:
    print("[Error] gRPC generated stubs not found. Please ensure you have run the protoc compilation step:")
    print("python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. runtime/messages.proto")
    raise

class GRPCDispatcher:
    def __init__(self, timeout=60):
        """
        Initializes the low-latency gRPC dispatch layer for remote task orchestration.
        """
        self.timeout = timeout

    def dispatch_task(self, worker_ip, port, task_id, task_type, script_path, data_size_mb, scheduled_start_time):
        """
        Establishes an insecure gRPC channel to the target edge worker (Jetson/Pi),
        packages the payload into a proto message, and waits for execution metrics.
        
        Returns a uniform dictionary matching the coordinator's tracking schema.
        """
        target_address = f"{worker_ip}:{port}"
        print(f"[gRPC Dispatcher] Packaging task '{task_id}' payload for execution endpoint: {target_address}")

        # Open an optimized, short-lived insecure channel to transfer the payload
        with grpc.insecure_channel(target_address) as channel:
            stub = messages_pb2_grpc.TaskDispatcherStub(channel)

            # Map Python standard types directly into the updated protobuf schema format
            request = messages_pb2.TaskRequest(
                task_id=str(task_id),
                task_type=str(task_type),
                script_path=str(script_path),
                data_size_mb=float(data_size_mb),
                scheduled_start_time=str(scheduled_start_time) # <-- ADDED: Matches your compiled proto stub contract
            )

            try:
                start_network_time = time.time()
                
                # Execute remote procedure call synchronously across the local network cluster
                response = stub.DispatchTask(request, timeout=self.timeout)
                
                rtt_latency = time.time() - start_network_time
                print(f"[gRPC Dispatcher] Execution complete for task '{task_id}' (Network RTT: {rtt_latency:.4f}s)")
                
                return {
                    "status": response.status,                     # "SUCCESS" or "FAILED"
                    "execution_time_sec": response.execution_time_sec, # True hardware execution time
                    "error_message": response.error_message
                }

            except grpc.RpcError as e:
                error_details = e.details() if hasattr(e, 'details') else str(e)
                error_code = e.code() if hasattr(e, 'code') else "UNKNOWN"
                
                print(f"[gRPC Dispatcher Link Error] Connection to {target_address} failed for task '{task_id}'. Code: {error_code}")
                return {
                    "status": "FAILED",
                    "execution_time_sec": -1.0,
                    "error_message": f"gRPC Exception [{error_code}]: {error_details}"
                }