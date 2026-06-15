# workloads/synthetic/cpu_task.py
import argparse
import time
import os
import sys

def main():
    # 1. Instantiate the formal command-line interface parser
    parser = argparse.ArgumentParser(description="Edge Cluster Workload Execution Target Harness")
    parser.add_argument("--task", type=str, required=True, help="Name of the pipeline task segment to execute")
    parser.add_argument("--duration", type=float, required=True, help="Computation processing threshold in seconds")
    
    args = parser.parse_args()
    
    print(f"\n[WORKLOAD-START] Node PID {os.getpid()} invoked target: '{args.task}'")
    print(f"[WORKLOAD-EXEC] Allocating local threads. Simulating work active loop for {args.duration}s...")
    
    # 2. Execute high-precision hardware time-tracking block
    start_time = time.perf_counter()
    
    # Run an active-wait loop instead of time.sleep to simulate actual CPU crunching
    while (time.perf_counter() - start_time) < args.duration:
        # Performing basic arithmetic operations continuously to generate real CPU load
        _ = 9999.0 * 9999.0
        
    elapsed = time.perf_counter() - start_time
    
    # 3. Dynamic output logging based on pipeline contextual flags
    if "Capture" in args.task:
        print("📸 [SUCCESS] Video frame matrix latched into buffer memory.")
    elif "Resize" in args.task or "Preprocess" in args.task:
        print("📐 [SUCCESS] High-definition matrix scaled down to 416x416 resolution.")
    elif "DNN" in args.task or "Detect" in args.task:
        print("🧠 [SUCCESS] TensorRT engine inference pass complete. Detected objects mapped cleanly.")
    elif "Overlay" in args.task or "Track" in args.task:
        print("🎨 [SUCCESS] Rendered bounding box coordinates onto display canvas.")
    elif "Logging" in args.task:
        print("💾 [SUCCESS] Metric logs committed to relational database cluster.")
    else:
        print("✅ [SUCCESS] Execution block completed cleanly.")
        
    print(f"[WORKLOAD-END] Task '{args.task}' finished processing in {elapsed:.4f} seconds.")

if __name__ == "__main__":
    main()