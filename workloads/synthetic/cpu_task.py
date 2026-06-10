import argparse
import time
import os
import sys

def main():
    parser = argparse.ArgumentParser(description="Edge Cluster Workload Execution Target")
    parser.add_argument("--task", type=str, required=True, help="Name of the task to execute")
    parser.add_argument("--duration", type=float, required=True, help="Simulated computation time in seconds")
    
    args = parser.parse_args()
    
    print(f"\n[WORKLOAD-START] Node PID {os.getpid()} invoked target: '{args.task}'")
    print(f"[WORKLOAD-EXEC] Allocating local threads. Simulating heavy math blocks for {args.duration}s...")
    
    # Simulate real work active loop
    start_time = time.time()
    time.sleep(args.duration)
    elapsed = time.time() - start_time
    
    # Custom print messages mimicking real image processing tasks
    if "Capture" in args.task:
        print("📸 [SUCCESS] Video frame matrix latched into buffer memory.")
    elif "Resize" in args.task:
        print("📐 [SUCCESS] High-definition matrix scaled down to 416x416 resolution.")
    elif "DNN" in args.task:
        print("🧠 [SUCCESS] TensorRT engine inference pass complete. Detected 4 objects.")
    elif "Overlay" in args.task:
        print("🎨 [SUCCESS] Rendered bounding box coordinates onto display canvas.")
    elif "Logging" in args.task:
        print("💾 [SUCCESS] Metric logs committed to relational database cluster.")
    else:
        print("✅ [SUCCESS] Execution block completed cleanly.")
        
    print(f"[WORKLOAD-END] Task '{args.task}' finished processing in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()