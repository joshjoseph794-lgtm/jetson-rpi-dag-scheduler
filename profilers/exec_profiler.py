# profilers/exec_profiler.py
import time
import subprocess
import sys
import os

def profile_execution(script_path, *args):
    """
    Executes a target python script as a subprocess and profiles its exact runtime.
    
    Args:
        script_path (str): Relative or absolute path to the python workload script.
        *args: Optional arguments to pass to the target script.
        
    Returns:
        dict: Telemetry results containing status, duration, and system codes.
    """
    if not os.path.exists(script_path):
        return {
            "status": "FAILED",
            "execution_time_sec": 0.0,
            "error_message": f"Workload script not found at target path: {script_path}"
        }

    print(f"⏱️ [PROFILER] Monitoring execution process for: {script_path}")
    
    cmd = [sys.executable, script_path] + list(args)
    start_time = time.perf_counter()
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120  # 2-minute safety guardrail timeout per task
        )
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"✅ [PROFILER] Process exited cleanly in {duration:.4f}s")
            return {
                "status": "SUCCESS",
                "execution_time_sec": float(duration),
                "error_message": ""
            }
        else:
            print(f"❌ [PROFILER] Process crashed with exit code {result.returncode}")
            return {
                "status": "FAILED",
                "execution_time_sec": float(duration),
                "error_message": f"Subprocess Stderr: {result.stderr.strip()}"
            }
            
    except subprocess.TimeoutExpired:
        print(f"⚠️ [PROFILER] Execution timed out after 120 seconds!")
        return {
            "status": "TIMEOUT",
            "execution_time_sec": 120.0,
            "error_message": "Task processing window exceeded cluster safety threshold."
        }
    except Exception as e:
        print(f"❌ [PROFILER] Unexpected hardware interruption: {str(e)}")
        return {
            "status": "FAILED",
            "execution_time_sec": 0.0,
            "error_message": str(e)
        }

if __name__ == "__main__":
    print("--- Testing Execution Profiler Isolation Layer ---")
    test_file = "test_workload.py"
    
    try:
        with open(test_file, "w") as f:
            f.write("import time\nprint('Processing data...')\ntime.sleep(0.5)")
            
        metrics = profile_execution(test_file)
        print("Profile Output Dictionary:", metrics)
        
    finally:
        # Guarantee cleanup even if an unexpected exception occurs during test setup
        if os.path.exists(test_file):
            os.remove(test_file)
            print("🧹 Temporary test file cleaned up successfully.")