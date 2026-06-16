# profilers/tegrastats_parser.py
import subprocess
import re
import time
import os

class TegrastatsParser:
    """
    Spawns and manages a background tegrastats process on NVIDIA Jetson boards
    using localized file streaming to parse real-time CPU, GPU, and Power telemetry.
    """
    def __init__(self, sampling_interval_ms=100, log_path="tegrastats.log"):
        self.interval = sampling_interval_ms
        self.log_path = log_path
        self.process = None

    def start(self):
        """Launches the tegrastats utility streaming directly to a file descriptor."""
        # Clean up any lingering old log files before starting
        if os.path.exists(self.log_path):
            try:
                os.remove(self.log_path)
            except OSError:
                pass
                
        try:
            # Open a file handle to avoid kernel PIPE buffer allocation limits
            self.log_file = open(self.log_path, "w")
            
            self.process = subprocess.Popen(
                ['tegrastats', '--interval', str(self.interval)],
                stdout=self.log_file,
                stderr=subprocess.DEVNULL,
                text=True
            )
            print("📊 [TEGRASTATS] Telemetry recording safely bound to disk stream.")
        except FileNotFoundError:
            print("⚠️ [TEGRASTATS] 'tegrastats' utility not found. Using simulation fallback.")
            self.process = None

    def stop(self):
        """Terminates the background execution loop and extracts the compiled metrics."""
        if not self.process:
            return self._get_empty_metrics("Tegrastats not initialized or unavailable on this architecture.")

        # Cleanly terminate the telemetry process
        self.process.terminate()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            
        # Close the file handle to flush all remaining bytes to disk
        self.log_file.close()

        power_samples = []
        gpu_samples = []

        # Read the file contents safely
        if os.path.exists(self.log_path):
            try:
                with open(self.log_path, "r") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        # 1. Parse GPU utilization percentage
                        gpu_match = re.search(r'GR3D_FREQ\s+(\d+)%', line)
                        if gpu_match:
                            gpu_samples.append(int(gpu_match.group(1)))

                        # 2. Parse Power Draw in milliwatts
                        power_match = re.search(r'POM_5V_IN\s+(\d+)mW', line) or re.search(r'VDD_IN\s+(\d+)mW', line)
                        if power_match:
                            power_samples.append(int(power_match.group(1)))
            finally:
                # Clean up the file to keep the folder spotless
                try:
                    os.remove(self.log_path)
                except OSError:
                    pass

        return self._compile_metrics(power_samples, gpu_samples)

    def _compile_metrics(self, power_samples, gpu_samples):
        """Computes summary statistics from the parsed sample lists."""
        if not power_samples and not gpu_samples:
            return self._get_empty_metrics("No data points captured during the profiling window.")

        avg_power_w = (sum(power_samples) / len(power_samples)) / 1000.0 if power_samples else 0.0
        peak_power_w = max(power_samples) / 1000.0 if power_samples else 0.0
        avg_gpu_util = sum(gpu_samples) / len(gpu_samples) if gpu_samples else 0.0
        peak_gpu_util = max(gpu_samples) if gpu_samples else 0.0

        return {
            "status": "SUCCESS",
            "avg_power_watts": round(avg_power_w, 2),
            "peak_power_watts": round(peak_power_w, 2),
            "avg_gpu_utilization_pct": round(avg_gpu_util, 1),
            "peak_gpu_utilization_pct": peak_gpu_util,
            "error_message": ""
        }

    def _get_empty_metrics(self, msg):
        return {
            "status": "SKIPPED",
            "avg_power_watts": 0.0,
            "peak_power_watts": 0.0,
            "avg_gpu_utilization_pct": 0.0,
            "peak_gpu_utilization_pct": 0,
            "error_message": msg
        }

if __name__ == "__main__":
    print("--- Simulating Jetson Hardware Telemetry Collection ---")
    parser = TegrastatsParser(sampling_interval_ms=100)
    parser.start()
    
    # Simulate workload processing cycle window
    time.sleep(2)
    
    summary = parser.stop()
    print("Compiled Tegrastats Telemetry:\n", summary)