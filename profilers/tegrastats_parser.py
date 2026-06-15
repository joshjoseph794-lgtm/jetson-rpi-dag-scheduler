# profilers/tegrastats_parser.py
import subprocess
import re
import time
import sys

class TegrastatsParser:
    """
    Spawns and manages a background tegrastats process on NVIDIA Jetson boards
    to parse real-time CPU, GPU, RAM, and hardware power draw telemetry.
    """
    def __init__(self, sampling_interval_ms=100):
        self.interval = sampling_interval_ms
        self.process = None
        self.power_samples = []
        self.gpu_samples = []

    def start(self):
        """Launches the tegrastats utility as a non-blocking background subprocess."""
        self.power_samples = []
        self.gpu_samples = []
        
        try:
            # Launch tegrastats with the specified sampling interval
            self.process = subprocess.Popen(
                ['tegrastats', '--interval', str(self.interval)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            print("📊 [TEGRASTATS] Telemetry recording started.")
        except FileNotFoundError:
            print("⚠️ [TEGRASTATS] 'tegrastats' utility not found. (Are you running on non-Jetson hardware?)")
            self.process = None

    def stop(self):
        """Terminates the background process and parses the collected text streams."""
        if not self.process:
            return self._get_empty_metrics("Tegrastats not initialized or unavailable.")

        # Kill the background process cleanly
        self.process.terminate()
        try:
            stdout, _ = self.process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.kill()
            stdout, _ = self.process.communicate()

        # Parse the text lines collected from the output stream
        lines = stdout.split('\n')
        for line in lines:
            if not line.strip():
                continue
            
            # 1. Parse GPU Utilization percentage (Format on Nano usually: GR3D_FREQ 12%@76)
            gpu_match = re.search(r'GR3D_FREQ\s+(\d+)%', line)
            if gpu_match:
                self.gpu_samples.append(int(gpu_match.group(1)))

            # 2. Parse Power Draw in milliwatts (Format: POM_5V_IN XXXXmW/YYYYmW or VDD_IN XXXXmW)
            # We look for the main input voltage rail on the Jetson Nano
            power_match = re.search(r'POM_5V_IN\s+(\d+)mW', line) or re.search(r'VDD_IN\s+(\d+)mW', line)
            if power_match:
                self.power_samples.append(int(power_match.group(1)))

        return self._compile_metrics()

    def _compile_metrics(self):
        """Computes summary statistics from the parsed sample lists."""
        if not self.power_samples and not self.gpu_samples:
            return self._get_empty_metrics("No data points captured during the profiling window.")

        avg_power_w = (sum(self.power_samples) / len(self.power_samples)) / 1000.0 if self.power_samples else 0.0
        peak_power_w = max(self.power_samples) / 1000.0 if self.power_samples else 0.0
        avg_gpu_util = sum(self.gpu_samples) / len(self.gpu_samples) if self.gpu_samples else 0.0
        peak_gpu_util = max(self.gpu_samples) if self.gpu_samples else 0.0

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

# Isolated test loop
if __name__ == "__main__":
    print("--- Simulating Jetson Hardware Telemetry Collection ---")
    parser = TegrastatsParser(sampling_interval_ms=100)
    parser.start()
    
    # Simulate a vision task processing for 2 seconds
    time.sleep(2)
    
    summary = parser.stop()
    print("Compiled Tegrastats Telemetry:\n", summary)