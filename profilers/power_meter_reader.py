# profilers/power_meter_reader.py
import time
import os

class PowerMeterReader:
    """
    Handles power telemetry tracking for non-NVIDIA edge nodes (like Raspberry Pi).
    Uses external smart plug APIs if available; otherwise, falls back to an 
    empirical software-estimation model based on instant CPU utilization.
    """
    def __init__(self, smart_plug_ip=None):
        self.plug_ip = smart_plug_ip
        self.is_recording = False
        self.start_time = 0
        self.utilization_samples = []
        self._last_cpu_times = None

    def _get_instant_cpu_utilization(self):
        """Reads instantaneous CPU utilization directly from Linux kernel statistics."""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
            parts = line.split()
            if not parts or parts[0] != "cpu":
                return 0.0
                
            # Convert jiffies fields to integers
            # user, nice, system, idle, iowait, irq, softirq...
            fields = [int(x) for x in parts[1:5]]
            user, nice, system, idle = fields
            
            total_work = user + nice + system
            total_time = total_work + idle
            
            if self._last_cpu_times is None:
                self._last_cpu_times = (total_work, total_time)
                return 0.0
                
            prev_work, prev_total = self._last_cpu_times
            diff_work = total_work - prev_work
            diff_total = total_time - prev_total
            
            self._last_cpu_times = (total_work, total_time)
            
            if diff_total == 0:
                return 0.0
            return float(diff_work / diff_total)
        except Exception:
            # Fallback for local debugging on non-Linux dev machines (e.g., Windows/macOS development)
            return 0.25 

    def start(self):
        """Starts the profiling window and clears historical sample buffers."""
        self.is_recording = True
        self.start_time = time.perf_counter()
        self.utilization_samples = []
        self._last_cpu_times = None
        # Prime the first reading block
        self._get_instant_cpu_utilization()
        print("🔌 [POWER_METER] Raspberry Pi power profiling active.")

    def sample_load(self):
        """
        Periodically samples instant CPU utilization to increase estimation accuracy.
        Call this within a background loop while the workload runs.
        """
        if not self.is_recording:
            return
        util = self._get_instant_cpu_utilization()
        self.utilization_samples.append(util)

    def stop(self):
        """Closes the monitoring window and computes power metrics."""
        if not self.is_recording:
            return self._get_empty_metrics("Profiler was not started cleanly.")

        self.is_recording = False
        duration = time.perf_counter() - self.start_time

        if self.plug_ip:
            hardware_metrics = self._scrape_external_hardware()
            if hardware_metrics:
                return hardware_metrics

        return self._estimate_pi_power()

    def _estimate_pi_power(self):
        """
        Empirical software model mapping Raspberry Pi 4 power curves:
        - Idle Baseline: ~2.5 Watts
        - Peak Multi-Core Load: ~6.2 Watts
        """
        avg_util = sum(self.utilization_samples) / len(self.utilization_samples) if self.utilization_samples else 0.10
        max_util = max(self.utilization_samples, default=0.10)
        
        idle_w = 2.5
        max_load_w = 6.2
        
        # Scale power linearly based on pure CPU core utility context maps
        avg_power_w = idle_w + (avg_util * (max_load_w - idle_w))
        peak_power_w = idle_w + (max_util * (max_load_w - idle_w))
        
        # Guardrail clamp safety bounds
        avg_power_w = min(max(avg_power_w, idle_w), max_load_w)
        peak_power_w = min(max(peak_power_w, idle_w), max_load_w)

        return {
            "status": "SUCCESS",
            "avg_power_watts": round(avg_power_w, 2),
            "peak_power_watts": round(peak_power_w, 2),
            "avg_gpu_utilization_pct": 0.0,
            "peak_gpu_utilization_pct": 0,
            "error_message": "Using empirical instantaneous software estimation model."
        }

    def _scrape_external_hardware(self):
        """Placeholder for network-attached power meters (Tuya/Kasa Smart Plugs)"""
        return None

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
    print("--- Simulating Raspberry Pi Power Footprint Analysis ---")
    meter = PowerMeterReader()
    meter.start()
    
    # Simulate sampling system load during an active task execution loop
    for _ in range(5):
        time.sleep(0.2)
        meter.sample_load()
        
    summary = meter.stop()
    print("Compiled Pi Power Footprint Summary:\n", summary)