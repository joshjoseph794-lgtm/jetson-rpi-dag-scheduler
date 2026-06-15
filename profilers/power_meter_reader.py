# profilers/power_meter_reader.py
import subprocess
import time
import sys
import os

class PowerMeterReader:
    """
    Handles power telemetry tracking for non-NVIDIA edge nodes (like Raspberry Pi).
    Uses external smart plug APIs if available; otherwise, falls back to an 
    empirical software-estimation model based on CPU load and core frequency.
    """
    def __init__(self, smart_plug_ip=None):
        self.plug_ip = smart_plug_ip
        self.is_recording = False
        self.start_time = 0
        self.load_samples = []

    def start(self):
        """Starts the profiling window and clears historical sample buffers."""
        self.is_recording = True
        self.start_time = time.perf_counter()
        self.load_samples = []
        print("🔌 [POWER_METER] Raspberry Pi power profiling active.")

    def sample_load(self):
        """
        Periodically samples system load to increase estimation accuracy.
        Call this within a background loop while the workload runs.
        """
        if not self.is_recording:
            return
            
        try:
            # Read 1-minute load average from the operating system
            load_avg = os.getloadavg()[0]
            self.load_samples.append(load_avg)
        except Exception:
            self.load_samples.append(1.0) # Default to single-core load if OS reading fails

    def stop(self):
        """Closes the monitoring window and computes power metrics."""
        if not self.is_recording:
            return self._get_empty_metrics("Profiler was not started cleanly.")

        self.is_recording = False
        duration = time.perf_counter() - self.start_time

        # If an external smart hardware meter is defined, try scraping its API
        if self.plug_ip:
            hardware_metrics = self._scrape_external_hardware()
            if hardware_metrics:
                return hardware_metrics

        # Fallback: Run our software empirical estimation model
        return self._estimate_pi_power()

    def _estimate_pi_power(self):
        """
        Empirical software model mapping Raspberry Pi 4 power curves:
        - Idle Baseline: ~2.5 Watts
        - Peak Multi-Core Load: ~6.0 - 6.5 Watts
        """
        avg_load = sum(self.load_samples) / len(self.load_samples) if self.load_samples else 0.5
        
        # Scale power linearly based on CPU load bounds (Max 4 cores on Pi 4)
        scaled_load_factor = min(avg_load / 4.0, 1.0)
        
        idle_w = 2.5
        max_load_w = 6.2
        
        avg_power_w = idle_w + (scaled_load_factor * (max_load_w - idle_w))
        peak_power_w = idle_w + (max(self.load_samples, default=1.0) / 4.0) * (max_load_w - idle_w)
        peak_power_w = min(peak_power_w, max_load_w)

        return {
            "status": "SUCCESS",
            "avg_power_watts": round(avg_power_w, 2),
            "peak_power_watts": round(peak_power_w, 2),
            "avg_gpu_utilization_pct": 0.0, # Raspberry Pi does not export direct GPU cores for matrix tasks
            "peak_gpu_utilization_pct": 0,
            "error_message": "Using empirical software estimation model."
        }

    def _scrape_external_hardware(self):
        """Placeholder for network-attached power meters (Tuya/Kasa Smart Plugs)"""
        # In full physical deployments, you'd integrate python-kasa or tinytuya here
        # to pull exact live milliwatt metrics via JSON-RPC over Wi-Fi.
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
    
    # Simulate sampling system load during an intensive task execution
    for _ in range(5):
        time.sleep(0.3)
        meter.sample_load()
        
    summary = meter.stop()
    print("Compiled Pi Power Footprint Summary:\n", summary)