import json
import os

class CostModel:
    def __init__(self, profiles_path="configs/profiles.json"):
        """
        Initializes the cost model by loading the heterogeneous device profiles 
        and network topology matrices.
        """
        self.profiles_path = profiles_path
        self.devices = {}
        self.network_matrix = {}
        self.load_profiles()

    def load_profiles(self):
        """Loads and parses the profiles JSON config file."""
        if not os.path.exists(self.profiles_path):
            raise FileNotFoundError(f"Configuration profile not found at: {self.profiles_path}")
            
        with open(self.profiles_path, 'r') as f:
            data = json.load(f)
            self.devices = data.get("devices", {})
            self.network_matrix = data.get("network_matrix_mbps", {})

    def get_computation_cost(self, worker_id, task_type, base_cost):
        """
        Dynamically scales the computation cost based on device properties.
        """
        # Ensure worker_id is treated consistently as a lowercase string or integer
        w_id = str(worker_id).lower()

        # 1. GPU bound tasks: Jetson Nano accelerates it, Laptop CPU struggles
        if task_type == "gpu_bound" or "inference" in task_type.lower():
            if "jetson" in w_id or w_id == "1":
                return base_cost * 0.1  # Jetson processes AI 10x faster!
            else:
                return base_cost * 1.5  # Laptop CPU takes 1.5x longer

        # 2. IO bound tasks: Laptop SSD is faster than Jetson's SD card reader
        if task_type == "io_bound":
            if "jetson" in w_id or w_id == "1":
                return base_cost * 2.0  # Slow SD card writes
            else:
                return base_cost * 0.8  # Fast Laptop NVMe SSD

        # Default fallback multiplier if no custom hardware rules apply
        return base_cost

    def get_communication_cost(self, source_device, target_device, data_size_mb):
        """
        Calculates data transfer latency between two edge devices.
        Formula: (Data Size in Megabytes * 8) / Bandwidth in Mbps = Time in seconds.
        If data stays on the same device, communication cost is 0.
        """
        if source_device == target_device:
            return 0.0
            
        # Check both directional keys in the network matrix mapping
        key_forward = f"{source_device}_to_{target_device}"
        key_backward = f"{target_device}_to_{source_device}"
        
        bandwidth = self.network_matrix.get(key_forward, self.network_matrix.get(key_backward, None))
        
        if bandwidth is None:
            # Silently fallback during active HEFT planning cycles
            bandwidth = 10.0 # Slow fallback baseline
            
        if bandwidth == 0:
            return float('inf')
            
        # Convert MegaBytes to Megabits (data_size * 8) and divide by Megabits per second (Mbps)
        return float((data_size_mb * 8.0) / bandwidth)