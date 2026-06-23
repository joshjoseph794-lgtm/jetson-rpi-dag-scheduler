# scheduler/cost_model.py
import json
import os

class CostModel:
    def __init__(self, compute_profile_path="worst_case_compute.json", network_profile_path="worst_case_network.json", **kwargs):
        """
        Initializes the empirical data-driven Cost Model by ingesting true 
        hardware execution benchmarks and network topology profiles.
        
        Supports legacy positional file paths as well as directory keyword 
        injection (e.g., profile_dir=...) used by orchestration scripts.
        """
        # Safely extract directory override if passed via run_benchmarks.py
        profile_dir = kwargs.get("profile_dir", None)
        
        if profile_dir:
            self.compute_profile_path = os.path.join(profile_dir, "worst_case_compute.json")
            self.network_profile_path = os.path.join(profile_dir, "worst_case_network.json")
        else:
            self.compute_profile_path = compute_profile_path
            self.network_profile_path = network_profile_path
        
        self.compute_matrix = {}
        self.network_matrix = {}
        
        self.load_profiles()

    def load_profiles(self):
        """Loads and parses the true profiling JSON matrices from disk."""
        # Ensure path strings use the targeted internal attributes set by __init__
        if os.path.exists(self.compute_profile_path):
            with open(self.compute_profile_path, 'r') as f:
                self.compute_matrix = json.load(f)
        else:
            # Try a direct relative path fallback if the absolute injection isn't reaching the folder
            fallback_path = os.path.join(os.path.dirname(__file__), "..", "configs", "worst_case_compute.json")
            if os.path.exists(fallback_path):
                with open(fallback_path, 'r') as f:
                    self.compute_matrix = json.load(f)
            else:
                print(f" [COST MODEL] Warning: Compute profile missing at {self.compute_profile_path}. Using safe defaults.")
                self.compute_matrix = {}

    def get_computation_cost(self, node_name, task_type):
        """
        Retrieves the true profiled worst-case execution time for a specific 
        task type on a targeted cluster hardware node.
        """
        node_clean = str(node_name).strip()
        
        # Directly extract the empirical benchmark value from our json data matrix
        if node_clean in self.compute_matrix and task_type in self.compute_matrix[node_clean]:
            return float(self.compute_matrix[node_clean][task_type])
            
        # Fallback baselines if a new node/task hasn't been profiled yet
        fallback_costs = {
            "Data_Preprocessing": 1.5,
            "Feature_Extraction": 1.5,
            "Model_Training": 1.5
        }
        return float(fallback_costs.get(task_type, 1.50))

    def get_communication_cost(self, source_node, target_node, data_size_mb):
        """
        Calculates the explicit data transfer latency between two edge devices.
        Formula: (Data Size in MB * 8) / Bandwidth in Mbps = Transmission Time in Seconds.
        """
        src = str(source_node).strip()
        dst = str(target_node).strip()

        # If data remains locally on the same hardware, transmission latency is completely zero
        if src == dst:
            return 0.0
            
        # Match directional keys in the network matrix mapping
        key_forward = f"{src}_to_{dst}"
        key_backward = f"{dst}_to_{src}"
        
        # Lookup actual bandwidth from network profile mapping
        bandwidth = self.network_matrix.get(key_forward, self.network_matrix.get(key_backward, None))
        
        # Fallback if link profile is missing
        if bandwidth is None:
            bandwidth = 10.0 # Standard local network assumption
            
        if float(bandwidth) == 0.0:
            return float('inf')
            
        # Convert MegaBytes to Megabits and divide by bandwidth (Mbps)
        return float((data_size_mb * 8.0) / float(bandwidth))