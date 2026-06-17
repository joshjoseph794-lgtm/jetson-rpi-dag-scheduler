import os
import json

# 1. Establish the absolute path anchor point
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
configs_dir = os.path.join(BASE_DIR, "configs")

# 2. Map the precise absolute locations matching profile_cluster.py
compute_profile_path = os.path.join(configs_dir, "worst_case_compute.json")
network_profile_path = os.path.join(configs_dir, "worst_case_network.json")

# 3. Example of how your loading block should safely consume them:
def load_cluster_profiles():
    # Load Compute Profiles
    if os.path.exists(compute_profile_path):
        with open(compute_profile_path, "r") as f:
            worst_case_compute = json.load(f)
    else:
        print(f" [COST MODEL] Warning: Compute profile missing at {compute_profile_path}. Using safe defaults.")
        worst_case_compute = {} # Or fallback code

    # Load Network Profiles
    if os.path.exists(network_profile_path):
        with open(network_profile_path, "r") as f:
            worst_case_network = json.load(f)
    else:
        print(f" [COST MODEL] Warning: Network profile missing at {network_profile_path}. Using safe defaults.")
        worst_case_network = {} # Or fallback code
        
    return worst_case_compute, worst_case_network