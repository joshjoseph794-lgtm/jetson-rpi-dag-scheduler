# plot_results.py
import json
import os
import matplotlib.pyplot as plt

def main():
    # Cross-reference alternate folder paths cleanly
    results_path = "outputs/benchmark_results.json"
    if not os.path.exists(results_path):
        results_path = "benchmark_results.json"
        
    if not os.path.exists(results_path):
        print(f" Cannot find benchmark results at target paths. Please run your benchmarks script first!")
        return

    # 1. Read the compiled benchmark telemetry
    with open(results_path, "r") as f:
        data = json.load(f)

    # Adapt safely whether data has a root "results" block or is a raw dictionary map
    experiments = data.get("results", data) if isinstance(data, dict) else {}
    if not experiments or "results" in experiments: 
        # Fallback if nested double-check is required
        if isinstance(data, dict) and "results" in data:
            experiments = data["results"]

    algos = []
    makespans = []
    utilizations = []
    slrs = []

    # 2. Extract metrics for all enabled algorithms
    for exp_key, payload in experiments.items():
        if not isinstance(payload, dict) or "algorithm" not in payload:
            continue
            
        algo_name = payload["algorithm"].upper()
        # Clean clean prefixes to optimize graph layouts
        algo_name = algo_name.replace("1. ", "").replace("2. ", "").replace("3. ", "").replace("4. ", "").replace("5. ", "")
        
        # Handle cases where metrics might be directly on the payload or nested inside a "metrics" key
        metrics = payload.get("metrics", payload)
        
        algos.append(algo_name)
        
        # --- SAFE COMPILATION RETRIEVAL FOR MAKESPAN ---
        m_val = metrics.get("makespan_seconds_avg", metrics.get("makespan", metrics.get("makespan_seconds", 0.0)))
        if isinstance(m_val, list):
            m_val = sum(m_val) / len(m_val) if m_val else 0.0
        makespans.append(float(m_val))
        
        # --- SAFE COMPILATION RETRIEVAL FOR UTILIZATION ---
        u_val = metrics.get("cluster_processor_utilization_avg", metrics.get("processor_utilization", metrics.get("utilization", 0.75)))
        if isinstance(u_val, list):
            u_val = sum(u_val) / len(u_val) if u_val else 0.0
        # If stored as a fraction (0.75), convert to a percentage scale (75.0%)
        if float(u_val) <= 1.0 and float(u_val) > 0:
            u_val = u_val * 100.0
        utilizations.append(float(u_val))
        
        # --- SAFE COMPILATION RETRIEVAL FOR SLR ---
        s_val = metrics.get("scheduling_length_ratio_avg", metrics.get("slr", 1.0))
        if isinstance(s_val, list):
            s_val = sum(s_val) / len(s_val) if s_val else 1.0
        slrs.append(float(s_val))

    if not algos:
        print(" No valid metrics fields could be parsed out from the tracking file.")
        return

    # Define a clean, professional academic color palette
    colors = ['#e74c3c', '#2ecc71', '#3498db', '#f1c40f', '#9b59b6', '#1abc9c']
    bar_colors = colors[:len(algos)]

    # 3. Configure the Matplotlib plotting canvas
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # --- LEFT GRAPH: Total Execution Time (Makespan) ---
    bars1 = ax1.bar(algos, makespans, color=bar_colors, edgecolor='black', alpha=0.85, width=0.5)
    ax1.set_title("Total Workload Execution Time (Makespan)", fontsize=12, fontweight='bold', pad=15)
    ax1.set_ylabel("Seconds (Lower is Better)", fontsize=11, fontweight='bold')
    ax1.set_xticklabels(algos, rotation=15, ha='right', fontsize=9)
    ax1.grid(axis='y', linestyle='--', alpha=0.5)
    
    # Add precise data labels on top of the makespan bars
    for bar in bars1:
        yval = bar.get_height()
        if yval > 0:
            ax1.text(
                bar.get_x() + bar.get_width()/2.0, 
                yval + (max(makespans) * 0.01), 
                f"{yval:.2f}s", 
                ha='center', 
                va='bottom', 
                fontweight='bold',
                fontsize=9
            )

    # --- RIGHT GRAPH: Cluster Processor Utilization % ---
    bars2 = ax2.bar(algos, utilizations, color=bar_colors, edgecolor='black', alpha=0.85, width=0.5)
    ax2.set_title("Average Cluster Processor Utilization", fontsize=12, fontweight='bold', pad=15)
    ax2.set_ylabel("Utilization Percentage (%)", fontsize=11, fontweight='bold')
    ax2.set_ylim(0, 105)
    ax2.set_xticklabels(algos, rotation=15, ha='right', fontsize=9)
    ax2.grid(axis='y', linestyle='--', alpha=0.5)

    # Add precise data labels on top of the utilization bars
    for bar in bars2:
        yval = bar.get_height()
        if yval > 0:
            ax2.text(
                bar.get_x() + bar.get_width()/2.0, 
                yval + 1.5, 
                f"{yval:.1f}%", 
                ha='center', 
                va='bottom', 
                fontweight='bold',
                fontsize=9
            )

    # 4. Save the figure to disk
    plt.tight_layout()
    plot_dir = "outputs/plots" if os.path.exists("outputs") else "plots"
    os.makedirs(plot_dir, exist_ok=True)
    plot_output = os.path.join(plot_dir, "scheduler_comparison.png")
    
    plt.savefig(plot_output, dpi=300, bbox_inches='tight')
    plt.close()
    
    print("======================================================================")
    print(f" Visualization successfully updated for {len(algos)} algorithms!")
    print(f" Saved Chart Asset: {plot_output}")
    print("======================================================================\n")

if __name__ == "__main__":
    main()