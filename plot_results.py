# plot_results.py
import json
import os
import matplotlib.pyplot as plt

def main():
    results_path = "outputs/benchmark_results.json"
    if not os.path.exists(results_path):
        print(f"❌ Cannot find benchmark results at '{results_path}'. Please run 'python3 run_benchmarks.py' first!")
        return

    # 1. Read the compiled benchmark telemetry
    with open(results_path, "r") as f:
        data = json.load(f)

    experiments = data.get("results", {})
    if not experiments:
        print("⚠️ No experiment data found inside the results JSON file.")
        return
    
    algos = []
    makespans = []
    utilizations = []
    slrs = []

    # 2. Extract metrics for all enabled algorithms
    for exp_id, payload in experiments.items():
        algo_name = payload["algorithm"].upper()
        metrics = payload["metrics"]
        
        algos.append(algo_name)
        makespans.append(metrics["makespan_seconds_avg"])
        utilizations.append(metrics["cluster_processor_utilization_avg"] * 100)
        slrs.append(metrics["scheduling_length_ratio_avg"])

    # Define a clean, professional academic color palette
    colors = ['#e74c3c', '#2ecc71', '#3498db', '#f1c40f', '#9b59b6', '#1abc9c']
    bar_colors = colors[:len(algos)]

    # 3. Configure the Matplotlib plotting canvas
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # --- LEFT GRAPH: Total Execution Time (Makespan) ---
    bars1 = ax1.bar(algos, makespans, color=bar_colors, edgecolor='black', alpha=0.85, width=0.6)
    ax1.set_title("Total Workload Execution Time (Makespan)", fontsize=13, fontweight='bold', pad=15)
    ax1.set_ylabel("Seconds (Lower is Better)", fontsize=11, fontweight='bold')
    ax1.tick_params(axis='x', labelsize=10)
    ax1.grid(axis='y', linestyle='--', alpha=0.5)
    
    # Add precise data labels on top of the makespan bars
    for bar in bars1:
        yval = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width()/2.0, 
            yval + (max(makespans) * 0.02), 
            f"{yval:.2f}s", 
            ha='center', 
            va='bottom', 
            fontweight='bold',
            fontsize=10
        )

    # --- RIGHT GRAPH: Cluster Processor Utilization % ---
    bars2 = ax2.bar(algos, utilizations, color=bar_colors, edgecolor='black', alpha=0.85, width=0.6)
    ax2.set_title("Average Cluster Processor Utilization", fontsize=13, fontweight='bold', pad=15)
    ax2.set_ylabel("Utilization Percentage (%)", fontsize=11, fontweight='bold')
    ax2.set_ylim(0, 100)
    ax2.tick_params(axis='x', labelsize=10)
    ax2.grid(axis='y', linestyle='--', alpha=0.5)

    # Add precise data labels on top of the utilization bars
    for bar in bars2:
        yval = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width()/2.0, 
            yval + 2, 
            f"{yval:.1f}%", 
            ha='center', 
            va='bottom', 
            fontweight='bold',
            fontsize=10
        )

    # 4. Save the figure to disk
    plt.tight_layout()
    plot_dir = "outputs/plots"
    os.makedirs(plot_dir, exist_ok=True)
    plot_output = os.path.join(plot_dir, "scheduler_comparison.png")
    
    plt.savefig(plot_output, dpi=300, bbox_inches='tight')
    plt.close()
    
    print("======================================================================")
    print(f"📊 Visualization successfully updated for {len(algos)} algorithms!")
    print(f"📂 Saved Chart Asset: {plot_output}")
    print("======================================================================\n")

if __name__ == "__main__":
    main()