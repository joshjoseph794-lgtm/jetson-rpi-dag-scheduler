import os
import matplotlib.pyplot as plt
import numpy as np

# Import the parsing layer from Phase 1
from dags.manifest_parser import load_dag_manifest

# Import the metrics from Phase 2
from scheduler.metrics import calculate_slr, calculate_speedup

# Import the baseline and PEFT algorithms
from scheduler.baselines import schedule_round_robin, schedule_random
from scheduler.peft import schedule_peft

# Try to import HEFT from your existing heft file
try:
    from scheduler.heft import schedule_heft
except ImportError:
    # Fallback placeholder in case your heft function uses a slightly different name
    print("⚠️ Could not find schedule_heft in scheduler.heft. Using a mock wrapper.")
    def schedule_heft(dag, comp_matrix):
        # Temporary fallback mapping to let evaluation run if heft needs tuning
        return schedule_peft(dag, comp_matrix)

def run_comparative_evaluation():
    # 1. Setup paths and directories
    manifest_path = os.path.join("dags", "vision_pipeline.json")
    output_dir = os.path.join("outputs", "plots")
    os.makedirs(output_dir, exist_ok=True)
    
    print("🚀 Loading DAG pipeline manifest...")
    dag, comp_matrix, ccr = load_dag_manifest(manifest_path)
    
    print(f"📊 Pipeline Loaded. Nodes: {dag.number_of_nodes()} | Edges: {dag.number_of_edges()} | CCR: {ccr:.4f}\n")
    
    # 2. Execute all 4 algorithms
    algorithms = {
        "Round-Robin": schedule_round_robin,
        "Random Alloc": lambda g, m: schedule_random(g, m, seed=42),
        "HEFT (Core)": schedule_heft,
        "PEFT (Rival)": schedule_peft
    }
    
    results = {}
    
    print("🤖 Running scheduling algorithms...")
    for name, algo_func in algorithms.items():
        # Get the allocation mapping and raw completion time (makespan)
        allocations, makespan = algo_func(dag, comp_matrix)
        
        # Calculate scientific comparative metrics
        slr = calculate_slr(makespan, dag, comp_matrix)
        speedup = calculate_speedup(makespan, comp_matrix)
        
        results[name] = {
            "Makespan": makespan,
            "SLR": slr,
            "Speedup": speedup,
            "Allocations": allocations
        }
        print(f"   ✅ Finished executing {name}")

    # 3. Print out a publication-style console summary table
    print("\n" + "="*65)
    print(f"{'ALGORITHM':<15} | {'MAKESPAN (ms)':<15} | {'SLR':<10} | {'SPEEDUP':<10}")
    print("="*65)
    for name, data in results.items():
        print(f"{name:<15} | {data['Makespan']:<15.2f} | {data['SLR']:<10.4f} | {data['Speedup']:<10.4f}")
    print("="*65 + "\n")
    
    # 4. Generate and save the comparative bar chart
    print("📈 Generating comparative plots for the paper...")
    names = list(results.keys())
    makespans = [data["Makespan"] for data in results.values()]
    
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # Use distinct academic colors
    colors = ['#e74c3c', '#f39c12', '#3498db', '#2ecc71']
    bars = ax.bar(names, makespans, color=colors, edgecolor='black', width=0.5)
    
    # Label modifications for clean presentation
    ax.set_ylabel("Total Execution Makespan (milliseconds)", fontsize=11, fontweight='bold')
    ax.set_title(f"Scheduling Performance Comparison (Pipeline CCR = {ccr:.3f})", fontsize=13, fontweight='bold', pad=15)
    
    # Add exact time value labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.1f} ms',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
                    
    plt.tight_layout()
    plot_save_path = os.path.join(output_dir, "makespan_comparison.png")
    plt.savefig(plot_save_path, dpi=300)
    plt.close()
    
    print(f"🎉 Evaluation Complete! Plot saved successfully to: {plot_save_path}")

if __name__ == "__main__":
    run_comparative_evaluation()