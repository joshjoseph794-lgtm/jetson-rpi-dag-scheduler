# Experimentation & Benchmarking Protocol

Follow this protocol to ensure clean, unskewed, and reproducible benchmarks when evaluating scheduling algorithms (Baseline vs. HEFT vs. PEFT).

## Pre-Requisites for Run Execution
1. **Device Thermal Check:** Ensure edge devices are not under thermal throttling limits before kicking off an experiment matrix.
2. **Process Isolation:** Kill any background luxury processes or Docker containers on the Jetson and Pi that could skew CPU/GPU cycles.

## Running the Benchmark Suite

To run a batch benchmark suite using the configured testing matrix:

1. Update your target test cases inside `configs/experiment_matrix.json`.
2. Run your root evaluation suite (or the automated bash runner once created):
   ```bash
   python3 evaluate.py