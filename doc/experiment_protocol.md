# Experimentation & Benchmarking Protocol

Follow this protocol to ensure clean, unskewed, and reproducible benchmarks when evaluating scheduling algorithms (Baseline vs. HEFT vs. PEFT vs. CPOP vs. Min-Max).

## Pre-Requisites for Run Execution
1. **Device Thermal Check:** Ensure edge devices are not under thermal throttling limits before kicking off an experiment matrix.
2. **Process Isolation:** Kill any background luxury processes or Docker containers on the Jetson and Pi that could skew CPU/GPU cycles.
3. **Network Stability:** Ensure no high-bandwidth local network transfers are happening concurrently to keep latency and bandwidth measurements accurate.

## Running the Benchmark Suite

To run a batch benchmark suite using the configured testing matrix:

1. Update your target test cases inside `configs/experiment_matrix.json`. You can now target any of your supported workload profiles:
   * `execution_pipeline.json` (Production Vision Grid)
   * `energy_pipeline.json` (Asymmetric IoT Tree)
   * `toy_diamond.json` (Branching Unit Test)
   * `vision_pipeline.json` (High-Data Payload Model)
2. Run your root evaluation suite:
   ```bash
   python3 run_benchmarks.py