# Hardware Setup Guide

This document outlines the hardware configuration and environment setup for the heterogeneous Jetson-Raspberry Pi edge cluster.

## Cluster Nodes Specification

| Node Hostname | Device Type | Processor / GPU | Operating System | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `coordinator` | Host PC / Server | x86_64 CPU | Ubuntu 22.04 LTS | Task Scheduling & Orchestration |
| `jetson_nano_01` | Edge GPU Node | Quad-core ARM + 128-core Maxwell | NVIDIA JetPack 4.6.x | Heavy compute / Vision inference |
| `raspberry_pi_01`| Edge CPU Node | Broadcom BCM2711 (Quad-core) | Raspberry Pi OS (64-bit) | Light compute / Data parsing |

## Node Initialization

### 1. Jetson Nano Setup
* Flash the JetPack OS image onto a high-speed microSD card (minimum 32GB, Class 10).
* Maximize performance mode by running:
  ```bash
  sudo nvpmodel -m 0
  sudo jetson_clocks