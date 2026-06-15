# Network Setup Protocol

This document details the network topology, IP assignments, and security configurations required for the coordinator and agents to communicate.

## Topology & IP Allocations

All devices must reside on the same local subnet (or virtual private network) with static IP mappings configured.

| Device Hostname | Role | Static IP Address | Communication Port |
| :--- | :--- | :--- | :--- |
| `coordinator` | Master / Scheduler | `192.168.1.100` | N/A (Initiates outbound) |
| `jetson_nano_01` | Worker Agent | `192.168.1.101` | `5000` |
| `raspberry_pi_01`| Worker Agent | `192.168.1.102` | `5000` |

## SSH Passwordless Authentication
The coordinator must be able to SSH into the worker nodes without a password prompt to allow runtime deployment scripts to execute smoothly.

1. Generate an SSH key on the coordinator (if not already present):
   ```bash
   ssh-keygen -t rsa -b 4096