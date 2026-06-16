# Network Setup Protocol

This document details the network topology, IP assignments, and security configurations required for the orchestrator and agents to communicate.

## Topology & IP Allocations

All devices must reside on the same local subnet (or virtual private network) with static IP mappings configured. 

| Device Hostname / Key | Role | Static IP Address | Communication Port |
| :--- | :--- | :--- | :--- |
| `laptop` | Master / Scheduler | `192.168.1.100` | N/A (Initiates outbound) |
| `jetson` | Worker Agent | `192.168.1.101` | `5000` |
| `raspberry_pi` | Worker Agent | `192.168.1.102` | `5000` |

> 💡 **Tip:** If your local router doesn't support fixed static IPs via DHCP reservation, you can map these names locally on your host laptop by adding them directly to your `/etc/hosts` file.

## SSH Passwordless Authentication
The master scheduling engine on the `laptop` must be able to SSH into the worker nodes without a password prompt to allow runtime tracking scripts to execute smoothly.

1. **Generate an SSH key** on your master `laptop` (if you haven't already):
   ```bash
   ssh-keygen -t rsa -b 4096