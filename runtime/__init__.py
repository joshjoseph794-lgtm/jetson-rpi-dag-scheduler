# runtime/__init__.py
"""
Runtime orchestration and network messaging layer for the heterogeneous edge cluster.
"""

import os
import sys

# Automatically inject the parent directory into the system path 
# to guarantee that generated Protobuf assets can always find each other.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))