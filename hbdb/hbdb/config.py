"""
HBDB Configuration
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration for HBDB cluster."""

    # Cluster
    num_shards: int = 4
    replication_factor: int = 3

    # Sequencer
    sequencer_nodes: int = 3  # Odd number for Raft

    # Compute
    compute_nodes: int = 2

    # Storage
    mvcc_gc_threshold: int = 1000  # GC old versions after N newer ones

    # Timeouts (ms)
    txn_timeout: int = 5000
    sequencer_timeout: int = 1000

    # Performance
    batch_size: int = 100  # Batch transactions for sequencing
    batch_timeout_ms: int = 10  # Max wait for batch to fill
