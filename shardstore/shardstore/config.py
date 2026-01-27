"""
Cluster Configuration

Tunable parameters for consistency vs availability tradeoffs.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class ConsistencyLevel(Enum):
    """Consistency level for reads/writes."""
    ONE = 1       # Only one replica needs to respond
    QUORUM = 2    # Majority of replicas
    ALL = 3       # All replicas must respond


@dataclass
class ClusterConfig:
    """
    Configuration for a ShardStore cluster.

    Tuning guide:
    - Higher replication_factor = more durability, more storage
    - write_consistency=QUORUM, read_consistency=QUORUM = strong consistency
    - write_consistency=ONE, read_consistency=ONE = highest availability
    - write_consistency=ALL = highest durability, lower availability
    """

    # Replication
    replication_factor: int = 3  # Number of copies of each key

    # Consistency levels
    write_consistency: ConsistencyLevel = ConsistencyLevel.QUORUM
    read_consistency: ConsistencyLevel = ConsistencyLevel.ONE

    # Timeouts (seconds)
    write_timeout: float = 5.0
    read_timeout: float = 2.0
    gossip_interval: float = 1.0
    failure_threshold: float = 10.0  # Mark node failed after N seconds

    # Hash ring
    virtual_nodes_per_node: int = 150

    # Storage
    data_dir: Optional[str] = None  # None = in-memory only
    compaction_threshold: int = 1000  # Compact after N tombstones

    # Anti-entropy
    read_repair: bool = True  # Repair stale replicas on read
    anti_entropy_interval: float = 60.0  # Full sync interval

    # Hinted handoff
    hinted_handoff: bool = True  # Store hints for failed nodes
    hint_ttl: float = 3600.0  # Discard hints after N seconds

    def required_responses(self, level: ConsistencyLevel) -> int:
        """Calculate required responses for a consistency level."""
        if level == ConsistencyLevel.ONE:
            return 1
        elif level == ConsistencyLevel.QUORUM:
            return (self.replication_factor // 2) + 1
        else:  # ALL
            return self.replication_factor


@dataclass
class NodeConfig:
    """Configuration for a single node."""
    node_id: str
    host: str = "localhost"
    port: int = 7000
    data_dir: Optional[str] = None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"
