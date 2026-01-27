"""
ShardStore - A distributed key-value store with eventual consistency

Features:
- Consistent hashing for data distribution
- Configurable replication factor
- Vector clocks for conflict resolution
- Quorum reads/writes for tunable consistency
- Anti-entropy via read repair
- Gossip-based failure detection
"""

from .cluster import Cluster
from .client import ShardClient
from .node import Node
from .config import ClusterConfig, NodeConfig, ConsistencyLevel

__version__ = "0.1.0"
__all__ = ["Cluster", "ShardClient", "Node", "ClusterConfig", "NodeConfig", "ConsistencyLevel"]
