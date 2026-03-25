import hashlib
from typing import List, Tuple

class ClusterTopology:
    """
    Manages the mapping between Keys and Storage Servers.
    For POC, we use static configuration.
    """
    def __init__(self, storage_nodes: List[Tuple[str, int]], replication_factor: int = 1):
        """
        :param storage_nodes: List of (host, port) tuples.
        :param replication_factor: Number of replicas per shard. 
                                   Total nodes must be divisible by RF.
        """
        self.nodes = storage_nodes
        self.rf = replication_factor
        
        if len(storage_nodes) % self.rf != 0:
            raise ValueError(f"Number of nodes ({len(storage_nodes)}) not divisible by RF ({self.rf})")
            
        self.num_shards = len(storage_nodes) // self.rf

    def get_nodes_for_key(self, key: str) -> List[Tuple[str, int]]:
        """
        Returns List of (host, port) replicas for the given key.
        """
        if not self.nodes:
            raise ValueError("No storage nodes defined")
            
        h = hashlib.md5(key.encode()).hexdigest()
        val = int(h[:8], 16)
        
        shard_idx = val % self.num_shards
        
        # Nodes for this shard
        start_node_idx = shard_idx * self.rf
        return self.nodes[start_node_idx : start_node_idx + self.rf]
    
    # Deprecated single node getter (returns primary)
    def get_node_for_key(self, key: str) -> Tuple[str, int]:
        return self.get_nodes_for_key(key)[0]

    def get_all_nodes(self) -> List[Tuple[str, int]]:
        return self.nodes
