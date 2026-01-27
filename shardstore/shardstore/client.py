"""
ShardStore Client

High-level client interface for the distributed store.
"""

from typing import Any, Optional, List, Dict
from .cluster import Cluster
from .config import ClusterConfig, NodeConfig, ConsistencyLevel


class ShardClient:
    """
    Client interface for ShardStore.

    Example:
        client = ShardClient.create_local_cluster(nodes=3)
        client.put("user:123", {"name": "Alice", "age": 30})
        user = client.get("user:123")
    """

    def __init__(self, cluster: Cluster):
        self._cluster = cluster

    @classmethod
    def create_local_cluster(
        cls,
        nodes: int = 3,
        replication_factor: int = 3,
        data_dir: Optional[str] = None
    ) -> "ShardClient":
        """
        Create a local cluster for testing/development.

        Args:
            nodes: Number of nodes in the cluster
            replication_factor: Number of replicas per key
            data_dir: Optional directory for persistence
        """
        config = ClusterConfig(
            replication_factor=min(replication_factor, nodes),
            write_consistency=ConsistencyLevel.QUORUM,
            read_consistency=ConsistencyLevel.ONE,
        )

        cluster = Cluster(config)

        for i in range(nodes):
            node_config = NodeConfig(
                node_id=f"node-{i}",
                host="localhost",
                port=7000 + i,
                data_dir=f"{data_dir}/node-{i}" if data_dir else None
            )
            cluster.add_node(node_config)

        return cls(cluster)

    # Basic Operations

    def put(self, key: str, value: Any) -> bool:
        """
        Store a key-value pair.

        Returns True if write succeeded (met quorum).
        """
        result = self._cluster.put(key, value)
        return result.success

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key.

        Returns None if key doesn't exist.
        """
        result = self._cluster.get(key)
        return result.value if result.success else None

    def delete(self, key: str) -> bool:
        """
        Delete a key.

        Returns True if delete succeeded.
        """
        result = self._cluster.delete(key)
        return result.success

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return self.get(key) is not None

    # Batch Operations

    def put_many(self, items: Dict[str, Any]) -> Dict[str, bool]:
        """
        Store multiple key-value pairs.

        Returns dict of key -> success status.
        """
        results = {}
        for key, value in items.items():
            results[key] = self.put(key, value)
        return results

    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Retrieve multiple keys.

        Returns dict of key -> value (missing keys omitted).
        """
        results = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                results[key] = value
        return results

    # Consistency Control

    def put_with_consistency(
        self,
        key: str,
        value: Any,
        consistency: ConsistencyLevel
    ) -> bool:
        """Write with explicit consistency level."""
        result = self._cluster.put(key, value, consistency)
        return result.success

    def get_with_consistency(
        self,
        key: str,
        consistency: ConsistencyLevel
    ) -> Optional[Any]:
        """Read with explicit consistency level."""
        result = self._cluster.get(key, consistency)
        return result.value if result.success else None

    # Metadata

    def get_key_nodes(self, key: str) -> List[str]:
        """Get the nodes responsible for a key."""
        return self._cluster.get_key_location(key)

    def get_stats(self) -> dict:
        """Get cluster statistics."""
        return self._cluster.get_stats()

    def get_nodes(self) -> List[str]:
        """Get all node IDs in the cluster."""
        return self._cluster.get_nodes()

    # Cluster Management

    def add_node(self, node_id: str, host: str = "localhost", port: int = 7000):
        """Add a node to the cluster."""
        config = NodeConfig(node_id=node_id, host=host, port=port)
        self._cluster.add_node(config)

    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        self._cluster.remove_node(node_id)

    def shutdown(self):
        """Shutdown the cluster."""
        self._cluster.shutdown()

    # Context manager

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        return False
