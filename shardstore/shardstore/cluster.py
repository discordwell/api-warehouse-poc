"""
Cluster Coordinator

Manages the distributed cluster, routing requests to appropriate nodes.
Handles replication, quorum enforcement, and consistency.
"""

import threading
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from .hash_ring import HashRing
from .node import Node
from .gossip import GossipProtocol, NodeState
from .vector_clock import VectorClock, VersionedValue, resolve_conflicts
from .config import ClusterConfig, NodeConfig, ConsistencyLevel

logger = logging.getLogger(__name__)


@dataclass
class WriteResult:
    """Result of a write operation."""
    success: bool
    clock: Optional[VectorClock]
    nodes_written: int
    errors: List[str]


@dataclass
class ReadResult:
    """Result of a read operation."""
    success: bool
    value: Optional[Any]
    clock: Optional[VectorClock]
    had_conflict: bool
    nodes_read: int
    repaired: bool


class Cluster:
    """
    Distributed cluster coordinator.

    Handles:
    - Request routing via consistent hashing
    - Replication to N nodes
    - Quorum reads/writes
    - Read repair for eventual consistency
    - Hinted handoff for availability
    """

    def __init__(self, config: ClusterConfig):
        self.config = config

        # Hash ring for key distribution
        self._ring = HashRing(virtual_nodes_per_node=config.virtual_nodes_per_node)

        # Local nodes (for single-process simulation)
        self._nodes: Dict[str, Node] = {}
        self._lock = threading.RLock()

        # Gossip protocol per node
        self._gossip: Dict[str, GossipProtocol] = {}

        # Thread pool for parallel operations
        self._executor = ThreadPoolExecutor(max_workers=32)

        # Stats
        self._stats = {
            "reads": 0,
            "writes": 0,
            "read_repairs": 0,
            "hinted_handoffs": 0,
            "quorum_failures": 0,
        }

        logger.info(f"Cluster initialized with RF={config.replication_factor}")

    def add_node(self, node_config: NodeConfig) -> Node:
        """Add a node to the cluster."""
        with self._lock:
            node_id = node_config.node_id

            if node_id in self._nodes:
                return self._nodes[node_id]

            # Create node
            node = Node(node_config)
            self._nodes[node_id] = node

            # Add to hash ring
            self._ring.add_node(node_id)

            # Create gossip for this node
            gossip = GossipProtocol(
                node_id=node_id,
                gossip_interval=self.config.gossip_interval,
                failure_threshold=self.config.failure_threshold,
            )

            # Add all existing nodes to gossip membership
            for other_id in self._nodes:
                if other_id != node_id:
                    gossip.add_node(other_id)
                    # Add new node to existing gossip
                    if other_id in self._gossip:
                        self._gossip[other_id].add_node(node_id)

            self._gossip[node_id] = gossip
            gossip.start()

            logger.info(f"Added node {node_id} to cluster")
            return node

    def remove_node(self, node_id: str):
        """Remove a node from the cluster."""
        with self._lock:
            if node_id not in self._nodes:
                return

            # Stop gossip
            if node_id in self._gossip:
                self._gossip[node_id].stop()
                del self._gossip[node_id]

            # Remove from ring and nodes
            self._ring.remove_node(node_id)
            del self._nodes[node_id]

            # Remove from other gossip memberships
            for gossip in self._gossip.values():
                gossip.remove_node(node_id)

            logger.info(f"Removed node {node_id} from cluster")

    def get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def get_nodes(self) -> List[str]:
        """Get all node IDs."""
        return list(self._nodes.keys())

    def _get_replica_nodes(self, key: str) -> List[str]:
        """Get the nodes responsible for a key."""
        return self._ring.get_nodes(key, self.config.replication_factor)

    def _is_node_alive(self, node_id: str) -> bool:
        """Check if a node is alive according to gossip."""
        # For simplicity, check any gossip instance
        for gossip in self._gossip.values():
            return gossip.is_alive(node_id)
        return node_id in self._nodes

    # Write Operations

    def put(
        self,
        key: str,
        value: Any,
        consistency: Optional[ConsistencyLevel] = None
    ) -> WriteResult:
        """
        Write a key-value pair to the cluster.

        Replicates to N nodes, waits for consistency level responses.
        """
        self._stats["writes"] += 1
        consistency = consistency or self.config.write_consistency
        required = self.config.required_responses(consistency)

        replica_nodes = self._get_replica_nodes(key)
        if not replica_nodes:
            return WriteResult(success=False, clock=None, nodes_written=0,
                             errors=["No nodes available"])

        # Try to write to all replicas in parallel
        futures = {}
        results = []
        errors = []

        for node_id in replica_nodes:
            if node_id in self._nodes:
                if self._is_node_alive(node_id):
                    future = self._executor.submit(
                        self._write_to_node, node_id, key, value
                    )
                    futures[future] = node_id
                else:
                    errors.append(f"Node {node_id} is not alive")

        # Collect results
        clocks = []
        for future in as_completed(futures, timeout=self.config.write_timeout):
            node_id = futures[future]
            try:
                success, clock = future.result()
                if success:
                    results.append(node_id)
                    clocks.append(clock)
                else:
                    errors.append(f"Write to {node_id} failed")
            except Exception as e:
                errors.append(f"Write to {node_id} error: {e}")

        # Check if we have quorum
        success = len(results) >= required

        if not success:
            self._stats["quorum_failures"] += 1

        # Merge clocks for result
        final_clock = None
        if clocks:
            final_clock = clocks[0]
            for c in clocks[1:]:
                final_clock = final_clock.merge(c)

        # Hinted handoff for failed nodes
        if self.config.hinted_handoff and final_clock:
            self._store_hints(key, value, final_clock, replica_nodes, results)

        return WriteResult(
            success=success,
            clock=final_clock,
            nodes_written=len(results),
            errors=errors
        )

    def _write_to_node(
        self,
        node_id: str,
        key: str,
        value: Any,
        clock: Optional[VectorClock] = None
    ) -> Tuple[bool, Optional[VectorClock]]:
        """Write to a single node."""
        node = self._nodes.get(node_id)
        if not node:
            return False, None

        try:
            new_clock = node.put(key, value, clock)
            return True, new_clock
        except Exception as e:
            logger.error(f"Write to {node_id} failed: {e}")
            return False, None

    def _store_hints(
        self,
        key: str,
        value: Any,
        clock: VectorClock,
        target_nodes: List[str],
        successful_nodes: List[str]
    ):
        """Store hints for nodes that didn't receive the write."""
        failed_nodes = set(target_nodes) - set(successful_nodes)
        if not failed_nodes:
            return

        versioned = VersionedValue(value=value, clock=clock)

        # Store hint on a successful node
        for hint_target in failed_nodes:
            for store_node in successful_nodes:
                node = self._nodes.get(store_node)
                if node:
                    node.store_hint(hint_target, key, versioned)
                    self._stats["hinted_handoffs"] += 1
                    break

    # Read Operations

    def get(
        self,
        key: str,
        consistency: Optional[ConsistencyLevel] = None
    ) -> ReadResult:
        """
        Read a key from the cluster.

        Reads from N replicas, returns after consistency level responses.
        Performs read repair if versions differ.
        """
        self._stats["reads"] += 1
        consistency = consistency or self.config.read_consistency
        required = self.config.required_responses(consistency)

        replica_nodes = self._get_replica_nodes(key)
        if not replica_nodes:
            return ReadResult(success=False, value=None, clock=None,
                            had_conflict=False, nodes_read=0, repaired=False)

        # Read from replicas in parallel
        futures = {}
        for node_id in replica_nodes:
            if node_id in self._nodes and self._is_node_alive(node_id):
                future = self._executor.submit(
                    self._read_from_node, node_id, key
                )
                futures[future] = node_id

        # Collect results
        versions: List[Tuple[str, VersionedValue]] = []
        nodes_read = 0

        try:
            for future in as_completed(futures, timeout=self.config.read_timeout):
                node_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        value, clock = result
                        if value is not None:
                            versions.append((node_id, VersionedValue(value=value, clock=clock)))
                        nodes_read += 1
                except Exception as e:
                    logger.warning(f"Read from {node_id} failed: {e}")

                # Early return if we have enough
                if nodes_read >= required and versions:
                    break
        except TimeoutError:
            pass

        if not versions:
            return ReadResult(
                success=nodes_read >= required,
                value=None,
                clock=None,
                had_conflict=False,
                nodes_read=nodes_read,
                repaired=False
            )

        # Resolve conflicts
        just_values = [v for _, v in versions]
        winner, had_conflict = resolve_conflicts(just_values)

        # Read repair if needed
        repaired = False
        if self.config.read_repair and len(versions) > 1:
            repaired = self._do_read_repair(key, winner, versions)

        return ReadResult(
            success=True,
            value=winner.value,
            clock=winner.clock,
            had_conflict=had_conflict,
            nodes_read=nodes_read,
            repaired=repaired
        )

    def _read_from_node(
        self,
        node_id: str,
        key: str
    ) -> Optional[Tuple[Any, VectorClock]]:
        """Read from a single node."""
        node = self._nodes.get(node_id)
        if not node:
            return None

        value, clock, _ = node.get(key)
        if value is None:
            return None
        return value, clock

    def _do_read_repair(
        self,
        key: str,
        winning: VersionedValue,
        versions: List[Tuple[str, VersionedValue]]
    ) -> bool:
        """Repair stale replicas with the winning version."""
        repaired = False

        for node_id, version in versions:
            if version.clock.compare(winning.clock) != winning.clock.compare(version.clock):
                # This node has a stale version
                node = self._nodes.get(node_id)
                if node:
                    node.put_versioned(key, winning)
                    repaired = True
                    self._stats["read_repairs"] += 1

        return repaired

    # Delete

    def delete(
        self,
        key: str,
        consistency: Optional[ConsistencyLevel] = None
    ) -> WriteResult:
        """Delete a key (tombstone)."""
        # Deletes are just writes of tombstones
        self._stats["writes"] += 1
        consistency = consistency or self.config.write_consistency
        required = self.config.required_responses(consistency)

        replica_nodes = self._get_replica_nodes(key)
        results = []
        errors = []
        clocks = []

        for node_id in replica_nodes:
            node = self._nodes.get(node_id)
            if node and self._is_node_alive(node_id):
                try:
                    clock = node.delete(key)
                    results.append(node_id)
                    clocks.append(clock)
                except Exception as e:
                    errors.append(f"Delete on {node_id} failed: {e}")

        success = len(results) >= required
        final_clock = clocks[0] if clocks else None

        return WriteResult(
            success=success,
            clock=final_clock,
            nodes_written=len(results),
            errors=errors
        )

    # Cluster Info

    def get_stats(self) -> dict:
        """Get cluster statistics."""
        node_stats = {}
        for node_id, node in self._nodes.items():
            node_stats[node_id] = node.get_stats()

        return {
            "cluster": self._stats,
            "nodes": node_stats,
            "ring_size": len(self._ring),
            "replication_factor": self.config.replication_factor,
        }

    def get_key_location(self, key: str) -> List[str]:
        """Get the nodes responsible for a key."""
        return self._get_replica_nodes(key)

    # Gossip exchange (for multi-process scenarios)

    def exchange_gossip(self, from_node: str, to_node: str):
        """Simulate gossip exchange between two nodes."""
        if from_node in self._gossip and to_node in self._gossip:
            state = self._gossip[from_node].get_state()
            self._gossip[to_node].merge_state(state)

    def shutdown(self):
        """Shutdown the cluster."""
        for gossip in self._gossip.values():
            gossip.stop()
        self._executor.shutdown(wait=False)
        logger.info("Cluster shutdown complete")
