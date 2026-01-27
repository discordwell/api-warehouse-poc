"""
Consistent Hashing Ring

Distributes keys across nodes with minimal redistribution when nodes join/leave.
Uses virtual nodes for better distribution.
"""

import hashlib
from bisect import bisect_right
from typing import List, Optional, Set
from dataclasses import dataclass


@dataclass
class VirtualNode:
    """A virtual node on the hash ring."""
    node_id: str
    position: int
    virtual_id: int


class HashRing:
    """
    Consistent hash ring with virtual nodes.

    Each physical node gets multiple positions on the ring (virtual nodes)
    for better key distribution.
    """

    def __init__(self, virtual_nodes_per_node: int = 150):
        self.virtual_nodes_per_node = virtual_nodes_per_node
        self.ring: List[VirtualNode] = []  # Sorted by position
        self.positions: List[int] = []  # Just positions for binary search
        self.nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """Hash a key to a position on the ring (0 to 2^32-1)."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2 ** 32)

    def add_node(self, node_id: str) -> List[int]:
        """
        Add a node to the ring.

        Returns list of positions this node now owns.
        """
        if node_id in self.nodes:
            return []

        self.nodes.add(node_id)
        new_positions = []

        # Add virtual nodes
        for i in range(self.virtual_nodes_per_node):
            vnode_key = f"{node_id}:vnode:{i}"
            position = self._hash(vnode_key)

            vnode = VirtualNode(
                node_id=node_id,
                position=position,
                virtual_id=i
            )

            # Insert in sorted order
            idx = bisect_right(self.positions, position)
            self.ring.insert(idx, vnode)
            self.positions.insert(idx, position)
            new_positions.append(position)

        return new_positions

    def remove_node(self, node_id: str) -> List[int]:
        """
        Remove a node from the ring.

        Returns list of positions that need to be reassigned.
        """
        if node_id not in self.nodes:
            return []

        self.nodes.discard(node_id)
        removed_positions = []

        # Remove all virtual nodes for this node
        new_ring = []
        new_positions = []

        for vnode, pos in zip(self.ring, self.positions):
            if vnode.node_id == node_id:
                removed_positions.append(pos)
            else:
                new_ring.append(vnode)
                new_positions.append(pos)

        self.ring = new_ring
        self.positions = new_positions

        return removed_positions

    def get_node(self, key: str) -> Optional[str]:
        """Get the primary node responsible for a key."""
        if not self.ring:
            return None

        key_hash = self._hash(key)
        idx = bisect_right(self.positions, key_hash)

        # Wrap around if past the end
        if idx >= len(self.ring):
            idx = 0

        return self.ring[idx].node_id

    def get_nodes(self, key: str, count: int) -> List[str]:
        """
        Get N distinct nodes responsible for a key (for replication).

        Returns nodes in preference order (primary first).
        """
        if not self.ring:
            return []

        key_hash = self._hash(key)
        idx = bisect_right(self.positions, key_hash)

        nodes = []
        seen = set()
        attempts = 0
        max_attempts = len(self.ring)

        while len(nodes) < count and attempts < max_attempts:
            pos_idx = (idx + attempts) % len(self.ring)
            node_id = self.ring[pos_idx].node_id

            if node_id not in seen:
                nodes.append(node_id)
                seen.add(node_id)

            attempts += 1

        return nodes

    def get_all_nodes(self) -> List[str]:
        """Get all physical nodes in the ring."""
        return list(self.nodes)

    def __len__(self) -> int:
        return len(self.nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self.nodes
