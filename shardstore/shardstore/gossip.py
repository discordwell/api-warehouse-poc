"""
Gossip Protocol

Epidemic protocol for spreading cluster state (membership, failures).
Each node periodically exchanges state with random peers.
"""

import threading
import time
import random
import logging
from typing import Dict, Set, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class NodeState(Enum):
    """Health state of a node."""
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"


@dataclass
class NodeStatus:
    """Status information about a node."""
    node_id: str
    state: NodeState = NodeState.ALIVE
    heartbeat: int = 0  # Logical clock, incremented on each gossip
    last_update: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "heartbeat": self.heartbeat,
            "last_update": self.last_update,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NodeStatus":
        return cls(
            node_id=data["node_id"],
            state=NodeState(data["state"]),
            heartbeat=data["heartbeat"],
            last_update=data["last_update"],
        )


class GossipProtocol:
    """
    SWIM-style gossip protocol for failure detection.

    Features:
    - Periodic heartbeats
    - Random peer selection for gossip
    - Suspicion mechanism before marking dead
    - Configurable failure thresholds
    """

    def __init__(
        self,
        node_id: str,
        gossip_interval: float = 1.0,
        failure_threshold: float = 10.0,
        suspect_threshold: float = 5.0,
    ):
        self.node_id = node_id
        self.gossip_interval = gossip_interval
        self.failure_threshold = failure_threshold
        self.suspect_threshold = suspect_threshold

        # Node states: node_id -> NodeStatus
        self._members: Dict[str, NodeStatus] = {}
        self._lock = threading.RLock()

        # Self status
        self._members[node_id] = NodeStatus(node_id=node_id)

        # Callbacks
        self._on_node_alive: Optional[Callable[[str], None]] = None
        self._on_node_suspect: Optional[Callable[[str], None]] = None
        self._on_node_dead: Optional[Callable[[str], None]] = None

        # Background thread
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start the gossip background thread."""
        self._running = True
        self._thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self._thread.start()
        logger.info(f"Gossip started for {self.node_id}")

    def stop(self):
        """Stop the gossip thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.info(f"Gossip stopped for {self.node_id}")

    def add_node(self, node_id: str):
        """Add a node to the membership."""
        with self._lock:
            if node_id not in self._members:
                self._members[node_id] = NodeStatus(node_id=node_id)
                logger.info(f"Added node {node_id} to membership")

    def remove_node(self, node_id: str):
        """Remove a node from membership."""
        with self._lock:
            self._members.pop(node_id, None)

    def get_alive_nodes(self) -> Set[str]:
        """Get set of alive node IDs."""
        with self._lock:
            return {
                node_id for node_id, status in self._members.items()
                if status.state == NodeState.ALIVE
            }

    def get_all_nodes(self) -> Dict[str, NodeStatus]:
        """Get all node statuses."""
        with self._lock:
            return {k: v for k, v in self._members.items()}

    def is_alive(self, node_id: str) -> bool:
        """Check if a node is considered alive."""
        with self._lock:
            status = self._members.get(node_id)
            return status is not None and status.state == NodeState.ALIVE

    def get_state(self) -> Dict[str, dict]:
        """Get current gossip state (for sending to peers)."""
        with self._lock:
            # Increment our own heartbeat
            self._members[self.node_id].heartbeat += 1
            self._members[self.node_id].last_update = time.time()

            return {
                node_id: status.to_dict()
                for node_id, status in self._members.items()
            }

    def merge_state(self, remote_state: Dict[str, dict]):
        """
        Merge state received from another node.

        Uses heartbeat as logical clock - higher heartbeat wins.
        """
        now = time.time()

        with self._lock:
            for node_id, remote_data in remote_state.items():
                remote_status = NodeStatus.from_dict(remote_data)

                if node_id not in self._members:
                    # New node
                    self._members[node_id] = remote_status
                    self._members[node_id].last_update = now
                    if self._on_node_alive:
                        self._on_node_alive(node_id)
                    continue

                local_status = self._members[node_id]

                # Update if remote has higher heartbeat
                if remote_status.heartbeat > local_status.heartbeat:
                    old_state = local_status.state
                    local_status.heartbeat = remote_status.heartbeat
                    local_status.last_update = now

                    # Node came back alive
                    if remote_status.state == NodeState.ALIVE and old_state != NodeState.ALIVE:
                        local_status.state = NodeState.ALIVE
                        if self._on_node_alive:
                            self._on_node_alive(node_id)

    def _gossip_loop(self):
        """Background gossip loop."""
        while self._running:
            try:
                self._check_node_health()
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")

            time.sleep(self.gossip_interval)

    def _check_node_health(self):
        """Check health of all nodes based on last update time."""
        now = time.time()

        with self._lock:
            for node_id, status in self._members.items():
                if node_id == self.node_id:
                    continue

                age = now - status.last_update

                if status.state == NodeState.ALIVE:
                    if age > self.suspect_threshold:
                        status.state = NodeState.SUSPECT
                        logger.warning(f"Node {node_id} is now SUSPECT")
                        if self._on_node_suspect:
                            self._on_node_suspect(node_id)

                elif status.state == NodeState.SUSPECT:
                    if age > self.failure_threshold:
                        status.state = NodeState.DEAD
                        logger.error(f"Node {node_id} is now DEAD")
                        if self._on_node_dead:
                            self._on_node_dead(node_id)

    def select_gossip_targets(self, count: int = 3) -> list:
        """Select random nodes to gossip with."""
        with self._lock:
            candidates = [
                node_id for node_id in self._members
                if node_id != self.node_id
            ]
            return random.sample(candidates, min(count, len(candidates)))

    # Callbacks

    def on_node_alive(self, callback: Callable[[str], None]):
        """Set callback for when a node becomes alive."""
        self._on_node_alive = callback

    def on_node_suspect(self, callback: Callable[[str], None]):
        """Set callback for when a node becomes suspect."""
        self._on_node_suspect = callback

    def on_node_dead(self, callback: Callable[[str], None]):
        """Set callback for when a node is marked dead."""
        self._on_node_dead = callback
