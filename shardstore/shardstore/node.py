"""
Storage Node

A single node in the cluster that stores a subset of the data.
Handles local reads, writes, and replication requests.
"""

import json
import threading
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict

from .vector_clock import VectorClock, VersionedValue, resolve_conflicts, ClockComparison
from .config import NodeConfig

logger = logging.getLogger(__name__)


@dataclass
class HintedHandoff:
    """A write hint for a temporarily unavailable node."""
    target_node: str
    key: str
    value: VersionedValue
    created_at: float = field(default_factory=time.time)


class Node:
    """
    A storage node in the cluster.

    Features:
    - In-memory storage with optional disk persistence
    - Vector clock versioning
    - Conflict detection and resolution
    - Hinted handoff storage
    """

    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id

        # Storage: key -> list of VersionedValue (for sibling tracking)
        self._data: Dict[str, List[VersionedValue]] = {}
        self._lock = threading.RLock()

        # Hinted handoff: target_node -> list of hints
        self._hints: Dict[str, List[HintedHandoff]] = defaultdict(list)
        self._hints_lock = threading.Lock()

        # Stats
        self._stats = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "conflicts": 0,
            "hints_stored": 0,
            "hints_delivered": 0,
        }

        # Persistence
        self._data_file = None
        if config.data_dir:
            data_path = Path(config.data_dir)
            data_path.mkdir(parents=True, exist_ok=True)
            self._data_file = data_path / f"{self.node_id}.json"
            self._load_from_disk()

        logger.info(f"Node {self.node_id} initialized")

    def get(self, key: str) -> Tuple[Optional[Any], Optional[VectorClock], bool]:
        """
        Get a value by key.

        Returns:
            (value, clock, had_conflict)
            Returns (None, None, False) if key doesn't exist
        """
        self._stats["reads"] += 1

        with self._lock:
            versions = self._data.get(key)

            if not versions:
                return None, None, False

            # Filter out tombstones
            live_versions = [v for v in versions if not v.deleted]

            if not live_versions:
                return None, None, False

            # Resolve if multiple versions
            if len(live_versions) == 1:
                return live_versions[0].value, live_versions[0].clock, False

            winner, had_conflict = resolve_conflicts(live_versions)
            if had_conflict:
                self._stats["conflicts"] += 1

            return winner.value, winner.clock, had_conflict

    def get_raw(self, key: str) -> List[VersionedValue]:
        """Get all versions of a key (for replication/repair)."""
        with self._lock:
            return list(self._data.get(key, []))

    def put(
        self,
        key: str,
        value: Any,
        clock: Optional[VectorClock] = None
    ) -> VectorClock:
        """
        Put a value.

        Args:
            key: The key
            value: The value to store
            clock: Optional existing clock (for updates)

        Returns:
            The new vector clock for this write
        """
        self._stats["writes"] += 1

        with self._lock:
            # Get or create clock
            if clock is None:
                existing = self._data.get(key, [])
                if existing:
                    # Merge all existing clocks
                    clock = existing[0].clock
                    for v in existing[1:]:
                        clock = clock.merge(v.clock)
                else:
                    clock = VectorClock()

            # Increment our counter
            new_clock = clock.increment(self.node_id)

            # Create versioned value
            versioned = VersionedValue(value=value, clock=new_clock)

            # Store (replace any dominated versions)
            self._merge_version(key, versioned)

            # Persist
            self._persist_key(key)

            return new_clock

    def put_versioned(self, key: str, versioned: VersionedValue) -> bool:
        """
        Put a pre-versioned value (for replication).

        Returns True if the value was newer and stored.
        """
        with self._lock:
            return self._merge_version(key, versioned)

    def delete(self, key: str, clock: Optional[VectorClock] = None) -> VectorClock:
        """
        Delete a key (tombstone).

        Returns the clock of the tombstone.
        """
        self._stats["deletes"] += 1

        with self._lock:
            if clock is None:
                existing = self._data.get(key, [])
                if existing:
                    clock = existing[0].clock
                    for v in existing[1:]:
                        clock = clock.merge(v.clock)
                else:
                    clock = VectorClock()

            new_clock = clock.increment(self.node_id)
            tombstone = VersionedValue(value=None, clock=new_clock, deleted=True)

            self._merge_version(key, tombstone)
            self._persist_key(key)

            return new_clock

    def _merge_version(self, key: str, new_version: VersionedValue) -> bool:
        """
        Merge a new version into existing versions.

        Keeps only non-dominated versions (siblings for concurrent writes).
        Returns True if the new version was kept.
        """
        existing = self._data.get(key, [])

        if not existing:
            self._data[key] = [new_version]
            return True

        # Check if new version is dominated by any existing
        dominated_by_existing = False
        to_keep = []

        for v in existing:
            cmp = new_version.clock.compare(v.clock)

            if cmp == ClockComparison.BEFORE:
                # New version is older, ignore it
                dominated_by_existing = True
                to_keep.append(v)
            elif cmp == ClockComparison.AFTER:
                # New version is newer, don't keep old
                pass
            elif cmp == ClockComparison.EQUAL:
                # Same version, keep existing
                dominated_by_existing = True
                to_keep.append(v)
            else:
                # Concurrent - keep both as siblings
                to_keep.append(v)

        if dominated_by_existing:
            self._data[key] = to_keep
            return False
        else:
            to_keep.append(new_version)
            self._data[key] = to_keep
            return True

    def keys(self) -> List[str]:
        """Get all keys (excluding tombstones)."""
        with self._lock:
            return [
                k for k, versions in self._data.items()
                if any(not v.deleted for v in versions)
            ]

    def all_keys(self) -> List[str]:
        """Get all keys including tombstones."""
        with self._lock:
            return list(self._data.keys())

    # Hinted Handoff

    def store_hint(self, target_node: str, key: str, value: VersionedValue):
        """Store a hint for a failed node."""
        with self._hints_lock:
            hint = HintedHandoff(
                target_node=target_node,
                key=key,
                value=value
            )
            self._hints[target_node].append(hint)
            self._stats["hints_stored"] += 1

    def get_hints(self, target_node: str, max_hints: int = 100) -> List[HintedHandoff]:
        """Get hints for a node (and remove them)."""
        with self._hints_lock:
            hints = self._hints[target_node][:max_hints]
            self._hints[target_node] = self._hints[target_node][max_hints:]
            self._stats["hints_delivered"] += len(hints)
            return hints

    def clear_old_hints(self, max_age: float):
        """Remove hints older than max_age seconds."""
        cutoff = time.time() - max_age
        with self._hints_lock:
            for target in list(self._hints.keys()):
                self._hints[target] = [
                    h for h in self._hints[target]
                    if h.created_at > cutoff
                ]
                if not self._hints[target]:
                    del self._hints[target]

    # Persistence

    def _persist_key(self, key: str):
        """Persist a single key to disk."""
        if not self._data_file:
            return
        # For simplicity, persist entire store
        # Production would use WAL or LSM tree
        self._save_to_disk()

    def _save_to_disk(self):
        """Save all data to disk."""
        if not self._data_file:
            return

        data = {}
        with self._lock:
            for key, versions in self._data.items():
                data[key] = [v.to_dict() for v in versions]

        with open(self._data_file, 'w') as f:
            json.dump(data, f)

    def _load_from_disk(self):
        """Load data from disk."""
        if not self._data_file or not self._data_file.exists():
            return

        try:
            with open(self._data_file) as f:
                data = json.load(f)

            with self._lock:
                for key, versions in data.items():
                    self._data[key] = [
                        VersionedValue.from_dict(v) for v in versions
                    ]

            logger.info(f"Loaded {len(self._data)} keys from disk")
        except Exception as e:
            logger.error(f"Failed to load from disk: {e}")

    # Stats

    def get_stats(self) -> dict:
        """Get node statistics."""
        with self._lock:
            key_count = len(self.keys())
            total_versions = sum(len(v) for v in self._data.values())

        return {
            **self._stats,
            "node_id": self.node_id,
            "keys": key_count,
            "total_versions": total_versions,
            "hints_pending": sum(len(h) for h in self._hints.values()),
        }
