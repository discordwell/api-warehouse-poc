"""
Vector Clocks for Conflict Detection

Tracks causality between events across distributed nodes.
Used to detect concurrent writes and resolve conflicts.
"""

from __future__ import annotations
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time


class ClockComparison(Enum):
    """Result of comparing two vector clocks."""
    EQUAL = "equal"           # Same version
    BEFORE = "before"         # This happened before other
    AFTER = "after"           # This happened after other
    CONCURRENT = "concurrent" # Concurrent updates (conflict!)


@dataclass
class VectorClock:
    """
    Vector clock for tracking causality.

    Each node maintains a counter. When a node writes, it increments
    its own counter. The full vector is passed with each value.
    """
    counters: Dict[str, int] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def increment(self, node_id: str) -> VectorClock:
        """Increment counter for a node, return new clock."""
        new_counters = self.counters.copy()
        new_counters[node_id] = new_counters.get(node_id, 0) + 1
        return VectorClock(counters=new_counters, timestamp=time.time())

    def merge(self, other: VectorClock) -> VectorClock:
        """Merge two clocks, taking max of each counter."""
        all_nodes = set(self.counters.keys()) | set(other.counters.keys())
        new_counters = {
            node: max(self.counters.get(node, 0), other.counters.get(node, 0))
            for node in all_nodes
        }
        return VectorClock(
            counters=new_counters,
            timestamp=max(self.timestamp, other.timestamp)
        )

    def compare(self, other: VectorClock) -> ClockComparison:
        """
        Compare this clock to another.

        Returns:
            EQUAL: Identical clocks
            BEFORE: This clock happened before other
            AFTER: This clock happened after other
            CONCURRENT: Neither dominates (conflict)
        """
        all_nodes = set(self.counters.keys()) | set(other.counters.keys())

        self_dominates = False
        other_dominates = False

        for node in all_nodes:
            self_val = self.counters.get(node, 0)
            other_val = other.counters.get(node, 0)

            if self_val > other_val:
                self_dominates = True
            elif other_val > self_val:
                other_dominates = True

        if self_dominates and other_dominates:
            return ClockComparison.CONCURRENT
        elif self_dominates:
            return ClockComparison.AFTER
        elif other_dominates:
            return ClockComparison.BEFORE
        else:
            return ClockComparison.EQUAL

    def __ge__(self, other: VectorClock) -> bool:
        """True if this clock is >= other (happened after or equal)."""
        cmp = self.compare(other)
        return cmp in (ClockComparison.AFTER, ClockComparison.EQUAL)

    def to_dict(self) -> dict:
        return {"counters": self.counters, "timestamp": self.timestamp}

    @classmethod
    def from_dict(cls, data: dict) -> VectorClock:
        return cls(
            counters=data.get("counters", {}),
            timestamp=data.get("timestamp", time.time())
        )


@dataclass
class VersionedValue:
    """A value with its vector clock for versioning."""
    value: any
    clock: VectorClock
    deleted: bool = False  # Tombstone for deletes

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "clock": self.clock.to_dict(),
            "deleted": self.deleted
        }

    @classmethod
    def from_dict(cls, data: dict) -> VersionedValue:
        return cls(
            value=data.get("value"),
            clock=VectorClock.from_dict(data.get("clock", {})),
            deleted=data.get("deleted", False)
        )


def resolve_conflicts(versions: List[VersionedValue]) -> Tuple[VersionedValue, bool]:
    """
    Resolve multiple versions of a value.

    Returns:
        (winning_version, had_conflict)

    Resolution strategy:
    1. If one version dominates all others, use it
    2. If concurrent, use last-write-wins (by timestamp)
    """
    if not versions:
        raise ValueError("No versions to resolve")

    if len(versions) == 1:
        return versions[0], False

    # Find dominating version
    for candidate in versions:
        dominates_all = True
        for other in versions:
            if other is candidate:
                continue
            cmp = candidate.clock.compare(other.clock)
            if cmp not in (ClockComparison.AFTER, ClockComparison.EQUAL):
                dominates_all = False
                break

        if dominates_all:
            return candidate, False

    # Concurrent versions - use last-write-wins
    winner = max(versions, key=lambda v: v.clock.timestamp)
    return winner, True
