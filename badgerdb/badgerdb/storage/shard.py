"""
Shard Management

Data is partitioned across shards by key hash.
Each shard is replicated for durability.
"""

from __future__ import annotations
import hashlib
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from ..types import Key, Value, Timestamp, TxnId
from .mvcc import MVCCStore


@dataclass
class ShardInfo:
    """Metadata about a shard."""
    shard_id: int
    start_key: str  # Inclusive
    end_key: str    # Exclusive
    replicas: List[str] = field(default_factory=list)  # Node IDs


class Shard:
    """
    A single shard storing a range of keys.

    Each shard has its own MVCC store.
    """

    def __init__(self, shard_id: int, start_key: str = "", end_key: str = ""):
        self.shard_id = shard_id
        self.start_key = start_key
        self.end_key = end_key
        self.store = MVCCStore()
        self._lock = threading.RLock()

    def owns_key(self, key: Key) -> bool:
        """Check if this shard owns the given key."""
        if self.start_key and key < self.start_key:
            return False
        if self.end_key and key >= self.end_key:
            return False
        return True

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read a key at a timestamp."""
        return self.store.read(key, timestamp)

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: Optional[TxnId] = None):
        """Write a key."""
        self.store.write(key, value, timestamp, txn_id)

    def delete(self, key: Key, timestamp: Timestamp, txn_id: Optional[TxnId] = None):
        """Delete a key."""
        self.store.delete(key, timestamp, txn_id)

    def scan(self, start: Key, end: Key, timestamp: Timestamp, limit: int = 100) -> List[Tuple[Key, Value]]:
        """Scan a key range."""
        return self.store.scan(start, end, timestamp, limit)

    def get_stats(self) -> dict:
        """Get shard statistics."""
        return {
            "shard_id": self.shard_id,
            **self.store.get_stats()
        }


class ShardManager:
    """
    Manages the collection of shards.

    Routes keys to appropriate shards using consistent hashing.
    """

    def __init__(self, num_shards: int = 4):
        self.num_shards = num_shards
        self.shards: Dict[int, Shard] = {}
        self._lock = threading.RLock()

        # Initialize shards
        self._init_shards()

    def _init_shards(self):
        """Initialize shard ring."""
        # Simple range-based partitioning
        # In production, would use consistent hashing
        for i in range(self.num_shards):
            self.shards[i] = Shard(shard_id=i)

    def _get_shard_id(self, key: Key) -> int:
        """Get the shard ID for a key using hash partitioning."""
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_val % self.num_shards

    def get_shard(self, key: Key) -> Shard:
        """Get the shard responsible for a key."""
        shard_id = self._get_shard_id(key)
        return self.shards[shard_id]

    def get_shards_for_keys(self, keys: List[Key]) -> Dict[int, List[Key]]:
        """Group keys by their shard."""
        result: Dict[int, List[Key]] = {}
        for key in keys:
            shard_id = self._get_shard_id(key)
            if shard_id not in result:
                result[shard_id] = []
            result[shard_id].append(key)
        return result

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read a key."""
        shard = self.get_shard(key)
        return shard.read(key, timestamp)

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: Optional[TxnId] = None):
        """Write a key."""
        shard = self.get_shard(key)
        shard.write(key, value, timestamp, txn_id)

    def delete(self, key: Key, timestamp: Timestamp, txn_id: Optional[TxnId] = None):
        """Delete a key."""
        shard = self.get_shard(key)
        shard.delete(key, timestamp, txn_id)

    def get_all_shards(self) -> List[Shard]:
        """Get all shards."""
        return list(self.shards.values())

    def get_stats(self) -> dict:
        """Get manager statistics."""
        shard_stats = {
            f"shard_{i}": shard.get_stats()
            for i, shard in self.shards.items()
        }
        return {
            "num_shards": self.num_shards,
            "shards": shard_stats
        }
