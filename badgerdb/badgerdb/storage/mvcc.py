"""
MVCC (Multi-Version Concurrency Control) Storage Engine

Each key stores multiple versions with timestamps.
Readers see a consistent snapshot at their read timestamp.
Writers create new versions without blocking readers.
"""

from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from bisect import bisect_right, insort

from ..types import Key, Value, Timestamp, MVCCValue, TxnId


class MVCCStore:
    """
    Multi-version key-value store.

    Data structure:
        key -> [(timestamp, value), (timestamp, value), ...]
        Sorted by timestamp, newest first for efficient reads.
    """

    def __init__(self, gc_threshold: int = 1000):
        # key -> list of MVCCValue sorted by timestamp (ascending)
        self._data: Dict[Key, List[MVCCValue]] = defaultdict(list)
        self._lock = threading.RLock()
        self._gc_threshold = gc_threshold

        # Stats
        self._stats = {
            "reads": 0,
            "writes": 0,
            "versions_created": 0,
            "gc_runs": 0,
        }

    def read(
        self,
        key: Key,
        timestamp: Timestamp,
        txn_id: Optional[TxnId] = None
    ) -> Optional[Value]:
        """
        Read the value of a key at a given timestamp.

        Returns the most recent version at or before the timestamp.
        """
        self._stats["reads"] += 1

        with self._lock:
            versions = self._data.get(key)
            if not versions:
                return None

            # Find the latest version at or before timestamp
            # Versions are sorted ascending, so we search backwards
            for v in reversed(versions):
                if v.timestamp <= timestamp:
                    if v.deleted:
                        return None
                    return v.value

            return None

    def write(
        self,
        key: Key,
        value: Value,
        timestamp: Timestamp,
        txn_id: Optional[TxnId] = None
    ):
        """
        Write a new version of a key at the given timestamp.
        """
        self._stats["writes"] += 1
        self._stats["versions_created"] += 1

        mvcc_value = MVCCValue(
            value=value,
            timestamp=timestamp,
            deleted=False,
            txn_id=txn_id
        )

        with self._lock:
            versions = self._data[key]

            # Insert in sorted order by timestamp
            # Find insertion point
            idx = 0
            for i, v in enumerate(versions):
                if v.timestamp > timestamp:
                    break
                idx = i + 1

            versions.insert(idx, mvcc_value)

            # GC old versions if needed
            if len(versions) > self._gc_threshold:
                self._gc_key(key)

    def delete(
        self,
        key: Key,
        timestamp: Timestamp,
        txn_id: Optional[TxnId] = None
    ):
        """
        Delete a key by writing a tombstone.
        """
        self._stats["writes"] += 1
        self._stats["versions_created"] += 1

        tombstone = MVCCValue(
            value=None,
            timestamp=timestamp,
            deleted=True,
            txn_id=txn_id
        )

        with self._lock:
            versions = self._data[key]
            idx = 0
            for i, v in enumerate(versions):
                if v.timestamp > timestamp:
                    break
                idx = i + 1
            versions.insert(idx, tombstone)

    def scan(
        self,
        start_key: Key,
        end_key: Key,
        timestamp: Timestamp,
        limit: int = 100
    ) -> List[Tuple[Key, Value]]:
        """
        Scan a range of keys at a given timestamp.
        """
        results = []

        with self._lock:
            for key in sorted(self._data.keys()):
                if key < start_key:
                    continue
                if key >= end_key:
                    break

                value = self.read(key, timestamp)
                if value is not None:
                    results.append((key, value))

                if len(results) >= limit:
                    break

        return results

    def get_all_versions(self, key: Key) -> List[MVCCValue]:
        """Get all versions of a key (for debugging/testing)."""
        with self._lock:
            return list(self._data.get(key, []))

    def _gc_key(self, key: Key):
        """
        Garbage collect old versions of a key.

        Keep only the N most recent versions.
        """
        versions = self._data[key]
        if len(versions) <= self._gc_threshold // 2:
            return

        # Keep recent versions
        self._data[key] = versions[-(self._gc_threshold // 2):]
        self._stats["gc_runs"] += 1

    def get_stats(self) -> dict:
        """Get storage statistics."""
        with self._lock:
            total_keys = len(self._data)
            total_versions = sum(len(v) for v in self._data.values())

        return {
            **self._stats,
            "total_keys": total_keys,
            "total_versions": total_versions,
        }


class MVCCTransaction:
    """
    Helper for transactional MVCC operations.

    Provides read-your-writes semantics within a transaction.
    """

    def __init__(self, store: MVCCStore, txn_id: TxnId, timestamp: Timestamp):
        self.store = store
        self.txn_id = txn_id
        self.timestamp = timestamp

        # Buffer writes until commit
        self._write_buffer: Dict[Key, Tuple[Value, bool]] = {}  # key -> (value, is_delete)
        self._read_set: Dict[Key, Value] = {}

    def read(self, key: Key) -> Optional[Value]:
        """Read, checking write buffer first."""
        # Check local writes first (read-your-writes)
        if key in self._write_buffer:
            value, is_delete = self._write_buffer[key]
            return None if is_delete else value

        # Read from store
        value = self.store.read(key, self.timestamp, self.txn_id)
        self._read_set[key] = value
        return value

    def write(self, key: Key, value: Value):
        """Buffer a write."""
        self._write_buffer[key] = (value, False)

    def delete(self, key: Key):
        """Buffer a delete."""
        self._write_buffer[key] = (None, True)

    def commit(self, commit_timestamp: Timestamp):
        """Apply all buffered writes."""
        for key, (value, is_delete) in self._write_buffer.items():
            if is_delete:
                self.store.delete(key, commit_timestamp, self.txn_id)
            else:
                self.store.write(key, value, commit_timestamp, self.txn_id)

    def abort(self):
        """Discard all buffered writes."""
        self._write_buffer.clear()
        self._read_set.clear()
