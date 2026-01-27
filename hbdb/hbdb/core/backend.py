import threading
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass
from sortedcontainers import SortedDict
from .bloom import get_bloom_filter

@dataclass
class VersionedValue:
    value: Any
    commit_ts: int

class VersionedKVStore:
    """
    Simulates the Storage Server role in FoundationDB.
    Stores multiple versions of keys (MVCC).
    
    Optimizations:
    - SortedDict for O(log n) range scans
    - Bloom filter integration for scan pruning
    """
    def __init__(self):
        # Key -> List of VersionedValue (sorted by commit_ts desc)
        # Using SortedDict for O(log n) key lookups and efficient range scans
        self._store: SortedDict = SortedDict()
        self._lock = threading.RLock()
        self._bloom = get_bloom_filter()

    def read(self, key: str, read_ts: int) -> Optional[Any]:
        """Read the latest version visible at read_ts."""
        with self._lock:
            versions = self._store.get(key, [])
            for v in versions:
                if v.commit_ts <= read_ts:
                    return v.value
            return None

    def write(self, key: str, value: Any, commit_ts: int):
        """Write a new version."""
        with self._lock:
            if key not in self._store:
                self._store[key] = []
            
            # Insert at front (assuming monotonic commit_ts usually)
            self._store[key].insert(0, VersionedValue(value, commit_ts))
            
            # Add to bloom filter
            self._bloom.add(key)

    def scan(self, start_key: str, end_key: str, read_ts: int) -> List[Tuple[str, Any]]:
        """
        Range scan [start_key, end_key).
        Uses SortedDict.irange() for O(log n + k) performance.
        """
        results = []
        with self._lock:
            # O(log n) to find start, then O(k) to iterate results
            for key in self._store.irange(start_key, end_key, inclusive=(True, False)):
                val = self.read(key, read_ts)
                if val is not None:
                    results.append((key, val))
        return results

    def scan_keys(self, start_key: str, end_key: str) -> List[str]:
        """Return just keys in range (for index lookups)."""
        with self._lock:
            return list(self._store.irange(start_key, end_key, inclusive=(True, False)))
