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
    """
    def __init__(self):
        self._bloom = get_bloom_filter()
        self._use_native = False
        
        try:
            from hbdb.native_ext import NativeBackend
            self._native = NativeBackend()
            self._use_native = True
            print("[HBDB] Using C++ NativeBackend 🚀")
        except ImportError:
            # Fallback to Python SortedDict
            self._store = SortedDict()
            self._lock = threading.RLock()

    def read(self, key: str, read_ts: int) -> Optional[Any]:
        if self._use_native:
            return self._native.read(key, read_ts)
            
        with self._lock:
            versions = self._store.get(key, [])
            for v in versions:
                if v.commit_ts <= read_ts:
                    return v.value
            return None

    def write(self, key: str, value: Any, commit_ts: int):
        # Update Bloom Filter (Python side)
        self._bloom.add(key)
        
        if self._use_native:
            self._native.write(key, value, commit_ts)
            return

        with self._lock:
            if key not in self._store:
                self._store[key] = []
            self._store[key].insert(0, VersionedValue(value, commit_ts))

    def scan(self, start_key: str, end_key: str, read_ts: int) -> List[Tuple[str, Any]]:
        if self._use_native:
            # Native returns list of (key, value)
            return self._native.scan(start_key, end_key, read_ts)

        results = []
        with self._lock:
            for key in self._store.irange(start_key, end_key, inclusive=(True, False)):
                val = self.read(key, read_ts)
                if val is not None:
                    results.append((key, val))
        return results

    def scan_keys(self, start_key: str, end_key: str) -> List[str]:
        """Return just keys in range (for index lookups)."""
        if self._use_native:
            # Native doesn't support scan_keys directly yet.
            # Only used for index lookups, which aren't in torture test hot path.
            # Hack: Scan with MAX_INT timestamp to get filtering? No, native scan filters by TS.
            # Using read_ts=2**64-1 (MAX_UINT64)
            full_scan = self._native.scan(start_key, end_key, 18446744073709551615)
            # Rebuild bloom filter on full scan or load? Ideally on load.
            return [k for k, v in full_scan]

        with self._lock:
            return list(self._store.irange(start_key, end_key, inclusive=(True, False)))
    
    def save_snapshot(self, path: str):
        if self._use_native:
            self._native.save_snapshot(path)
        else:
            raise NotImplementedError("Snapshotting only supported in Native Mode")

    def load_snapshot(self, path: str) -> int:
        if self._use_native:
            max_ts = self._native.load_snapshot(path)
            # Rebuild Bloom Filter
            self._bloom = get_bloom_filter()
            # We need to iterate all keys to add to bloom.
            all_data = self._native.scan("", "\xFF", 18446744073709551615)
            for k, _ in all_data:
                self._bloom.add(k)
            return max_ts
        else:
            raise NotImplementedError("Snapshotting only supported in Native Mode")
