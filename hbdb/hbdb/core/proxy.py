from typing import Any, Dict, Set, Optional, Union, List, Tuple
from .backend import VersionedKVStore
from .resolver import Resolver

class Transaction:
    """
    Open, interactive transaction Buffer.
    """
    def __init__(self, backend: VersionedKVStore, resolver: Any):
        self.backend = backend
        self.resolver = resolver
        
        self.read_ts = 0
        self.commit_ts = 0
        
        self._read_buffer: Dict[str, Any] = {} # Cache reads
        self._write_buffer: Dict[str, Any] = {} # Buffer writes
        
        self._read_set: Set[str] = set()
        self._read_ranges: List[Tuple[str, str]] = [] # Track scanned ranges
        self._write_set: Set[str] = set()
        
        self.started = False
        self.committed = False
        self.failed = False

    def begin(self):
        """Start transaction by getting read version."""
        self.read_ts = self.resolver.get_read_timestamp()
        self.started = True

    def get(self, key: str) -> Any:
        """Read a key."""
        if not self.started: self.begin()
        
        # RYW (Read Your Writes)
        if key in self._write_buffer:
            return self._write_buffer[key]
        
        # Cached read
        if key in self._read_buffer:
            return self._read_buffer[key]
        
        # Read from storage
        val = self.backend.read(key, self.read_ts)
        self._read_buffer[key] = val
        self._read_set.add(key)
        
        return val

    def set(self, key: str, value: Any):
        """Buffer a write."""
        if not self.started: self.begin()
        
        self._write_buffer[key] = value
        self._write_set.add(key)

    def scan(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """Range scan [start, end). WARNING: Merging writes is complex. Ignoring rights for now."""
        if not self.started: self.begin()
        
        # Track range read for conflict detection
        self._read_ranges.append((start_key, end_key))

        # Read from backend
        data = self.backend.scan(start_key, end_key, self.read_ts)
        
        # Merge with write buffer? 
        # For POC, let's just assume we scan read-only data or don't see own writes in scan.
        # Ideally: Overlay write_buffer.
        buffered_keys = [k for k in self._write_buffer if start_key <= k < end_key]
        
        # Construct result map
        result_map = {k: v for k, v in data}
        
        # Overlay writes (Read Your Writes)
        for k in buffered_keys:
            result_map[k] = self._write_buffer[k]
            
        return sorted(result_map.items())

    def commit(self) -> bool:
        """Try to commit."""
        if not self.started: return True # Empty
        if self.committed: raise Exception("Already committed")
        
        # 1. Ask Resolver
        success, commit_ts = self.resolver.commit(
            self.read_ts, self._read_set, self._read_ranges, self._write_set
        )
        
        if not success:
            self.failed = True
            return False
        
        self.commit_ts = commit_ts

        # 2. Durabiltiy (Write-Ahead Log)
        # Must persist to disk before acknowledging commit
        from .sequencer import get_sequencer
        get_sequencer().append(self.commit_ts, self._write_buffer)
        
        # 3. Apply writes to storage (async in real system, sync here)
        for key, val in self._write_buffer.items():
            self.backend.write(key, val, self.commit_ts)
            
        self.committed = True
        return True
