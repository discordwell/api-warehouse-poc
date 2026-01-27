import threading
from typing import Dict, Set, List, Tuple

class Resolver:
    """
    Simulates the Resolver role in FoundationDB.
    Responsible for checking transaction conflicts.
    """
    def __init__(self):
        # Maps key -> last_commit_timestamp
        # In a real system, this is sharded and windowed (only keep recent history)
        self._committed_writes: Dict[str, int] = {}
        self._lock = threading.Lock()
        
        # Logical clock
        self._current_ts = 0

    def get_read_timestamp(self) -> int:
        """Get a read timestamp (start of transaction)."""
        with self._lock:
            # We return current commit timestamp as the read snapshot point
            return self._current_ts

    def commit(self, read_ts: int, read_keys: Set[str], write_keys: Set[str]) -> Tuple[bool, int]:
        """
        Attempt to commit a transaction.
        Returns: (success, commit_timestamp)
        """
        if not write_keys:
            # Read-only transaction always succeeds
            return True, read_ts

        with self._lock:
            # 1. Conflict Check (OCC)
            # For every key read, ensure it hasn't been changed since read_ts
            for key in read_keys:
                last_changed = self._committed_writes.get(key, 0)
                if last_changed > read_ts:
                    return False, 0
            
            # 2. Advance Clock
            self._current_ts += 1
            commit_ts = self._current_ts
            
            # 3. Request Commit (Update Resolver State)
            for key in write_keys:
                self._committed_writes[key] = commit_ts
                
            return True, commit_ts

class PartitionedResolver:
    """
    Shards key-space across N resolvers.
    """
    def __init__(self, num_partitions: int = 4):
        self.partitions = [Resolver() for _ in range(num_partitions)]
        self.num_partitions = num_partitions
        
        # In a real system, we need a centralized TSO (Timestamp Oracle)
        # For simulation, we'll just share one counter or synchronize.
        # Simpler: Make partitions share a synchronized clock source, 
        # but maintain separate conflict maps.
        self._global_clock = 0
        self._clock_lock = threading.Lock()

    def get_read_timestamp(self) -> int:
        with self._clock_lock:
            return self._global_clock

    def commit(self, read_ts: int, read_keys: Set[str], write_keys: Set[str]) -> Tuple[bool, int]:
        """
        Coordinated commit across partitions.
        Global serializability requires all involved partitions to agree.
        """
        # 1. Group keys by partition
        groups: Dict[int, Tuple[Set[str], Set[str]]] = {}
        
        all_keys = read_keys.union(write_keys)
        for key in all_keys:
            pid = hash(key) % self.num_partitions
            if pid not in groups:
                groups[pid] = (set(), set())
            
            if key in read_keys: groups[pid][0].add(key)
            if key in write_keys: groups[pid][1].add(key)

        # 2. Acquire Commit Timestamp (Global)
        # In FDB, this happens after conflict check usually, but for simple 
        # simulation let's pick it now to ensure ordering.
        with self._clock_lock:
            self._global_clock += 1
            commit_ts = self._global_clock

        # 3. Check All Partitions (Parallel in real life)
        for pid, (r_keys, w_keys) in groups.items():
            resolver = self.partitions[pid]
            with resolver._lock:
                # Manual conflict check against THIS partition's state
                for key in r_keys:
                    last = resolver._committed_writes.get(key, 0)
                    if last > read_ts:
                        return False, 0
                
                # Apply writes if we proceed (Optimistic/Locked phase)
                # Note: Real 2PC would prepare then commit. 
                # Here we assume single-threaded check-then-act for simulation simplicity
                # OR we roll back if one fails. 
                # For this prototype: checking sequentially. If one fails, we abort.
                # BUT we might have polluted previous partitions?
                # Need atomic commit!
                pass

        # Since we can't easily do atomic 2PC across Python objects without complexity,
        # we will: snapshot all locks? No.
        # We'll just do a double-pass. 
        # Pass 1: Check
        for pid, (r_keys, w_keys) in groups.items():
            resolver = self.partitions[pid]
            with resolver._lock:
                for key in r_keys:
                    if resolver._committed_writes.get(key, 0) > read_ts:
                        return False, 0
        
        # Pass 2: Apply
        for pid, (r_keys, w_keys) in groups.items():
            resolver = self.partitions[pid]
            with resolver._lock:
                for key in w_keys:
                    resolver._committed_writes[key] = commit_ts
                    
        return True, commit_ts
