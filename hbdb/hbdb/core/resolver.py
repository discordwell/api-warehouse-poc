import threading
from typing import Dict, List, Set, Tuple

from sortedcontainers import SortedDict


class PyResolver:
    """
    Pure-Python reference resolver (OCC conflict checker).

    Tracks the last commit timestamp per key and rejects transactions
    whose reads were invalidated by a later commit. Used directly when
    the C++ extension is unavailable, and as the per-partition store
    inside PartitionedResolver.
    """

    def __init__(self):
        # Maps key -> last_commit_timestamp
        self._committed_writes: SortedDict = SortedDict()
        self._lock = threading.Lock()
        self._current_ts = 0

    def get_read_timestamp(self) -> int:
        with self._lock:
            return self._current_ts

    def set_current_timestamp(self, ts: int):
        """Restore the clock after recovery (API parity with NativeResolver)."""
        with self._lock:
            if ts > self._current_ts:
                self._current_ts = ts

    def commit(self, read_ts: int, read_keys: Set[str], read_ranges: List[Tuple[str, str]], write_keys: Set[str]) -> Tuple[bool, int]:
        if not write_keys:
            return True, read_ts

        with self._lock:
            # 1. Conflict Check (OCC)

            # A. Check Read Keys
            for key in read_keys:
                last_changed = self._committed_writes.get(key, 0)
                if last_changed > read_ts:
                    return False, 0

            # B. Check Read Ranges (Phantom Read Protection)
            for start, end in read_ranges:
                for key in self._committed_writes.irange(start, end, inclusive=(True, False)):
                    if self._committed_writes[key] > read_ts:
                        return False, 0

            # 2. Advance Clock
            self._current_ts += 1
            commit_ts = self._current_ts

            # 3. Request Commit
            for key in write_keys:
                self._committed_writes[key] = commit_ts

            return True, commit_ts

    # --- Partition-level helpers (used by PartitionedResolver, which
    #     provides atomicity across partitions via its own commit lock) ---

    def check_keys(self, keys: Set[str], read_ts: int) -> bool:
        """True if none of `keys` was committed after `read_ts`."""
        with self._lock:
            for key in keys:
                if self._committed_writes.get(key, 0) > read_ts:
                    return False
            return True

    def check_ranges(self, ranges: List[Tuple[str, str]], read_ts: int) -> bool:
        """True if no key in any [start, end) range was committed after `read_ts`."""
        with self._lock:
            for start, end in ranges:
                for key in self._committed_writes.irange(start, end, inclusive=(True, False)):
                    if self._committed_writes[key] > read_ts:
                        return False
            return True

    def record_writes(self, keys: Set[str], commit_ts: int):
        """Record committed writes at an externally assigned timestamp."""
        with self._lock:
            for key in keys:
                self._committed_writes[key] = commit_ts
            if commit_ts > self._current_ts:
                self._current_ts = commit_ts


try:
    from hbdb.native_ext import NativeResolver
    _HAS_NATIVE = True
    print("[HBDB] Using C++ NativeResolver 🚀")
except ImportError as e:
    NativeResolver = PyResolver
    _HAS_NATIVE = False
    print(f"[HBDB] Falling back to Python Resolver ({e})")

# Alias for external use
Resolver = NativeResolver


class PartitionedResolver:
    """
    Shards conflict-tracking metadata across N resolvers.

    If the C++ NativeResolver is available, sharding is bypassed and a
    single high-performance native resolver covers the whole keyspace
    (it manages its own clock, so it cannot share an external one).

    In pure-Python mode, point-key metadata is sharded by hash across
    `num_partitions` PyResolvers while a single global clock assigns
    commit timestamps. Check + apply run under the commit lock, giving
    the same serializable semantics as the single-resolver reference.
    """

    def __init__(self, num_partitions: int = 4, force_python: bool = False):
        self.native_mode = _HAS_NATIVE and not force_python

        if self.native_mode:
            self.resolver = NativeResolver()
        else:
            self.partitions = [PyResolver() for _ in range(num_partitions)]
            self.num_partitions = num_partitions
            self._global_clock = 0
            self._clock_lock = threading.Lock()

    def get_read_timestamp(self) -> int:
        if self.native_mode:
            return self.resolver.get_read_timestamp()

        with self._clock_lock:
            return self._global_clock

    def restore_clock(self, ts: int):
        if self.native_mode:
            self.resolver.set_current_timestamp(ts)
        else:
            with self._clock_lock:
                if ts > self._global_clock:
                    self._global_clock = ts

    def _partition_for(self, key: str) -> int:
        # hash() is salted per-process for str; that's fine because
        # partition assignment only needs to be stable within a process.
        return hash(key) % self.num_partitions

    def commit(self, read_ts: int, read_keys: Set[str], read_ranges: List[Tuple[str, str]], write_keys: Set[str]) -> Tuple[bool, int]:
        if self.native_mode:
            # Delegate entirely to C++
            return self.resolver.commit(read_ts, read_keys, read_ranges, write_keys)

        if not write_keys:
            return True, read_ts

        # Group point keys by owning partition
        read_groups: Dict[int, Set[str]] = {}
        write_groups: Dict[int, Set[str]] = {}
        for key in read_keys:
            read_groups.setdefault(self._partition_for(key), set()).add(key)
        for key in write_keys:
            write_groups.setdefault(self._partition_for(key), set()).add(key)

        with self._clock_lock:
            # 1. Conflict check: point reads against their partitions.
            for pid, keys in read_groups.items():
                if not self.partitions[pid].check_keys(keys, read_ts):
                    return False, 0

            # 2. Conflict check: ranges against every partition, since
            #    hash sharding scatters a lexicographic range across all.
            if read_ranges:
                for partition in self.partitions:
                    if not partition.check_ranges(read_ranges, read_ts):
                        return False, 0

            # 3. Advance the global clock and record writes.
            self._global_clock += 1
            commit_ts = self._global_clock

            for pid, keys in write_groups.items():
                self.partitions[pid].record_writes(keys, commit_ts)

            return True, commit_ts
