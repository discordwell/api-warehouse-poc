import pickle
import struct
import threading
from typing import Dict, Any, Tuple, Optional, List, BinaryIO
from dataclasses import dataclass
from sortedcontainers import SortedDict
from .bloom import TableBloomFilter

# Snapshot format (shared with the C++ NativeBackend, see native/backend.cpp):
#   [Magic:4 "HBDB"][Version:4][NumKeys:8]
#   For each key:
#     [KeyLen:4][KeyBytes...]
#     [NumVers:4]
#     For each version (oldest first):
#       [TS:8][ValLen:4][ValPickledBytes...]
# The native code writes raw C structs, which are little-endian on every
# platform this builds for (x86-64/arm64), so "<" keeps files compatible
# across the Python and native backends.
_SNAP_MAGIC = b"HBDB"
_SNAP_VERSION = 1
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")


def _read_exact(f: BinaryIO, n: int) -> bytes:
    data = f.read(n)
    if len(data) != n:
        raise ValueError("Truncated snapshot file")
    return data


@dataclass
class VersionedValue:
    value: Any
    commit_ts: int

class VersionedKVStore:
    """
    Simulates the Storage Server role in FoundationDB.
    Stores multiple versions of keys (MVCC).
    """
    def __init__(self, force_python: bool = False):
        # Per-store filter: load_snapshot resets it, which must not wipe
        # entries belonging to other stores in the same process.
        self._bloom = TableBloomFilter()
        self._use_native = False

        if not force_python:
            try:
                from hbdb.native_ext import NativeBackend
                self._native = NativeBackend()
                self._use_native = True
                print("[HBDB] Using C++ NativeBackend 🚀")
            except ImportError:
                pass

        if not self._use_native:
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
            versions = self._store.setdefault(key, [])
            # Keep newest-first regardless of arrival order: WAL replay
            # and racing same-key commits can apply writes out of
            # timestamp order.
            i = 0
            while i < len(versions) and versions[i].commit_ts > commit_ts:
                i += 1
            versions.insert(i, VersionedValue(value, commit_ts))

    def scan(self, start_key: str, end_key: str, read_ts: int) -> List[Tuple[str, Any]]:
        if self._use_native:
            # Native returns list of (key, value). Filter None tombstones
            # (e.g. SQL DELETE) here for parity with the Python branch —
            # the C++ scan returns them verbatim.
            return [(k, v) for k, v in self._native.scan(start_key, end_key, read_ts)
                    if v is not None]

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
            return

        with self._lock:
            with open(path, "wb") as out:
                out.write(_SNAP_MAGIC)
                out.write(_U32.pack(_SNAP_VERSION))
                out.write(_U64.pack(len(self._store)))

                for key, versions in self._store.items():
                    kbytes = key.encode("utf-8")
                    out.write(_U32.pack(len(kbytes)))
                    out.write(kbytes)
                    out.write(_U32.pack(len(versions)))
                    # On disk versions are oldest-first (native append
                    # order); in memory we keep them newest-first.
                    for v in reversed(versions):
                        out.write(_U64.pack(v.commit_ts))
                        vbytes = pickle.dumps(v.value)
                        out.write(_U32.pack(len(vbytes)))
                        out.write(vbytes)

    def load_snapshot(self, path: str) -> int:
        if self._use_native:
            max_ts = self._native.load_snapshot(path)
            keys = (k for k, _ in self._native.scan("", "\xFF", 18446744073709551615))
        else:
            max_ts = self._py_load_snapshot(path)
            with self._lock:
                keys = list(self._store.keys())

        # Rebuild Bloom Filter from the restored keyspace
        self._bloom.reset()
        for k in keys:
            self._bloom.add(k)
        return max_ts

    def _py_load_snapshot(self, path: str) -> int:
        # Parse into a fresh store and swap at the end, so a corrupt or
        # truncated file raises without destroying the current state.
        new_store = SortedDict()
        max_ts = 0

        with open(path, "rb") as f:
            if _read_exact(f, 4) != _SNAP_MAGIC:
                raise ValueError("Invalid snapshot magic")
            (version,) = _U32.unpack(_read_exact(f, 4))
            if version != _SNAP_VERSION:
                raise ValueError(f"Unsupported snapshot version: {version}")
            (num_keys,) = _U64.unpack(_read_exact(f, 8))

            for _ in range(num_keys):
                (klen,) = _U32.unpack(_read_exact(f, 4))
                key = _read_exact(f, klen).decode("utf-8")
                (nver,) = _U32.unpack(_read_exact(f, 4))

                versions = []
                for _ in range(nver):
                    (ts,) = _U64.unpack(_read_exact(f, 8))
                    if ts > max_ts:
                        max_ts = ts
                    (vlen,) = _U32.unpack(_read_exact(f, 4))
                    value = pickle.loads(_read_exact(f, vlen))
                    versions.append(VersionedValue(value, ts))

                versions.reverse()  # disk is oldest-first; memory is newest-first
                new_store[key] = versions

        with self._lock:
            self._store = new_store

        return max_ts
