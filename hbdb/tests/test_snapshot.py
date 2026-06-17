#!/usr/bin/env python3
"""
Snapshot Test Suite

Covers the pure-Python snapshot save/load path in VersionedKVStore
(previously NotImplementedError without the C++ extension), the binary
format shared with the native backend, and end-to-end snapshot +
recovery through HBDB in forced-Python mode.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tempfile

from hbdb.core.backend import VersionedKVStore


def _snap_path(tmpdir: str) -> str:
    return os.path.join(tmpdir, "snapshot.bin")


def test_python_roundtrip():
    """Pure-Python store: save and reload values, versions, and max_ts."""
    print("Testing Python snapshot round-trip...")

    store = VersionedKVStore(force_python=True)
    assert not store._use_native

    store.write("alpha", "value-a", 1)
    store.write("beta", {"nested": [1, 2, 3]}, 2)
    store.write("gamma", 42, 3)
    store.write("alpha", "value-a2", 4)  # second version of alpha

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _snap_path(tmpdir)
        store.save_snapshot(path)

        restored = VersionedKVStore(force_python=True)
        max_ts = restored.load_snapshot(path)

    assert max_ts == 4, f"Expected max_ts 4, got {max_ts}"
    assert restored.read("alpha", 4) == "value-a2"
    assert restored.read("alpha", 1) == "value-a", "Old MVCC version must survive"
    assert restored.read("alpha", 0) is None
    assert restored.read("beta", 4) == {"nested": [1, 2, 3]}
    assert restored.read("gamma", 4) == 42
    assert restored.read("missing", 4) is None

    scanned = restored.scan("", "\xFF", 4)
    assert [k for k, _ in scanned] == ["alpha", "beta", "gamma"]

    print("  PASSED")


def test_load_replaces_existing_state():
    """load_snapshot must clear whatever the store held before."""
    print("Testing load replaces existing state...")

    source = VersionedKVStore(force_python=True)
    source.write("k1", "v1", 1)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _snap_path(tmpdir)
        source.save_snapshot(path)

        target = VersionedKVStore(force_python=True)
        target.write("stale", "gone", 5)
        target.load_snapshot(path)

    assert target.read("k1", 10) == "v1"
    assert target.read("stale", 10) is None, "Pre-load state must be dropped"

    print("  PASSED")


def test_load_rejects_bad_magic():
    """Files that aren't HBDB snapshots are refused."""
    print("Testing bad-magic rejection...")

    store = VersionedKVStore(force_python=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _snap_path(tmpdir)
        with open(path, "wb") as f:
            f.write(b"NOPE" + b"\x00" * 16)

        try:
            store.load_snapshot(path)
            assert False, "Expected ValueError for bad magic"
        except ValueError as e:
            assert "magic" in str(e)

    print("  PASSED")


def test_load_rejects_truncated():
    """A snapshot cut off mid-record raises instead of silently loading."""
    print("Testing truncated-file rejection...")

    store = VersionedKVStore(force_python=True)
    store.write("key-that-makes-the-file-longer", "x" * 100, 1)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _snap_path(tmpdir)
        store.save_snapshot(path)

        size = os.path.getsize(path)
        with open(path, "rb") as f:
            data = f.read(size - 10)
        with open(path, "wb") as f:
            f.write(data)

        fresh = VersionedKVStore(force_python=True)
        fresh.write("existing", "kept", 1)
        try:
            fresh.load_snapshot(path)
            assert False, "Expected ValueError for truncated file"
        except ValueError as e:
            assert "Truncated" in str(e)

        # A failed load must not destroy the store's prior state
        assert fresh.read("existing", 1) == "kept"

    print("  PASSED")


def test_bloom_rebuilt_after_load():
    """The table bloom filter reflects the restored keyspace."""
    print("Testing bloom rebuild after load...")

    store = VersionedKVStore(force_python=True)
    store.write("/t/7/_r/1", {"id": 1}, 1)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _snap_path(tmpdir)
        store.save_snapshot(path)

        restored = VersionedKVStore(force_python=True)
        restored.load_snapshot(path)

    assert restored._bloom.might_contain("/t/7/_r/1")

    print("  PASSED")


def test_cross_mode_compatibility():
    """Native-saved snapshots load in Python mode and vice versa."""
    print("Testing native/Python snapshot compatibility...")

    native_store = VersionedKVStore()
    if not native_store._use_native:
        print("  SKIPPED (C++ extension not built)")
        return

    data = [("a", "plain", 1), ("b", {"deep": ["mix", 2]}, 2), ("c", 3.5, 3)]

    for key, value, ts in data:
        native_store.write(key, value, ts)

    with tempfile.TemporaryDirectory() as tmpdir:
        native_path = os.path.join(tmpdir, "native.bin")
        py_path = os.path.join(tmpdir, "python.bin")

        # Native -> Python
        native_store.save_snapshot(native_path)
        py_store = VersionedKVStore(force_python=True)
        max_ts = py_store.load_snapshot(native_path)
        assert max_ts == 3
        for key, value, ts in data:
            assert py_store.read(key, 3) == value, f"native->py mismatch for {key}"

        # Python -> Native
        py_store.save_snapshot(py_path)
        native_restored = VersionedKVStore()
        max_ts = native_restored.load_snapshot(py_path)
        assert max_ts == 3
        for key, value, ts in data:
            assert native_restored.read(key, 3) == value, f"py->native mismatch for {key}"

    print("  PASSED")


def test_hbdb_python_mode_recovery():
    """End-to-end: take_snapshot + WAL replay with the Python backend."""
    print("Testing HBDB snapshot/recovery in forced-Python mode...")

    from hbdb.db import HBDB

    prev_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)  # HBDB durability files are CWD-relative
        try:
            db1 = HBDB(force_python=True)
            assert not db1.backend._use_native

            tx = db1.transaction()
            tx.set("pre_snap", "v1")
            assert tx.commit()

            db1.take_snapshot()
            assert os.path.exists("snapshot.bin")

            tx = db1.transaction()
            tx.set("post_snap", "v2")
            assert tx.commit()
            del db1

            db2 = HBDB(force_python=True)
            recovered_ts = db2.resolver.get_read_timestamp()
            assert recovered_ts == 2, f"Clock should recover to 2, got {recovered_ts}"

            tx = db2.transaction()
            assert tx.get("pre_snap") == "v1", "Snapshot data must survive restart"
            assert tx.get("post_snap") == "v2", "WAL data must survive restart"

            # New commits must stamp above the recovered clock, or they
            # would be shadowed by recovered data
            tx = db2.transaction()
            tx.set("post_recovery", "v3")
            assert tx.commit()
            assert tx.commit_ts > recovered_ts, \
                f"commit_ts {tx.commit_ts} not above recovered clock {recovered_ts}"
        finally:
            os.chdir(prev_cwd)

    print("  PASSED")


def test_wal_out_of_order_replay():
    """Replay must keep WAL entries whose timestamps arrive out of order.

    Commits append to the WAL outside the resolver lock, so a slow
    transaction can land after a faster one with a higher timestamp.
    Replay used to skip such entries as 'already snapshotted'.
    """
    print("Testing out-of-order WAL replay...")

    from hbdb.db import HBDB

    prev_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            with open("transaction.log", "w") as f:
                f.write('{"ts": 2, "ops": {"fast": "v2"}}\n')
                f.write('{"ts": 1, "ops": {"slow": "v1"}}\n')

            db = HBDB(force_python=True)
            tx = db.transaction()
            assert tx.get("fast") == "v2"
            assert tx.get("slow") == "v1", "Out-of-order WAL entry was dropped"
            assert db.resolver.get_read_timestamp() == 2
        finally:
            os.chdir(prev_cwd)

    print("  PASSED")


def test_orphaned_archive_replay():
    """Recovery must replay WAL archives left by a crashed snapshot.

    take_snapshot() rotates the live log before writing the snapshot; a
    crash in between leaves the commits only in transaction.log.archive.*,
    which recovery used to ignore entirely.
    """
    print("Testing orphaned-archive replay...")

    from hbdb.db import HBDB

    prev_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        try:
            # Simulate the crash window: rotated archive, fresh live log,
            # no snapshot written.
            with open("transaction.log.archive.1700000000", "w") as f:
                f.write('{"ts": 1, "ops": {"archived": "v1"}}\n')
            with open("transaction.log", "w") as f:
                f.write('{"ts": 2, "ops": {"live": "v2"}}\n')

            db = HBDB(force_python=True)
            tx = db.transaction()
            assert tx.get("archived") == "v1", "Archived commit was lost"
            assert tx.get("live") == "v2"
        finally:
            os.chdir(prev_cwd)

    print("  PASSED")


def run_all_tests():
    print("=" * 60)
    print("Snapshot Test Suite")
    print("=" * 60)
    print()

    tests = [
        test_python_roundtrip,
        test_load_replaces_existing_state,
        test_load_rejects_bad_magic,
        test_load_rejects_truncated,
        test_bloom_rebuilt_after_load,
        test_cross_mode_compatibility,
        test_hbdb_python_mode_recovery,
        test_wal_out_of_order_replay,
        test_orphaned_archive_replay,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1

    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
