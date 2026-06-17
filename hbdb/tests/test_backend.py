#!/usr/bin/env python3
"""
Backend Facade Test Suite

Pins down VersionedKVStore contracts that must hold identically in the
pure-Python and C++ native modes:

- scan() never returns None tombstones (SQL DELETE writes them; the raw
  C++ scan returns them verbatim, which crashed DELETE -> SELECT).
- write() keeps MVCC version order correct even when writes for the same
  key arrive out of timestamp order (WAL replay, racing commits).
- Transaction.scan() hides keys deleted in the transaction's own buffer.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hbdb.core.backend import VersionedKVStore


def _native_available() -> bool:
    return VersionedKVStore()._use_native


def _check_tombstone_scan(store):
    store.write("row1", {"id": 1}, 1)
    store.write("row2", {"id": 2}, 1)
    store.write("row1", None, 2)  # delete

    visible = store.scan("row", "row~", 2)
    assert visible == [("row2", {"id": 2})], f"Tombstone leaked into scan: {visible}"

    # The old version is still visible below the delete
    at_ts1 = store.scan("row", "row~", 1)
    assert ("row1", {"id": 1}) in at_ts1

    assert store.read("row1", 2) is None


def test_scan_filters_tombstones_python():
    """Python mode: deleted keys must not appear in scans."""
    print("Testing tombstone filtering (Python)...")
    _check_tombstone_scan(VersionedKVStore(force_python=True))
    print("  PASSED")


def test_scan_filters_tombstones_native():
    """Native mode: deleted keys must not appear in scans."""
    print("Testing tombstone filtering (native)...")
    if not _native_available():
        print("  SKIPPED (C++ extension not built)")
        return
    _check_tombstone_scan(VersionedKVStore())
    print("  PASSED")


def _check_out_of_order_writes(store):
    store.write("k", "newest", 6)
    store.write("k", "older", 5)  # arrives late (WAL replay / racing commit)

    assert store.read("k", 10) == "newest", "Late arrival must not shadow newer version"
    assert store.read("k", 5) == "older"
    assert store.read("k", 6) == "newest"
    assert store.read("k", 4) is None


def test_out_of_order_writes_python():
    """Python mode: late same-key writes keep MVCC order."""
    print("Testing out-of-order writes (Python)...")
    _check_out_of_order_writes(VersionedKVStore(force_python=True))
    print("  PASSED")


def test_out_of_order_writes_native():
    """Native mode: late same-key writes keep MVCC order."""
    print("Testing out-of-order writes (native)...")
    if not _native_available():
        print("  SKIPPED (C++ extension not built)")
        return
    _check_out_of_order_writes(VersionedKVStore())
    print("  PASSED")


def test_txn_scan_hides_own_deletes():
    """A buffered None (delete) must hide the key from the txn's own scan."""
    print("Testing transaction scan read-your-deletes...")

    from hbdb.core.proxy import Transaction
    from hbdb.core.resolver import PartitionedResolver

    backend = VersionedKVStore(force_python=True)
    resolver = PartitionedResolver(num_partitions=1, force_python=True)

    setup = Transaction(backend, resolver)
    setup.set("a", 1)
    setup.set("b", 2)
    assert setup.commit()

    tx = Transaction(backend, resolver)
    tx.set("a", None)  # delete in-buffer
    visible = tx.scan("a", "z")
    assert visible == [("b", 2)], f"Buffered delete leaked into scan: {visible}"

    print("  PASSED")


def run_all_tests():
    print("=" * 60)
    print("Backend Facade Test Suite")
    print("=" * 60)
    print()

    tests = [
        test_scan_filters_tombstones_python,
        test_scan_filters_tombstones_native,
        test_out_of_order_writes_python,
        test_out_of_order_writes_native,
        test_txn_scan_hides_own_deletes,
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
