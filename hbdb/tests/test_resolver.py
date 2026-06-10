#!/usr/bin/env python3
"""
Resolver Test Suite

Covers the pure-Python OCC resolver path (PyResolver and
PartitionedResolver with force_python=True), which is what runs on
machines without the C++ native extension. Regression test for the
fallback commit() path that previously skipped conflict detection and
returned commit_ts=0 for every transaction.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrent.futures import ThreadPoolExecutor

from hbdb.core.resolver import PyResolver, PartitionedResolver, _HAS_NATIVE


def test_py_resolver_basic():
    """Single PyResolver: commit advances clock, stale reads abort."""
    print("Testing PyResolver basics...")

    r = PyResolver()
    assert r.get_read_timestamp() == 0

    ok, ts1 = r.commit(read_ts=0, read_keys=set(), read_ranges=[], write_keys={"a"})
    assert ok and ts1 == 1, f"First commit should get ts=1, got {ts1}"

    # A transaction that read "a" before ts1 must abort
    ok, _ = r.commit(read_ts=0, read_keys={"a"}, read_ranges=[], write_keys={"b"})
    assert not ok, "Stale read of 'a' should abort"

    # Reading at the current timestamp is fine
    ok, ts2 = r.commit(read_ts=ts1, read_keys={"a"}, read_ranges=[], write_keys={"b"})
    assert ok and ts2 == 2

    print("  PASSED")


def test_partitioned_commit_ts_nonzero():
    """Partitioned Python mode must hand out real, increasing timestamps."""
    print("Testing partitioned commit timestamps...")

    pr = PartitionedResolver(num_partitions=4, force_python=True)
    assert not pr.native_mode

    prev = 0
    for i in range(20):
        ok, ts = pr.commit(read_ts=prev, read_keys=set(), read_ranges=[], write_keys={f"k{i}"})
        assert ok, f"Disjoint write {i} should commit"
        assert ts == prev + 1, f"Expected ts {prev + 1}, got {ts}"
        prev = ts

    assert pr.get_read_timestamp() == prev

    print("  PASSED")


def test_partitioned_conflict_detection():
    """Stale point reads must abort across partition boundaries."""
    print("Testing partitioned conflict detection...")

    pr = PartitionedResolver(num_partitions=4, force_python=True)

    # Seed several keys so they land on different partitions
    keys = [f"key{i}" for i in range(8)]
    for k in keys:
        ok, _ = pr.commit(read_ts=pr.get_read_timestamp(), read_keys=set(),
                          read_ranges=[], write_keys={k})
        assert ok

    snapshot_ts = pr.get_read_timestamp()

    # Another writer bumps every key after our snapshot
    for k in keys:
        ok, _ = pr.commit(read_ts=snapshot_ts, read_keys=set(),
                          read_ranges=[], write_keys={k})
        assert ok

    # Any transaction that read one of those keys at snapshot_ts must abort
    for k in keys:
        ok, ts = pr.commit(read_ts=snapshot_ts, read_keys={k},
                           read_ranges=[], write_keys={"other"})
        assert not ok, f"Stale read of {k} should abort"
        assert ts == 0

    print("  PASSED")


def test_partitioned_range_conflict():
    """Phantom protection: a write inside a scanned range aborts the scanner."""
    print("Testing partitioned range conflicts...")

    pr = PartitionedResolver(num_partitions=4, force_python=True)

    ok, _ = pr.commit(read_ts=0, read_keys=set(), read_ranges=[], write_keys={"user/5"})
    assert ok
    snapshot_ts = pr.get_read_timestamp()

    # Concurrent insert into the range after our snapshot
    ok, _ = pr.commit(read_ts=snapshot_ts, read_keys=set(), read_ranges=[],
                      write_keys={"user/3"})
    assert ok

    # We scanned user/0..user/9 at snapshot_ts: must abort
    ok, _ = pr.commit(read_ts=snapshot_ts, read_keys=set(),
                      read_ranges=[("user/0", "user/9")], write_keys={"agg"})
    assert not ok, "Range scan invalidated by concurrent insert should abort"

    # A scan over an untouched range is fine
    ok, _ = pr.commit(read_ts=snapshot_ts, read_keys=set(),
                      read_ranges=[("zone/0", "zone/9")], write_keys={"agg"})
    assert ok

    print("  PASSED")


def test_partitioned_read_only():
    """Read-only transactions commit trivially at their read timestamp."""
    print("Testing read-only commit...")

    pr = PartitionedResolver(num_partitions=2, force_python=True)
    ok, ts = pr.commit(read_ts=7, read_keys={"a", "b"}, read_ranges=[], write_keys=set())
    assert ok and ts == 7

    print("  PASSED")


def test_partitioned_restore_clock():
    """After recovery, new commits must be stamped above the restored clock."""
    print("Testing clock restore...")

    pr = PartitionedResolver(num_partitions=2, force_python=True)
    pr.restore_clock(100)
    assert pr.get_read_timestamp() == 100

    ok, ts = pr.commit(read_ts=100, read_keys=set(), read_ranges=[], write_keys={"x"})
    assert ok and ts == 101

    # Restoring backwards must not rewind
    pr.restore_clock(5)
    assert pr.get_read_timestamp() == 101

    print("  PASSED")


def test_partitioned_concurrent_disjoint():
    """Concurrent disjoint writers all commit with unique timestamps."""
    print("Testing concurrent disjoint commits...")

    pr = PartitionedResolver(num_partitions=4, force_python=True)

    def writer(i):
        return pr.commit(read_ts=0, read_keys=set(), read_ranges=[],
                         write_keys={f"w{i}"})

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(writer, range(100)))

    assert all(ok for ok, _ in results), "All disjoint writes should commit"
    timestamps = [ts for _, ts in results]
    assert len(set(timestamps)) == 100, "Commit timestamps must be unique"
    assert sorted(timestamps) == list(range(1, 101)), "Clock must not skip"

    print("  PASSED")


def test_native_mode_default():
    """Without force_python, native mode is used iff the extension exists."""
    print("Testing native mode selection...")

    pr = PartitionedResolver(num_partitions=4)
    assert pr.native_mode == _HAS_NATIVE

    if pr.native_mode:
        ok, ts = pr.commit(read_ts=0, read_keys=set(), read_ranges=[], write_keys={"n"})
        assert ok and ts > 0, "Native commit should return a real timestamp"

    print("  PASSED")


def run_all_tests():
    print("=" * 60)
    print("Resolver Test Suite")
    print("=" * 60)
    print()

    tests = [
        test_py_resolver_basic,
        test_partitioned_commit_ts_nonzero,
        test_partitioned_conflict_detection,
        test_partitioned_range_conflict,
        test_partitioned_read_only,
        test_partitioned_restore_clock,
        test_partitioned_concurrent_disjoint,
        test_native_mode_default,
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
