#!/usr/bin/env python3
"""
HBDB Test Suite

Tests all components of the distributed SQL database.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from hbdb.database import HBDB
from hbdb.config import Config
from hbdb.types import Timestamp, TxnId, ReadWriteSet


def test_timestamp():
    """Test Timestamp ordering and operations."""
    print("Testing Timestamp...")

    t1 = Timestamp(physical=1000, logical=0)
    t2 = Timestamp(physical=1000, logical=1)
    t3 = Timestamp(physical=1001, logical=0)

    assert t1 < t2, "Same physical, t1 should be < t2"
    assert t2 < t3, "t2 should be < t3"
    assert t1 < t3, "t1 should be < t3"

    # Test next
    t1_next = t1.next()
    assert t1_next.logical == 1, "next() should increment logical"

    # Test advance
    advanced = t1.advance(t2)
    assert advanced > t2, "advance should produce timestamp > other"

    print("  PASSED")


def test_txn_id():
    """Test TxnId generation."""
    print("Testing TxnId...")

    id1 = TxnId.generate()
    id2 = TxnId.generate()

    assert id1 != id2, "Generated IDs should be unique"
    assert len(str(id1)) == 8, "ID should be 8 chars"

    print("  PASSED")


def test_rw_set_conflicts():
    """Test ReadWriteSet conflict detection."""
    print("Testing ReadWriteSet conflicts...")

    # Write-write conflict
    rw1 = ReadWriteSet()
    rw1.add_write("table:key1")

    rw2 = ReadWriteSet()
    rw2.add_write("table:key1")

    assert rw1.conflicts_with(rw2), "Write-write should conflict"

    # Read-write conflict
    rw3 = ReadWriteSet()
    rw3.add_read("table:key1")

    rw4 = ReadWriteSet()
    rw4.add_write("table:key1")

    assert rw3.conflicts_with(rw4), "Read-write should conflict"

    # No conflict
    rw5 = ReadWriteSet()
    rw5.add_read("table:key1")

    rw6 = ReadWriteSet()
    rw6.add_read("table:key2")

    assert not rw5.conflicts_with(rw6), "Read-read should not conflict"

    print("  PASSED")


def test_create_table():
    """Test CREATE TABLE."""
    print("Testing CREATE TABLE...")

    with HBDB() as db:
        result = db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        assert result.success, f"CREATE TABLE failed: {result.error}"

        # Duplicate should fail
        result = db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        assert not result.success, "Duplicate CREATE TABLE should fail"

    print("  PASSED")


def test_insert_select():
    """Test INSERT and SELECT."""
    print("Testing INSERT and SELECT...")

    with HBDB() as db:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        # Insert
        result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        assert result.success, f"INSERT failed: {result.error}"
        assert result.affected_rows == 1, "Should affect 1 row"

        # Select all
        result = db.execute("SELECT * FROM users")
        assert result.success, f"SELECT failed: {result.error}"
        assert len(result.rows) == 1, "Should have 1 row"
        assert result.rows[0]['name'] == 'Alice', "Name should be Alice"

        # Select specific columns
        result = db.execute("SELECT name FROM users WHERE id = 1")
        assert result.success, f"SELECT with WHERE failed: {result.error}"
        assert result.rows[0]['name'] == 'Alice'

    print("  PASSED")


def test_update():
    """Test UPDATE."""
    print("Testing UPDATE...")

    with HBDB() as db:
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, count INTEGER)")
        db.execute("INSERT INTO items (id, count) VALUES (1, 10)")

        result = db.execute("UPDATE items SET count = 20 WHERE id = 1")
        assert result.success, f"UPDATE failed: {result.error}"
        assert result.affected_rows == 1, "Should affect 1 row"

        # Verify
        result = db.execute("SELECT count FROM items WHERE id = 1")
        assert result.rows[0]['count'] == 20, "Count should be 20"

    print("  PASSED")


def test_delete():
    """Test DELETE."""
    print("Testing DELETE...")

    with HBDB() as db:
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO items (id) VALUES (1)")
        db.execute("INSERT INTO items (id) VALUES (2)")

        result = db.execute("DELETE FROM items WHERE id = 1")
        assert result.success, f"DELETE failed: {result.error}"
        assert result.affected_rows == 1, "Should affect 1 row"

        # Verify
        result = db.execute("SELECT * FROM items")
        assert len(result.rows) == 1, "Should have 1 row left"
        assert result.rows[0]['id'] == 2, "Remaining row should be id=2"

    print("  PASSED")


def test_drop_table():
    """Test DROP TABLE."""
    print("Testing DROP TABLE...")

    with HBDB() as db:
        db.execute("CREATE TABLE temp (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO temp (id) VALUES (1)")

        result = db.execute("DROP TABLE temp")
        assert result.success, f"DROP TABLE failed: {result.error}"

        # Table should be gone
        result = db.execute("SELECT * FROM temp")
        assert not result.success, "Select from dropped table should fail"

    print("  PASSED")


def test_multiple_inserts():
    """Test multiple sequential inserts."""
    print("Testing multiple inserts...")

    with HBDB() as db:
        db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)")

        for i in range(100):
            result = db.execute(f"INSERT INTO numbers (id, value) VALUES ({i}, {i * 10})")
            assert result.success, f"Insert {i} failed: {result.error}"

        result = db.execute("SELECT * FROM numbers")
        assert len(result.rows) == 100, f"Should have 100 rows, got {len(result.rows)}"

    print("  PASSED")


def test_concurrent_reads():
    """Test concurrent read operations."""
    print("Testing concurrent reads...")

    with HBDB() as db:
        db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")

        for i in range(10):
            db.execute(f"INSERT INTO data (id, value) VALUES ({i}, 'value_{i}')")

        # Concurrent reads
        def read_all():
            result = db.execute("SELECT * FROM data")
            return len(result.rows)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(read_all) for _ in range(50)]
            results = [f.result() for f in as_completed(futures)]

        assert all(r == 10 for r in results), "All reads should return 10 rows"

    print("  PASSED")


def test_concurrent_writes():
    """Test concurrent write operations (deterministic ordering)."""
    print("Testing concurrent writes...")

    with HBDB(Config(num_shards=4)) as db:
        db.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, count INTEGER)")
        db.execute("INSERT INTO counter (id, count) VALUES (1, 0)")

        # Concurrent writes to different keys (should all succeed)
        def insert_value(i):
            result = db.execute(f"INSERT INTO counter (id, count) VALUES ({100 + i}, {i})")
            return result.success

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(insert_value, i) for i in range(50)]
            results = [f.result() for f in as_completed(futures)]

        assert all(results), "All inserts should succeed"

        result = db.execute("SELECT * FROM counter")
        assert len(result.rows) == 51, f"Should have 51 rows, got {len(result.rows)}"

    print("  PASSED")


def test_sharding():
    """Test that data is distributed across shards."""
    print("Testing sharding...")

    with HBDB(Config(num_shards=4)) as db:
        db.execute("CREATE TABLE distributed (id INTEGER PRIMARY KEY)")

        for i in range(100):
            db.execute(f"INSERT INTO distributed (id) VALUES ({i})")

        stats = db.get_stats()
        shard_stats = stats['storage']['shards']

        # Check that multiple shards have data
        shards_with_data = sum(1 for s in shard_stats.values() if s['total_keys'] > 0)
        assert shards_with_data > 1, "Data should be distributed across shards"

    print("  PASSED")


def test_convenience_methods():
    """Test high-level convenience methods."""
    print("Testing convenience methods...")

    with HBDB() as db:
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")

        # insert()
        assert db.insert('items', {'id': 1, 'name': 'Apple', 'qty': 10})

        # select()
        rows = db.select('items', columns=['name', 'qty'], where={'id': 1})
        assert len(rows) == 1
        assert rows[0]['name'] == 'Apple'

        # update()
        affected = db.update('items', {'qty': 20}, {'id': 1})
        assert affected == 1

        # Verify
        rows = db.select('items', where={'id': 1})
        assert rows[0]['qty'] == 20

        # delete()
        affected = db.delete('items', {'id': 1})
        assert affected == 1

        # Verify
        rows = db.select('items')
        assert len(rows) == 0

    print("  PASSED")


def test_query_convenience():
    """Test query() method that raises on error."""
    print("Testing query() method...")

    with HBDB() as db:
        db.execute("CREATE TABLE simple (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO simple (id) VALUES (42)")

        rows = db.query("SELECT * FROM simple")
        assert len(rows) == 1
        assert rows[0]['id'] == 42

        # Error case
        try:
            db.query("SELECT * FROM nonexistent")
            assert False, "Should have raised"
        except Exception as e:
            assert "does not exist" in str(e)

    print("  PASSED")


def test_database_not_started():
    """Test error when database not started."""
    print("Testing database not started error...")

    db = HBDB()  # Don't start it

    result = db.execute("SELECT 1")
    assert not result.success
    assert "not started" in result.error.lower()

    print("  PASSED")


def test_duplicate_primary_key():
    """Test duplicate primary key error."""
    print("Testing duplicate primary key...")

    with HBDB() as db:
        db.execute("CREATE TABLE unique_test (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO unique_test (id) VALUES (1)")

        result = db.execute("INSERT INTO unique_test (id) VALUES (1)")
        assert not result.success
        assert "already exists" in result.error.lower()

    print("  PASSED")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("HBDB Test Suite")
    print("=" * 60)
    print()

    tests = [
        test_timestamp,
        test_txn_id,
        test_rw_set_conflicts,
        test_create_table,
        test_insert_select,
        test_update,
        test_delete,
        test_drop_table,
        test_multiple_inserts,
        test_concurrent_reads,
        test_concurrent_writes,
        test_sharding,
        test_convenience_methods,
        test_query_convenience,
        test_database_not_started,
        test_duplicate_primary_key,
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
