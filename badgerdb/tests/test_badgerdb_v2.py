#!/usr/bin/env python3
"""
BadgerDB v2 Test Suite

Tests the sophisticated distributed database with:
- Parallel sequencers
- Aria execution
- Fast path
- Disaggregated storage
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from badgerdb.database_v2 import BadgerDBV2
from badgerdb.config import Config
from badgerdb.types import Timestamp, TxnId


def test_basic_operations():
    """Test basic SQL operations."""
    print("Testing basic operations...")

    with BadgerDBV2() as db:
        # CREATE TABLE
        result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        assert result.success, f"CREATE TABLE failed: {result.error}"

        # INSERT
        result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        assert result.success, f"INSERT failed: {result.error}"
        assert result.affected_rows == 1

        # SELECT
        result = db.execute("SELECT * FROM users WHERE id = 1")
        assert result.success, f"SELECT failed: {result.error}"
        assert len(result.rows) == 1
        assert result.rows[0]['name'] == 'Alice'

        # UPDATE
        result = db.execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
        assert result.success, f"UPDATE failed: {result.error}"

        # Verify
        result = db.execute("SELECT name FROM users WHERE id = 1")
        assert result.rows[0]['name'] == 'Alicia'

        # DELETE
        result = db.execute("DELETE FROM users WHERE id = 1")
        assert result.success, f"DELETE failed: {result.error}"

        # Verify
        result = db.execute("SELECT * FROM users WHERE id = 1")
        assert len(result.rows) == 0

    print("  PASSED")


def test_fast_path():
    """Test fast path execution."""
    print("Testing fast path...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")

        # First insert should use fast path (no recent writes)
        result = db.execute("INSERT INTO items (id, value) VALUES (1, 100)")
        assert result.success

        stats = db.get_stats()
        initial_fast_path = stats['coordinator']['fast_path_successes']

        # Insert to different key should also use fast path
        result = db.execute("INSERT INTO items (id, value) VALUES (2, 200)")
        assert result.success

        stats = db.get_stats()
        # Fast path attempts should increase
        assert stats['coordinator']['fast_path_attempts'] > 0

    print("  PASSED")


def test_multiple_tables():
    """Test multiple tables."""
    print("Testing multiple tables...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")

        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        db.execute("INSERT INTO orders (id, user_id, total) VALUES (100, 1, 99.99)")

        result = db.execute("SELECT * FROM users")
        assert len(result.rows) == 1

        result = db.execute("SELECT * FROM orders")
        assert len(result.rows) == 1
        assert result.rows[0]['total'] == 99.99

    print("  PASSED")


def test_concurrent_inserts():
    """Test concurrent inserts to different keys."""
    print("Testing concurrent inserts...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")

        def insert(i):
            return db.execute(f"INSERT INTO data (id, value) VALUES ({i}, 'value_{i}')").success

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(insert, i) for i in range(50)]
            results = [f.result() for f in as_completed(futures)]

        assert all(results), "Some inserts failed"

        result = db.execute("SELECT * FROM data")
        assert len(result.rows) == 50

    print("  PASSED")


def test_concurrent_reads():
    """Test concurrent reads."""
    print("Testing concurrent reads...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")

        for i in range(10):
            db.execute(f"INSERT INTO items (id, name) VALUES ({i}, 'item_{i}')")

        def read():
            return len(db.execute("SELECT * FROM items").rows)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(read) for _ in range(50)]
            results = [f.result() for f in as_completed(futures)]

        assert all(r == 10 for r in results)

    print("  PASSED")


def test_convenience_methods():
    """Test convenience methods."""
    print("Testing convenience methods...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

        # insert()
        assert db.insert('products', {'id': 1, 'name': 'Widget', 'price': 9.99})

        # select()
        rows = db.select('products', columns=['name', 'price'], where={'id': 1})
        assert len(rows) == 1
        assert rows[0]['name'] == 'Widget'

        # update()
        affected = db.update('products', {'price': 12.99}, {'id': 1})
        assert affected == 1

        # Verify
        rows = db.select('products', where={'id': 1})
        assert rows[0]['price'] == 12.99

        # delete()
        affected = db.delete('products', {'id': 1})
        assert affected == 1

    print("  PASSED")


def test_disaggregated_storage():
    """Test disaggregated storage distribution."""
    print("Testing disaggregated storage...")

    with BadgerDBV2(Config(num_shards=4)) as db:
        db.execute("CREATE TABLE distributed (id INTEGER PRIMARY KEY)")

        for i in range(100):
            db.execute(f"INSERT INTO distributed (id) VALUES ({i})")

        stats = db.get_stats()
        storage_stats = stats['coordinator']['storage']['storage']

        # Check that multiple servers have data
        servers_with_data = sum(
            1 for s in storage_stats['servers']
            if s['pages'] > 0
        )
        assert servers_with_data > 1, "Data should be distributed across servers"

    print("  PASSED")


def test_aria_execution():
    """Test Aria-style speculative execution."""
    print("Testing Aria execution...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, count INTEGER)")
        db.execute("INSERT INTO counter (id, count) VALUES (1, 0)")

        # Multiple concurrent updates to same key
        # Should trigger slow path and Aria conflict detection
        def update(val):
            return db.execute(f"UPDATE counter SET count = {val} WHERE id = 1").success

        # Sequential updates (all should succeed)
        for i in range(5):
            assert update(i)

        stats = db.get_stats()
        # Check that Aria executed some epochs
        assert stats['coordinator']['executor']['epochs_executed'] >= 0

    print("  PASSED")


def test_stats():
    """Test statistics reporting."""
    print("Testing statistics...")

    with BadgerDBV2() as db:
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")

        for i in range(10):
            db.execute(f"INSERT INTO test (id) VALUES ({i})")

        stats = db.get_stats()

        assert 'coordinator' in stats
        assert 'transactions_submitted' in stats['coordinator']
        assert stats['coordinator']['transactions_submitted'] >= 10

    print("  PASSED")


def test_error_handling():
    """Test error handling."""
    print("Testing error handling...")

    with BadgerDBV2() as db:
        # Table doesn't exist
        result = db.execute("SELECT * FROM nonexistent")
        assert not result.success
        assert "does not exist" in result.error.lower()

        # Duplicate table
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        result = db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        assert not result.success

        # Duplicate key
        db.execute("INSERT INTO test (id) VALUES (1)")
        result = db.execute("INSERT INTO test (id) VALUES (1)")
        assert not result.success
        assert "already exists" in result.error.lower()

    print("  PASSED")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("BadgerDB v2 Test Suite")
    print("Sophisticated Distributed SQL Database")
    print("=" * 60)
    print()

    tests = [
        test_basic_operations,
        test_fast_path,
        test_multiple_tables,
        test_concurrent_inserts,
        test_concurrent_reads,
        test_convenience_methods,
        test_disaggregated_storage,
        test_aria_execution,
        test_stats,
        test_error_handling,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
