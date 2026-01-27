#!/usr/bin/env python3
"""
BadgerDB v2 Benchmark

Measures throughput for the sophisticated distributed database.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from badgerdb.database_v2 import BadgerDBV2
from badgerdb.config import Config


def benchmark_fast_path(db, num_ops):
    """Benchmark fast-path writes (non-conflicting)."""
    db.execute("CREATE TABLE fast_path (id INTEGER PRIMARY KEY, data TEXT)")

    start = time.perf_counter()
    for i in range(num_ops):
        db.execute(f"INSERT INTO fast_path (id, data) VALUES ({i}, 'test_data_{i}')")
    elapsed = time.perf_counter() - start

    stats = db.get_stats()
    fast_path_rate = stats['coordinator']['fast_path_success_rate']

    return num_ops / elapsed, fast_path_rate


def benchmark_slow_path(db, num_ops):
    """Benchmark slow-path writes (through sequencer)."""
    db.execute("CREATE TABLE slow_path (id INTEGER PRIMARY KEY, counter INTEGER)")
    db.execute("INSERT INTO slow_path (id, counter) VALUES (1, 0)")

    # Updates to same key force slow path
    start = time.perf_counter()
    for i in range(num_ops):
        db.execute(f"UPDATE slow_path SET counter = {i} WHERE id = 1")
    elapsed = time.perf_counter() - start

    return num_ops / elapsed


def benchmark_concurrent_writes(db, num_ops, num_workers):
    """Benchmark concurrent writes to different keys."""
    db.execute("CREATE TABLE conc_writes (id INTEGER PRIMARY KEY, data TEXT)")

    counter = [0]
    lock = threading.Lock()

    def do_insert():
        with lock:
            idx = counter[0]
            counter[0] += 1
        return db.execute(f"INSERT INTO conc_writes (id, data) VALUES ({idx}, 'data_{idx}')").success

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(do_insert) for _ in range(num_ops)]
        results = [f.result() for f in as_completed(futures)]
    elapsed = time.perf_counter() - start

    success_count = sum(results)
    return success_count / elapsed


def benchmark_reads(db, num_ops):
    """Benchmark read operations."""
    db.execute("CREATE TABLE reads (id INTEGER PRIMARY KEY, data TEXT)")

    for i in range(100):
        db.execute(f"INSERT INTO reads (id, data) VALUES ({i}, 'test_data_{i}')")

    start = time.perf_counter()
    for i in range(num_ops):
        db.execute(f"SELECT * FROM reads WHERE id = {i % 100}")
    elapsed = time.perf_counter() - start

    return num_ops / elapsed


def main():
    print("=" * 70)
    print("BadgerDB v2 Benchmark - Sophisticated Distributed SQL")
    print("=" * 70)
    print()
    print("Features:")
    print("  - Parallel sequencers (BOHM-style)")
    print("  - Aria speculative execution")
    print("  - Detock fast-path optimization")
    print("  - Aurora-style disaggregated storage")
    print()

    config = Config(num_shards=4)

    # Fast path benchmark
    print("1. Fast Path Writes (1000 ops)")
    print("-" * 50)
    with BadgerDBV2(config) as db:
        ops_per_sec, fast_path_rate = benchmark_fast_path(db, 1000)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")
    print(f"   Fast path rate: {fast_path_rate:.1%}")

    # Slow path benchmark
    print("\n2. Slow Path Updates (100 ops, same key)")
    print("-" * 50)
    with BadgerDBV2(config) as db:
        ops_per_sec = benchmark_slow_path(db, 100)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Concurrent writes
    print("\n3. Concurrent Writes (1000 ops, 10 workers)")
    print("-" * 50)
    with BadgerDBV2(config) as db:
        ops_per_sec = benchmark_concurrent_writes(db, 1000, 10)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Reads
    print("\n4. Sequential Reads (1000 ops)")
    print("-" * 50)
    with BadgerDBV2(config) as db:
        ops_per_sec = benchmark_reads(db, 1000)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Stats
    print("\n5. System Statistics")
    print("-" * 50)
    with BadgerDBV2(config) as db:
        db.execute("CREATE TABLE stats_test (id INTEGER PRIMARY KEY)")
        for i in range(100):
            db.execute(f"INSERT INTO stats_test (id) VALUES ({i})")

        stats = db.get_stats()
        coord = stats['coordinator']

        print(f"   Transactions: {coord['transactions_submitted']}")
        print(f"   Fast path attempts: {coord['fast_path_attempts']}")
        print(f"   Fast path success rate: {coord['fast_path_success_rate']:.1%}")
        print(f"   Slow path executions: {coord['slow_path_executions']}")

        exec_stats = coord['executor']
        print(f"   Epochs executed: {exec_stats['epochs_executed']}")
        print(f"   Speculative success rate: {exec_stats['speculative_success_rate']:.1%}")

        storage = coord['storage']['storage']
        print(f"   Storage servers: {storage['num_servers']}")
        print(f"   Total pages: {storage['total_pages']}")

    print("\n" + "=" * 70)
    print("Benchmark complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
