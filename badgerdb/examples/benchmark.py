#!/usr/bin/env python3
"""
BadgerDB Benchmark

Measures throughput for various operations.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from badgerdb.database import BadgerDB
from badgerdb.config import Config


def benchmark_sequential_writes(db, num_ops):
    """Benchmark sequential write operations."""
    db.execute("CREATE TABLE seq_writes (id INTEGER PRIMARY KEY, data TEXT)")

    start = time.perf_counter()
    for i in range(num_ops):
        db.execute(f"INSERT INTO seq_writes (id, data) VALUES ({i}, 'test_data_{i}')")
    elapsed = time.perf_counter() - start

    return num_ops / elapsed


def benchmark_sequential_reads(db, num_ops):
    """Benchmark sequential read operations."""
    db.execute("CREATE TABLE seq_reads (id INTEGER PRIMARY KEY, data TEXT)")

    # Pre-populate
    for i in range(100):
        db.execute(f"INSERT INTO seq_reads (id, data) VALUES ({i}, 'test_data_{i}')")

    start = time.perf_counter()
    for i in range(num_ops):
        db.execute(f"SELECT * FROM seq_reads WHERE id = {i % 100}")
    elapsed = time.perf_counter() - start

    return num_ops / elapsed


def benchmark_concurrent_writes(db, num_ops, num_workers):
    """Benchmark concurrent write operations."""
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


def benchmark_concurrent_reads(db, num_ops, num_workers):
    """Benchmark concurrent read operations."""
    db.execute("CREATE TABLE conc_reads (id INTEGER PRIMARY KEY, data TEXT)")

    # Pre-populate
    for i in range(100):
        db.execute(f"INSERT INTO conc_reads (id, data) VALUES ({i}, 'test_data_{i}')")

    def do_select(idx):
        return db.execute(f"SELECT * FROM conc_reads WHERE id = {idx % 100}").success

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(do_select, i) for i in range(num_ops)]
        results = [f.result() for f in as_completed(futures)]
    elapsed = time.perf_counter() - start

    success_count = sum(results)
    return success_count / elapsed


def benchmark_mixed_workload(db, num_ops, read_ratio=0.8):
    """Benchmark mixed read/write workload."""
    db.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY, counter INTEGER)")

    # Pre-populate
    for i in range(100):
        db.execute(f"INSERT INTO mixed (id, counter) VALUES ({i}, 0)")

    import random
    random.seed(42)

    read_count = 0
    write_count = 0

    start = time.perf_counter()
    for i in range(num_ops):
        if random.random() < read_ratio:
            db.execute(f"SELECT * FROM mixed WHERE id = {random.randint(0, 99)}")
            read_count += 1
        else:
            db.execute(f"UPDATE mixed SET counter = {i} WHERE id = {random.randint(0, 99)}")
            write_count += 1
    elapsed = time.perf_counter() - start

    return num_ops / elapsed, read_count, write_count


def main():
    print("=" * 70)
    print("BadgerDB Benchmark - Calvin-Style Deterministic Transaction Throughput")
    print("=" * 70)
    print()

    config = Config(num_shards=4)

    # Sequential writes
    print("1. Sequential Writes (1000 ops)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec = benchmark_sequential_writes(db, 1000)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Sequential reads
    print("\n2. Sequential Reads (1000 ops)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec = benchmark_sequential_reads(db, 1000)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Concurrent writes
    print("\n3. Concurrent Writes (1000 ops, 10 workers)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec = benchmark_concurrent_writes(db, 1000, 10)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Concurrent reads
    print("\n4. Concurrent Reads (1000 ops, 10 workers)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec = benchmark_concurrent_reads(db, 1000, 10)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Mixed workload
    print("\n5. Mixed Workload (80% reads, 20% writes, 1000 ops)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec, reads, writes = benchmark_mixed_workload(db, 1000, 0.8)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec ({reads} reads, {writes} writes)")

    # Larger scale
    print("\n6. Large Scale Sequential Writes (5000 ops)")
    print("-" * 50)
    with BadgerDB(config) as db:
        ops_per_sec = benchmark_sequential_writes(db, 5000)
    print(f"   Throughput: {ops_per_sec:,.0f} ops/sec")

    # Sharding efficiency
    print("\n7. Shard Distribution Analysis")
    print("-" * 50)
    with BadgerDB(Config(num_shards=8)) as db:
        db.execute("CREATE TABLE shard_test (id INTEGER PRIMARY KEY, data TEXT)")
        for i in range(1000):
            db.execute(f"INSERT INTO shard_test (id, data) VALUES ({i}, 'data')")

        stats = db.get_stats()
        shard_stats = stats['storage']['shards']

        total_keys = sum(s['total_keys'] for s in shard_stats.values())
        keys_per_shard = [s['total_keys'] for s in shard_stats.values()]
        avg = total_keys / len(shard_stats)
        variance = sum((k - avg) ** 2 for k in keys_per_shard) / len(keys_per_shard)
        std_dev = variance ** 0.5
        cv = (std_dev / avg * 100) if avg > 0 else 0

        print(f"   Total keys: {total_keys}")
        print(f"   Keys per shard: {keys_per_shard}")
        print(f"   Distribution variance: {cv:.1f}% coefficient of variation")

    print("\n" + "=" * 70)
    print("Benchmark complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
