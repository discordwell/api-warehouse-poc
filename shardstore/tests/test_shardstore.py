#!/usr/bin/env python3
"""
ShardStore Test Suite
"""

import sys
import time
import threading
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from shardstore import ShardClient, Cluster, ClusterConfig, NodeConfig, ConsistencyLevel
from shardstore.hash_ring import HashRing
from shardstore.vector_clock import VectorClock, VersionedValue, resolve_conflicts, ClockComparison


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, name):
        self.passed += 1
        print(f"  ✓ {name}")

    def fail(self, name, reason):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  ✗ {name}: {reason}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed")
        if self.errors:
            print("\nFailures:")
            for name, reason in self.errors:
                print(f"  - {name}: {reason}")
        return self.failed == 0


results = TestResult()


def test_hash_ring():
    """Test consistent hashing ring."""
    print("\n[HashRing Tests]")

    ring = HashRing(virtual_nodes_per_node=100)

    # Test adding nodes
    ring.add_node("node-0")
    ring.add_node("node-1")
    ring.add_node("node-2")

    if len(ring) == 3:
        results.ok("add_node: 3 nodes added")
    else:
        results.fail("add_node", f"expected 3, got {len(ring)}")

    # Test key routing
    node = ring.get_node("test-key")
    if node in ["node-0", "node-1", "node-2"]:
        results.ok(f"get_node: routed to {node}")
    else:
        results.fail("get_node", f"invalid node {node}")

    # Test consistency - same key always goes to same node
    node1 = ring.get_node("consistent-key")
    node2 = ring.get_node("consistent-key")
    if node1 == node2:
        results.ok("consistency: same key -> same node")
    else:
        results.fail("consistency", f"{node1} != {node2}")

    # Test replication nodes
    nodes = ring.get_nodes("replica-key", 3)
    if len(nodes) == 3 and len(set(nodes)) == 3:
        results.ok(f"get_nodes: got 3 distinct nodes {nodes}")
    else:
        results.fail("get_nodes", f"expected 3 distinct, got {nodes}")

    # Test node removal
    ring.remove_node("node-1")
    if len(ring) == 2 and "node-1" not in ring:
        results.ok("remove_node: node-1 removed")
    else:
        results.fail("remove_node", "node-1 still present")

    # Test distribution fairness
    ring2 = HashRing(virtual_nodes_per_node=150)
    for i in range(5):
        ring2.add_node(f"n{i}")

    counts = {f"n{i}": 0 for i in range(5)}
    for i in range(10000):
        node = ring2.get_node(f"key-{i}")
        counts[node] += 1

    min_count = min(counts.values())
    max_count = max(counts.values())
    variance = (max_count - min_count) / 10000 * 100

    if variance < 15:  # Less than 15% variance
        results.ok(f"distribution: {variance:.1f}% variance (counts: {list(counts.values())})")
    else:
        results.fail("distribution", f"{variance:.1f}% variance too high")


def test_vector_clock():
    """Test vector clocks and conflict detection."""
    print("\n[VectorClock Tests]")

    # Test increment
    clock = VectorClock()
    clock2 = clock.increment("node-0")

    if clock2.counters.get("node-0") == 1:
        results.ok("increment: counter increased")
    else:
        results.fail("increment", f"expected 1, got {clock2.counters}")

    # Test comparison - AFTER
    clock_a = VectorClock(counters={"n0": 1})
    clock_b = VectorClock(counters={"n0": 2})

    if clock_b.compare(clock_a) == ClockComparison.AFTER:
        results.ok("compare: b AFTER a")
    else:
        results.fail("compare AFTER", f"got {clock_b.compare(clock_a)}")

    # Test comparison - BEFORE
    if clock_a.compare(clock_b) == ClockComparison.BEFORE:
        results.ok("compare: a BEFORE b")
    else:
        results.fail("compare BEFORE", f"got {clock_a.compare(clock_b)}")

    # Test comparison - CONCURRENT
    clock_x = VectorClock(counters={"n0": 1, "n1": 0})
    clock_y = VectorClock(counters={"n0": 0, "n1": 1})

    if clock_x.compare(clock_y) == ClockComparison.CONCURRENT:
        results.ok("compare: concurrent clocks detected")
    else:
        results.fail("compare CONCURRENT", f"got {clock_x.compare(clock_y)}")

    # Test merge
    merged = clock_x.merge(clock_y)
    if merged.counters == {"n0": 1, "n1": 1}:
        results.ok(f"merge: {merged.counters}")
    else:
        results.fail("merge", f"expected n0:1,n1:1, got {merged.counters}")

    # Test conflict resolution
    v1 = VersionedValue(value="first", clock=VectorClock(counters={"n0": 1}, timestamp=1.0))
    v2 = VersionedValue(value="second", clock=VectorClock(counters={"n1": 1}, timestamp=2.0))

    winner, had_conflict = resolve_conflicts([v1, v2])
    if had_conflict and winner.value == "second":
        results.ok("resolve_conflicts: LWW picked later timestamp")
    else:
        results.fail("resolve_conflicts", f"conflict={had_conflict}, winner={winner.value}")


def test_basic_crud():
    """Test basic CRUD operations."""
    print("\n[Basic CRUD Tests]")

    client = ShardClient.create_local_cluster(nodes=3, replication_factor=3)

    try:
        # PUT
        success = client.put("test:1", {"name": "Alice"})
        if success:
            results.ok("put: write succeeded")
        else:
            results.fail("put", "write failed")

        # GET
        value = client.get("test:1")
        if value == {"name": "Alice"}:
            results.ok("get: read correct value")
        else:
            results.fail("get", f"expected Alice, got {value}")

        # UPDATE
        client.put("test:1", {"name": "Bob"})
        value = client.get("test:1")
        if value == {"name": "Bob"}:
            results.ok("update: value updated")
        else:
            results.fail("update", f"expected Bob, got {value}")

        # DELETE
        client.delete("test:1")
        value = client.get("test:1")
        if value is None:
            results.ok("delete: value removed")
        else:
            results.fail("delete", f"expected None, got {value}")

        # GET non-existent
        value = client.get("does-not-exist")
        if value is None:
            results.ok("get_missing: returns None")
        else:
            results.fail("get_missing", f"expected None, got {value}")

        # EXISTS
        client.put("exists:1", "yes")
        if client.exists("exists:1") and not client.exists("nope"):
            results.ok("exists: correct behavior")
        else:
            results.fail("exists", "incorrect behavior")

    finally:
        client.shutdown()


def test_replication():
    """Test data replication across nodes."""
    print("\n[Replication Tests]")

    config = ClusterConfig(replication_factor=3)
    cluster = Cluster(config)

    for i in range(5):
        cluster.add_node(NodeConfig(node_id=f"node-{i}"))

    client = ShardClient(cluster)

    try:
        # Write a key
        client.put("replicated:1", "data")

        # Check which nodes have it
        replica_nodes = client.get_key_nodes("replicated:1")
        if len(replica_nodes) == 3:
            results.ok(f"replication: key on 3 nodes {replica_nodes}")
        else:
            results.fail("replication", f"expected 3 replicas, got {len(replica_nodes)}")

        # Verify each replica has the data
        found_count = 0
        for node_id in replica_nodes:
            node = cluster.get_node(node_id)
            if node:
                value, _, _ = node.get("replicated:1")
                if value == "data":
                    found_count += 1

        if found_count == 3:
            results.ok("replication: all 3 replicas have correct data")
        else:
            results.fail("replication verify", f"only {found_count}/3 have data")

    finally:
        cluster.shutdown()


def test_node_failure():
    """Test availability during node failures."""
    print("\n[Node Failure Tests]")

    config = ClusterConfig(
        replication_factor=3,
        write_consistency=ConsistencyLevel.QUORUM,
        read_consistency=ConsistencyLevel.ONE,
    )
    cluster = Cluster(config)

    for i in range(5):
        cluster.add_node(NodeConfig(node_id=f"node-{i}"))

    client = ShardClient(cluster)

    try:
        # Write with all nodes up
        client.put("survive:1", "original")

        # Remove one node
        cluster.remove_node("node-0")

        # Read should still work
        value = client.get("survive:1")
        if value == "original":
            results.ok("failure: read works after 1 node down")
        else:
            results.fail("failure read", f"expected 'original', got {value}")

        # Write should still work (quorum = 2, we have 4 nodes)
        success = client.put("survive:2", "new")
        if success:
            results.ok("failure: write works after 1 node down")
        else:
            results.fail("failure write", "write failed")

        # Remove another node
        cluster.remove_node("node-1")

        # Should still work with 3 nodes
        value = client.get("survive:2")
        if value == "new":
            results.ok("failure: read works after 2 nodes down")
        else:
            results.fail("failure 2 nodes", f"got {value}")

    finally:
        cluster.shutdown()


def test_consistency_levels():
    """Test different consistency levels."""
    print("\n[Consistency Level Tests]")

    client = ShardClient.create_local_cluster(nodes=5, replication_factor=3)

    try:
        # Write with ONE
        success = client.put_with_consistency("cl:1", "one", ConsistencyLevel.ONE)
        if success:
            results.ok("consistency ONE: write succeeded")
        else:
            results.fail("consistency ONE", "write failed")

        # Write with QUORUM
        success = client.put_with_consistency("cl:2", "quorum", ConsistencyLevel.QUORUM)
        if success:
            results.ok("consistency QUORUM: write succeeded")
        else:
            results.fail("consistency QUORUM", "write failed")

        # Write with ALL
        success = client.put_with_consistency("cl:3", "all", ConsistencyLevel.ALL)
        if success:
            results.ok("consistency ALL: write succeeded")
        else:
            results.fail("consistency ALL", "write failed")

        # Read with different levels
        v1 = client.get_with_consistency("cl:1", ConsistencyLevel.ONE)
        v2 = client.get_with_consistency("cl:2", ConsistencyLevel.QUORUM)

        if v1 == "one" and v2 == "quorum":
            results.ok("consistency reads: correct values")
        else:
            results.fail("consistency reads", f"v1={v1}, v2={v2}")

    finally:
        client.shutdown()


def test_concurrent_writes():
    """Test concurrent write handling."""
    print("\n[Concurrent Write Tests]")

    client = ShardClient.create_local_cluster(nodes=3, replication_factor=3)

    try:
        # Concurrent writes to same key from multiple threads
        key = "concurrent:1"
        write_count = 100
        written_values = []
        lock = threading.Lock()

        def writer(thread_id):
            for i in range(write_count):
                value = f"thread-{thread_id}-write-{i}"
                client.put(key, value)
                with lock:
                    written_values.append(value)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Final read should return one of the written values
        final_value = client.get(key)
        if final_value and final_value.startswith("thread-"):
            results.ok(f"concurrent: final value is valid ({final_value[:20]}...)")
        else:
            results.fail("concurrent", f"invalid final value: {final_value}")

        # Check stats for conflicts
        stats = client.get_stats()
        # May or may not have conflicts depending on timing
        results.ok(f"concurrent: completed with {stats['cluster'].get('conflicts', 0)} conflicts detected")

    finally:
        client.shutdown()


def test_batch_operations():
    """Test batch put/get operations."""
    print("\n[Batch Operation Tests]")

    client = ShardClient.create_local_cluster(nodes=3, replication_factor=3)

    try:
        # Batch put
        items = {f"batch:{i}": f"value-{i}" for i in range(50)}
        results_map = client.put_many(items)

        success_count = sum(1 for v in results_map.values() if v)
        if success_count == 50:
            results.ok("batch put: all 50 writes succeeded")
        else:
            results.fail("batch put", f"only {success_count}/50 succeeded")

        # Batch get
        keys = [f"batch:{i}" for i in range(50)]
        values = client.get_many(keys)

        if len(values) == 50:
            results.ok("batch get: all 50 keys retrieved")
        else:
            results.fail("batch get", f"only {len(values)}/50 retrieved")

        # Verify values
        correct = all(values.get(f"batch:{i}") == f"value-{i}" for i in range(50))
        if correct:
            results.ok("batch verify: all values correct")
        else:
            results.fail("batch verify", "some values incorrect")

    finally:
        client.shutdown()


def test_large_values():
    """Test with larger values."""
    print("\n[Large Value Tests]")

    client = ShardClient.create_local_cluster(nodes=3, replication_factor=3)

    try:
        # 1KB value
        small = "x" * 1024
        client.put("size:1kb", small)
        if client.get("size:1kb") == small:
            results.ok("large: 1KB value works")
        else:
            results.fail("large 1KB", "value mismatch")

        # 100KB value
        medium = "y" * (100 * 1024)
        client.put("size:100kb", medium)
        if client.get("size:100kb") == medium:
            results.ok("large: 100KB value works")
        else:
            results.fail("large 100KB", "value mismatch")

        # 1MB value
        large = "z" * (1024 * 1024)
        client.put("size:1mb", large)
        if client.get("size:1mb") == large:
            results.ok("large: 1MB value works")
        else:
            results.fail("large 1MB", "value mismatch")

        # Complex nested structure
        nested = {
            "users": [{"id": i, "name": f"user-{i}", "data": list(range(100))} for i in range(100)],
            "metadata": {"created": time.time(), "version": 1}
        }
        client.put("complex", nested)
        retrieved = client.get("complex")
        if retrieved == nested:
            results.ok("large: complex nested structure works")
        else:
            results.fail("large nested", "structure mismatch")

    finally:
        client.shutdown()


def test_stress():
    """Stress test with many operations."""
    print("\n[Stress Tests]")

    client = ShardClient.create_local_cluster(nodes=5, replication_factor=3)

    try:
        # Write 1000 keys
        start = time.time()
        for i in range(1000):
            client.put(f"stress:{i}", {"id": i, "data": "x" * 100})
        write_time = time.time() - start
        write_ops = 1000 / write_time

        if write_ops > 1000:
            results.ok(f"stress write: {write_ops:.0f} ops/sec")
        else:
            results.fail("stress write", f"only {write_ops:.0f} ops/sec")

        # Read 1000 keys
        start = time.time()
        for i in range(1000):
            client.get(f"stress:{i}")
        read_time = time.time() - start
        read_ops = 1000 / read_time

        if read_ops > 1000:
            results.ok(f"stress read: {read_ops:.0f} ops/sec")
        else:
            results.fail("stress read", f"only {read_ops:.0f} ops/sec")

        # Verify all keys readable
        missing = 0
        for i in range(1000):
            if client.get(f"stress:{i}") is None:
                missing += 1

        if missing == 0:
            results.ok("stress verify: all 1000 keys readable")
        else:
            results.fail("stress verify", f"{missing} keys missing")

    finally:
        client.shutdown()


def run_all_tests():
    print("=" * 60)
    print("ShardStore Test Suite")
    print("=" * 60)

    test_hash_ring()
    test_vector_clock()
    test_basic_crud()
    test_replication()
    test_node_failure()
    test_consistency_levels()
    test_concurrent_writes()
    test_batch_operations()
    test_large_values()
    test_stress()

    return results.summary()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
