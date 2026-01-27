#!/usr/bin/env python3
"""
ShardStore Demo

Demonstrates the distributed key-value store with:
- Basic CRUD operations
- Replication across nodes
- Node failure handling
- Conflict resolution
"""

import sys
import time
import random
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from shardstore import ShardClient, ClusterConfig, Cluster, NodeConfig
from shardstore.config import ConsistencyLevel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def demo_basic_operations():
    """Basic CRUD operations."""
    print("\n" + "=" * 60)
    print("DEMO 1: Basic Operations")
    print("=" * 60)

    with ShardClient.create_local_cluster(nodes=5, replication_factor=3) as client:
        # Write
        print("\n[1] Writing key 'user:1'...")
        success = client.put("user:1", {"name": "Alice", "age": 30})
        print(f"    Write success: {success}")

        # Read
        print("\n[2] Reading key 'user:1'...")
        value = client.get("user:1")
        print(f"    Value: {value}")

        # Show which nodes have the key
        nodes = client.get_key_nodes("user:1")
        print(f"    Replicated to nodes: {nodes}")

        # Update
        print("\n[3] Updating key 'user:1'...")
        client.put("user:1", {"name": "Alice", "age": 31})
        value = client.get("user:1")
        print(f"    Updated value: {value}")

        # Delete
        print("\n[4] Deleting key 'user:1'...")
        client.delete("user:1")
        value = client.get("user:1")
        print(f"    After delete: {value}")

        # Stats
        print("\n[5] Cluster stats:")
        stats = client.get_stats()
        print(f"    Reads: {stats['cluster']['reads']}")
        print(f"    Writes: {stats['cluster']['writes']}")


def demo_distribution():
    """Show how keys are distributed across nodes."""
    print("\n" + "=" * 60)
    print("DEMO 2: Key Distribution")
    print("=" * 60)

    with ShardClient.create_local_cluster(nodes=5, replication_factor=3) as client:
        # Write 100 keys
        print("\n[1] Writing 100 keys...")
        for i in range(100):
            client.put(f"key:{i}", f"value-{i}")

        # Check distribution
        print("\n[2] Key distribution across nodes:")
        stats = client.get_stats()
        for node_id, node_stats in stats['nodes'].items():
            print(f"    {node_id}: {node_stats['keys']} keys")

        # Show specific key locations
        print("\n[3] Sample key locations:")
        for i in [0, 25, 50, 75, 99]:
            nodes = client.get_key_nodes(f"key:{i}")
            print(f"    key:{i} -> {nodes}")


def demo_node_failure():
    """Demonstrate availability during node failure."""
    print("\n" + "=" * 60)
    print("DEMO 3: Node Failure Handling")
    print("=" * 60)

    config = ClusterConfig(
        replication_factor=3,
        write_consistency=ConsistencyLevel.QUORUM,  # Need 2/3
        read_consistency=ConsistencyLevel.ONE,       # Need 1/3
    )
    cluster = Cluster(config)

    # Add nodes
    for i in range(5):
        cluster.add_node(NodeConfig(node_id=f"node-{i}"))

    client = ShardClient(cluster)

    try:
        # Write some data
        print("\n[1] Writing data with all nodes up...")
        client.put("critical:1", "important data")
        print(f"    Nodes for 'critical:1': {client.get_key_nodes('critical:1')}")

        # Simulate node failure
        print("\n[2] Simulating node failure (removing node-0)...")
        cluster.remove_node("node-0")
        print(f"    Active nodes: {client.get_nodes()}")

        # Read should still work (consistency=ONE)
        print("\n[3] Reading data after node failure...")
        value = client.get("critical:1")
        print(f"    Value: {value}")
        print(f"    Read succeeded: {value is not None}")

        # Write should still work (QUORUM = 2, we have 4 nodes)
        print("\n[4] Writing new data after node failure...")
        success = client.put("critical:2", "new data")
        print(f"    Write success: {success}")

        # Remove another node
        print("\n[5] Removing another node (node-1)...")
        cluster.remove_node("node-1")
        print(f"    Active nodes: {client.get_nodes()}")

        # Still works with 3 nodes
        value = client.get("critical:2")
        print(f"    Read 'critical:2': {value}")

    finally:
        cluster.shutdown()


def demo_consistency_levels():
    """Show different consistency levels."""
    print("\n" + "=" * 60)
    print("DEMO 4: Consistency Levels")
    print("=" * 60)

    with ShardClient.create_local_cluster(nodes=5, replication_factor=3) as client:
        # Write with QUORUM (default)
        print("\n[1] Write with QUORUM consistency...")
        success = client.put_with_consistency(
            "data:1", "quorum write",
            ConsistencyLevel.QUORUM
        )
        print(f"    Success: {success} (required 2/3 nodes)")

        # Write with ALL
        print("\n[2] Write with ALL consistency...")
        success = client.put_with_consistency(
            "data:2", "all write",
            ConsistencyLevel.ALL
        )
        print(f"    Success: {success} (required 3/3 nodes)")

        # Write with ONE
        print("\n[3] Write with ONE consistency...")
        success = client.put_with_consistency(
            "data:3", "one write",
            ConsistencyLevel.ONE
        )
        print(f"    Success: {success} (required 1/3 nodes)")

        # Read comparisons
        print("\n[4] Read consistency comparison:")

        # ONE - fastest, may read stale
        start = time.time()
        for _ in range(100):
            client.get_with_consistency("data:1", ConsistencyLevel.ONE)
        one_time = time.time() - start

        # QUORUM - balanced
        start = time.time()
        for _ in range(100):
            client.get_with_consistency("data:1", ConsistencyLevel.QUORUM)
        quorum_time = time.time() - start

        print(f"    ONE (100 reads):    {one_time*1000:.1f}ms")
        print(f"    QUORUM (100 reads): {quorum_time*1000:.1f}ms")


def demo_concurrent_writes():
    """Demonstrate conflict detection and resolution."""
    print("\n" + "=" * 60)
    print("DEMO 5: Concurrent Writes & Conflict Resolution")
    print("=" * 60)

    config = ClusterConfig(replication_factor=3)
    cluster = Cluster(config)

    for i in range(3):
        cluster.add_node(NodeConfig(node_id=f"node-{i}"))

    try:
        # Write initial value
        print("\n[1] Initial write...")
        result = cluster.put("counter", 0)
        print(f"    Written: 0, clock: {result.clock.counters if result.clock else 'N/A'}")

        # Simulate concurrent writes by writing directly to nodes
        print("\n[2] Simulating concurrent writes to different nodes...")

        node0 = cluster.get_node("node-0")
        node1 = cluster.get_node("node-1")

        # Both nodes write without seeing each other's update
        clock0 = node0.put("conflict-key", "value from node-0")
        clock1 = node1.put("conflict-key", "value from node-1")

        print(f"    Node-0 wrote: 'value from node-0' (clock: {clock0.counters})")
        print(f"    Node-1 wrote: 'value from node-1' (clock: {clock1.counters})")

        # Read - will detect conflict and resolve
        print("\n[3] Reading key (will resolve conflict)...")
        result = cluster.get("conflict-key")
        print(f"    Resolved value: {result.value}")
        print(f"    Had conflict: {result.had_conflict}")
        print(f"    Read repaired: {result.repaired}")

    finally:
        cluster.shutdown()


def demo_benchmark():
    """Simple performance benchmark."""
    print("\n" + "=" * 60)
    print("DEMO 6: Performance Benchmark")
    print("=" * 60)

    with ShardClient.create_local_cluster(nodes=5, replication_factor=3) as client:
        # Write benchmark
        print("\n[1] Write benchmark (1000 keys)...")
        start = time.time()
        for i in range(1000):
            client.put(f"bench:{i}", {"id": i, "data": "x" * 100})
        write_time = time.time() - start
        write_ops = 1000 / write_time

        print(f"    Time: {write_time:.2f}s")
        print(f"    Throughput: {write_ops:.0f} ops/sec")

        # Read benchmark
        print("\n[2] Read benchmark (1000 keys)...")
        start = time.time()
        for i in range(1000):
            client.get(f"bench:{i}")
        read_time = time.time() - start
        read_ops = 1000 / read_time

        print(f"    Time: {read_time:.2f}s")
        print(f"    Throughput: {read_ops:.0f} ops/sec")

        # Mixed workload
        print("\n[3] Mixed workload (80% read, 20% write)...")
        start = time.time()
        for i in range(1000):
            if random.random() < 0.8:
                client.get(f"bench:{random.randint(0, 999)}")
            else:
                client.put(f"bench:{random.randint(0, 999)}", {"updated": True})
        mixed_time = time.time() - start
        mixed_ops = 1000 / mixed_time

        print(f"    Time: {mixed_time:.2f}s")
        print(f"    Throughput: {mixed_ops:.0f} ops/sec")

        # Final stats
        print("\n[4] Final cluster stats:")
        stats = client.get_stats()
        print(f"    Total reads: {stats['cluster']['reads']}")
        print(f"    Total writes: {stats['cluster']['writes']}")
        print(f"    Read repairs: {stats['cluster']['read_repairs']}")


if __name__ == "__main__":
    demo_basic_operations()
    demo_distribution()
    demo_node_failure()
    demo_consistency_levels()
    demo_concurrent_writes()
    demo_benchmark()

    print("\n" + "=" * 60)
    print("All demos complete!")
    print("=" * 60)
