# ShardStore

A distributed key-value store with eventual consistency, inspired by Amazon Dynamo and Apache Cassandra.

## Features

- **Consistent Hashing**: Keys distributed across nodes with minimal redistribution on topology changes
- **Replication**: Configurable replication factor (N copies of each key)
- **Tunable Consistency**: Choose between ONE, QUORUM, or ALL for reads/writes
- **Vector Clocks**: Causality tracking and conflict detection
- **Last-Write-Wins**: Automatic conflict resolution with timestamp tiebreaker
- **Read Repair**: Anti-entropy mechanism to fix stale replicas on read
- **Hinted Handoff**: Store writes for temporarily unavailable nodes
- **Gossip Protocol**: SWIM-style failure detection

## CAP Tradeoffs

ShardStore is an **AP system** (Availability + Partition Tolerance) with eventual consistency:

| Setting | Behavior |
|---------|----------|
| `write=ONE, read=ONE` | Highest availability, may read stale |
| `write=QUORUM, read=QUORUM` | Strong consistency (R+W > N) |
| `write=ALL, read=ONE` | Highest durability, lower write availability |

## Quick Start

```python
from shardstore import ShardClient

# Create a 5-node cluster with replication factor 3
client = ShardClient.create_local_cluster(nodes=5, replication_factor=3)

# Basic operations
client.put("user:123", {"name": "Alice", "age": 30})
user = client.get("user:123")
client.delete("user:123")

# Check key distribution
nodes = client.get_key_nodes("user:123")  # ["node-2", "node-4", "node-0"]

# Batch operations
client.put_many({"a": 1, "b": 2, "c": 3})
values = client.get_many(["a", "b", "c"])

# Explicit consistency
from shardstore.config import ConsistencyLevel
client.put_with_consistency("critical", data, ConsistencyLevel.ALL)
value = client.get_with_consistency("critical", ConsistencyLevel.QUORUM)

# Cleanup
client.shutdown()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        ShardClient                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         Cluster                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  HashRing   │  │   Gossip    │  │  Quorum     │         │
│  │ (routing)   │  │ (failure    │  │ (consistency)│         │
│  │             │  │  detection) │  │             │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │  Node 0  │        │  Node 1  │        │  Node 2  │
    │┌────────┐│        │┌────────┐│        │┌────────┐│
    ││ Vector ││        ││ Vector ││        ││ Vector ││
    ││ Clocks ││        ││ Clocks ││        ││ Clocks ││
    │└────────┘│        │└────────┘│        │└────────┘│
    │┌────────┐│        │┌────────┐│        │┌────────┐│
    ││ Storage││        ││ Storage││        ││ Storage││
    │└────────┘│        │└────────┘│        │└────────┘│
    └──────────┘        └──────────┘        └──────────┘
```

## Configuration

```python
from shardstore import ClusterConfig
from shardstore.config import ConsistencyLevel

config = ClusterConfig(
    # Replication
    replication_factor=3,              # Copies per key

    # Consistency
    write_consistency=ConsistencyLevel.QUORUM,
    read_consistency=ConsistencyLevel.ONE,

    # Timeouts
    write_timeout=5.0,
    read_timeout=2.0,

    # Failure detection
    gossip_interval=1.0,
    failure_threshold=10.0,

    # Anti-entropy
    read_repair=True,
    hinted_handoff=True,
)
```

## Run Demo

```bash
cd examples
python demo.py
```

## How It Works

### Consistent Hashing

Keys are hashed to a position on a ring (0 to 2^32). Each node owns multiple positions (virtual nodes) for better distribution. A key is stored on the N nodes clockwise from its hash position.

### Vector Clocks

Each write increments the writing node's counter in the vector clock. When reading, clocks are compared:
- One dominates → use it
- Concurrent (neither dominates) → conflict, resolve with timestamp

### Quorum

With N replicas:
- `QUORUM = floor(N/2) + 1`
- If `R + W > N`, you get strong consistency
- Lower values trade consistency for availability

### Read Repair

When a read returns different versions from replicas, the coordinator sends the winning version back to stale replicas, healing inconsistencies in the background.

## Limitations (POC)

- Single-process only (nodes are threads, not separate processes)
- No network layer (would need gRPC/HTTP for real distribution)
- No persistent WAL (just JSON snapshots)
- No compaction for tombstones
- No range queries or secondary indexes

## Production Alternatives

For production use cases, consider:
- **Apache Cassandra**: Full-featured wide-column store
- **Amazon DynamoDB**: Managed, serverless
- **ScyllaDB**: Cassandra-compatible, C++ performance
- **Riak**: Dynamo-style, Erlang
- **FoundationDB**: Ordered KV with ACID transactions
