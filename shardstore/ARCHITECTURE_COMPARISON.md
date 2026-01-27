# Distributed Database Architectures: A Comparison

## The CAP Theorem (Quick Primer)

You can only pick 2 of 3:

```
                    Consistency
                        △
                       /|\
                      / | \
                     /  |  \
                    /   |   \
                   / CP | CA \
                  /     |     \
                 /______|______\
        Partition             Availability
        Tolerance      AP
```

| Type | Guarantees | Sacrifices | Examples |
|------|-----------|------------|----------|
| **CP** | Consistency + Partition Tolerance | Availability (blocks during partitions) | MongoDB, HBase, Spanner |
| **AP** | Availability + Partition Tolerance | Consistency (may read stale) | Cassandra, DynamoDB, **ShardStore** |
| **CA** | Consistency + Availability | Partition Tolerance (single node only) | PostgreSQL, MySQL (non-clustered) |

**ShardStore is AP** - it stays available during network partitions but may return stale data.

---

## Architecture Comparison

### 1. Single-Leader Replication (PostgreSQL, MySQL)

```
   Writes ──→ [Leader] ──→ Replicas
                 │            ↑
                 └── sync ────┘

   Reads ←── [Leader or Replica]
```

**How it works:**
- One node accepts all writes
- Replicates to followers (sync or async)
- Reads can go to leader (strong) or replicas (eventual)

**Pros:**
- Simple mental model
- Strong consistency easy
- Transactions!

**Cons:**
- Leader is bottleneck
- Failover is complex (split-brain risk)
- Writes don't scale horizontally

**ShardStore difference:** No single leader. Any node can accept writes for keys it owns.

---

### 2. Multi-Leader Replication (CockroachDB, TiDB)

```
   [Leader A] ←──────→ [Leader B] ←──────→ [Leader C]
       ↓                   ↓                   ↓
   [Replica]           [Replica]           [Replica]
```

**How it works:**
- Multiple nodes accept writes
- Leaders sync with each other
- Conflict resolution needed

**Pros:**
- Better write throughput
- Geographic distribution
- Tolerates datacenter failures

**Cons:**
- Conflict resolution is hard
- More complex than single-leader
- Potential for write conflicts

**ShardStore difference:** Similar concept, but ShardStore uses consistent hashing to assign key ownership rather than having general-purpose leaders.

---

### 3. Leaderless / Dynamo-Style (Cassandra, DynamoDB, Riak, **ShardStore**)

```
        ┌─────────────────────────────────────┐
        │           Hash Ring                 │
        │                                     │
        │    [Node A]───[Node B]───[Node C]   │
        │        \         |         /        │
        │         \        |        /         │
        │          [Node D]───[Node E]        │
        │                                     │
        └─────────────────────────────────────┘

   Write "user:123" → Hash → Nodes [B, C, D] (RF=3)
```

**How it works:**
- No leader - any node can coordinate
- Consistent hashing determines key placement
- Replication to N nodes clockwise on ring
- Quorum reads/writes for consistency tuning

**Pros:**
- Highly available (no single point of failure)
- Linear write scalability
- Tunable consistency

**Cons:**
- Eventual consistency by default
- Conflict resolution needed
- No multi-key transactions

**This is ShardStore's architecture.**

---

### 4. Consensus-Based (etcd, ZooKeeper, Raft-based)

```
   Client ──→ [Leader]
                 │
         ┌──────┼──────┐
         ↓      ↓      ↓
     [Follower][Follower][Follower]
         │      │      │
         └──────┴──────┘
              Raft/Paxos
              Consensus
```

**How it works:**
- Leader elected via consensus protocol
- All writes go through leader
- Majority must agree before commit
- Strong consistency guaranteed

**Pros:**
- Strong consistency
- Linearizable reads available
- Well-understood failure modes

**Cons:**
- Limited write throughput (consensus overhead)
- Typically 3-7 nodes max
- Not designed for large datasets

**ShardStore difference:** No consensus protocol. Uses vector clocks + last-write-wins instead of agreement.

---

### 5. NewSQL / Calvin-Style (FaunaDB, Calvin)

```
   [Sequencer] ──→ Assigns global order to transactions
        │
        ↓
   [Node A] [Node B] [Node C]  ← All execute in same order
```

**How it works:**
- Deterministic transaction ordering
- All nodes execute same operations in same order
- No coordination needed during execution

**Pros:**
- Strong consistency
- Good write scalability
- ACID transactions

**Cons:**
- Complex implementation
- Requires deterministic execution
- Sequencer can be bottleneck

**ShardStore difference:** No transaction ordering. Each key is independent.

---

## Feature Comparison Table

| Feature | ShardStore | Cassandra | DynamoDB | MongoDB | PostgreSQL |
|---------|------------|-----------|----------|---------|------------|
| **Architecture** | Leaderless | Leaderless | Leaderless | Single-leader | Single-leader |
| **Consistency** | Eventual | Eventual | Eventual | Strong | Strong |
| **Partition Tolerance** | ✓ | ✓ | ✓ | ✓ | ✗ |
| **Transactions** | ✗ | Limited | Limited | ✓ | ✓ |
| **Data Model** | KV | Wide-column | KV/Doc | Document | Relational |
| **Query Language** | API | CQL | API | MQL | SQL |
| **Horizontal Scale** | ✓ | ✓ | ✓ | ✓ (sharded) | ✗ |
| **Conflict Resolution** | LWW | LWW | LWW | Last-write | Locks |

---

## Deep Dive: ShardStore vs Cassandra

Both are Dynamo-style, but:

| Aspect | ShardStore | Cassandra |
|--------|------------|-----------|
| **Language** | Python | Java |
| **Storage** | In-memory + JSON | LSM Tree (SSTable) |
| **Gossip** | Simple SWIM | Full protocol |
| **Anti-entropy** | Read repair only | Read repair + Merkle trees |
| **Compaction** | None | Size/Leveled tiered |
| **Secondary Indexes** | None | Yes |
| **Materialized Views** | None | Yes |
| **Production Ready** | No (POC) | Yes |

**What Cassandra adds:**
1. **LSM Tree Storage**: Write-optimized, handles huge datasets
2. **Merkle Trees**: Efficient detection of out-of-sync replicas
3. **Hint Replay**: Automatic delivery of stored hints
4. **Repair**: Background process to fix inconsistencies
5. **Compaction**: Merges SSTables, removes tombstones
6. **CQL**: SQL-like query language

---

## Deep Dive: ShardStore vs DynamoDB

| Aspect | ShardStore | DynamoDB |
|--------|------------|----------|
| **Deployment** | Self-hosted | AWS managed |
| **Pricing** | Free | Pay per request/capacity |
| **Consistency Options** | ONE/QUORUM/ALL | Eventual/Strong |
| **Global Tables** | No | Yes |
| **Streams** | No | Yes (change data capture) |
| **TTL** | No | Yes |
| **Encryption** | No | Yes |

**What DynamoDB adds:**
1. **Managed**: No ops, automatic scaling
2. **Global Tables**: Multi-region replication
3. **DynamoDB Streams**: React to data changes
4. **DAX**: In-memory caching layer
5. **On-demand**: Pay per request pricing

---

## When to Use What

### Use ShardStore / Dynamo-style when:
- High availability is critical (can't afford downtime)
- Write-heavy workload
- Data is naturally key-based
- Can tolerate eventual consistency
- Simple access patterns (get/put by key)

### Use Consensus-based (etcd, Consul) when:
- Strong consistency required
- Small dataset (< 10GB)
- Configuration/coordination data
- Leader election needed

### Use Single-leader (PostgreSQL, MySQL) when:
- ACID transactions required
- Complex queries (joins, aggregations)
- Dataset fits on one machine
- Strong consistency required

### Use NewSQL (CockroachDB, Spanner) when:
- Need both: SQL + horizontal scaling
- Global distribution with strong consistency
- ACID transactions across shards
- Can pay the latency cost

---

## ShardStore: What's Missing for Production

### Must Have:
1. **Persistent Storage**: Replace JSON with LSM tree or B-tree
2. **Network Layer**: gRPC/HTTP for real distribution
3. **Merkle Trees**: Efficient anti-entropy
4. **Compaction**: Tombstone cleanup
5. **Authentication**: Who can read/write

### Nice to Have:
6. **TTL**: Auto-expire keys
7. **Secondary Indexes**: Query by non-key fields
8. **Compression**: Reduce storage/network
9. **Encryption**: At rest and in transit
10. **Monitoring**: Metrics, dashboards

### To Match Cassandra:
11. **CQL**: Query language
12. **Materialized Views**: Pre-computed queries
13. **Lightweight Transactions**: Compare-and-set
14. **Multi-datacenter**: Geographic replication
15. **Repair**: Background consistency checking

---

## The Consistency Spectrum

```
Weak                                                          Strong
  |                                                              |
  |  Eventual    Causal    Sequential    Linearizable   Strict   |
  |     |          |           |              |            |     |
  |   Dynamo    COPS      Zookeeper       Spanner      Single   |
  |   Cassandra            (Raft)                       Node    |
  |   ShardStore                                                 |
  |                                                              |
  └──────────────────────────────────────────────────────────────┘
       ↑                                                    ↑
  High Availability                                   High Consistency
  Low Latency                                         Higher Latency
```

**ShardStore sits at "Eventual"** - the weakest but most available end.

---

## Summary

ShardStore implements the **Dynamo paper architecture** (2007):

✓ Consistent hashing
✓ Vector clocks
✓ Quorum reads/writes
✓ Hinted handoff
✓ Read repair
✓ Gossip failure detection

It's a teaching/POC implementation. For production, use:
- **Cassandra**: Open source, battle-tested
- **DynamoDB**: Managed, serverless
- **ScyllaDB**: Cassandra-compatible, faster

The architecture is proven at massive scale (Amazon, Netflix, Apple all use Dynamo-style systems).
