# BadgerDB: A Better CockroachDB

## Overview

A distributed SQL database combining the best ideas:
- **Disaggregated architecture** (Aurora/Neon style)
- **Deterministic transactions** (Calvin style)
- **Simplified leaderless consensus** (EPaxos-inspired)
- **Full SQL** (SQLite parser + custom execution)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       CLIENT                                    │
│                   (Postgres wire protocol)                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    COMPUTE LAYER                                │
│                   (Stateless SQL Nodes)                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ SQL Parser → Query Planner → Executor                   │   │
│  │                    │                                     │   │
│  │                    ▼                                     │   │
│  │            Transaction Manager                           │   │
│  │         (assigns to sequencer)                           │   │
│  └─────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                   SEQUENCER LAYER                               │
│              (Deterministic Ordering - Calvin)                  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Global Transaction Log                      │   │
│  │                                                          │   │
│  │   [Txn1] → [Txn2] → [Txn3] → [Txn4] → ...              │   │
│  │                                                          │   │
│  │   Replicated via Raft (3 sequencer nodes)               │   │
│  └─────────────────────────────────────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    STORAGE LAYER                                │
│                (Partitioned Key-Value)                          │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Shard 0  │  │ Shard 1  │  │ Shard 2  │  │ Shard 3  │       │
│  │ keys a-g │  │ keys h-n │  │ keys o-t │  │ keys u-z │       │
│  │          │  │          │  │          │  │          │       │
│  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │       │
│  │ │Replica│ │  │ │Replica│ │  │ │Replica│ │  │ │Replica│ │       │
│  │ │ x 3  │ │  │ │ x 3  │ │  │ │ x 3  │ │  │ │ x 3  │ │       │
│  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

## Key Innovations Over CockroachDB

### 1. Disaggregated Compute/Storage
```
CockroachDB:                    BadgerDB:
┌─────────────┐                 ┌─────────────┐
│   Node 1    │                 │  Compute 1  │ ← Stateless
│ SQL+Storage │                 │  Compute 2  │    Scale independently
│             │                 │  Compute N  │
└─────────────┘                 └──────┬──────┘
                                       │
                                ┌──────▼──────┐
                                │   Storage   │ ← Scale independently
                                │   Cluster   │
                                └─────────────┘

Benefit: Scale compute and storage independently
         Instant failover (compute is stateless)
```

### 2. Deterministic Transactions (Calvin)
```
CockroachDB:                    BadgerDB:

Txn arrives → Execute →         Txn arrives → Sequence FIRST
              Lock →                           │
              Coordinate →                     ▼
              Commit                   [Ordered Log]
                                              │
                                              ▼
                                       Execute deterministically
                                       (no coordination needed!)

Benefit: No distributed locking during execution
         Higher throughput on contended data
```

### 3. Simplified Consensus
```
CockroachDB: Raft per range (leader bottleneck)

BadgerDB:
  - Sequencer uses Raft (but only for ordering, lightweight)
  - Storage uses quorum writes (simpler than full consensus)
  - Reads go to any replica (MVCC makes this safe)
```

## Components to Implement

### Phase 1: Foundation
- [ ] Project structure
- [ ] Configuration management
- [ ] Basic types (Timestamp, Key, Value, etc.)

### Phase 2: Storage Layer
- [ ] MVCC storage engine (key → [(timestamp, value)])
- [ ] Shard management
- [ ] Quorum writes
- [ ] Snapshot reads

### Phase 3: Sequencer
- [ ] Transaction log structure
- [ ] Simplified Raft for log replication
- [ ] Epoch-based sequencing

### Phase 4: SQL Layer
- [ ] SQL parser (use sqlparse or build simple one)
- [ ] Schema management (CREATE TABLE)
- [ ] Query planning
- [ ] Basic execution (SELECT, INSERT, UPDATE, DELETE)

### Phase 5: Transaction Execution
- [ ] Calvin-style deterministic execution
- [ ] Read/write set analysis
- [ ] Parallel execution of non-conflicting txns

### Phase 6: Compute Layer
- [ ] Stateless SQL nodes
- [ ] Connection handling
- [ ] Query routing

### Phase 7: Wire Protocol
- [ ] Postgres wire protocol (basic)
- [ ] Or simple HTTP/JSON API

## File Structure

```
badgerdb/
├── badgerdb/
│   ├── __init__.py
│   ├── types.py           # Core types (Timestamp, Key, TxnId, etc.)
│   ├── config.py          # Configuration
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── mvcc.py        # MVCC storage engine
│   │   ├── shard.py       # Shard management
│   │   └── replica.py     # Replication
│   │
│   ├── sequencer/
│   │   ├── __init__.py
│   │   ├── log.py         # Transaction log
│   │   ├── raft.py        # Simplified Raft
│   │   └── epoch.py       # Epoch management
│   │
│   ├── sql/
│   │   ├── __init__.py
│   │   ├── parser.py      # SQL parsing
│   │   ├── planner.py     # Query planning
│   │   ├── executor.py    # Query execution
│   │   └── schema.py      # Schema management
│   │
│   ├── txn/
│   │   ├── __init__.py
│   │   ├── manager.py     # Transaction lifecycle
│   │   ├── calvin.py      # Deterministic execution
│   │   └── analysis.py    # Read/write set analysis
│   │
│   ├── compute/
│   │   ├── __init__.py
│   │   ├── node.py        # Compute node
│   │   └── router.py      # Query routing
│   │
│   └── server/
│       ├── __init__.py
│       └── postgres.py    # Postgres wire protocol
│
├── tests/
│   └── ...
│
├── examples/
│   └── demo.py
│
├── requirements.txt
└── README.md
```

## Data Flow

### Write Path
```
1. Client: INSERT INTO users (id, name) VALUES (1, 'Alice')
                              │
2. Compute Node: Parse SQL    │
                 Analyze: writes to users:1
                              │
3. Sequencer: Assign position │
              Log: [Txn-42: INSERT users:1 = 'Alice']
              Replicate log entry
                              │
4. Storage: All shards see ordered log
            Shard for 'users:1' applies write
            MVCC: store (timestamp=42, value='Alice')
                              │
5. Client: "INSERT 1"         ▼
```

### Read Path
```
1. Client: SELECT * FROM users WHERE id = 1
                              │
2. Compute Node: Parse SQL    │
                 Plan: point lookup users:1
                              │
3. Storage: Read from any replica
            MVCC: return latest committed version
                              │
4. Client: [{id: 1, name: 'Alice'}]
```

### Transaction Path (Multi-Statement)
```
1. Client: BEGIN
           UPDATE accounts SET bal = bal - 100 WHERE id = 1
           UPDATE accounts SET bal = bal + 100 WHERE id = 2
           COMMIT
                              │
2. Compute Node: Buffer statements
                 On COMMIT: analyze read/write sets
                 Write set: [accounts:1, accounts:2]
                              │
3. Sequencer: Assign single position for entire txn
              Log: [Txn-99: {read: [acc:1, acc:2],
                            write: [acc:1, acc:2],
                            ops: [...]}]
                              │
4. Storage: Execute deterministically
            All shards execute Txn-99 at same logical time
            No coordination needed!
                              │
5. Client: "COMMIT"
```

## Consistency Guarantees

- **Serializable**: All transactions execute in sequencer order
- **Linearizable reads**: Read your own writes guaranteed
- **Snapshot isolation**: Reads see consistent snapshot

## Performance Targets

| Metric | CockroachDB | BadgerDB Target |
|--------|-------------|-----------------|
| Write latency | 5-15ms | 3-8ms |
| Read latency | 2-5ms | 1-3ms |
| Txn throughput | 50k/s | 100k/s |
| Failover time | Seconds | Instant (compute) |

## Limitations (POC Scope)

- Single-process simulation (no real network)
- Simplified Raft (not production-grade)
- Basic SQL subset (no JOINs initially)
- In-memory storage (no persistence)
- No geo-distribution

## Success Criteria

1. ✅ SQL queries work (SELECT, INSERT, UPDATE, DELETE)
2. ✅ Transactions are serializable
3. ✅ Survives compute node "failures"
4. ✅ Demonstrates deterministic execution
5. ✅ Shows separation of compute/storage
