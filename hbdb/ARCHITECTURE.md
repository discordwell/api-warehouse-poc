# HBDB v2 - Sophisticated Distributed SQL

## Architecture Overview

```
                         ┌─────────────────────────────────────┐
                         │            Coordinator              │
                         │   • Fast-path detection             │
                         │   • Transaction routing             │
                         │   • Load balancing                  │
                         └─────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
     ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
     │  Sequencer 0    │       │  Sequencer 1    │       │  Sequencer 2    │
     │  (Raft Group A) │◄─────►│  (Raft Group B) │◄─────►│  (Raft Group C) │
     │  Partition: 0-3 │       │  Partition: 4-7 │       │  Partition: 8-B │
     └─────────────────┘       └─────────────────┘       └─────────────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         ▼
                         ┌─────────────────────────────────────┐
                         │          Epoch Assembler            │
                         │   • Collects batches per epoch      │
                         │   • Deterministic merge sort        │
                         │   • Broadcasts execution order      │
                         └─────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
     ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
     │   Compute 0     │       │   Compute 1     │       │   Compute 2     │
     │   (Stateless)   │       │   (Stateless)   │       │   (Stateless)   │
     │   Aria Engine   │       │   Aria Engine   │       │   Aria Engine   │
     └─────────────────┘       └─────────────────┘       └─────────────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         ▼
                         ┌─────────────────────────────────────┐
                         │        Disaggregated Storage        │
                         │  ┌───────┐ ┌───────┐ ┌───────┐     │
                         │  │Page 0 │ │Page 1 │ │Page 2 │ ... │
                         │  │Server │ │Server │ │Server │     │
                         │  └───────┘ └───────┘ └───────┘     │
                         │         Shared Log (Raft)          │
                         └─────────────────────────────────────┘
```

## Key Innovations

### 1. Parallel Sequencers (BOHM-style)

Unlike single-sequencer Calvin, we partition the key space across multiple sequencers:

- Each sequencer handles a key range (partition)
- Each sequencer runs Raft internally for HA (no SPOF!)
- Transactions spanning partitions go to multiple sequencers
- Epoch-based batching for throughput

```
Epoch N:
  Seq0: [T1, T4, T7]     ─┐
  Seq1: [T2, T5, T8]     ─┼─► Deterministic Merge ─► [T1, T2, T3, T4, T5, T6, T7, T8, T9]
  Seq2: [T3, T6, T9]     ─┘
```

### 2. Aria-Style Execution

Speculative execution with deterministic reordering:

**Phase 1: Speculative Execution**
- Execute all transactions in parallel optimistically
- Track read/write sets during execution

**Phase 2: Conflict Detection**
- Compare read/write sets
- Mark conflicting transactions

**Phase 3: Deterministic Reorder**
- Re-execute conflicting transactions in sequence order
- Non-conflicting transactions already committed!

This gives us:
- Parallelism for non-conflicting transactions
- Determinism for conflicting ones
- No wasted work (unlike OCC retry)

### 3. Fast Path (Detock-style)

Skip the sequencer for transactions that are clearly non-conflicting:

```
Transaction arrives
       │
       ▼
┌─────────────────┐
│ Conflict Check  │──── No conflict ────► Execute immediately
│ (bloom filter)  │                       (fast path)
└─────────────────┘
       │
    Potential
    conflict
       │
       ▼
┌─────────────────┐
│   Sequencer     │──── Slow path ────► Deterministic execution
└─────────────────┘
```

### 4. Disaggregated Storage (Aurora-style)

Compute and storage are separate:

**Compute Nodes:**
- Stateless - can fail and recover instantly
- Cache pages but don't own them
- Scale independently

**Storage Nodes:**
- Own pages and handle durability
- Replicate via shared Raft log
- Scale independently

Benefits:
- Faster failover (no data migration)
- Better resource utilization
- Independent scaling

### 5. Epoch-Based Batching

Group transactions into epochs (e.g., 10ms windows):

```
Time ──────────────────────────────────────────►

     │◄── Epoch 1 ──►│◄── Epoch 2 ──►│◄── Epoch 3 ──►│

     [T1,T2,T3,T4]   [T5,T6,T7]       [T8,T9,T10,T11]
           │              │                  │
           ▼              ▼                  ▼
      Batch execute  Batch execute     Batch execute
```

Benefits:
- Amortize coordination overhead
- Better throughput under load
- Natural batching for storage writes

## Comparison with CockroachDB

| Aspect | CockroachDB | HBDB v2 |
|--------|-------------|-------------|
| Sequencing | None (optimistic) | Parallel partitioned |
| Conflicts | Retry (waste work) | Aria reorder (no waste) |
| Fast path | Default | Bloom filter gated |
| Storage | Coupled per node | Disaggregated |
| Failover | Raft re-election + catch-up | Instant (stateless compute) |
| SPOF | None | None (multiple sequencers) |

## Implementation Components

1. `coordinator.py` - Transaction routing, fast path detection
2. `sequencer/parallel.py` - Partitioned sequencers with Raft
3. `sequencer/epoch.py` - Epoch batching and deterministic merge
4. `execution/aria.py` - Speculative execution engine
5. `storage/disaggregated.py` - Aurora-style storage layer
6. `storage/page_server.py` - Individual page servers
7. `consensus/raft.py` - Raft implementation for sequencers

## References

- **Calvin** (2012): Deterministic database systems
- **BOHM** (2014): Partition-parallel deterministic execution
- **Aria** (2020): Deterministic concurrency control with speculative execution
- **Detock** (2023): Hybrid deterministic/optimistic approach
- **Aurora** (2017): Disaggregated storage architecture
