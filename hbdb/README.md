# HBDB

A distributed SQL database POC. The codebase hosts two engines that share
storage/SQL building blocks:

1. **Calvin engine** (`hbdb.database.HBDB`) — deterministic transactions:
   global ordering via a sequencer, then coordination-free execution.
   Documented below and in `ARCHITECTURE.md`.
2. **FDB-style engine** (`hbdb.db.HBDB`) — FoundationDB-style unbundled
   architecture: optimistic interactive transactions validated by a
   Resolver (C++ native extension with pure-Python fallback), WAL + binary
   snapshot durability, and an optional TCP coordinator/storage cluster
   (`hbdb.server.main`, `hbdb.cli`). Read-only transactions serialize at
   their read timestamp; read-write transactions get OCC validation with
   range (phantom) protection.

## Key Innovations (vs CockroachDB)

1. **Calvin-Style Deterministic Execution**
   - Transactions are ordered globally BEFORE execution
   - All nodes execute in the same order = same result
   - No coordination during execution = higher throughput

2. **Disaggregated Architecture**
   - Separate compute and storage layers
   - Independent scaling of each

3. **Simplified Consistency**
   - Single sequencer for ordering (vs distributed Raft)
   - No 2PC for single-shard transactions

## Quick Start

```python
from hbdb.database import HBDB

with HBDB() as db:
    # Create table
    db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)

    # Insert
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")

    # Query
    result = db.execute("SELECT * FROM users WHERE id = 1")
    print(result.rows)  # [{'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}]

    # Update
    db.execute("UPDATE users SET name = 'Alicia' WHERE id = 1")

    # Delete
    db.execute("DELETE FROM users WHERE id = 1")
```

## Convenience Methods

```python
# Insert
db.insert('users', {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'})

# Select
rows = db.select('users', columns=['name'], where={'id': 1})

# Update
db.update('users', {'name': 'Bob'}, {'id': 1})

# Delete
db.delete('users', {'id': 1})

# Query with exception on error
rows = db.query("SELECT * FROM users")
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                HBDB                      │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │              SQL Layer                │  │
│  │   Parser → Analyzer → Executor        │  │
│  └───────────────────────────────────────┘  │
│                     │                       │
│  ┌───────────────────────────────────────┐  │
│  │        Calvin Sequencer               │  │
│  │   Global ordering of transactions     │  │
│  └───────────────────────────────────────┘  │
│                     │                       │
│  ┌───────────────────────────────────────┐  │
│  │         Shard Manager                 │  │
│  │   ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐    │  │
│  │   │ S0  │ │ S1  │ │ S2  │ │ S3  │    │  │
│  │   └─────┘ └─────┘ └─────┘ └─────┘    │  │
│  └───────────────────────────────────────┘  │
│                     │                       │
│  ┌───────────────────────────────────────┐  │
│  │          MVCC Storage                 │  │
│  │   Multi-version concurrency control   │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

## Components

- **SQL Parser**: Tokenizer + recursive descent parser for SQL
- **Executor**: Translates SQL to key-value operations
- **Sequencer**: Calvin-style global transaction ordering
- **CalvinExecutor**: Deterministic execution of sequenced transactions
- **ShardManager**: Consistent hashing across shards
- **MVCCStore**: Multi-version storage with garbage collection

## Performance

```
Sequential Writes:   ~788 ops/sec
Sequential Reads:    ~785 ops/sec
Concurrent Writes:   ~7,182 ops/sec (10 workers)
Concurrent Reads:    ~6,932 ops/sec (10 workers)
Shard Distribution:  4.2% variance (excellent)
```

## Running Tests

```bash
# (optional) build the C++ native extension; everything falls back to
# pure Python without it
python setup.py build_ext --inplace

# Calvin engine suite
python tests/test_hbdb.py

# Resolver (OCC) suite, incl. the pure-Python fallback path
python tests/test_resolver.py

# FDB-style engine verification scripts (write WAL/snapshot files to CWD)
python tests/verify_durability.py
python tests/verify_recovery.py
python tests/verify_snapshot.py
python tests/verify_truncation.py
python tests/verify_wal_corruption.py
python tests/verify_range.py
python tests/verify_sql_index.py

# Cluster integration (spawns local coordinator + storage subprocesses)
python tests/verify_sharding.py
python tests/verify_replication.py
```

## Running Benchmark

```bash
python examples/benchmark.py
```

## SQL Support

- `CREATE TABLE` (with PRIMARY KEY)
- `DROP TABLE`
- `INSERT INTO`
- `SELECT` (with WHERE, column filtering)
- `UPDATE` (with WHERE)
- `DELETE` (with WHERE)

(The FDB-style engine's SQL layer additionally supports `CREATE INDEX`
with index-scan execution; see `hbdb/sql/engine.py` and
`tests/verify_sql_index.py`.)
