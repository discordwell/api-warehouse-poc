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
pip install -r requirements.txt

# (optional) build the C++ native extension; everything falls back to
# pure Python without it
python setup.py build_ext --inplace

# Everything (each verify script runs in an isolated temp dir):
python tests/run_all.py               # add --skip-cluster to avoid ports 9000-9004
```

Individual suites can also be run directly:

```bash
# Calvin engine suite
python tests/test_hbdb.py

# Resolver (OCC) suite, incl. the pure-Python fallback path
python tests/test_resolver.py

# Snapshot + recovery suite (pure-Python path, native cross-compatibility,
# WAL archive replay)
python tests/test_snapshot.py

# Storage facade contracts (tombstone scans, out-of-order MVCC writes)
python tests/test_backend.py

# FDB-style engine verification scripts (write WAL/snapshot files to CWD)
python tests/verify_durability.py
python tests/verify_recovery.py
python tests/verify_snapshot.py
python tests/verify_truncation.py
python tests/verify_wal_corruption.py
python tests/verify_range.py
python tests/verify_sql_index.py
python tests/verify_sql_predicates.py
python tests/verify_sql_insert.py
python tests/verify_sql_orderlimit.py
python tests/verify_sql_aggregates.py
python tests/verify_sql_join.py
python tests/verify_sql_project.py
python tests/verify_sql_functions.py

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
- `INSERT INTO` (single- and multi-row `VALUES (..), (..), ..`)
- `SELECT` (with WHERE, column projection including aliases/expressions like
  `age * 2 AS doubled`, `ORDER BY`, `LIMIT`/`OFFSET`)
- `SELECT ... GROUP BY` with aggregate functions (`COUNT`, `SUM`, `AVG`,
  `MIN`, `MAX`, including `COUNT(DISTINCT col)`) and `HAVING`
- `SELECT DISTINCT`
- `JOIN` (`INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, and comma joins),
  including multi-table and self-joins
- Scalar functions in any expression position (SELECT, WHERE, ORDER BY,
  GROUP BY, HAVING, `UPDATE ... SET`): `COALESCE`, `NULLIF`, `UPPER`, `LOWER`,
  `LENGTH`, `TRIM`, `ABS`, `CEIL`, `FLOOR`, `ROUND`, `CONCAT` / `||`, and `CAST`
- `CASE` expressions — searched (`CASE WHEN cond THEN r ... [ELSE d] END`) and
  simple (`CASE x WHEN v THEN r ... END`) — in any expression position
- `UPDATE` (with WHERE)
- `DELETE` (with WHERE)

(The FDB-style engine's SQL layer additionally supports `CREATE INDEX`
with index-scan execution; see `hbdb/sql/engine.py` and
`tests/verify_sql_index.py`.)

`WHERE` clauses in the FDB-style engine support `=`, `!=`/`<>`, `<`, `<=`,
`>`, `>=`, `AND`, `OR`, `NOT`, parentheses, `IS [NOT] NULL`,
`[NOT] IN (value, ...)` (a parenthesized value list — a subquery
`IN (SELECT ...)` is unsupported and fails loud, never silently matching),
`[NOT] BETWEEN`, and `[NOT] LIKE`/`ILIKE` (with `%`/`_` wildcards and an
optional `ESCAPE` character), all with SQL three-valued (NULL) logic;
predicate evaluation lives in `hbdb/sql/predicates.py` and is shared by the
filter/update/delete operators, `HAVING`, and join `ON`
(`tests/verify_sql_predicates.py`).

`SELECT col, ...` projects to the listed columns, `SELECT *` returns all, and
`SELECT t.*` returns one table's columns; an aliased or computed item
(`SELECT name AS who`, `SELECT price * qty AS total`) projects through the
shared operand resolver to exactly that output column, on the single-table path
as well as in joins (`tests/verify_sql_project.py`). An *unimplemented* scalar
function in the SELECT list (e.g. `SUBSTRING(x, ...)`) raises rather than
silently streaming the raw row.

Scalar functions — `COALESCE`, `NULLIF`, `UPPER`, `LOWER`, `LENGTH`, `TRIM`,
`ABS`, `CEIL`, `FLOOR`, `ROUND`, `CONCAT` / `||` and `CAST` — are evaluated by
the same shared operand resolver (`hbdb/sql/predicates.py`), so a function
works identically in every clause that takes an operand: SELECT, WHERE,
ORDER BY, GROUP BY, HAVING and `UPDATE ... SET` (`tests/verify_sql_functions.py`).
SQL NULL rules are honored: `COALESCE` returns the first non-NULL argument,
`NULLIF(a, b)` is NULL when `a = b`, every other function returns NULL for a
NULL argument, and `||` / `CONCAT` propagate NULL (ANSI) — wrap an argument in
`COALESCE` to treat NULL as the empty string. `ROUND` rounds halves away from
zero (`ROUND(2.5) = 3`), not Python's round-half-to-even. Numeric functions
(`ABS`, `ROUND`, a numeric `CAST`) accept numeric *strings* — the same coercion
`WHERE` and `SUM` use — but fail loud on a genuinely non-numeric argument
rather than silently coercing it to 0/NULL. `CAST` to an integer truncates
toward zero (`CAST(3.7 AS INTEGER) = 3`).

`CASE` expressions resolve through that same shared operand resolver, so they
work in every clause an operand is allowed (SELECT, WHERE, ORDER BY, GROUP BY,
HAVING, `UPDATE ... SET`) — see `tests/verify_sql_case.py`. Both forms are
supported: searched (`CASE WHEN cond THEN r ... [ELSE d] END`) and simple
(`CASE x WHEN v THEN r ... END`). A searched `WHEN` is a full predicate under
three-valued logic, so an UNKNOWN (NULL) condition does not match and a row
matching no `WHEN` takes the `ELSE` (or NULL when there is none); a simple
`CASE` compares the operand to each `WHEN` value with the engine's `=`, so a
NULL operand — or a `WHEN NULL` — never matches. Only the selected branch is
evaluated, but an unsupported function in the branch that *is* taken still
fails loud.

`INSERT`/`UPDATE` value literals are coerced through that same operand
resolver, so floats, negative numbers, booleans and `NULL` keep their real
types instead of being mangled into strings (`tests/verify_sql_insert.py`).
The SQL read cache is scoped to each `HBDB` instance, so two databases in one
process never serve one another's rows.

`ORDER BY` (multi-key, `ASC`/`DESC`, `NULLS FIRST`/`LAST`, expressions like
`age * -1`, and positional `ORDER BY 1`) plus `LIMIT`/`OFFSET` are honored by
the FDB-style engine; sort keys resolve through the shared operand resolver,
so they use the same numeric/string coercion as `WHERE`
(`tests/verify_sql_orderlimit.py`). NULL ordering follows SQL's "NULL is the
smallest value" default unless an explicit `NULLS FIRST`/`LAST` is given.

`GROUP BY` and aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`,
including `COUNT(DISTINCT col)` and `SUM(DISTINCT col)`), `HAVING`, and
`SELECT DISTINCT` are implemented in `hbdb/sql/aggregates.py` and the
`AggregateExecutor`/`DistinctExecutor` operators (`tests/verify_sql_aggregates.py`).
Grouping supports multiple keys and positional `GROUP BY 1`; `HAVING` may
reference aggregates that are not in the SELECT list; `ORDER BY` can sort by an
aggregate or alias (`... GROUP BY dept ORDER BY COUNT(*) DESC`). SQL NULL rules
are honored: `COUNT(*)` counts rows while `COUNT(col)` and `SUM`/`AVG`/`MIN`/`MAX`
skip NULLs, a global aggregate over an empty table still returns one row, and a
non-aggregated column that is not a `GROUP BY` key is rejected rather than
guessed. Aggregate arguments and group keys resolve through the same operand
resolver as `WHERE`/`ORDER BY`, so `SUM(price * qty)` and numeric-string columns
behave consistently. Unaliased aggregate output columns are named by their SQL
text (e.g. `COUNT(*)`); alias them (`COUNT(*) AS n`) for friendlier keys.

`JOIN` is implemented in `hbdb/sql/parser.py` (binding) and the
`JoinExecutor`/`QualifyExecutor` operators (`tests/verify_sql_join.py`).
`INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS` and comma joins are supported,
including chained multi-table joins and self-joins via table aliases
(`FROM emp e JOIN emp m ON e.mgr_id = m.id`). The full `ON` predicate is
evaluated through the shared operand resolver, so it works regardless of which
side of `=` each column sits on and supports non-equi (`a.x < b.y`) and
compound (`... AND ...`) conditions; outer joins NULL-pad unmatched rows.
Merged rows carry `table.col` keys, so same-named columns from different
tables (the ubiquitous `id`) coexist instead of clobbering each other —
reference them qualified (`users.id`), and `SELECT *` keeps both under their
qualified names. `NULL` never equi-joins (SQL's `NULL = NULL` is UNKNOWN), on
both the hash-join fast path (INNER equi-joins) and the nested-loop path. An
*unqualified* reference to a column that exists in more than one joined table
is **ambiguous and fails loud** (`ValueError`), as do two SELECT items that
would land on the same output key (`SELECT a.id, b.id` — alias one), rather
than silently picking a side.

Clauses the engine still does not implement — subqueries in any position
(a scalar `(SELECT ...)`, `IN (SELECT ...)`, `EXISTS (...)`, `= ANY/ALL (...)`),
window/analytic functions (`ROW_NUMBER() OVER (...)`), aggregates beyond the
five above (`STDDEV`, ...), and scalar functions outside the set listed above
(`SUBSTRING`, `REPLACE`, the `TRIM(... FROM ...)` forms, ...) — raise
`NotImplementedError` rather than silently dropping the clause and returning the
wrong rows (the same fail-loud contract the `WHERE` evaluator uses).
