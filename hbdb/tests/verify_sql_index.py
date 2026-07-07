"""
Verify SQL Index Scan optimization.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hbdb.db import HBDB
from hbdb.sql.engine import SQLEngine
from hbdb.sql.schema import Column, DataType

def verify_index():
    print("Verifying SQL Index Scan...")
    db = HBDB()
    engine = SQLEngine(db)
    
    # 1. Create Table & Index
    engine.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    engine.execute("CREATE INDEX idx_age ON users (age)")
    
    # 2. Insert Data
    engine.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
    engine.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
    engine.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
    engine.execute("INSERT INTO users VALUES (4, 'Dave', 30)") # Duplicate age
    
    # 3. Query with Index (age=30)
    print("Step 3: Querying WHERE age = 30...")
    # We inspect the plan to ensure index is used (optimizer log or just trust result?)
    # Let's trust result first, then maybe verify performance or plan class if possible.
    # The optimization happens internally.
    
    result = list(engine.execute("SELECT id, name, age FROM users WHERE age = 30"))
    
    # Expect Alice(1) and Dave(4)
    expected = [(1, "Alice", 30), (4, "Dave", 30)]
    
    # Sort by ID for consistency
    result.sort(key=lambda x: x['id'])
    
    print(f"   Result: {result}")
    
    match_count = 0
    for row in result:
        if row['age'] == 30 and row['name'] in ('Alice', 'Dave'):
            match_count += 1
            
    if match_count == 2 and len(result) == 2:
        print("✅ SUCCESS: Index Scan returned correct rows.")
    else:
        print("❌ FAILURE: Incorrect results.")
        exit(1)

    # 4. Verify Index was actually created in storage
    # Scan keys with prefix /t/{tid}/_i/
    # We'll peek into backend
    # Table ID likely 1, Index ID likely 1
    # Index Key: /t/1/_i/1/30/{pk} -> pk
    print("Step 4: Verifying Storage format...")
    keys = db.backend.scan_keys("/t/1/_i/", "/t/1/_i/~")
    print(f"   Index Keys found: {len(keys)}")
    if len(keys) >= 4:
        print("✅ SUCCESS: Index keys exist in storage.")
    else:
        print("❌ FAILURE: Index keys missing.")
        print(keys)
        exit(1)

    # 5. DELETE writes a None tombstone; SELECT must skip it
    # (regression: the native scan used to return the tombstone and
    # crash row decoding)
    print("Step 5: DELETE then SELECT...")
    engine.execute("DELETE FROM users WHERE id = 1")
    remaining = list(engine.execute("SELECT id, name, age FROM users"))
    ids = sorted(row['id'] for row in remaining)

    if ids == [2, 3, 4]:
        print("✅ SUCCESS: Deleted row no longer visible.")
    else:
        print(f"❌ FAILURE: Expected ids [2, 3, 4], got {ids}")
        exit(1)


def verify_write_consistency():
    """UPDATE/DELETE must be visible to later reads and keep indexes in sync.

    Regression for two bugs:
      - The SQL read cache was never invalidated on write, so a SELECT after
        an UPDATE returned the stale (pre-update) row.
      - UPDATE/DELETE never maintained secondary indexes, so the index kept
        pointing at the old value (or a deleted row).
    """
    print("\nVerifying write consistency (read cache + index upkeep)...")
    db = HBDB()
    engine = SQLEngine(db)

    engine.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)")
    engine.execute("CREATE INDEX idx_balance ON accounts (balance)")
    engine.execute("INSERT INTO accounts VALUES (1, 'Alice', 100)")
    engine.execute("INSERT INTO accounts VALUES (2, 'Bob', 300)")

    table = engine.catalog.get_table("accounts")
    idx = engine.catalog.get_indexes_for_table(table.id)[0]
    idx_lo, idx_hi = f"/t/{table.id}/_i/", f"/t/{table.id}/_i/~"

    def live_index_keys():
        # txn.scan hides None tombstones, so this is the set of live entries.
        return sorted(k for k, _ in db.transaction().scan(idx_lo, idx_hi))

    # Warm the read cache, then mutate and read again.
    engine.execute("SELECT id, owner, balance FROM accounts")
    engine.execute("UPDATE accounts SET balance = 200 WHERE id = 1")

    after = {r["id"]: r["balance"] for r in engine.execute("SELECT id, owner, balance FROM accounts")}
    if after.get(1) == 200:
        print("✅ SUCCESS: read after UPDATE reflects the new value.")
    else:
        print(f"❌ FAILURE: stale read after UPDATE: {after}")
        exit(1)

    keys = live_index_keys()
    if f"/t/{table.id}/_i/{idx.id}/200/1" in keys and not any("/100/" in k for k in keys):
        print("✅ SUCCESS: UPDATE rewrote the secondary index entry.")
    else:
        print(f"❌ FAILURE: index not updated: {keys}")
        exit(1)

    engine.execute("DELETE FROM accounts WHERE id = 2")
    keys = live_index_keys()
    if not any(k.endswith("/300/2") for k in keys):
        print("✅ SUCCESS: DELETE tombstoned the secondary index entry.")
    else:
        print(f"❌ FAILURE: stale index entry after DELETE: {keys}")
        exit(1)

    remaining = sorted(r["id"] for r in engine.execute("SELECT id, owner, balance FROM accounts"))
    if remaining == [1]:
        print("✅ SUCCESS: table reflects the DELETE.")
    else:
        print(f"❌ FAILURE: expected ids [1], got {remaining}")
        exit(1)


def verify_backfill():
    """CREATE INDEX on an already-populated table must backfill the index.

    Regression: the index used to start empty regardless of existing rows,
    so `INSERT x N -> CREATE INDEX -> SELECT WHERE col = x` silently
    returned nothing -- every pre-existing row was invisible to the index
    scan while a full scan would have found it.
    """
    print("\nVerifying CREATE INDEX backfills existing rows...")
    db = HBDB()
    engine = SQLEngine(db)

    engine.execute("CREATE TABLE backfill_t (id INTEGER PRIMARY KEY, "
                   "city TEXT, pop INTEGER)")
    engine.execute("INSERT INTO backfill_t VALUES (1, 'ada', 30), "
                   "(2, 'bly', 25), (3, 'cor', 30)")
    engine.execute("INSERT INTO backfill_t (id, city) VALUES (4, 'dun')")  # pop NULL
    # The index arrives *after* the rows.
    engine.execute("CREATE INDEX idx_pop ON backfill_t (pop)")

    got = sorted(r["id"] for r in engine.execute(
        "SELECT id FROM backfill_t WHERE pop = 30"))
    if got == [1, 3]:
        print("✅ SUCCESS: index scan sees rows inserted before CREATE INDEX.")
    else:
        print(f"❌ FAILURE: expected ids [1, 3], got {got}")
        exit(1)

    table = engine.catalog.get_table("backfill_t")
    idx = engine.catalog.get_indexes_for_table(table.id)[0]
    keys = sorted(k for k, _ in db.transaction().scan(
        f"/t/{table.id}/_i/", f"/t/{table.id}/_i/~"))
    expected = {f"/t/{table.id}/_i/{idx.id}/30/1",
                f"/t/{table.id}/_i/{idx.id}/25/2",
                f"/t/{table.id}/_i/{idx.id}/30/3"}
    if expected.issubset(keys) and not any(k.endswith("/4") for k in keys):
        print("✅ SUCCESS: backfill wrote entries for non-NULL values only.")
    else:
        print(f"❌ FAILURE: backfilled index keys wrong: {keys}")
        exit(1)

    # Rows inserted after the index coexist with backfilled ones.
    engine.execute("INSERT INTO backfill_t VALUES (5, 'eze', 30)")
    got = sorted(r["id"] for r in engine.execute(
        "SELECT id FROM backfill_t WHERE pop = 30"))
    if got == [1, 3, 5]:
        print("✅ SUCCESS: pre- and post-index rows both served by the index.")
    else:
        print(f"❌ FAILURE: expected ids [1, 3, 5], got {got}")
        exit(1)

    # And index maintenance keeps working on a backfilled entry.
    engine.execute("UPDATE backfill_t SET pop = 31 WHERE id = 1")
    got30 = sorted(r["id"] for r in engine.execute(
        "SELECT id FROM backfill_t WHERE pop = 30"))
    got31 = sorted(r["id"] for r in engine.execute(
        "SELECT id FROM backfill_t WHERE pop = 31"))
    if got30 == [3, 5] and got31 == [1]:
        print("✅ SUCCESS: UPDATE re-points a backfilled index entry.")
    else:
        print(f"❌ FAILURE: after UPDATE got pop=30 -> {got30}, "
              f"pop=31 -> {got31}")
        exit(1)


if __name__ == "__main__":
    verify_index()
    verify_write_consistency()
    verify_backfill()
