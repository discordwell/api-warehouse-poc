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

if __name__ == "__main__":
    verify_index()
