"""
Verify C++ Native Snapshotting.
"""
from hbdb.db import HBDB
import os
import shutil

SNAPSHOT_FILE = "snapshot.bin"
LOG_FILE = "transaction.log"

def verify_snapshot():
    print("Verifying C++ Snapshotting...")
    
    # Clean up
    if os.path.exists(SNAPSHOT_FILE): os.remove(SNAPSHOT_FILE)
    if os.path.exists(LOG_FILE): os.remove(LOG_FILE)
    
    # 1. Write Data
    db1 = HBDB()
    tx1 = db1.transaction()
    tx1.set("key1", "value1")
    tx1.set("key2", {"nested": [1, 2, 3]}) # Complex object
    assert tx1.commit() == True
    print("Step 1: Data written.")
    
    # 2. Take Snapshot
    print("Step 2: Taking snapshot...")
    db1.backend.save_snapshot(SNAPSHOT_FILE)
    assert os.path.exists(SNAPSHOT_FILE)
    
    del db1
    
    # 3. Load Data from Snapshot (New DB)
    print("Step 3: Loading from snapshot...")
    # Simulate fresh start without log replay (to test snapshot only)
    # We manually load snapshot on backend for this test
    db2 = HBDB()
    
    # Clear db2 state to be sure
    db2.backend = type(db2.backend)() 
    # Actually wait, HBDB() constructor creates fresh backend.
    
    max_ts = db2.backend.load_snapshot(SNAPSHOT_FILE)
    print(f"   Max TS found: {max_ts}")
    db2.resolver.restore_clock(max_ts)
    
    # 4. Verify
    tx2 = db2.transaction()
    val1 = tx2.get("key1")
    val2 = tx2.get("key2")
    
    if val1 == "value1" and val2 == {"nested": [1, 2, 3]}:
        print("✅ SUCCESS: Snapshot saved and loaded correctly.")
        print(f"   key1: {val1}")
        print(f"   key2: {val2}")
    else:
        print("❌ FAILURE: Snapshot data mismatch.")
        print(f"   key1: {val1}")
        print(f"   key2: {val2}")
        exit(1)

if __name__ == "__main__":
    verify_snapshot()
