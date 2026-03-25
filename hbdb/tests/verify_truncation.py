"""
Verify Snapshot + Log Truncation.
"""
from hbdb.db import HBDB
import os
import json

def verify_truncation():
    print("Verifying Snapshot & Truncation...")
    
    log_path = "transaction.log"
    snap_path = "snapshot.bin"
    
    # Cleaning
    if os.path.exists(log_path): os.remove(log_path)
    if os.path.exists(snap_path): os.remove(snap_path)
    
    # 1. Fill Log
    print("Step 1: Writing initial data...")
    db = HBDB()
    tx = db.transaction()
    tx.set("k1", "v1")
    tx.commit()
    
    # Check log size
    size_before = os.path.getsize(log_path)
    print(f"   Log size before: {size_before} bytes")
    assert size_before > 0
    
    # 2. Take Snapshot
    print("Step 2: Taking Snapshot...")
    db.take_snapshot()
    
    # Check log size (should be 0 or small)
    size_after = os.path.getsize(log_path)
    print(f"   Log size after: {size_after} bytes")
    assert size_after < size_before
    assert os.path.exists(snap_path)
    
    # 3. Write NEW data (Post-Snapshot)
    print("Step 3: Writing post-snapshot data...")
    tx2 = db.transaction()
    tx2.set("k2", "v2")
    tx2.commit()
    
    del db
    
    # 4. Recover
    print("Step 4: Recovering...")
    db2 = HBDB()
    tx3 = db2.transaction()
    
    v1 = tx3.get("k1") # From Snapshot
    v2 = tx3.get("k2") # From Log
    
    if v1 == "v1" and v2 == "v2":
        print("✅ SUCCESS: Data survived snapshot + log replay.")
    else:
        print(f"❌ FAILURE: k1={v1}, k2={v2}")
        exit(1)

if __name__ == "__main__":
    verify_truncation()
