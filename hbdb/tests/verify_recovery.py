"""
Verify Resilience/Recovery.
Writes data, closes DB, re-opens DB, and verifies data exists.
"""
from hbdb.db import HBDB
import os

LOG_FILE = "transaction.log"

def verify_recovery():
    print("Verifying Recovery...")
    
    # 1. Clean Slate
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
        
    # 2. Start DB 1, Write Data
    print("Step 1: Writing data...")
    db1 = HBDB()
    tx = db1.transaction()
    tx.set("persist_key", "I survived!")
    tx.commit()
    
    # "Close" DB1 (just discard object)
    del db1
    
    # 3. Start DB 2 (Should recover)
    print("Step 2: Restarting DB...")
    db2 = HBDB()
    
    # 4. Read Data
    tx2 = db2.transaction()
    val = tx2.get("persist_key")
    
    if val == "I survived!":
        print(f"✅ SUCCESS: Recovered value: '{val}'")
    else:
        print(f"❌ FAILURE: Expected 'I survived!', got '{val}'")
        exit(1)

if __name__ == "__main__":
    verify_recovery()
