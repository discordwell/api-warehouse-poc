"""
Verify Durability (WAL).
Ensures that committed transactions are written to the transaction log file.
"""
import os
import json
from hbdb.db import HBDB

LOG_FILE = "transaction.log"

def verify_durability():
    print("Verifying Durability (WAL)...")
    
    # Clean prev log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
        
    db = HBDB()
    
    # 1. Commit a transaction
    tx = db.transaction()
    tx.set("durable_key", "persistent_value")
    tx.set("num", 123)
    assert tx.commit() == True, "Commit should succeed"
    print("Transaction committed.")
    
    # 2. Verify Log File
    if not os.path.exists(LOG_FILE):
        print("❌ FAILURE: No transaction log file found!")
        exit(1)
        
    print("Log file exists. Checking content...")
    
    found = False
    with open(LOG_FILE, "r") as f:
        for line in f:
            entry = json.loads(line)
            ops = entry.get("ops", {})
            if "durable_key" in ops and ops["durable_key"] == "persistent_value":
                found = True
                print(f"✅ Found transaction in log: {entry}")
                break
    
    if found:
        print("✅ SUCCESS: Transaction persisted to disk via Sequencer.")
    else:
        print("❌ FAILURE: Transaction not found in log file!")
        exit(1)

if __name__ == "__main__":
    verify_durability()
