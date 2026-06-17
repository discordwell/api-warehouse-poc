"""
Chaos Workload.
Continuously increments a counter in HBDB.
Used by chaos_monkey.py.
"""
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hbdb.db import HBDB

def run():
    db = HBDB()
    
    # Get current value
    tx = db.transaction()
    val = tx.get("counter")
    if val is None: val = 0
    start_val = val
    
    print(f"[Workload] Starting at counter={start_val}")
    
    current = start_val
    while True:
        current += 1
        tx = db.transaction()
        tx.set("counter", current)
        if tx.commit():
            # Update external truth file
            # We use atomic write to truth file to ensure it's valid
            with open("truth.txt.tmp", "w") as f:
                f.write(str(current))
            os.rename("truth.txt.tmp", "truth.txt")
            
            # Periodically snapshot to test that too
            if current % 100 == 0:
                print(f"[Workload] Taking snapshot at {current}")
                db.take_snapshot()
            
            if current % 10 == 0:
                print(f"[Workload] Committed {current}")
        
        # Go fast, but let IO happen
        time.sleep(0.001)

if __name__ == "__main__":
    run()
