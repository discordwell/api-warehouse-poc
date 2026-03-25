"""
Chaos Monkey.
Orchestrates the chaos test.
"""
import subprocess
import time
import signal
import os
import random
import sys

WORKLOAD_SCRIPT = "tests/chaos_workload.py"
TRUTH_FILE = "truth.txt"

def run_chaos():
    print("🐵 Starting Chaos Monkey...")
    
    # Cleanup
    if os.path.exists(TRUTH_FILE): os.remove(TRUTH_FILE)
    if os.path.exists("transaction.log"): os.remove("transaction.log")
    if os.path.exists("snapshot.bin"): os.remove("snapshot.bin")
    
    start_time = time.time()
    crashes = 0
    
    # Run for 30 seconds
    while time.time() - start_time < 30:
        print(f"\n[Chaos] Iteration {crashes + 1}...")
        
        # 1. Start Workload
        p = subprocess.Popen([sys.executable, WORKLOAD_SCRIPT])
        
        # 2. Let it run for random time
        sleep_time = random.uniform(0.5, 2.0)
        time.sleep(sleep_time)
        
        # 3. KILL IT
        print(f"[Chaos] 🔪 Killing PID {p.pid} after {sleep_time:.2f}s")
        p.send_signal(signal.SIGKILL)
        try:
            p.wait(timeout=1)
        except:
            pass
            
        crashes += 1
        
        # 4. Verify Consistency
        # We start a checker process (or just check manually)
        # Check integrity of log and snapshot
        
        # Read Truth
        if os.path.exists(TRUTH_FILE):
            with open(TRUTH_FILE, "r") as f:
                expected = int(f.read().strip())
        else:
            expected = 0
            
        # Read DB
        # We spawn a helper to read DB state because we can't easily open DB in this process 
        # (NativeBackend singleton issues or lock files?)
        # Actually HBDB logic is embedded, safe to instantiate transiently?
        # Better to verify sequentially.
        
        # We'll rely on the Workload script's startup logic to print its start value
        # Or checking logically.
        # "Durability Guarantee": DB Counter >= Expected Counter
        # Why >= ? Because we update Truth AFTER commit.
        # If we crash between Commit and truth update, DB > Truth.
        # If DB < Truth, we LOST data.
        
        # Let's verify specifically.
        verify_cmd = [sys.executable, "-c", 
                      "from hbdb.db import HBDB; db=HBDB(); tx=db.transaction(); print(tx.get('counter'))"]
        
        try:
            result = subprocess.check_output(verify_cmd, stderr=subprocess.STDOUT).decode().strip()
            # Result might contain logging output "[HBDB] ..."
            # Last line should be the number
            last_line = result.splitlines()[-1]
            if last_line == "None": 
                db_val = 0
            else:
                db_val = int(last_line)
                
            print(f"[Chaos] Verification: Truth={expected}, DB={db_val}")
            
            if db_val < expected:
                print("❌ DATA LOSS DETECTED! Stopping.")
                print(f"Expected at least {expected}, got {db_val}")
                exit(1)
            elif db_val > expected + 1:
                # Should be close
                print(f"⚠️  DB is significantly ahead ({db_val} > {expected}). Suspicious but durable.")
            else:
                print("✅ Consistency OK.")
                
        except Exception as e:
            print(f"❌ Verification Failed to run: {e}")
            exit(1)

    print(f"\n🐵 Chaos Test Passed! Survived {crashes} crashes with NO data loss.")

if __name__ == "__main__":
    run_chaos()
