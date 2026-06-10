"""
Verify corrupt-WAL recovery.
Commits transactions, injects corrupt lines into the WAL (torn write,
wrong-shape JSON), then restarts and checks that recovery skips the
corrupt entries, keeps replaying past them, and applies every valid one.
"""
from hbdb.db import HBDB
import os

LOG_FILE = "transaction.log"
SNAPSHOT_FILE = "snapshot.bin"

def verify_wal_corruption():
    print("Verifying corrupt-WAL recovery...")

    # 1. Clean slate
    for path in (LOG_FILE, SNAPSHOT_FILE):
        if os.path.exists(path):
            os.remove(path)

    # 2. Write good transactions
    print("Step 1: Writing data...")
    db1 = HBDB()
    for i in range(3):
        tx = db1.transaction()
        tx.set(f"good{i}", f"value{i}")
        assert tx.commit(), f"Commit {i} should succeed"
    del db1

    # 3. Inject corruption, then one more valid entry after it
    print("Step 2: Corrupting the log...")
    with open(LOG_FILE, "a") as f:
        f.write('{"ts": "not-an-int", "ops": {"evil": 1}}\n')  # wrong ts type
        f.write('{"ts": 98, "ops": [1, 2, 3]}\n')              # wrong ops type
        f.write('{"no_ts_here": true}\n')                      # missing keys
        f.write('this is not json at all\n')                   # garbage
        f.write('{"ts": 50, "ops": {"late": "entry"}}\n')      # valid, after corruption
        f.write('{"ts": 99, "ops": {"torn"')                   # torn final line

    # 4. Restart: recovery must not crash and must keep all valid data
    print("Step 3: Restarting DB...")
    db2 = HBDB()
    tx = db2.transaction()

    for i in range(3):
        val = tx.get(f"good{i}")
        if val != f"value{i}":
            print(f"❌ FAILURE: good{i} = {val!r}, expected 'value{i}'")
            exit(1)

    if tx.get("late") != "entry":
        print(f"❌ FAILURE: valid entry after corruption was not replayed")
        exit(1)

    if tx.get("evil") is not None:
        print("❌ FAILURE: corrupt entry was applied!")
        exit(1)

    # Clock must sit at the highest replayed ts so new commits stamp above it
    read_ts = db2.resolver.get_read_timestamp()
    if read_ts < 50:
        print(f"❌ FAILURE: clock at {read_ts}, expected >= 50")
        exit(1)

    print("✅ SUCCESS: Recovery skipped corrupt WAL lines and kept all valid data.")

if __name__ == "__main__":
    verify_wal_corruption()
