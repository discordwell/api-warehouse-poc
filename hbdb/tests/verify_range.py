"""
Verify Range-Based Conflict Detection.
Tests that standard read-write conflicts are detected, AND new "Phantom Read" conflicts
(inserting into a scanned range) are detected.
"""
from hbdb.db import HBDB
import time

def verify_range_conflict():
    print("Verifying Range Conflict Detection...")
    db = HBDB(num_partitions=1)
    
    # Init data
    tx = db.transaction()
    tx.set("k1", "v1")
    tx.set("k3", "v3")
    tx.commit()
    
    # ---------------------------------------------------------
    # Scenario 1: T1 scans [k1, k4) and writes an aggregate based
    # on what it saw. Writer inserts k2 into the range concurrently.
    # This IS a Phantom Read conflict in Serializable mode.
    #
    # Note: T1 must WRITE for validation to trigger. Read-only
    # transactions serialize at their read timestamp (FDB-style)
    # and always commit trivially.
    # ---------------------------------------------------------

    # T1: Scans range, then writes a value derived from the scan
    t1 = db.transaction()
    res = t1.scan("k1", "k4")
    print(f"T1 scanned keys: {[k for k, v in res]}")
    t1.set("range_count", len(res))

    # T2: Inserts k2 (middle of range)
    t2 = db.transaction()
    t2.set("k2", "v2-phantom")
    assert t2.commit() == True, "T2 should commit"
    print("T2 committed insert of k2")

    # T1: Try to commit
    # Should FAIL because the range [k1, k4) was modified (phantom k2 appeared)
    # The Resolver should see that 'k2' was written with timestamp > t1.read_ts
    # and 'k2' falls within t1._read_ranges.
    success = t1.commit()

    if success:
        print("❌ FAILURE: T1 committed despite Phantom Read!")
        exit(1)
    else:
        print("✅ SUCCESS: T1 aborted due to Phantom Read (Range Conflict).")

    # ---------------------------------------------------------
    # Scenario 2: T3 scans [k1, k2) and writes. Writer inserts k3.
    # This is NOT a conflict (write lands outside the scanned range).
    # ---------------------------------------------------------

    # T3: Scans [k1, k2), writes an aggregate
    t3 = db.transaction()
    res3 = t3.scan("k1", "k2")
    t3.set("narrow_count", len(res3))

    # T4: Inserts k3 (outside range)
    t4 = db.transaction()
    t4.set("k3", "v3-new")
    assert t4.commit() == True

    # T3: Commit
    success = t3.commit()
    if success:
        print("✅ SUCCESS: T3 committed (non-overlapping write).")
    else:
        print("❌ FAILURE: T3 aborted unnecessarily!")
        exit(1)

    # ---------------------------------------------------------
    # Scenario 3: Read-only scan over a modified range still
    # commits — it serializes at its read timestamp.
    # ---------------------------------------------------------

    t5 = db.transaction()
    t5.scan("k1", "k4")

    t6 = db.transaction()
    t6.set("k2", "v2-again")
    assert t6.commit() == True

    success = t5.commit()
    if success:
        print("✅ SUCCESS: Read-only T5 committed at its snapshot.")
    else:
        print("❌ FAILURE: Read-only T5 should not abort!")
        exit(1)

if __name__ == "__main__":
    verify_range_conflict()
