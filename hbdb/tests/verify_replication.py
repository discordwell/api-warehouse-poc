"""
Replication Integration Test (RF=2).
Topology:
- Coord: 9000
- Storage: 9001, 9002, 9003, 9004
- Shards: 2 (Total 4 nodes / RF 2)
  - Shard 0: 9001, 9002
  - Shard 1: 9003, 9004
"""
import subprocess
import time
import sys
import os
from hbdb.db import HBDB
from hbdb.core.topology import ClusterTopology

def test_replication():
    print("🚧 Starting Replication Test Cluster (RF=2)...")
    
    # 1. Start Storage Nodes
    ports = [9001, 9002, 9003, 9004]
    procs = []
    
    for p in ports:
        proc = subprocess.Popen([
            sys.executable, "-m", "hbdb.server.main", 
            "--role", "storage", "--port", str(p)
        ])
        procs.append(proc)
    
    storage_nodes = ",".join([f"127.0.0.1:{p}" for p in ports])
    
    # 2. Start Coordinator
    coord = subprocess.Popen([
        sys.executable, "-m", "hbdb.server.main", 
        "--role", "coordinator", 
        "--port", "9000",
        "--storage-nodes", storage_nodes,
        "--rf", "2"
    ])
    
    time.sleep(3) # Wait for startup
    
    conn_str = f"127.0.0.1:9000;{storage_nodes}"
    
    try:
        print("🔌 Connecting Client (RF=2)...")
        db = HBDB(connect_to=conn_str, rf=2)
        
        # 3. Write Data
        print("✍️  Writing data...")
        # key "shard0" -> md5 starts with... ?
        # We'll just write items and verify they are robust.
        tx = db.transaction()
        tx.set("k1", "v1") # Shard 0 or 1
        tx.set("k2", "v2") # Shard 0 or 1
        if not tx.commit():
            print("❌ Commit Failed")
            exit(1)
            
        print("✅ Data Written (Replicated)")
        
        # 4. KILL ONE NODE
        # Let's kill Node 9001 (Shard 0, Replica 1)
        print("💀 Killing Node 9001...")
        procs[0].kill()
        time.sleep(1)
        
        # 5. Read Data (Failover test)
        print("📖 Reading k1...")
        # If k1 was on Shard 0, client should failover to 9002.
        # If k1 was on Shard 1 (9003, 9004), it's unaffected.
        # Let's read both to be sure.
        
        tx = db.transaction()
        v1 = tx.get("k1")
        v2 = tx.get("k2")
        
        print(f"Read: k1={v1}, k2={v2}")
        
        if v1 == "v1" and v2 == "v2":
            print("✅ SUCCESS: Data available despite node failure!")
        else:
            print("❌ FAIL: Data missing.")
            exit(1)

    finally:
        print("🛑 Shutting down cluster...")
        for p in procs: p.kill()
        coord.kill()

if __name__ == "__main__":
    test_replication()
