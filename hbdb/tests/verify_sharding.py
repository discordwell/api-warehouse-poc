"""
Sharding Integration Test.
Launches Cluster:
- Coordinator: 9000
- Storage 1: 9001
- Storage 2: 9002
"""
import subprocess
import time
import sys
import os
from hbdb.db import HBDB
from hbdb.core.topology import ClusterTopology

def test_sharding():
    print("🚧 Starting Sharding Test Cluster...")
    
    # 1. Start Storage Nodes
    s1 = subprocess.Popen([sys.executable, "-m", "hbdb.server.main", "--role", "storage", "--port", "9001"])
    s2 = subprocess.Popen([sys.executable, "-m", "hbdb.server.main", "--role", "storage", "--port", "9002"])
    
    # 2. Start Coordinator
    # Coordinator needs to know storage nodes
    coord = subprocess.Popen([
        sys.executable, "-m", "hbdb.server.main", 
        "--role", "coordinator", 
        "--port", "9000",
        "--storage-nodes", "127.0.0.1:9001,127.0.0.1:9002"
    ])
    
    time.sleep(2) # Wait for startup
    
    try:
        print("🔌 Connecting Client...")
        conn_str = "127.0.0.1:9000;127.0.0.1:9001,127.0.0.1:9002"
        db = HBDB(connect_to=conn_str)
        
        # 3. Analyze Topology
        topo = ClusterTopology([("127.0.0.1", 9001), ("127.0.0.1", 9002)])
        
        # Find keys that map to different shards
        key_shard1 = "key_for_9001"
        key_shard2 = "key_for_9002"
        
        # Brute force find keys
        i = 0
        while True:
            k = f"k{i}"
            node = topo.get_node_for_key(k)
            if node[1] == 9001: key_shard1 = k
            else: key_shard2 = k
            if key_shard1 != "key_for_9001" and key_shard2 != "key_for_9002":
                break
            i += 1
            
        print(f"Key [Shard 1/9001]: {key_shard1}")
        print(f"Key [Shard 2/9002]: {key_shard2}")
        
        # 4. Write Data
        print("✍️  Writing data to both shards...")
        tx = db.transaction()
        tx.set(key_shard1, "val1")
        tx.set(key_shard2, "val2")
        if tx.commit():
            print("✅ Commit Successful")
        else:
            print("❌ Commit Failed")
            exit(1)
            
        # 5. Read Back
        print("📖 Reading back...")
        tx = db.transaction()
        v1 = tx.get(key_shard1)
        v2 = tx.get(key_shard2)
        print(f"Got: {v1}, {v2}")
        
        if v1 == "val1" and v2 == "val2":
            print("✅ Data correctly retrieved from distributed shards.")
        else:
            print("❌ Data Mismatch!")
            exit(1)
            
        # 6. Verify Isolation (Optional)
        # We could inspect internal logs of servers but unnecessary if end-to-end works.
        
    finally:
        print("🛑 Shutting down cluster...")
        s1.kill()
        s2.kill()
        coord.kill()

if __name__ == "__main__":
    test_sharding()
