import time
import threading
import argparse
from benchmarks.adapters.hbdb_adapter import HBDBAdapter
from benchmarks.adapters.crdb_adapter import CRDBAdapter
from benchmarks.workload import BankTransferWorkload, KVRandomWorkload

def run_worker(workload_cls, adapter_cls, duration, results, index):
    """Worker thread function."""
    # Each worker needs its own adapter instance? 
    # For HBDB (in-process), sharing instance is tricky depending on locking.
    # HBDB is thread-safe internally? Yes, Coordinator handles it.
    # So we can share the DB adapter instance for HBDB? 
    # Actually, DatabaseV2.execute is thread safe.
    # But for CRDB, we need separate connections per thread.
    
    # We will instantiate adapter once per thread for CRDB, 
    # but for HBDB we MUST share the same instance to see same data!
    
    # Wait, if we instantiate HBDBAdapter inside worker, we get NEW database!
    # That's wrong.
    pass

def main():
    parser = argparse.ArgumentParser(description="HBDB vs CockroachDB Benchmark")
    parser.add_argument('--db', choices=['hbdb', 'crdb'], required=True)
    parser.add_argument('--workload', choices=['transfer', 'kv'], required=True)
    parser.add_argument('--threads', type=int, default=1)
    parser.add_argument('--duration', type=int, default=10)
    args = parser.parse_args()

    print(f"Running {args.workload} benchmark on {args.db} with {args.threads} threads for {args.duration}s")

    # Initialize Adapter
    main_adapter = None
    if args.db == 'hbdb':
        main_adapter = HBDBAdapter()
    else:
        main_adapter = CRDBAdapter()
        
    main_adapter.connect()
    main_adapter.setup_schema()

    # Select workload
    workload_cls = BankTransferWorkload if args.workload == 'transfer' else KVRandomWorkload
    
    # Prepare data
    prep_workload = workload_cls(main_adapter, 0)
    prep_workload.prepare()
    
    # Shared counters
    success_count = 0
    failure_count = 0
    lock = threading.Lock()
    running = True

    def worker_func():
        nonlocal success_count, failure_count
        
        # For CRDB, get new connection
        local_adapter = main_adapter
        if args.db == 'crdb':
            local_adapter = CRDBAdapter()
            local_adapter.connect()
        # HBDB sharing is fine
            
        # For HBDB, use main_adapter (shared)
        
        wl = workload_cls(local_adapter, 0)
        
        l_success = 0
        l_fail = 0
        
        while running:
            if wl.run_step():
                l_success += 1
            else:
                l_fail += 1
        
        if args.db == 'crdb':
            local_adapter.close()
            
        with lock:
            success_count += l_success
            failure_count += l_fail

    # Start threads
    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=worker_func)
        t.start()
        threads.append(t)

    # Run for duration
    time.sleep(args.duration)
    running = False
    
    for t in threads:
        t.join()

    main_adapter.close()

    total_ops = success_count + failure_count
    tps = total_ops / args.duration
    
    print("\n--- Results ---")
    print(f"Total Operations: {total_ops}")
    print(f"Successful:       {success_count}")
    print(f"Failed:           {failure_count}")
    print(f"Throughput:       {tps:.2f} ops/sec")
    print("---------------")

if __name__ == "__main__":
    main()
