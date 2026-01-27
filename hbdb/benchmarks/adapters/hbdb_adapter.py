from hbdb.db import HBDB
from .base import DatabaseAdapter

class HBDBAdapter(DatabaseAdapter):
    def __init__(self, num_partitions=4):
        self.db = HBDB(num_partitions=num_partitions)

    def connect(self):
        # In-memory, no connection needed
        pass

    def close(self):
        pass

    def setup_schema(self):
        pass

    def execute(self, sql: str) -> bool:
        # Benchmark workload expects execute to run SQL or KV ops?
        # The workload.py uses adapter based on workload type.
        # If it's KV workload, it might call adapter.put/get/transfer?
        # Check workload.py implies specific methods on adapter.
        pass
    
    # Implementing methods required by Workloads
    # BankTransferWorkload calls: transfer(src, dst)
    # KVRandomWorkload calls: get(key), put(key, val)
    
    def transfer(self, src: int, dst: int) -> bool:
        txn = self.db.transaction()
        try:
            # Read balances
            # Assuming keys are just strings like "acc:1"
            k1 = f"acc:{src}"
            k2 = f"acc:{dst}"
            
            b1 = txn.get(k1)
            b2 = txn.get(k2)
            
            if b1 is None or b2 is None:
                # Initialize if missing
                if b1 is None: txn.set(k1, 1000)
                if b2 is None: txn.set(k2, 1000)
                b1 = 1000
                b2 = 1000
                
            txn.set(k1, b1 - 1)
            txn.set(k2, b2 + 1)
            
            return txn.commit()
        except:
            return False

    def get(self, key: str):
        txn = self.db.transaction()
        return txn.get(key)

    def put(self, key: str, val: str):
        txn = self.db.transaction()
        txn.set(key, val)
        return txn.commit()
