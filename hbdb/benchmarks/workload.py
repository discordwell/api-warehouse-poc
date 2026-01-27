import random
import time
from abc import ABC, abstractmethod
from typing import List, Tuple

class Workload(ABC):
    """Base class for workloads."""
    
    def __init__(self, db, num_ops: int):
        self.db = db
        self.num_ops = num_ops

    @abstractmethod
    def prepare(self):
        """Prepare data (seed database)."""
        pass

    @abstractmethod
    def run_step(self) -> bool:
        """Run a single step of the workload."""
        pass

class BankTransferWorkload(Workload):
    """
    High-contention workload: Transfers between a small set of accounts.
    Simulates the 'Improvement' case where HBDB's deterministic reordering wins.
    """
    
    def __init__(self, db, num_ops: int, num_accounts: int = 10):
        super().__init__(db, num_ops)
        self.num_accounts = num_accounts

    def prepare(self):
        print(f"Seeding {self.num_accounts} accounts with $1000 each...")
        self.db.execute("CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        self.db.clear_data()
        
        for i in range(self.num_accounts):
            self.db.execute(f"INSERT INTO accounts (id, balance) VALUES ({i}, 1000)")

    def run_step(self) -> bool:
        """Execute a transfer transaction."""
        # Pick two random accounts
        from_id = random.randint(0, self.num_accounts - 1)
        to_id = random.randint(0, self.num_accounts - 1)
        while to_id == from_id:
            to_id = random.randint(0, self.num_accounts - 1)
        
        amount = 10

        # Note: In a real SQL internal like HBDB, we might execute this as a block 
        # or separate statements. For fair comparison, we execute pure SQL.
        # Ideally this should be a transaction.
        # HBDB executes individual statements as txns unless we use a procedure, 
        # but here we simulate the client-side retry loop or server-side txn.
        
        # For this test, we emulate a client-side transaction logic:
        # READ balance, CHECK, UPDATE, UPDATE.
        # But this is hard to compare directly if one is Python API and other is SQL.
        
        # SIMPLIFICATION:
        # We will dispatch a single "Transfer" logic if supported, or raw SQL.
        # For HBDB, we rely on its deterministic nature handling the updates.
        
        # To provoke conflicts, we update the same rows.
        try:
            # We must wrap this in a transaction on the DB side if possible.
            # But adapters execute SQL strings.
            # For Cockroach: BEGIN; SELECT...; UPDATE...; COMMIT;
            # For HBDB: We don't have explicit BEGIN/COMMIT in the V2 prototype yet 
            # (it's auto-commit per execute call in the current API usually, 
            # unless we expose manual txn control).
            
            # Checking HBDB implementation: database_v2.py execute() runs one stmt as one txn.
            # It doesn't seem to support multi-statement transactions in one go easily via `execute`.
            # HOWEVER, the 'improvement' claim relies on Aria handling concurrent updates.
            # So simple updates are enough:
            # UPDATE accounts SET balance = balance + 10 WHERE id = X
            
            self.db.execute(f"UPDATE accounts SET balance = balance - {amount} WHERE id = {from_id}")
            self.db.execute(f"UPDATE accounts SET balance = balance + {amount} WHERE id = {to_id}")
            return True
        except Exception:
            return False

class KVRandomWorkload(Workload):
    """
    Low-contention workload: Random R/W over large space.
    """
    def __init__(self, db, num_ops: int, num_keys: int = 10000):
        super().__init__(db, num_ops)
        self.num_keys = num_keys

    def prepare(self):
        print(f"Creating kv table...")
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, val TEXT)")
        self.db.clear_data()
        # Pre-seed some? No, let's insert on fly or pre-seed?
        # Let's simple insert/select.

    def run_step(self) -> bool:
        key = random.randint(0, self.num_keys)
        op = random.choice(['read', 'write'])
        
        try:
            if op == 'read':
                self.db.execute(f"SELECT val FROM kv WHERE id = {key}")
            else:
                # Upsert logic roughly
                val = f"val-{random.randint(0, 100000)}"
                # Since we don't have UPSERT in generic SQL easily without support, 
                # we'll just try INSERT and ignore error, or UPDATE.
                # Let's do a simple INSERT for now to test write throughput,
                # or UPDATE if we seeded.
                # Let's do INSERT.
                self.db.execute(f"INSERT INTO kv (id, val) VALUES ({key}, '{val}')")
            return True
        except Exception:
            # duplicate key or something
            return False
