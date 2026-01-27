"""
SQL Torture Test Suite: HBDB vs CockroachDB

Tests:
1. High-Contention Bank Transfer (hot account)
2. Wide Table Scan with Filter
3. Multi-Table JOIN
4. Batch INSERT throughput
5. UPDATE Storm (many concurrent updates to same row)
"""
import time
import threading
import random
from dataclasses import dataclass
from typing import List, Callable

@dataclass
class BenchmarkResult:
    name: str
    db_name: str
    ops: int
    duration: float
    errors: int
    
    @property
    def ops_per_sec(self):
        return self.ops / self.duration if self.duration > 0 else 0

class HBDBRunner:
    def __init__(self):
        from hbdb.db import HBDB
        from hbdb.sql.engine import SQLEngine
        from hbdb.sql.types import Schema, Column, DataType
        
        self.db = HBDB()
        self.engine = SQLEngine(self.db)
        self.Schema = Schema
        self.Column = Column
        self.DataType = DataType
        
    def setup(self):
        # Create tables
        self.engine.create_table("accounts", self.Schema(columns=[
            self.Column("id", self.DataType.INTEGER, primary_key=True),
            self.Column("balance", self.DataType.INTEGER),
            self.Column("name", self.DataType.STRING)
        ]))
        self.engine.create_table("orders", self.Schema(columns=[
            self.Column("order_id", self.DataType.INTEGER, primary_key=True),
            self.Column("account_id", self.DataType.INTEGER),
            self.Column("amount", self.DataType.INTEGER)
        ]))
        
        # Seed data
        for i in range(1, 101):
            self.engine.execute(f"INSERT INTO accounts (id, balance, name) VALUES ({i}, 1000, 'User{i}')")
        for i in range(1, 51):
            self.engine.execute(f"INSERT INTO orders (order_id, account_id, amount) VALUES ({i}, {(i % 100) + 1}, {random.randint(10, 500)})")
    
    def execute(self, sql: str) -> bool:
        try:
            self.engine.execute(sql)
            return True
        except:
            return False
    
    def query(self, sql: str):
        return self.engine.execute(sql)

class CockroachRunner:
    def __init__(self):
        import psycopg2
        self.conn = psycopg2.connect(
            dbname="defaultdb",
            user="root",
            host="localhost",
            port=26257
        )
        self.conn.autocommit = True
        
    def setup(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS orders")
        cur.execute("DROP TABLE IF EXISTS accounts")
        cur.execute("""
            CREATE TABLE accounts (
                id INT PRIMARY KEY,
                balance INT,
                name STRING
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                order_id INT PRIMARY KEY,
                account_id INT,
                amount INT
            )
        """)
        for i in range(1, 101):
            cur.execute(f"INSERT INTO accounts (id, balance, name) VALUES ({i}, 1000, 'User{i}')")
        for i in range(1, 51):
            cur.execute(f"INSERT INTO orders (order_id, account_id, amount) VALUES ({i}, {(i % 100) + 1}, {random.randint(10, 500)})")
        cur.close()
    
    def execute(self, sql: str) -> bool:
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            return False
    
    def query(self, sql: str):
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows

def run_benchmark(name: str, db_name: str, runner, workload_fn: Callable, duration: float = 5.0, threads: int = 8) -> BenchmarkResult:
    """Run a benchmark workload for the specified duration."""
    ops = 0
    errors = 0
    stop = False
    lock = threading.Lock()
    
    def worker():
        nonlocal ops, errors
        local_ops = 0
        local_errors = 0
        while not stop:
            if workload_fn(runner):
                local_ops += 1
            else:
                local_errors += 1
        with lock:
            ops += local_ops
            errors += local_errors
    
    workers = [threading.Thread(target=worker) for _ in range(threads)]
    start = time.time()
    for w in workers:
        w.start()
    
    time.sleep(duration)
    stop = True
    
    for w in workers:
        w.join()
    
    elapsed = time.time() - start
    return BenchmarkResult(name, db_name, ops, elapsed, errors)

# Workloads
def workload_bank_transfer(runner) -> bool:
    """High contention: Transfer between hot accounts 1-10"""
    src = random.randint(1, 10)
    dst = random.randint(1, 10)
    if src == dst:
        dst = (src % 10) + 1
    return runner.execute(f"UPDATE accounts SET balance = balance - 1 WHERE id = {src}") and \
           runner.execute(f"UPDATE accounts SET balance = balance + 1 WHERE id = {dst}")

def workload_table_scan(runner) -> bool:
    """Wide scan with filter"""
    threshold = random.randint(500, 1500)
    runner.query(f"SELECT * FROM accounts WHERE balance > {threshold}")
    return True

def workload_join(runner) -> bool:
    """JOIN query"""
    runner.query("SELECT * FROM accounts JOIN orders ON id = account_id")
    return True

def workload_insert(runner) -> bool:
    """Batch insert"""
    oid = random.randint(10000, 99999)
    return runner.execute(f"INSERT INTO orders (order_id, account_id, amount) VALUES ({oid}, 1, 100)")

def main():
    print("=" * 70)
    print("SQL TORTURE TEST: HBDB vs CockroachDB")
    print("=" * 70)
    
    results = []
    
    # Test HBDB
    print("\n[HBDB] Setting up...")
    hbdb = HBDBRunner()
    hbdb.setup()
    
    print("[HBDB] Running Bank Transfer (High Contention)...")
    results.append(run_benchmark("Bank Transfer", "HBDB", hbdb, workload_bank_transfer, 5.0, 8))
    
    print("[HBDB] Running Table Scan...")
    results.append(run_benchmark("Table Scan", "HBDB", hbdb, workload_table_scan, 5.0, 8))
    
    print("[HBDB] Running JOIN Query...")
    results.append(run_benchmark("JOIN Query", "HBDB", hbdb, workload_join, 5.0, 8))
    
    print("[HBDB] Running INSERT Storm...")
    results.append(run_benchmark("INSERT Storm", "HBDB", hbdb, workload_insert, 5.0, 8))
    
    # Test CockroachDB
    print("\n[CockroachDB] Setting up...")
    try:
        crdb = CockroachRunner()
        crdb.setup()
        
        print("[CockroachDB] Running Bank Transfer (High Contention)...")
        results.append(run_benchmark("Bank Transfer", "CockroachDB", crdb, workload_bank_transfer, 5.0, 8))
        
        print("[CockroachDB] Running Table Scan...")
        results.append(run_benchmark("Table Scan", "CockroachDB", crdb, workload_table_scan, 5.0, 8))
        
        print("[CockroachDB] Running JOIN Query...")
        results.append(run_benchmark("JOIN Query", "CockroachDB", crdb, workload_join, 5.0, 8))
        
        print("[CockroachDB] Running INSERT Storm...")
        results.append(run_benchmark("INSERT Storm", "CockroachDB", crdb, workload_insert, 5.0, 8))
    except Exception as e:
        print(f"[CockroachDB] Connection failed: {e}")
    
    # Print results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"{'Test':<20} {'Database':<15} {'Ops/sec':>12} {'Total Ops':>12} {'Errors':>8}")
    print("-" * 70)
    
    for r in results:
        print(f"{r.name:<20} {r.db_name:<15} {r.ops_per_sec:>12.1f} {r.ops:>12} {r.errors:>8}")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
