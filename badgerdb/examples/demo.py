#!/usr/bin/env python3
"""
BadgerDB Demo - Distributed SQL with Deterministic Transactions

Demonstrates:
1. Basic SQL operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
2. Calvin-style deterministic execution
3. MVCC storage
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from badgerdb.database import BadgerDB
from badgerdb.config import Config


def main():
    print("=" * 60)
    print("BadgerDB Demo - Calvin-Style Deterministic Transactions")
    print("=" * 60)

    # Create database with default config
    config = Config(num_shards=4)

    with BadgerDB(config) as db:
        print("\n1. CREATE TABLE")
        print("-" * 40)
        result = db.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER
            )
        """)
        print(f"   Result: success={result.success}, error={result.error}")

        print("\n2. INSERT rows")
        print("-" * 40)

        # Insert using SQL
        result = db.execute("INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)")
        print(f"   Alice: success={result.success}, affected={result.affected_rows}")

        result = db.execute("INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)")
        print(f"   Bob: success={result.success}, affected={result.affected_rows}")

        # Insert using convenience method
        success = db.insert('users', {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35})
        print(f"   Charlie: success={success}")

        print("\n3. SELECT all rows")
        print("-" * 40)
        result = db.execute("SELECT * FROM users")
        print(f"   Found {len(result.rows)} rows:")
        for row in result.rows:
            print(f"      {row}")

        print("\n4. SELECT with WHERE clause")
        print("-" * 40)
        result = db.execute("SELECT name, email FROM users WHERE id = 2")
        print(f"   User with id=2: {result.rows}")

        # Using convenience method
        rows = db.select('users', columns=['name', 'age'], where={'id': 1})
        print(f"   User with id=1: {rows}")

        print("\n5. UPDATE rows")
        print("-" * 40)
        result = db.execute("UPDATE users SET age = 31 WHERE id = 1")
        print(f"   Updated Alice's age: affected={result.affected_rows}")

        # Verify update
        result = db.execute("SELECT * FROM users WHERE id = 1")
        print(f"   Alice now: {result.rows}")

        print("\n6. DELETE rows")
        print("-" * 40)
        result = db.execute("DELETE FROM users WHERE id = 3")
        print(f"   Deleted Charlie: affected={result.affected_rows}")

        # Verify delete
        result = db.execute("SELECT * FROM users")
        print(f"   Remaining users: {len(result.rows)}")
        for row in result.rows:
            print(f"      {row}")

        print("\n7. Multiple tables")
        print("-" * 40)

        db.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product TEXT,
                amount REAL
            )
        """)

        db.execute("INSERT INTO orders (id, user_id, product, amount) VALUES (101, 1, 'Widget', 29.99)")
        db.execute("INSERT INTO orders (id, user_id, product, amount) VALUES (102, 1, 'Gadget', 49.99)")
        db.execute("INSERT INTO orders (id, user_id, product, amount) VALUES (103, 2, 'Widget', 29.99)")

        result = db.execute("SELECT * FROM orders")
        print(f"   Orders: {len(result.rows)} rows")
        for row in result.rows:
            print(f"      {row}")

        print("\n8. Database statistics")
        print("-" * 40)
        stats = db.get_stats()
        print(f"   Sequencer: {stats['sequencer']}")
        print(f"   Executor: {stats['executor']}")
        print(f"   Storage: {stats['storage']}")

        print("\n9. DROP TABLE")
        print("-" * 40)
        result = db.execute("DROP TABLE orders")
        print(f"   Dropped orders table: success={result.success}")

        # Verify it's gone
        result = db.execute("SELECT * FROM orders")
        print(f"   Select from dropped table: success={result.success}, error={result.error}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
