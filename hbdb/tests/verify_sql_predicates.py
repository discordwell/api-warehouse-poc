"""
Verify WHERE-predicate evaluation and column projection in the FDB-style
SQL engine.

Regression for a data-loss bug: the executors only understood ``col =
literal`` and treated every other predicate as "matches all rows", so
``WHERE x > 5`` returned the whole table and -- far worse -- ``DELETE ...
WHERE x > 5`` wiped it. This exercises comparison operators, AND/OR/NOT,
IS [NOT] NULL, IN, projection, and confirms unsupported predicates now
fail loudly instead of matching everything.

Note: every HBDB in one process+CWD shares the WAL, so a second instance
recovers the first one's catalog. Each scenario therefore uses a uniquely
named table (the same convention verify_sql_index.py relies on).
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlglot
from hbdb.db import HBDB
from hbdb.sql.engine import SQLEngine
from hbdb.sql.predicates import evaluate

PASS, FAIL = "✅ SUCCESS", "❌ FAILURE"


def _check(label, got, expected):
    if got == expected:
        print(f"{PASS}: {label}")
    else:
        print(f"{FAIL}: {label}: got {got!r}, expected {expected!r}")
        sys.exit(1)


def _populate(engine, table):
    engine.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    engine.execute(f"INSERT INTO {table} VALUES (1, 'Alice', 30)")
    engine.execute(f"INSERT INTO {table} VALUES (2, 'Bob', 25)")
    engine.execute(f"INSERT INTO {table} VALUES (3, 'Charlie', 35)")
    engine.execute(f"INSERT INTO {table} VALUES (4, 'Dave', 20)")
    engine.execute(f"INSERT INTO {table} (id, name) VALUES (5, 'Eve')")  # age IS NULL


def verify_comparisons(engine):
    print("Verifying comparison operators...")

    def names(where):
        return sorted(r["name"] for r in engine.execute(f"SELECT * FROM cmp WHERE {where}"))

    _check("age > 30", names("age > 30"), ["Charlie"])
    _check("age < 25", names("age < 25"), ["Dave"])
    _check("age >= 30", names("age >= 30"), ["Alice", "Charlie"])
    _check("age <= 25", names("age <= 25"), ["Bob", "Dave"])
    _check("age != 30", names("age != 30"), ["Bob", "Charlie", "Dave"])
    _check("age <> 30", names("age <> 30"), ["Bob", "Charlie", "Dave"])
    _check("age = 30", names("age = 30"), ["Alice"])
    _check("name = 'Bob'", names("name = 'Bob'"), ["Bob"])


def verify_compound(engine):
    print("\nVerifying AND / OR / NOT / parentheses...")

    def names(where):
        return sorted(r["name"] for r in engine.execute(f"SELECT * FROM cmp WHERE {where}"))

    _check("age >= 30 AND age < 35", names("age >= 30 AND age < 35"), ["Alice"])
    _check("age < 25 OR age > 30", names("age < 25 OR age > 30"), ["Charlie", "Dave"])
    _check("NOT age = 30", names("NOT age = 30"), ["Bob", "Charlie", "Dave"])
    _check("(age = 25 OR age = 35) AND name != 'Bob'",
           names("(age = 25 OR age = 35) AND name != 'Bob'"), ["Charlie"])


def verify_null_and_in(engine):
    print("\nVerifying IS NULL / IS NOT NULL / IN...")

    def names(where):
        return sorted(r["name"] for r in engine.execute(f"SELECT * FROM cmp WHERE {where}"))

    _check("age IS NULL", names("age IS NULL"), ["Eve"])
    _check("age IS NOT NULL", names("age IS NOT NULL"), ["Alice", "Bob", "Charlie", "Dave"])
    _check("age IN (20, 35)", names("age IN (20, 35)"), ["Charlie", "Dave"])
    _check("name IN ('Alice', 'Eve')", names("name IN ('Alice', 'Eve')"), ["Alice", "Eve"])
    # NOT IN excludes the matches and the NULL-age row (UNKNOWN, not a match).
    _check("age NOT IN (20, 35)", names("age NOT IN (20, 35)"), ["Alice", "Bob"])
    # The classic SQL gotcha: a NULL anywhere in a NOT IN list makes every
    # row UNKNOWN, so it matches nothing (regression guard for IN three-valued logic).
    _check("age NOT IN (25, NULL) matches nothing", names("age NOT IN (25, NULL)"), [])
    # NULL never matches an ordering comparison.
    _check("age > 0 excludes NULL", names("age > 0"), ["Alice", "Bob", "Charlie", "Dave"])
    # Incomparable operands (TEXT column vs numeric literal) are UNKNOWN, not
    # a lexicographic guess, so they match nothing.
    _check("name > 5 (incomparable) matches nothing", names("name > 5"), [])


def verify_projection(engine):
    print("\nVerifying column projection...")
    one = engine.execute("SELECT name FROM cmp WHERE id = 1")[0]
    _check("SELECT name -> only name", sorted(one.keys()), ["name"])
    two = engine.execute("SELECT id, age FROM cmp WHERE id = 1")[0]
    _check("SELECT id, age -> id+age", sorted(two.keys()), ["age", "id"])
    star = engine.execute("SELECT * FROM cmp WHERE id = 1")[0]
    _check("SELECT * -> all columns", sorted(star.keys()), ["age", "id", "name"])


def verify_no_data_loss():
    """The headline regression, on both the default (native) and pure-Python
    backends, since DELETE/UPDATE go through the storage scan + tombstone path."""
    for force_python in (False, True):
        mode = "python" if force_python else "native/default"
        table = "del_py" if force_python else "del_nat"
        print(f"\nVerifying UPDATE/DELETE do not over-match ({mode})...")

        engine = SQLEngine(HBDB(force_python=force_python))
        _populate(engine, table)

        def ids():
            return sorted(r["id"] for r in engine.execute(f"SELECT * FROM {table}"))

        def names():
            return sorted(r["name"] for r in engine.execute(f"SELECT * FROM {table}"))

        # DELETE with a predicate that matches nothing must delete nothing.
        engine.execute(f"DELETE FROM {table} WHERE age > 100")
        _check(f"DELETE WHERE age > 100 ({mode})", ids(), [1, 2, 3, 4, 5])

        # DELETE a range subset (Dave=20; Eve is NULL and must not match).
        engine.execute(f"DELETE FROM {table} WHERE age < 25")
        _check(f"DELETE WHERE age < 25 ({mode})", ids(), [1, 2, 3, 5])

        # UPDATE a range subset only (Charlie=35).
        engine.execute(f"UPDATE {table} SET name = 'OLD' WHERE age >= 35")
        _check(f"UPDATE WHERE age >= 35 touched only Charlie ({mode})",
               names(), ["Alice", "Bob", "Eve", "OLD"])


def verify_unsupported_fails_loud():
    """An unhandled predicate must raise, never silently match every row
    (that fail-open behavior was the root of the data-loss bug)."""
    print("\nVerifying unsupported predicates fail loudly...")
    cases = {
        "LIKE (predicate)": "SELECT * FROM t WHERE name LIKE 'A%'",
        "% / MOD (operand)": "SELECT * FROM t WHERE x = 10 % 3",
    }
    for label, sql in cases.items():
        where = sqlglot.parse_one(sql).args["where"]
        try:
            evaluate(where, {"name": "Alice", "x": 1})
        except NotImplementedError:
            print(f"{PASS}: unsupported {label} raises NotImplementedError")
        else:
            print(f"{FAIL}: unsupported {label} did not raise (fail-open regression)")
            sys.exit(1)


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    _populate(shared, "cmp")
    verify_comparisons(shared)
    verify_compound(shared)
    verify_null_and_in(shared)
    verify_projection(shared)
    verify_no_data_loss()
    verify_unsupported_fails_loud()
    print("\nAll predicate/projection checks passed.")
