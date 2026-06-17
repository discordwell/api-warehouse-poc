"""
Verify ORDER BY / LIMIT / OFFSET in the FDB-style SQL engine, and that
genuinely-unsupported SELECT clauses fail loudly.

Regression for a "silently wrong" class of bug: the parser used to drop
ORDER BY, LIMIT and OFFSET on the floor, so ``SELECT * FROM t LIMIT 1``
returned the whole table, ``ORDER BY age`` returned rows in primary-key
order, and ``SELECT COUNT(*)`` returned every raw row instead of a count.
This mirrors the fail-loud contract predicates.py established for WHERE:
an unhandled construct raises instead of returning the wrong answer.

Note: every HBDB in one process+CWD shares the WAL, so a second instance
recovers the first one's catalog. Each scenario therefore uses a uniquely
named table (the convention verify_sql_predicates.py / verify_sql_index.py
rely on).
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hbdb.db import HBDB
from hbdb.sql.engine import SQLEngine

PASS, FAIL = "✅ SUCCESS", "❌ FAILURE"


def _check(label, got, expected):
    if got == expected:
        print(f"{PASS}: {label}")
    else:
        print(f"{FAIL}: {label}: got {got!r}, expected {expected!r}")
        sys.exit(1)


def _populate(engine, table):
    engine.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    # Insert out of PK order so a missing ORDER BY can't accidentally look sorted.
    engine.execute(f"INSERT INTO {table} VALUES (5, 'Eve', 50)")
    engine.execute(f"INSERT INTO {table} VALUES (1, 'Alice', 10)")
    engine.execute(f"INSERT INTO {table} VALUES (4, 'Dave', 40)")
    engine.execute(f"INSERT INTO {table} VALUES (2, 'Bob', 20)")
    engine.execute(f"INSERT INTO {table} VALUES (3, 'Carol', 30)")


def verify_order_by(engine):
    print("Verifying ORDER BY...")

    def ids(sql):
        return [r["id"] for r in engine.execute(sql)]

    _check("ORDER BY age ASC", ids("SELECT id FROM ord ORDER BY age"), [1, 2, 3, 4, 5])
    _check("ORDER BY age DESC", ids("SELECT id FROM ord ORDER BY age DESC"), [5, 4, 3, 2, 1])
    _check("ORDER BY name (text)", ids("SELECT id FROM ord ORDER BY name"), [1, 2, 3, 4, 5])
    # ORDER BY can reference a column that is not in the SELECT list.
    names = [r["name"] for r in engine.execute("SELECT name FROM ord ORDER BY age DESC")]
    _check("ORDER BY non-selected column", names, ["Eve", "Dave", "Carol", "Bob", "Alice"])
    # ORDER BY an arithmetic expression.
    _check("ORDER BY age * -1", ids("SELECT id FROM ord ORDER BY age * -1"), [5, 4, 3, 2, 1])
    # Positional ORDER BY refers to the n-th output column (here, id).
    _check("ORDER BY 1 DESC", ids("SELECT id, name FROM ord ORDER BY 1 DESC"), [5, 4, 3, 2, 1])


def verify_multikey(engine):
    print("\nVerifying multi-key ORDER BY...")
    engine.execute("CREATE TABLE mk (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT)")
    for i, g, n in [(1, 1, "a"), (2, 2, "b"), (3, 1, "c"), (4, 2, "d"), (5, 1, "e")]:
        engine.execute(f"INSERT INTO mk VALUES ({i}, {g}, '{n}')")
    # grp ASC, id DESC within each group.
    got = [(r["grp"], r["id"]) for r in engine.execute("SELECT id, grp FROM mk ORDER BY grp ASC, id DESC")]
    _check("grp ASC, id DESC", got, [(1, 5), (1, 3), (1, 1), (2, 4), (2, 2)])


def verify_nulls(engine):
    print("\nVerifying NULL ordering...")
    engine.execute("CREATE TABLE nul (id INTEGER PRIMARY KEY, age INTEGER)")
    engine.execute("INSERT INTO nul VALUES (1, 30)")
    engine.execute("INSERT INTO nul (id) VALUES (2)")  # age NULL
    engine.execute("INSERT INTO nul VALUES (3, 10)")

    def ids(sql):
        return [r["id"] for r in engine.execute(sql)]

    # Default: NULL is the smallest value -> first ascending, last descending.
    _check("NULL first when ASC", ids("SELECT id FROM nul ORDER BY age"), [2, 3, 1])
    _check("NULL last when DESC", ids("SELECT id FROM nul ORDER BY age DESC"), [1, 3, 2])
    # Explicit NULLS FIRST / LAST overrides the default.
    _check("ASC NULLS LAST", ids("SELECT id FROM nul ORDER BY age ASC NULLS LAST"), [3, 1, 2])
    _check("DESC NULLS FIRST", ids("SELECT id FROM nul ORDER BY age DESC NULLS FIRST"), [2, 1, 3])


def verify_limit_offset(engine):
    print("\nVerifying LIMIT / OFFSET...")

    def ids(sql):
        return [r["id"] for r in engine.execute(sql)]

    _check("LIMIT 2 (sorted)", ids("SELECT id FROM ord ORDER BY age LIMIT 2"), [1, 2])
    _check("LIMIT 0 returns nothing", ids("SELECT id FROM ord ORDER BY age LIMIT 0"), [])
    _check("LIMIT beyond table size", ids("SELECT id FROM ord ORDER BY age LIMIT 100"), [1, 2, 3, 4, 5])
    _check("OFFSET 2", ids("SELECT id FROM ord ORDER BY age LIMIT 100 OFFSET 2"), [3, 4, 5])
    _check("LIMIT 2 OFFSET 1", ids("SELECT id FROM ord ORDER BY age LIMIT 2 OFFSET 1"), [2, 3])
    _check("OFFSET past end", ids("SELECT id FROM ord ORDER BY age LIMIT 5 OFFSET 99"), [])
    # LIMIT without ORDER BY still caps the row count (order is unspecified).
    _check("bare LIMIT caps count", len(engine.execute("SELECT id FROM ord LIMIT 3")), 3)


def verify_numeric_text_coercion(engine):
    """ORDER BY uses the same number/string coercion as WHERE, so a TEXT
    column holding numeric strings sorts numerically, not lexicographically."""
    print("\nVerifying numeric/text sort coercion...")
    engine.execute("CREATE TABLE codes (id INTEGER PRIMARY KEY, code TEXT)")
    for i, c in [(1, "10"), (2, "9"), (3, "100"), (4, "2")]:
        engine.execute(f"INSERT INTO codes VALUES ({i}, '{c}')")
    got = [r["code"] for r in engine.execute("SELECT code FROM codes ORDER BY code")]
    _check("numeric strings sort numerically", got, ["2", "9", "10", "100"])


def verify_with_where_and_index(engine):
    """ORDER BY / LIMIT must compose with WHERE and with index selection
    (the optimizer now reaches the scan through the Project/Sort/Limit wrappers)."""
    print("\nVerifying ORDER BY + WHERE + index...")
    engine.execute("CREATE TABLE idx (id INTEGER PRIMARY KEY, age INTEGER)")
    engine.execute("CREATE INDEX idx_age ON idx (age)")
    for i, a in [(1, 30), (2, 25), (3, 30), (4, 35), (5, 30)]:
        engine.execute(f"INSERT INTO idx VALUES ({i}, {a})")
    # WHERE age = 30 uses the index; ORDER BY id DESC then LIMIT 2 on top.
    got = [r["id"] for r in engine.execute("SELECT id FROM idx WHERE age = 30 ORDER BY id DESC LIMIT 2")]
    _check("indexed WHERE + ORDER BY DESC + LIMIT", got, [5, 3])


def verify_python_backend():
    """The sort/limit operators sit above the storage scan, so they are
    backend-agnostic -- but confirm parity on the pure-Python backend too."""
    print("\nVerifying ORDER BY / LIMIT on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "ord_py")
    got = [r["id"] for r in engine.execute("SELECT id FROM ord_py ORDER BY age DESC LIMIT 3 OFFSET 1")]
    _check("python backend ORDER BY DESC LIMIT/OFFSET", got, [4, 3, 2])


def verify_unsupported_fails_loud(engine):
    """Aggregates / GROUP BY / HAVING / DISTINCT must raise, never silently
    return raw rows."""
    print("\nVerifying unsupported SELECT clauses fail loudly...")
    cases = {
        "COUNT(*)": "SELECT COUNT(*) FROM ord",
        "MAX(age)": "SELECT MAX(age) FROM ord",
        "DISTINCT": "SELECT DISTINCT age FROM ord",
        "GROUP BY": "SELECT age, COUNT(*) FROM ord GROUP BY age",
        "HAVING": "SELECT age FROM ord GROUP BY age HAVING COUNT(*) > 1",
    }
    for label, sql in cases.items():
        try:
            engine.execute(sql)
        except NotImplementedError:
            print(f"{PASS}: unsupported {label} raises NotImplementedError")
        else:
            print(f"{FAIL}: unsupported {label} did not raise (silently-wrong regression)")
            sys.exit(1)


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    _populate(shared, "ord")
    verify_order_by(shared)
    verify_multikey(shared)
    verify_nulls(shared)
    verify_limit_offset(shared)
    verify_numeric_text_coercion(shared)
    verify_with_where_and_index(shared)
    verify_python_backend()
    verify_unsupported_fails_loud(shared)
    print("\nAll ORDER BY / LIMIT / OFFSET checks passed.")
