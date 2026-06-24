"""
Verify expression / aliased column projection in single-table SELECTs.

Regression for a "silently wrong" bug: only a *plain* column list
(``SELECT a, b``) or ``SELECT *`` was projected. An aliased or computed
SELECT item -- ``SELECT name AS who``, ``SELECT age * 2 AS d`` -- fell
through with no projection at all, so the engine streamed *every* column and
the result row had no ``who`` / ``d`` key. That is exactly the "return the
wrong rows" failure the rest of the engine is hardened against; the JOIN path
already projected expressions correctly, the single-table path did not.

This exercises aliases, arithmetic expressions, ``SELECT *`` mixed with
expressions, and their composition with DISTINCT / ORDER BY / LIMIT, and
confirms unsupported operands (``SUBSTRING(x, ...)``) and duplicate output
names still fail loud rather than silently leaking the raw row. (Scalar
functions like ``UPPER`` are now implemented; see ``verify_sql_functions.py``.)

Note: every HBDB in one process+CWD shares the WAL, so a second instance
recovers the first one's catalog. Each scenario therefore uses a uniquely
named table (the convention the other verify_sql_*.py suites rely on).
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


def _expect_raises(label, fn, exc=Exception):
    try:
        fn()
    except exc:
        print(f"{PASS}: {label} raised {exc.__name__}")
    except Exception as other:  # noqa: BLE001 -- want the specific type
        print(f"{FAIL}: {label}: raised {type(other).__name__}, expected "
              f"{exc.__name__}")
        sys.exit(1)
    else:
        print(f"{FAIL}: {label}: did not raise (silently-wrong regression)")
        sys.exit(1)


def _populate(engine, table):
    engine.execute(
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    # Insert out of PK order so an accidental no-op projection can't look right.
    engine.execute(f"INSERT INTO {table} VALUES (3, 'Charlie', 40)")
    engine.execute(f"INSERT INTO {table} VALUES (1, 'Alice', 30)")
    engine.execute(f"INSERT INTO {table} VALUES (2, 'Bob', 25)")
    engine.execute(f"INSERT INTO {table} (id, name) VALUES (4, 'Dave')")  # age NULL


def verify_alias_projection(engine):
    print("Verifying aliased-column projection...")
    rows = engine.execute("SELECT name AS who FROM proj")
    # The headline bug: only the aliased column should come back, nothing else.
    _check("alias output keys", sorted({k for r in rows for k in r}), ["who"])
    _check("alias values", sorted(r["who"] for r in rows),
           ["Alice", "Bob", "Charlie", "Dave"])


def verify_expression_projection(engine):
    print("\nVerifying expression projection...")
    rows = engine.execute("SELECT id, age * 2 AS double FROM proj WHERE id <= 3")
    by_id = {r["id"]: r for r in rows}
    _check("expr output keys", sorted({k for r in rows for k in r}),
           ["double", "id"])
    _check("age*2 for id=1", by_id[1]["double"], 60)
    _check("age*2 for id=2", by_id[2]["double"], 50)
    _check("age*2 for id=3", by_id[3]["double"], 80)
    # An expression over a NULL column is NULL (three-valued arithmetic).
    null_row = engine.execute("SELECT age + 1 AS a FROM proj WHERE id = 4")
    _check("NULL + 1 IS NULL", null_row[0]["a"], None)


def verify_unaliased_expression_name(engine):
    print("\nVerifying unaliased expression output name...")
    rows = engine.execute("SELECT age * 2 FROM proj WHERE id = 1")
    # Unaliased expressions are named by their SQL text (same rule the
    # aggregate path uses for an unaliased COUNT(*)).
    (only_key,) = rows[0].keys()
    _check("single output column", len(rows[0]), 1)
    _check("unaliased value correct", rows[0][only_key], 60)


def verify_star_with_expression(engine):
    print("\nVerifying SELECT * mixed with an expression...")
    rows = engine.execute("SELECT *, age * 2 AS double FROM proj WHERE id = 1")
    _check("star expands + keeps expr", sorted(rows[0].keys()),
           ["age", "double", "id", "name"])
    _check("star row values", (rows[0]["id"], rows[0]["name"],
                               rows[0]["age"], rows[0]["double"]),
           (1, "Alice", 30, 60))


def verify_qualified_star(engine):
    print("\nVerifying qualified star (t.*) projection...")
    # Regression: `t.*` parses as a Column whose `this` is a Star (name '*'),
    # so it used to project a bogus {'*': None} instead of expanding.
    lone = engine.execute("SELECT proj.* FROM proj WHERE id = 1")
    _check("t.* expands to all columns", sorted(lone[0].keys()),
           ["age", "id", "name"])
    _check("t.* values", (lone[0]["id"], lone[0]["name"], lone[0]["age"]),
           (1, "Alice", 30))
    _check("t.* keeps every row", len(engine.execute("SELECT proj.* FROM proj")), 4)
    # t.* mixed with an expression goes through the expression-projection path.
    mixed = engine.execute("SELECT proj.*, age * 2 AS double FROM proj WHERE id = 1")
    _check("t.* + expr", sorted(mixed[0].keys()), ["age", "double", "id", "name"])
    _check("t.* + expr value", mixed[0]["double"], 60)


def verify_distinct_over_expression(engine):
    print("\nVerifying DISTINCT over an expression...")
    # age * 0 collapses every non-NULL age to 0; NULL stays its own group.
    rows = engine.execute("SELECT DISTINCT age * 0 AS z FROM proj")
    _check("distinct expr keys", sorted({k for r in rows for k in r}), ["z"])
    _check("distinct expr values", sorted((r["z"] for r in rows),
                                          key=lambda v: (v is not None, v)),
           [None, 0])


def verify_order_by_with_projection(engine):
    print("\nVerifying ORDER BY composed with expression projection...")

    def vals(sql, col):
        return [r[col] for r in engine.execute(sql)]

    # ORDER BY the alias.
    _check("ORDER BY alias DESC",
           vals("SELECT name AS who FROM proj ORDER BY who DESC", "who"),
           ["Dave", "Charlie", "Bob", "Alice"])
    # ORDER BY the expression's output, descending (NULL last when DESC).
    _check("ORDER BY expr DESC",
           vals("SELECT age * 2 AS d FROM proj ORDER BY d DESC", "d"),
           [80, 60, 50, None])
    # ORDER BY a column that is NOT in the SELECT list (sort sits below the
    # projection, so it can still see `age`).
    _check("ORDER BY non-selected column",
           vals("SELECT name AS who FROM proj WHERE id <= 3 ORDER BY age", "who"),
           ["Bob", "Alice", "Charlie"])
    # Positional ORDER BY 1 -> the first (and only) output item.
    _check("positional ORDER BY 1 over expr",
           vals("SELECT age + 1 AS a FROM proj WHERE id <= 3 ORDER BY 1 DESC", "a"),
           [41, 31, 26])
    # LIMIT/OFFSET on top of expression projection + ORDER BY.
    _check("expr + ORDER BY + LIMIT/OFFSET",
           vals("SELECT age * 2 AS d FROM proj WHERE id <= 3 "
                "ORDER BY d DESC LIMIT 1 OFFSET 1", "d"),
           [60])


def verify_fail_loud(engine):
    print("\nVerifying projection still fails loud where it must...")
    # An unsupported scalar function must raise, not silently stream the row.
    # (Many scalar functions are now implemented -- see verify_sql_functions.py;
    # SUBSTRING is deliberately still unsupported, so it exercises the
    # fail-loud contract for an unimplemented function in the SELECT list.)
    _expect_raises(
        "unsupported function projection",
        lambda: engine.execute("SELECT SUBSTRING(name, 1, 2) AS u FROM proj"),
        NotImplementedError)
    # Two SELECT items landing on the same output key cannot both fit in a row.
    _expect_raises(
        "duplicate output column",
        lambda: engine.execute("SELECT age AS x, name AS x FROM proj"),
        ValueError)


def verify_unchanged_paths(engine):
    """The plain-column-list and SELECT * paths must be untouched."""
    print("\nVerifying plain-column and SELECT * paths unchanged...")
    star = engine.execute("SELECT * FROM proj WHERE id = 1")
    _check("SELECT * keeps all columns", sorted(star[0].keys()),
           ["age", "id", "name"])
    bare = engine.execute("SELECT name, age FROM proj WHERE id = 1")
    _check("plain column list projects exactly", sorted(bare[0].keys()),
           ["age", "name"])


def verify_python_backend():
    """Expression projection lives in the parser/operators above the storage
    scan, so it is backend-agnostic -- confirm parity on pure Python too."""
    print("\nVerifying expression projection on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "proj_py")
    rows = engine.execute("SELECT id, age * 2 AS double FROM proj_py WHERE id = 3")
    _check("python backend expr projection",
           (sorted(rows[0].keys()), rows[0]["double"]), (["double", "id"], 80))


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    _populate(shared, "proj")
    verify_alias_projection(shared)
    verify_expression_projection(shared)
    verify_unaliased_expression_name(shared)
    verify_star_with_expression(shared)
    verify_qualified_star(shared)
    verify_distinct_over_expression(shared)
    verify_order_by_with_projection(shared)
    verify_fail_loud(shared)
    verify_unchanged_paths(shared)
    verify_python_backend()
    print("\nAll single-table projection checks passed.")
