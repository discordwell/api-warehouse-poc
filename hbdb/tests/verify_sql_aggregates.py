"""
Verify GROUP BY, aggregate functions (COUNT/SUM/AVG/MIN/MAX, incl. DISTINCT),
HAVING and SELECT DISTINCT in the FDB-style SQL engine.

These clauses used to be rejected with NotImplementedError (the fail-loud
guard added once the engine started returning real WHERE/ORDER BY results).
This suite is the regression net for actually implementing them -- and for the
SQL semantics that are easy to get subtly wrong:

  * COUNT(*) counts rows; COUNT(col) ignores NULLs; COUNT never returns NULL.
  * SUM/AVG/MIN/MAX ignore NULLs and return NULL for an empty/all-NULL group.
  * A global aggregate over an empty table still returns exactly one row.
  * GROUP BY over an empty table returns zero rows.
  * NULLs form their own group.
  * Non-grouped, non-aggregated columns are rejected (value undefined).

Like the other SQL verify scripts: every HBDB in one process+CWD shares the
WAL, so each scenario uses a uniquely named table.
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


def _one(engine, sql):
    """Run a query expected to return exactly one row; return that row."""
    rows = engine.execute(sql)
    if len(rows) != 1:
        print(f"{FAIL}: {sql}: expected 1 row, got {len(rows)}")
        sys.exit(1)
    return rows[0]


def _populate_sales(engine, table):
    engine.execute(
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, dept TEXT, "
        f"amount REAL, bonus INTEGER)")
    # bonus is NULL for some rows to exercise NULL-skipping aggregates.
    rows = [
        (1, 'eng',   100.0, 10),
        (2, 'eng',   200.0, None),
        (3, 'eng',   300.0, 30),
        (4, 'sales',  50.0, 5),
        (5, 'sales',  75.0, None),
    ]
    for r in rows:
        bonus = "NULL" if r[3] is None else r[3]
        engine.execute(
            f"INSERT INTO {table} (id, dept, amount, bonus) "
            f"VALUES ({r[0]}, '{r[1]}', {r[2]}, {bonus})")


def verify_global_aggregates(engine):
    print("Verifying global aggregates (no GROUP BY)...")
    row = _one(engine, "SELECT COUNT(*), SUM(amount), AVG(amount), "
                       "MIN(amount), MAX(amount) FROM agg")
    _check("COUNT(*)", row["COUNT(*)"], 5)
    _check("SUM(amount)", row["SUM(amount)"], 725.0)
    _check("AVG(amount)", row["AVG(amount)"], 145.0)
    _check("MIN(amount)", row["MIN(amount)"], 50.0)
    _check("MAX(amount)", row["MAX(amount)"], 300.0)

    # COUNT(col) ignores NULLs (bonus is NULL for 2 of 5 rows); COUNT(*) doesn't.
    row = _one(engine, "SELECT COUNT(*) AS all_rows, COUNT(bonus) AS with_bonus, "
                       "SUM(bonus) AS bonus_sum FROM agg")
    _check("COUNT(*) all rows", row["all_rows"], 5)
    _check("COUNT(bonus) skips NULL", row["with_bonus"], 3)
    _check("SUM(bonus) skips NULL", row["bonus_sum"], 45)


def verify_group_by(engine):
    print("\nVerifying GROUP BY + aggregates...")
    rows = engine.execute(
        "SELECT dept, COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS top "
        "FROM agg GROUP BY dept ORDER BY dept")
    _check("two groups", [r["dept"] for r in rows], ["eng", "sales"])
    _check("eng count", rows[0]["n"], 3)
    _check("eng total", rows[0]["total"], 600.0)
    _check("eng max", rows[0]["top"], 300.0)
    _check("sales count", rows[1]["n"], 2)
    _check("sales total", rows[1]["total"], 125.0)


def verify_default_output_name(engine):
    """An unaliased aggregate is named by its SQL text (e.g. ``COUNT(*)``)."""
    print("\nVerifying default (unaliased) output column names...")
    rows = engine.execute("SELECT dept, COUNT(*) FROM agg GROUP BY dept ORDER BY dept")
    _check("default name is COUNT(*)", sorted(rows[0].keys()), ["COUNT(*)", "dept"])
    _check("eng COUNT(*)", rows[0]["COUNT(*)"], 3)


def verify_having(engine):
    print("\nVerifying HAVING...")
    rows = engine.execute(
        "SELECT dept, COUNT(*) AS n FROM agg GROUP BY dept HAVING COUNT(*) > 2")
    _check("HAVING COUNT(*) > 2 keeps only eng", [r["dept"] for r in rows], ["eng"])

    # HAVING may reference an aggregate that is not in the SELECT list.
    rows = engine.execute(
        "SELECT dept FROM agg GROUP BY dept HAVING SUM(amount) < 200 ORDER BY dept")
    _check("HAVING on non-selected aggregate", [r["dept"] for r in rows], ["sales"])


def verify_order_and_limit_on_aggregate(engine):
    print("\nVerifying ORDER BY / LIMIT over aggregates...")
    # ORDER BY an aliased aggregate, descending.
    rows = engine.execute(
        "SELECT dept, SUM(amount) AS total FROM agg GROUP BY dept ORDER BY total DESC")
    _check("ORDER BY alias DESC", [r["dept"] for r in rows], ["eng", "sales"])
    # ORDER BY the aggregate expression itself (must appear in SELECT).
    rows = engine.execute(
        "SELECT dept, COUNT(*) FROM agg GROUP BY dept ORDER BY COUNT(*) ASC, dept")
    _check("ORDER BY COUNT(*) ASC", [r["dept"] for r in rows], ["sales", "eng"])
    # ORDER BY a positional reference to the aggregated output, plus LIMIT.
    rows = engine.execute(
        "SELECT dept, SUM(amount) AS total FROM agg GROUP BY dept "
        "ORDER BY 2 DESC LIMIT 1")
    _check("ORDER BY position + LIMIT", [r["dept"] for r in rows], ["eng"])


def verify_where_group_having_order(engine):
    """The classic analytics pipeline composed end-to-end."""
    print("\nVerifying WHERE + GROUP BY + HAVING + ORDER BY together...")
    rows = engine.execute(
        "SELECT dept, COUNT(*) AS n FROM agg WHERE amount > 100 "
        "GROUP BY dept HAVING COUNT(*) >= 1 ORDER BY n DESC, dept")
    # amount > 100 keeps eng(200, 300) and sales() -> only eng survives, n=2.
    _check("full pipeline", [(r["dept"], r["n"]) for r in rows],
           [("eng", 2)])


def verify_multikey_group(engine):
    print("\nVerifying multi-key GROUP BY...")
    engine.execute("CREATE TABLE agg_mk (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
    for i, a, b in [(1, "x", "p"), (2, "x", "q"), (3, "x", "p"),
                    (4, "y", "p"), (5, "y", "q")]:
        engine.execute(f"INSERT INTO agg_mk VALUES ({i}, '{a}', '{b}')")
    rows = engine.execute(
        "SELECT a, b, COUNT(*) AS n FROM agg_mk GROUP BY a, b ORDER BY a, b")
    _check("multi-key groups", [(r["a"], r["b"], r["n"]) for r in rows],
           [("x", "p", 2), ("x", "q", 1), ("y", "p", 1), ("y", "q", 1)])


def verify_distinct_aggregate(engine):
    print("\nVerifying COUNT(DISTINCT ...) / SUM(DISTINCT ...)...")
    engine.execute("CREATE TABLE agg_d (id INTEGER PRIMARY KEY, v INTEGER)")
    for i, v in [(1, 10), (2, 10), (3, 20), (4, 20), (5, 20)]:
        engine.execute(f"INSERT INTO agg_d VALUES ({i}, {v})")
    row = _one(engine, "SELECT COUNT(v) AS c, COUNT(DISTINCT v) AS d, "
                       "SUM(DISTINCT v) AS s FROM agg_d")
    _check("COUNT(v)", row["c"], 5)
    _check("COUNT(DISTINCT v)", row["d"], 2)
    _check("SUM(DISTINCT v)", row["s"], 30)


def verify_distinct_value_coercion(engine):
    """DISTINCT counts values by the engine's value equality: numeric values
    (incl. numeric strings, ints and floats) collapse, while TRUE stays
    distinct from 1 -- consistent with WHERE/ORDER BY coercion."""
    print("\nVerifying DISTINCT numeric value coercion...")
    engine.execute("CREATE TABLE agg_co (id INTEGER PRIMARY KEY, v TEXT)")
    engine.execute("INSERT INTO agg_co VALUES (1, 10)")     # int 10
    engine.execute("INSERT INTO agg_co VALUES (2, '10')")   # string "10"
    engine.execute("INSERT INTO agg_co VALUES (3, 1.0)")    # float 1.0
    engine.execute("INSERT INTO agg_co VALUES (4, 1)")      # int 1
    # {10, "10"} -> 10 ; {1.0, 1} -> 1 ; so two distinct numeric values.
    _check("numeric values collapse",
           _one(engine, "SELECT COUNT(DISTINCT v) AS d FROM agg_co")["d"], 2)

    engine.execute("CREATE TABLE agg_bool (id INTEGER PRIMARY KEY, v BOOLEAN)")
    engine.execute("INSERT INTO agg_bool VALUES (1, TRUE)")
    engine.execute("INSERT INTO agg_bool VALUES (2, 1)")
    _check("TRUE stays distinct from 1",
           _one(engine, "SELECT COUNT(DISTINCT v) AS d FROM agg_bool")["d"], 2)


def verify_null_group_and_empty_aggs(engine):
    print("\nVerifying NULL groups and empty/all-NULL aggregates...")
    engine.execute("CREATE TABLE agg_n (id INTEGER PRIMARY KEY, grp TEXT, v INTEGER)")
    engine.execute("INSERT INTO agg_n VALUES (1, 'a', 5)")
    engine.execute("INSERT INTO agg_n (id, v) VALUES (2, 7)")      # grp NULL
    engine.execute("INSERT INTO agg_n (id, v) VALUES (3, 9)")      # grp NULL
    engine.execute("INSERT INTO agg_n VALUES (4, 'a', 1)")
    rows = engine.execute("SELECT grp, COUNT(*) AS n FROM agg_n GROUP BY grp")
    by_grp = {r["grp"]: r["n"] for r in rows}
    _check("NULL forms its own group", by_grp, {"a": 2, None: 2})

    # SUM/AVG/MIN/MAX over an all-NULL column -> NULL; COUNT -> 0.
    engine.execute("CREATE TABLE agg_e (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO agg_e (id) VALUES (1)")  # v NULL
    row = _one(engine, "SELECT COUNT(v) AS c, SUM(v) AS s, AVG(v) AS a, "
                       "MIN(v) AS mn, MAX(v) AS mx FROM agg_e")
    _check("COUNT all-NULL", row["c"], 0)
    _check("SUM all-NULL is NULL", row["s"], None)
    _check("AVG all-NULL is NULL", row["a"], None)
    _check("MIN all-NULL is NULL", row["mn"], None)
    _check("MAX all-NULL is NULL", row["mx"], None)


def verify_empty_table(engine):
    print("\nVerifying aggregates over an empty table...")
    engine.execute("CREATE TABLE agg_empty (id INTEGER PRIMARY KEY, v INTEGER)")
    # Global aggregate over zero rows still returns exactly one row.
    row = _one(engine, "SELECT COUNT(*) AS c, SUM(v) AS s FROM agg_empty")
    _check("COUNT(*) over empty is 0", row["c"], 0)
    _check("SUM over empty is NULL", row["s"], None)
    # GROUP BY over zero rows returns zero rows.
    _check("GROUP BY over empty -> no rows",
           engine.execute("SELECT v, COUNT(*) FROM agg_empty GROUP BY v"), [])


def verify_aggregate_arithmetic(engine):
    print("\nVerifying arithmetic over aggregates...")
    rows = engine.execute(
        "SELECT dept, SUM(amount) / COUNT(*) AS mean FROM agg "
        "GROUP BY dept ORDER BY dept")
    _check("SUM/COUNT per group", [(r["dept"], r["mean"]) for r in rows],
           [("eng", 200.0), ("sales", 62.5)])


def verify_select_distinct(engine):
    print("\nVerifying SELECT DISTINCT...")
    _check("DISTINCT single column",
           [r["dept"] for r in engine.execute(
               "SELECT DISTINCT dept FROM agg ORDER BY dept")],
           ["eng", "sales"])
    engine.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
    for i, a, b in [(1, "x", 1), (2, "x", 1), (3, "x", 2), (4, "y", 1)]:
        engine.execute(f"INSERT INTO dst VALUES ({i}, '{a}', {b})")
    rows = engine.execute("SELECT DISTINCT a, b FROM dst ORDER BY a, b")
    _check("DISTINCT multi-column", [(r["a"], r["b"]) for r in rows],
           [("x", 1), ("x", 2), ("y", 1)])
    # DISTINCT composes with LIMIT.
    _check("DISTINCT + LIMIT",
           len(engine.execute("SELECT DISTINCT a FROM dst LIMIT 1")), 1)


def verify_validation_errors(engine):
    """Fail-loud: things that are genuinely invalid or unimplemented must raise,
    never silently return the wrong rows."""
    print("\nVerifying invalid/unsupported queries fail loudly...")

    def expect_raise(label, sql, exc):
        try:
            engine.execute(sql)
        except exc:
            print(f"{PASS}: {label} raises {exc.__name__}")
        except Exception as e:  # noqa: BLE001 - report the wrong exception type
            print(f"{FAIL}: {label} raised {type(e).__name__}, expected {exc.__name__}")
            sys.exit(1)
        else:
            print(f"{FAIL}: {label} did not raise (silently-wrong regression)")
            sys.exit(1)

    # A non-grouped, non-aggregated column is undefined across the group.
    expect_raise("ungrouped column in aggregate",
                 "SELECT dept, amount FROM agg GROUP BY dept", ValueError)
    expect_raise("bare column with global aggregate",
                 "SELECT dept, COUNT(*) FROM agg", ValueError)
    # A bare (non-grouped) column in HAVING is just as undefined.
    expect_raise("ungrouped column in HAVING",
                 "SELECT dept, COUNT(*) FROM agg GROUP BY dept HAVING amount > 5",
                 ValueError)
    # ORDER BY an aggregate that is not in the SELECT list.
    expect_raise("ORDER BY aggregate not selected",
                 "SELECT dept FROM agg GROUP BY dept ORDER BY SUM(amount)",
                 NotImplementedError)
    # ORDER BY a column that is neither grouped nor a SELECT output column:
    # nothing to sort against once aggregated, so it must not silently no-op.
    expect_raise("ORDER BY non-output column",
                 "SELECT dept, COUNT(*) FROM agg GROUP BY dept ORDER BY amount",
                 NotImplementedError)
    # SUM/AVG over non-numeric operands fail loud instead of silently dropping.
    expect_raise("SUM over non-numeric text",
                 "SELECT SUM(dept) FROM agg", ValueError)
    # Window functions and unimplemented aggregates still fail loudly.
    expect_raise("window function",
                 "SELECT dept, ROW_NUMBER() OVER (ORDER BY amount) FROM agg",
                 NotImplementedError)
    expect_raise("unsupported aggregate (STDDEV)",
                 "SELECT STDDEV(amount) FROM agg", NotImplementedError)
    expect_raise("SELECT * with GROUP BY",
                 "SELECT * FROM agg GROUP BY dept", NotImplementedError)


def verify_python_backend():
    """Aggregate operators sit above the storage scan, so they are
    backend-agnostic -- confirm parity on the pure-Python backend too."""
    print("\nVerifying aggregates on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate_sales(engine, "agg_py")
    rows = engine.execute(
        "SELECT dept, COUNT(*) AS n, SUM(amount) AS total FROM agg_py "
        "GROUP BY dept ORDER BY total DESC")
    _check("python backend GROUP BY", [(r["dept"], r["n"], r["total"]) for r in rows],
           [("eng", 3, 600.0), ("sales", 2, 125.0)])


def verify_motivating_query():
    """The repo README's own verification query shape:
    ``SELECT type, COUNT(*) FROM hn_items GROUP BY type``."""
    print("\nVerifying the README's GROUP BY query shape...")
    engine = SQLEngine(HBDB(force_python=True))
    engine.execute("CREATE TABLE hn (id INTEGER PRIMARY KEY, type TEXT)")
    for i, t in [(1, "story"), (2, "comment"), (3, "story"),
                 (4, "comment"), (5, "comment")]:
        engine.execute(f"INSERT INTO hn VALUES ({i}, '{t}')")
    rows = engine.execute(
        "SELECT type, COUNT(*) AS c FROM hn GROUP BY type ORDER BY c DESC, type")
    _check("count by type", [(r["type"], r["c"]) for r in rows],
           [("comment", 3), ("story", 2)])


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    _populate_sales(shared, "agg")
    verify_global_aggregates(shared)
    verify_group_by(shared)
    verify_default_output_name(shared)
    verify_having(shared)
    verify_order_and_limit_on_aggregate(shared)
    verify_where_group_having_order(shared)
    verify_multikey_group(shared)
    verify_distinct_aggregate(shared)
    verify_distinct_value_coercion(shared)
    verify_null_group_and_empty_aggs(shared)
    verify_empty_table(shared)
    verify_aggregate_arithmetic(shared)
    verify_select_distinct(shared)
    verify_validation_errors(shared)
    verify_python_backend()
    verify_motivating_query()
    print("\nAll GROUP BY / aggregate / DISTINCT checks passed.")
