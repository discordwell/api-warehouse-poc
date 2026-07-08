"""
Verify set operations (UNION / UNION ALL / INTERSECT [ALL] / EXCEPT [ALL])
in the FDB-style SQL engine.

The engine executes a set operation by materialization (hbdb/sql/setops.py):
each side runs as its own SELECT inside the statement's transaction, rows are
reduced to positional value tuples, combined with SQL's set semantics, and
re-labeled with the FIRST side's output column names. This suite pins the
semantics that are easy to get silently wrong:

  * positional column matching (names need not agree; the first side wins),
    with a hard failure on a column-count mismatch;
  * DISTINCT-form de-duplication under the engine's value equality -- NULLs
    are equal to each other for set purposes ("not distinct"), numeric
    strings match their numbers -- and bag semantics for INTERSECT ALL /
    EXCEPT ALL;
  * ORDER BY on the combined result is restricted to output columns (name or
    position), and LIMIT/OFFSET apply after combining;
  * sqlglot parses `a UNION b INTERSECT c` left-to-right, which contradicts
    the SQL standard's INTERSECT-binds-tighter rule -- executing it would be
    silently wrong, so that chain must fail loud until parenthesized;
  * set operations work as subquery bodies (IN / EXISTS / scalar / ANY / ALL)
    and as an INSERT ... SELECT source, with correlated leaves still loud.

Like the other SQL verify scripts: every HBDB in one process+CWD shares the
WAL, so each scenario uses uniquely named tables.
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


def _vals(engine, sql, col):
    """The ``col`` value of every result row, in result order."""
    return [r[col] for r in engine.execute(sql)]


def _expect_raises(label, fn, exc):
    try:
        fn()
    except exc:
        print(f"{PASS}: {label} raised {exc.__name__}")
    except Exception as other:  # noqa: BLE001 -- want the specific type
        print(f"{FAIL}: {label}: raised {type(other).__name__}: {other}, "
              f"expected {exc.__name__}")
        sys.exit(1)
    else:
        print(f"{FAIL}: {label}: did not raise (silently-wrong regression)")
        sys.exit(1)


def verify_union(engine):
    print("Verifying UNION / UNION ALL...")
    engine.execute("CREATE TABLE so_u_a (id INTEGER PRIMARY KEY, "
                   "v INTEGER, tag TEXT)")
    engine.execute("INSERT INTO so_u_a VALUES "
                   "(1,10,'x'), (2,20,'y'), (3,30,'z'), (4,20,'y')")
    engine.execute("CREATE TABLE so_u_b (id INTEGER PRIMARY KEY, "
                   "w INTEGER, lbl TEXT)")
    engine.execute("INSERT INTO so_u_b VALUES "
                   "(1,20,'y'), (2,40,'q'), (3,20,'y')")

    # Dedup within AND across sides; the output column is the first side's.
    _check("UNION de-duplicates and keeps the first side's column name",
           engine.execute("SELECT v FROM so_u_a UNION SELECT w FROM so_u_b "
                          "ORDER BY v"),
           [{"v": 10}, {"v": 20}, {"v": 30}, {"v": 40}])
    _check("UNION ALL keeps every duplicate",
           _vals(engine, "SELECT v FROM so_u_a UNION ALL "
                         "SELECT w FROM so_u_b ORDER BY 1", "v"),
           [10, 20, 20, 20, 20, 30, 40])
    # Whole-row identity: (20,'y') collapses across sides, per column pair.
    _check("multi-column UNION de-duplicates whole rows",
           engine.execute("SELECT v, tag FROM so_u_a UNION "
                          "SELECT w, lbl FROM so_u_b ORDER BY v"),
           [{"v": 10, "tag": "x"}, {"v": 20, "tag": "y"},
            {"v": 30, "tag": "z"}, {"v": 40, "tag": "q"}])
    # An aliased first side names the output; ORDER BY uses output names.
    _check("first side's alias names the output column",
           engine.execute("SELECT v AS val FROM so_u_a UNION "
                          "SELECT w FROM so_u_b ORDER BY val DESC LIMIT 2"),
           [{"val": 40}, {"val": 30}])
    # A parenthesized side keeps its own ORDER BY / LIMIT; UNION ALL then
    # concatenates left side first.
    _check("parenthesized sides run their own ORDER BY / LIMIT",
           _vals(engine, "(SELECT v FROM so_u_a ORDER BY v DESC LIMIT 2) "
                         "UNION ALL "
                         "(SELECT w FROM so_u_b ORDER BY w LIMIT 1)", "v"),
           [30, 20, 20])
    # Aggregates are full SELECTs on each side.
    _check("aggregate sides combine",
           _vals(engine, "SELECT MAX(v) AS m FROM so_u_a UNION "
                         "SELECT MIN(w) AS m FROM so_u_b ORDER BY m", "m"),
           [20, 30])


def verify_null_and_coercion(engine):
    print("\nVerifying NULL identity and numeric coercion in set ops...")
    engine.execute("CREATE TABLE so_n1 (id INTEGER PRIMARY KEY, x INTEGER)")
    engine.execute("INSERT INTO so_n1 (id) VALUES (1), (3)")  # x NULL
    engine.execute("INSERT INTO so_n1 VALUES (2, 1)")
    engine.execute("CREATE TABLE so_n2 (id INTEGER PRIMARY KEY, y INTEGER)")
    engine.execute("INSERT INTO so_n2 (id) VALUES (1)")       # y NULL
    engine.execute("INSERT INTO so_n2 VALUES (2, 2)")

    # SQL's set operations pair NULL with NULL ("not distinct"), unlike `=`.
    _check("UNION collapses NULLs to one row (NULLS first ascending)",
           _vals(engine, "SELECT x FROM so_n1 UNION SELECT y FROM so_n2 "
                         "ORDER BY x", "x"),
           [None, 1, 2])
    _check("INTERSECT matches NULL with NULL",
           _vals(engine, "SELECT x FROM so_n1 INTERSECT "
                         "SELECT y FROM so_n2", "x"),
           [None])
    _check("EXCEPT removes the NULL matched on the right",
           _vals(engine, "SELECT x FROM so_n1 EXCEPT SELECT y FROM so_n2",
                 "x"),
           [1])
    _check("NULLS LAST is honored on the combined result",
           _vals(engine, "SELECT x FROM so_n1 UNION SELECT y FROM so_n2 "
                         "ORDER BY x ASC NULLS LAST", "x"),
           [1, 2, None])

    # Row identity uses the engine's value equality: '10' and 10 are one.
    engine.execute("CREATE TABLE so_c1 (id INTEGER PRIMARY KEY, s TEXT)")
    engine.execute("INSERT INTO so_c1 VALUES (1, '10')")
    engine.execute("CREATE TABLE so_c2 (id INTEGER PRIMARY KEY, m INTEGER)")
    engine.execute("INSERT INTO so_c2 VALUES (1, 10)")
    _check("numeric-string rows dedupe against their numbers",
           engine.execute("SELECT s FROM so_c1 UNION SELECT m FROM so_c2"),
           [{"s": "10"}])


def verify_intersect_except(engine):
    print("\nVerifying INTERSECT [ALL] / EXCEPT [ALL]...")
    engine.execute("CREATE TABLE so_i1 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_i1 VALUES "
                   "(1,1), (2,2), (3,2), (4,3), (5,3), (6,3)")
    engine.execute("CREATE TABLE so_i2 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_i2 VALUES (1,2), (2,3), (3,3), (4,4)")
    engine.execute("CREATE TABLE so_i3 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_i3 VALUES (1,1)")

    _check("INTERSECT returns each common value once",
           _vals(engine, "SELECT v FROM so_i1 INTERSECT "
                         "SELECT v FROM so_i2 ORDER BY v", "v"),
           [2, 3])
    _check("EXCEPT returns distinct left-only values",
           _vals(engine, "SELECT v FROM so_i1 EXCEPT "
                         "SELECT v FROM so_i2 ORDER BY v", "v"),
           [1])
    # Bag semantics: left has 2 x2, 3 x3; right has 2 x1, 3 x2, 4 x1.
    _check("INTERSECT ALL keeps min(left, right) occurrences",
           _vals(engine, "SELECT v FROM so_i1 INTERSECT ALL "
                         "SELECT v FROM so_i2 ORDER BY v", "v"),
           [2, 3, 3])
    _check("EXCEPT ALL cancels one left occurrence per right occurrence",
           _vals(engine, "SELECT v FROM so_i1 EXCEPT ALL "
                         "SELECT v FROM so_i2 ORDER BY v", "v"),
           [1, 2, 3])
    # Equal-precedence chains associate left, per the standard.
    _check("EXCEPT chains left-associatively",
           _vals(engine, "SELECT v FROM so_i1 EXCEPT SELECT v FROM so_i2 "
                         "EXCEPT SELECT v FROM so_i3", "v"),
           [])
    # INTERSECT deeper than UNION is the grouping the standard prescribes.
    _check("a INTERSECT b UNION c groups the INTERSECT first",
           _vals(engine, "SELECT v FROM so_i1 INTERSECT SELECT v FROM so_i2 "
                         "UNION SELECT v FROM so_i3 ORDER BY v", "v"),
           [1, 2, 3])


def verify_precedence_grouping(engine):
    print("\nVerifying mixed-chain precedence handling...")
    engine.execute("CREATE TABLE so_p1 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_p1 VALUES (1,1)")
    engine.execute("CREATE TABLE so_p2 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_p2 VALUES (1,1), (2,2)")
    engine.execute("CREATE TABLE so_p3 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_p3 VALUES (1,2)")

    # The two groupings genuinely differ on this data -- which is exactly why
    # executing sqlglot's left-to-right parse of the bare chain (against the
    # standard's INTERSECT-first rule) would be silently wrong.
    _check("(a UNION b) INTERSECT c",
           _vals(engine, "(SELECT v FROM so_p1 UNION SELECT v FROM so_p2) "
                         "INTERSECT SELECT v FROM so_p3", "v"),
           [2])
    _check("a UNION (b INTERSECT c)",
           _vals(engine, "SELECT v FROM so_p1 UNION "
                         "(SELECT v FROM so_p2 INTERSECT "
                         "SELECT v FROM so_p3) ORDER BY v", "v"),
           [1, 2])
    _expect_raises(
        "bare a UNION b INTERSECT c (ambiguous vs the standard)",
        lambda: engine.execute("SELECT v FROM so_p1 UNION "
                               "SELECT v FROM so_p2 INTERSECT "
                               "SELECT v FROM so_p3"),
        NotImplementedError)


def verify_toplevel_parenthesized(engine):
    print("\nVerifying a fully parenthesized set operation as a statement...")
    engine.execute("CREATE TABLE so_tp1 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_tp1 VALUES (1,1), (2,2), (3,3)")
    engine.execute("CREATE TABLE so_tp2 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_tp2 VALUES (1,3), (2,4)")

    # `(a UNION b)` parses as a Subquery *wrapper*, not a SetOperation, so
    # without unwrapping the engine would grab it as a scalar subquery and
    # report a misleading "scalar subquery returned N rows". These are all
    # standard, common statements.
    _check("bare (a UNION b) statement",
           _vals(engine, "(SELECT v FROM so_tp1 UNION SELECT v FROM so_tp2) "
                         "ORDER BY v", "v"),
           [1, 2, 3, 4])
    # The trailing ORDER BY / LIMIT / OFFSET ride on the parentheses wrapper
    # and must apply to the *combined* result.
    _check("(a UNION b) ORDER BY v DESC LIMIT 2",
           _vals(engine, "(SELECT v FROM so_tp1 UNION SELECT v FROM so_tp2) "
                         "ORDER BY v DESC LIMIT 2", "v"),
           [4, 3])
    _check("(a UNION b) ORDER BY 1 LIMIT 2 OFFSET 1 (position + slice)",
           _vals(engine, "(SELECT v FROM so_tp1 UNION SELECT v FROM so_tp2) "
                         "ORDER BY 1 LIMIT 2 OFFSET 1", "v"),
           [2, 3])
    _check("nested ((a UNION b)) unwraps every layer",
           _vals(engine, "((SELECT v FROM so_tp1 UNION SELECT v FROM so_tp2)) "
                         "ORDER BY v", "v"),
           [1, 2, 3, 4])
    _check("(a INTERSECT b) statement",
           _vals(engine, "(SELECT v FROM so_tp1 INTERSECT "
                         "SELECT v FROM so_tp2)", "v"),
           [3])
    # A plain parenthesized SELECT is a query too, not a scalar operand.
    _check("plain (SELECT ...) statement",
           _vals(engine, "(SELECT v FROM so_tp1 ORDER BY v DESC)", "v"),
           [3, 2, 1])
    # The unwrapped set operation keeps the ORDER-BY-output-columns-only rule.
    _expect_raises(
        "(a UNION b) ORDER BY a non-output column",
        lambda: engine.execute("(SELECT v FROM so_tp1 UNION "
                               "SELECT v FROM so_tp2) ORDER BY id"),
        NotImplementedError)
    # INSERT from a parenthesized source keeps its inner ORDER BY / LIMIT.
    engine.execute("CREATE TABLE so_tp_t (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_tp_t (SELECT id, v FROM so_tp1 "
                   "UNION SELECT id + 10, v FROM so_tp2 ORDER BY v DESC "
                   "LIMIT 2)")
    _check("INSERT from a parenthesized UNION honors inner ORDER BY / LIMIT",
           _vals(engine, "SELECT v FROM so_tp_t ORDER BY v", "v"),
           [3, 4])


def verify_order_limit_fail_loud(engine):
    print("\nVerifying set-op ORDER BY / LIMIT restrictions...")
    engine.execute("CREATE TABLE so_f1 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_f1 VALUES (1,1), (2,2), (3,3)")
    engine.execute("CREATE TABLE so_f2 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_f2 VALUES (1,3), (2,4)")

    _check("ORDER BY position with LIMIT/OFFSET on the combined result",
           _vals(engine, "SELECT v FROM so_f1 UNION SELECT v FROM so_f2 "
                         "ORDER BY 1 DESC LIMIT 2 OFFSET 1", "v"),
           [3, 2])
    _expect_raises(
        "ORDER BY a non-output column on a set operation",
        lambda: engine.execute("SELECT v FROM so_f1 UNION "
                               "SELECT v FROM so_f2 ORDER BY id"),
        NotImplementedError)
    _expect_raises(
        "ORDER BY an expression on a set operation",
        lambda: engine.execute("SELECT v FROM so_f1 UNION "
                               "SELECT v FROM so_f2 ORDER BY v + 1"),
        NotImplementedError)
    _expect_raises(
        "ORDER BY position out of range on a set operation",
        lambda: engine.execute("SELECT v FROM so_f1 UNION "
                               "SELECT v FROM so_f2 ORDER BY 5"),
        ValueError)
    _expect_raises(
        "column-count mismatch between sides",
        lambda: engine.execute("SELECT id, v FROM so_f1 UNION "
                               "SELECT id FROM so_f2"),
        ValueError)
    _expect_raises(
        "duplicate output names on the first side",
        lambda: engine.execute("SELECT v, v FROM so_f1 UNION "
                               "SELECT id, v FROM so_f2"),
        ValueError)
    _expect_raises(
        "UNION BY NAME (columns are matched positionally)",
        lambda: engine.execute("SELECT v FROM so_f1 UNION BY NAME "
                               "SELECT v FROM so_f2"),
        NotImplementedError)
    _expect_raises(
        "WITH over a set operation",
        lambda: engine.execute("WITH c AS (SELECT v FROM so_f1) "
                               "SELECT v FROM so_f1 UNION SELECT v FROM c"),
        NotImplementedError)
    _expect_raises(
        "VALUES as a set-operation side",
        lambda: engine.execute("SELECT v FROM so_f1 UNION VALUES (9)"),
        NotImplementedError)
    # The engine executes set operations before binding; a direct bind of a
    # set operation (bypassing the engine) must fail with the real reason.
    _expect_raises(
        "binding a set operation standalone",
        lambda: engine.parser.parse(
            "SELECT v FROM so_f1 UNION SELECT v FROM so_f2"),
        NotImplementedError)


def verify_subquery_positions(engine):
    print("\nVerifying set operations as subquery bodies and INSERT sources...")
    engine.execute("CREATE TABLE so_s1 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_s1 VALUES (1,1), (2,2), (3,3)")
    engine.execute("CREATE TABLE so_s2 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_s2 VALUES (1,2)")
    engine.execute("CREATE TABLE so_s3 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_s3 VALUES (1,3), (2,4)")
    engine.execute("CREATE TABLE so_s4 (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_s4 (id) VALUES (1)")  # v NULL

    _check("IN over a UNION subquery",
           _vals(engine, "SELECT id FROM so_s1 WHERE v IN "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3) "
                         "ORDER BY id", "id"),
           [2, 3])
    # A NULL surviving the UNION keeps three-valued NOT IN: matches nothing.
    _check("NOT IN over a UNION carrying a NULL matches nothing",
           _vals(engine, "SELECT id FROM so_s1 WHERE v NOT IN "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s4)",
                 "id"),
           [])
    _check("scalar UNION subquery (dedup makes it single-row)",
           _vals(engine, "SELECT id FROM so_s1 WHERE v = "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s2)",
                 "id"),
           [2])
    _expect_raises(
        "scalar UNION subquery with more than one row",
        lambda: engine.execute("SELECT id FROM so_s1 WHERE v = "
                               "(SELECT v FROM so_s2 UNION "
                               "SELECT v FROM so_s3)"),
        ValueError)
    _check("EXISTS over an empty UNION is FALSE",
           _vals(engine, "SELECT id FROM so_s1 WHERE EXISTS "
                         "(SELECT v FROM so_s2 WHERE v = 99 UNION "
                         "SELECT v FROM so_s3 WHERE v = 99)", "id"),
           [])
    _check("EXISTS over a nonempty UNION is TRUE",
           _vals(engine, "SELECT id FROM so_s1 WHERE EXISTS "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3) "
                         "ORDER BY id", "id"),
           [1, 2, 3])
    # A LIMIT on the union applies to the *combined* result -- the EXISTS
    # probe cap must not defeat an explicit LIMIT 0.
    _check("EXISTS honors LIMIT 0 on the combined union",
           _vals(engine, "SELECT id FROM so_s1 WHERE EXISTS "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3 "
                         "LIMIT 0)", "id"),
           [])
    _check("< ALL over a UNION",
           _vals(engine, "SELECT id FROM so_s1 WHERE v < ALL "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3)",
                 "id"),
           [1])
    _check("= ANY over a UNION",
           _vals(engine, "SELECT id FROM so_s1 WHERE v = ANY "
                         "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3) "
                         "ORDER BY id", "id"),
           [2, 3])
    # Correlation stays loud when a union side references the outer query.
    _expect_raises(
        "correlated leaf inside a UNION subquery",
        lambda: engine.execute("SELECT id FROM so_s1 WHERE v IN "
                               "(SELECT v FROM so_s2 WHERE so_s2.v = so_s1.v "
                               "UNION SELECT v FROM so_s3)"),
        NotImplementedError)

    # INSERT INTO ... <set operation>: positional mapping, first side's
    # names irrelevant to the target's.
    engine.execute("CREATE TABLE so_tgt (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_tgt "
                   "SELECT id, v FROM so_s1 WHERE id = 1 "
                   "UNION ALL SELECT id + 10, v FROM so_s2")
    _check("INSERT INTO ... UNION ALL inserts the combined rows",
           engine.execute("SELECT * FROM so_tgt ORDER BY id"),
           [{"id": 1, "v": 1}, {"id": 11, "v": 2}])

    # And the data-loss angle: a DML predicate over a union hits exactly the
    # right rows.
    engine.execute("CREATE TABLE so_del (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_del VALUES (1,1), (2,2), (3,3), (4,4)")
    engine.execute("DELETE FROM so_del WHERE v IN "
                   "(SELECT v FROM so_s2 UNION SELECT v FROM so_s3)")
    _check("DELETE with a UNION predicate removes exactly the matches",
           _vals(engine, "SELECT id FROM so_del", "id"),
           [1])


def verify_python_backend():
    """Set operations materialize above the storage layer, so they are
    backend-agnostic -- confirm parity on the pure-Python backend."""
    print("\nVerifying set operations on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    engine.execute("CREATE TABLE so_py_a (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_py_a VALUES (1,1), (2,2), (3,2)")
    engine.execute("CREATE TABLE so_py_b (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_py_b VALUES (1,2), (2,3)")

    _check("python backend UNION",
           _vals(engine, "SELECT v FROM so_py_a UNION SELECT v FROM so_py_b "
                         "ORDER BY v", "v"),
           [1, 2, 3])
    _check("python backend INTERSECT ALL",
           _vals(engine, "SELECT v FROM so_py_a INTERSECT ALL "
                         "SELECT v FROM so_py_b", "v"),
           [2])
    _check("python backend EXCEPT",
           _vals(engine, "SELECT v FROM so_py_a EXCEPT "
                         "SELECT v FROM so_py_b", "v"),
           [1])
    _check("python backend IN over a UNION",
           _vals(engine, "SELECT id FROM so_py_a WHERE v IN "
                         "(SELECT v FROM so_py_b UNION SELECT v FROM so_py_b)"
                         " ORDER BY id", "id"),
           [2, 3])
    engine.execute("CREATE TABLE so_py_t (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("INSERT INTO so_py_t SELECT id, v FROM so_py_a "
                   "EXCEPT SELECT id, v FROM so_py_b")
    _check("python backend INSERT ... EXCEPT",
           engine.execute("SELECT * FROM so_py_t ORDER BY id"),
           [{"id": 1, "v": 1}, {"id": 2, "v": 2}, {"id": 3, "v": 2}])


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_union(shared)
    verify_null_and_coercion(shared)
    verify_intersect_except(shared)
    verify_precedence_grouping(shared)
    verify_toplevel_parenthesized(shared)
    verify_order_limit_fail_loud(shared)
    verify_subquery_positions(shared)
    verify_python_backend()
    print("\nAll set-operation checks passed.")
