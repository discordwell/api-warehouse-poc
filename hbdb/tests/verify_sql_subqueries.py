"""
Verify uncorrelated-subquery support in the FDB-style SQL engine, plus the
fail-loud guards that shipped with it.

The engine materializes each uncorrelated subquery once per statement --
inside the statement's own transaction -- and splices the result back into
the expression tree as literals (``hbdb/sql/subqueries.py``), so a subquery
works in every clause an operand or predicate works in. This suite pins down
the semantics that are easy to get subtly (and silently) wrong:

  * ``IN (SELECT ...)`` keeps SQL's three-valued behavior: a NULL produced by
    the subquery makes an unmatched ``NOT IN`` UNKNOWN (matches *nothing*),
    while an *empty* subquery result makes ``IN`` FALSE / ``NOT IN`` TRUE
    even for a NULL left operand.
  * A scalar subquery yields NULL on an empty result and fails loud on more
    than one row or column.
  * ``EXISTS`` is TRUE on any row (a NULL-filled row included); the LIMIT-1
    probe optimization must not override an explicit ``LIMIT 0``.
  * ``ANY``/``ALL`` fold with OR/AND under three-valued logic; over an empty
    set ANY is FALSE and ALL is TRUE regardless of the left operand.
  * Correlated subqueries fail loud: run standalone, an outer-column
    reference would resolve to NULL and silently return the wrong rows.

It also covers ``INSERT INTO ... SELECT`` (positional column mapping through
the same engine path) and regression-guards three constructs that used to be
*silently wrong or lossy*: derived tables (bound to the inner table, dropping
the subquery's WHERE/projection), ``CREATE TABLE AS SELECT`` (created an
empty zero-column table), and ``WITH`` (CTE silently dropped).

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


def _ids(engine, sql):
    """The list of ``id`` values across the result rows, in order."""
    return [r["id"] for r in engine.execute(sql)]


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


def _populate(engine, emp, dept):
    """Two tables with the NULL/empty shapes the subquery semantics need.

    emp by id: 1 a/eng/100  2 b/eng/200  3 c/sales/150  4 d/ops/50
               5 e/NULL-dept/120  6 NULL-sal/eng
    dept: eng/1000, sales/700, ops/NULL-budget
    """
    engine.execute(f"CREATE TABLE {emp} (id INTEGER PRIMARY KEY, name TEXT, "
                   f"dept TEXT, sal INTEGER)")
    engine.execute(f"INSERT INTO {emp} VALUES "
                   f"(1,'a','eng',100), (2,'b','eng',200), "
                   f"(3,'c','sales',150), (4,'d','ops',50)")
    engine.execute(f"INSERT INTO {emp} (id, name, sal) VALUES (5,'e',120)")
    engine.execute(f"INSERT INTO {emp} (id, name, dept) VALUES (6,'f','eng')")
    engine.execute(f"CREATE TABLE {dept} (dname TEXT PRIMARY KEY, "
                   f"budget INTEGER)")
    engine.execute(f"INSERT INTO {dept} VALUES ('eng',1000), ('sales',700)")
    engine.execute(f"INSERT INTO {dept} (dname) VALUES ('ops')")


def verify_in_subquery(engine):
    print("Verifying [NOT] IN (SELECT ...)...")
    _populate(engine, "sq_in_e", "sq_in_d")

    _check("IN over a nonempty subquery",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept IN "
                        "(SELECT dname FROM sq_in_d WHERE budget > 800) "
                        "ORDER BY id"),
           [1, 2, 6])
    # NULL left operand: UNKNOWN, row excluded (SQL, not Python, membership).
    _check("IN skips a NULL left operand",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept IN "
                        "(SELECT dname FROM sq_in_d) ORDER BY id"),
           [1, 2, 3, 4, 6])
    _check("NOT IN over a NULL-free subquery",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept NOT IN "
                        "(SELECT dname FROM sq_in_d WHERE budget > 800) "
                        "ORDER BY id"),
           [3, 4])
    # The classic: the subquery result contains a NULL (dept of id 5), so an
    # unmatched NOT IN is UNKNOWN -- it must match *nothing*, not everything.
    _check("NOT IN with a NULL in the subquery result matches nothing",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept NOT IN "
                        "(SELECT dept FROM sq_in_e) ORDER BY id"),
           [])
    # Empty subquery result: IN is FALSE and NOT IN is TRUE -- even for the
    # NULL-dept row (id 5), the one place IN's usual NULL rule does not apply.
    _check("IN over an empty subquery matches nothing",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept IN "
                        "(SELECT dname FROM sq_in_d WHERE budget > 9999) "
                        "ORDER BY id"),
           [])
    _check("NOT IN over an empty subquery matches everything (NULL row too)",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept NOT IN "
                        "(SELECT dname FROM sq_in_d WHERE budget > 9999) "
                        "ORDER BY id"),
           [1, 2, 3, 4, 5, 6])
    # Engine value equality applies to materialized candidates: numeric
    # strings and numbers coerce, so sal IN (subquery of TEXT numbers) works.
    engine.execute("CREATE TABLE sq_in_t (id INTEGER PRIMARY KEY, v TEXT)")
    engine.execute("INSERT INTO sq_in_t VALUES (1,'100'), (2,'150')")
    _check("IN coerces numeric strings like the rest of the engine",
           _ids(engine, "SELECT id FROM sq_in_e WHERE sal IN "
                        "(SELECT v FROM sq_in_t) ORDER BY id"),
           [1, 3])
    # Duplicates in the subquery result are deduplicated invisibly.
    _check("duplicate subquery values do not change the result",
           _ids(engine, "SELECT id FROM sq_in_e WHERE dept IN "
                        "(SELECT dept FROM sq_in_e WHERE dept = 'eng') "
                        "ORDER BY id"),
           [1, 2, 6])
    # The fail-loud contract survives the empty-set rewrite: an unsupported
    # expression on the LEFT of IN must still raise, not silently vanish.
    _expect_raises(
        "unsupported LHS of IN over an empty subquery still fails loud",
        lambda: engine.execute(
            "SELECT id FROM sq_in_e WHERE SUBSTRING(name, 1, 1) IN "
            "(SELECT dname FROM sq_in_d WHERE budget > 9999)"),
        NotImplementedError)


def verify_in_dml(engine):
    print("\nVerifying IN-subqueries in DELETE / UPDATE (the data-loss shape)...")
    _populate(engine, "sq_dml_e", "sq_dml_d")

    # The shape that once wiped tables: DELETE ... NOT IN (SELECT ...).
    res = engine.execute(
        "DELETE FROM sq_dml_e WHERE id NOT IN "
        "(SELECT id FROM sq_dml_e WHERE sal >= 100)")
    # sal >= 100 -> ids 1,2,3,5 stay; ids 4 (sal 50) and 6 (sal NULL) go.
    _check("DELETE ... NOT IN deleted exactly the complement",
           res, [{"count": 2}])
    _check("DELETE ... NOT IN kept the matching rows (table not wiped)",
           _ids(engine, "SELECT id FROM sq_dml_e ORDER BY id"),
           [1, 2, 3, 5])

    engine.execute("UPDATE sq_dml_e SET sal = sal + 1000 WHERE dept IN "
                   "(SELECT dname FROM sq_dml_d WHERE budget > 800)")
    _check("UPDATE ... IN raised exactly the eng salaries",
           [r["sal"] for r in engine.execute(
               "SELECT sal FROM sq_dml_e ORDER BY id")],
           [1100, 1200, 150, 120])


def verify_scalar_subquery(engine):
    print("\nVerifying scalar (SELECT ...) subqueries...")
    _populate(engine, "sq_sc_e", "sq_sc_d")

    _check("scalar subquery in WHERE",
           _ids(engine, "SELECT id FROM sq_sc_e WHERE sal = "
                        "(SELECT MAX(sal) FROM sq_sc_e)"),
           [2])
    _check("scalar subquery in the SELECT list",
           [r["top"] for r in engine.execute(
               "SELECT id, (SELECT MAX(budget) FROM sq_sc_d) AS top "
               "FROM sq_sc_e WHERE id = 1")],
           [1000])
    _check("scalar subquery inside arithmetic",
           _ids(engine, "SELECT id FROM sq_sc_e WHERE sal > "
                        "(SELECT MAX(sal) FROM sq_sc_e) - 100 ORDER BY id"),
           [2, 3, 5])
    # Nested: the second-highest salary.
    _check("nested scalar subqueries",
           _ids(engine, "SELECT id FROM sq_sc_e WHERE sal = "
                        "(SELECT MAX(sal) FROM sq_sc_e WHERE sal < "
                        "(SELECT MAX(sal) FROM sq_sc_e))"),
           [3])
    # ORDER BY an output alias inside the subquery (the alias is part of the
    # subquery's own scope, so the correlation check must accept it).
    _check("subquery may ORDER BY its own SELECT alias",
           _ids(engine, "SELECT id FROM sq_sc_e WHERE sal = "
                        "(SELECT sal AS s FROM sq_sc_e WHERE sal IS NOT NULL "
                        "ORDER BY s DESC LIMIT 1)"),
           [2])
    # Empty result -> NULL: x = NULL is UNKNOWN -> no rows...
    _check("empty scalar subquery is NULL in a comparison (no rows)",
           _ids(engine, "SELECT id FROM sq_sc_e WHERE sal = "
                        "(SELECT sal FROM sq_sc_e WHERE sal > 9999)"),
           [])
    # ...and projects as NULL in the SELECT list.
    _check("empty scalar subquery projects as NULL",
           [r["v"] for r in engine.execute(
               "SELECT (SELECT sal FROM sq_sc_e WHERE sal > 9999) AS v "
               "FROM sq_sc_e WHERE id = 1")],
           [None])
    # CASE composes: the subquery materializes before CASE evaluates.
    _check("scalar subquery inside CASE",
           [r["lvl"] for r in engine.execute(
               "SELECT CASE WHEN sal >= (SELECT AVG(sal) FROM sq_sc_e) "
               "THEN 'hi' ELSE 'lo' END AS lvl "
               "FROM sq_sc_e WHERE sal IS NOT NULL ORDER BY id")],
           ["lo", "hi", "hi", "lo", "lo"])   # AVG = 124
    # HAVING over a scalar subquery.
    _check("scalar subquery in HAVING",
           [r["dept"] for r in engine.execute(
               "SELECT dept, COUNT(*) AS n FROM sq_sc_e "
               "WHERE dept IS NOT NULL GROUP BY dept "
               "HAVING COUNT(*) > (SELECT COUNT(*) FROM sq_sc_d) - 2 "
               "ORDER BY dept")],
           ["eng"])   # dept counts: eng 3, sales 1, ops 1; threshold > 1
    # UPDATE ... SET via a scalar subquery.
    engine.execute("UPDATE sq_sc_e SET sal = (SELECT MAX(budget) "
                   "FROM sq_sc_d) WHERE id = 4")
    _check("scalar subquery in UPDATE ... SET",
           [r["sal"] for r in engine.execute(
               "SELECT sal FROM sq_sc_e WHERE id = 4")],
           [1000])
    # INSERT ... VALUES with a subquery expression.
    engine.execute("INSERT INTO sq_sc_e (id, name, sal) VALUES "
                   "(7, 'g', (SELECT MAX(sal) FROM sq_sc_e) + 1)")
    _check("scalar subquery in INSERT ... VALUES",
           [r["sal"] for r in engine.execute(
               "SELECT sal FROM sq_sc_e WHERE id = 7")],
           [1001])

    _expect_raises(
        "scalar subquery with more than one row",
        lambda: engine.execute("SELECT id FROM sq_sc_e WHERE sal = "
                               "(SELECT sal FROM sq_sc_e)"),
        ValueError)
    _expect_raises(
        "scalar subquery with more than one column",
        lambda: engine.execute("SELECT id FROM sq_sc_e WHERE sal = "
                               "(SELECT id, sal FROM sq_sc_e WHERE id = 1)"),
        ValueError)
    _expect_raises(
        "IN subquery with more than one column",
        lambda: engine.execute("SELECT id FROM sq_sc_e WHERE sal IN "
                               "(SELECT id, sal FROM sq_sc_e)"),
        ValueError)


def verify_exists(engine):
    print("\nVerifying [NOT] EXISTS...")
    _populate(engine, "sq_ex_e", "sq_ex_d")

    _check("EXISTS over a nonempty subquery keeps every row",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE EXISTS "
                        "(SELECT 1 FROM sq_ex_d WHERE budget > 900) "
                        "ORDER BY id"),
           [1, 2, 3, 4, 5, 6])
    _check("EXISTS over an empty subquery keeps none",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE EXISTS "
                        "(SELECT 1 FROM sq_ex_d WHERE budget > 9999)"),
           [])
    _check("NOT EXISTS flips both",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE NOT EXISTS "
                        "(SELECT 1 FROM sq_ex_d WHERE budget > 9999) "
                        "ORDER BY id"),
           [1, 2, 3, 4, 5, 6])
    # A row of NULLs still exists (EXISTS tests rows, not values): ops has a
    # NULL budget but is a row.
    _check("EXISTS is TRUE for rows holding only NULL values",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE EXISTS "
                        "(SELECT budget FROM sq_ex_d WHERE budget IS NULL) "
                        "ORDER BY id"),
           [1, 2, 3, 4, 5, 6])
    _check("EXISTS accepts SELECT *",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE EXISTS "
                        "(SELECT * FROM sq_ex_d) AND id = 1"),
           [1])
    # The engine caps the probe with LIMIT 1 -- but only when the subquery
    # has no LIMIT of its own. An explicit LIMIT 0 must stay empty -> FALSE.
    _check("EXISTS honors an explicit LIMIT 0 (probe cap must not override)",
           _ids(engine, "SELECT id FROM sq_ex_e WHERE EXISTS "
                        "(SELECT 1 FROM sq_ex_d LIMIT 0)"),
           [])


def verify_quantified(engine):
    print("\nVerifying <op> ANY / SOME / ALL (SELECT ...)...")
    _populate(engine, "sq_qt_e", "sq_qt_d")
    # sq_qt_d budgets: 1000, 700, NULL.

    _check("= ANY behaves as IN",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE dept = ANY "
                        "(SELECT dname FROM sq_qt_d) ORDER BY id"),
           [1, 2, 3, 4, 6])
    _check("SOME is a synonym for ANY",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE dept = SOME "
                        "(SELECT dname FROM sq_qt_d) ORDER BY id"),
           [1, 2, 3, 4, 6])
    _check("> ANY matches above the minimum candidate",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal > ANY "
                        "(SELECT budget FROM sq_qt_d) ORDER BY id"),
           [])   # no salary beats even the 700 budget
    _check("< ANY with a NULL candidate still matches on a true comparison",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal < ANY "
                        "(SELECT budget FROM sq_qt_d) ORDER BY id"),
           [1, 2, 3, 4, 5])   # id 6 has NULL sal -> UNKNOWN
    _check("<> ALL behaves as NOT IN (NULL candidate -> nothing matches)",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal <> ALL "
                        "(SELECT budget FROM sq_qt_d)"),
           [])   # the NULL budget makes every unmatched row UNKNOWN
    _check("<> ALL over a NULL-free set behaves as NOT IN",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal <> ALL "
                        "(SELECT budget FROM sq_qt_d "
                        "WHERE budget IS NOT NULL) ORDER BY id"),
           [1, 2, 3, 4, 5])
    _check(">= ALL finds the maximum",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal >= ALL "
                        "(SELECT sal FROM sq_qt_e WHERE sal IS NOT NULL)"),
           [2])
    # Empty set: ANY -> FALSE, ALL -> TRUE, regardless of the left operand
    # (the NULL-sal row id 6 must appear under ALL too).
    _check("op ANY over an empty set is FALSE",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal > ANY "
                        "(SELECT budget FROM sq_qt_d WHERE budget > 9999)"),
           [])
    _check("op ALL over an empty set is TRUE (NULL operand included)",
           _ids(engine, "SELECT id FROM sq_qt_e WHERE sal > ALL "
                        "(SELECT budget FROM sq_qt_d WHERE budget > 9999) "
                        "ORDER BY id"),
           [1, 2, 3, 4, 5, 6])


def verify_correlation_fails_loud(engine):
    print("\nVerifying correlated subqueries fail loud...")
    _populate(engine, "sq_co_e", "sq_co_d")

    # Executed standalone, sq_co_e.dept would resolve to NULL inside the
    # subquery and silently join nothing / everything; the scope check must
    # reject it before execution instead.
    _expect_raises(
        "correlated EXISTS (qualified outer reference)",
        lambda: engine.execute(
            "SELECT id FROM sq_co_e WHERE EXISTS (SELECT 1 FROM sq_co_d "
            "WHERE sq_co_d.dname = sq_co_e.dept)"),
        NotImplementedError)
    _expect_raises(
        "correlated IN (bare outer column name)",
        lambda: engine.execute(
            "SELECT id FROM sq_co_e WHERE id IN "
            "(SELECT budget FROM sq_co_d WHERE dname = dept)"),
        NotImplementedError)
    _expect_raises(
        "unknown column in a subquery (would silently resolve to NULL)",
        lambda: engine.execute(
            "SELECT id FROM sq_co_e WHERE sal = "
            "(SELECT MAX(no_such_col) FROM sq_co_d)"),
        NotImplementedError)
    # Nested one level down: the inner subquery references the *middle*
    # subquery's table -- caught when the middle level materializes.
    _expect_raises(
        "correlation between subquery levels",
        lambda: engine.execute(
            "SELECT id FROM sq_co_e WHERE sal IN "
            "(SELECT budget FROM sq_co_d WHERE budget IN "
            "(SELECT sal FROM sq_co_e WHERE name = dname))"),
        NotImplementedError)


def verify_insert_from_select(engine):
    print("\nVerifying INSERT INTO ... SELECT...")
    _populate(engine, "sq_is_e", "sq_is_d")

    engine.execute("CREATE TABLE sq_is_arch (id INTEGER PRIMARY KEY, "
                   "nm TEXT, pay INTEGER)")
    # Positional mapping: name -> nm, sal -> pay.
    res = engine.execute("INSERT INTO sq_is_arch SELECT id, name, sal "
                         "FROM sq_is_e WHERE sal >= 120")
    _check("INSERT ... SELECT reports the inserted count", res, [{"count": 3}])
    _check("INSERT ... SELECT mapped columns positionally",
           engine.execute("SELECT * FROM sq_is_arch ORDER BY id"),
           [{"id": 2, "nm": "b", "pay": 200},
            {"id": 3, "nm": "c", "pay": 150},
            {"id": 5, "nm": "e", "pay": 120}])
    # Explicit target column list, expression + alias source, ORDER/LIMIT.
    engine.execute("INSERT INTO sq_is_arch (id, nm, pay) "
                   "SELECT id + 100, UPPER(name) AS nm, sal * 2 "
                   "FROM sq_is_e WHERE sal IS NOT NULL "
                   "ORDER BY sal DESC LIMIT 2")
    _check("INSERT ... SELECT with expressions, ORDER BY and LIMIT",
           engine.execute(
               "SELECT * FROM sq_is_arch WHERE id > 100 ORDER BY id"),
           [{"id": 102, "nm": "B", "pay": 400},
            {"id": 103, "nm": "C", "pay": 300}])
    # Aggregate source.
    engine.execute("CREATE TABLE sq_is_sum (dept TEXT PRIMARY KEY, "
                   "n INTEGER)")
    engine.execute("INSERT INTO sq_is_sum SELECT dept, COUNT(*) "
                   "FROM sq_is_e WHERE dept IS NOT NULL GROUP BY dept")
    _check("INSERT ... SELECT from an aggregate",
           engine.execute("SELECT * FROM sq_is_sum ORDER BY dept"),
           [{"dept": "eng", "n": 3}, {"dept": "ops", "n": 1},
            {"dept": "sales", "n": 1}])
    # Self-insert: the source rows materialize before the first write, so
    # this doubles the table exactly once (no chasing our own inserts).
    before = len(engine.execute("SELECT id FROM sq_is_e"))
    engine.execute("INSERT INTO sq_is_e SELECT id + 10, name, dept, sal "
                   "FROM sq_is_e")
    _check("self INSERT ... SELECT doubles the table exactly",
           len(engine.execute("SELECT id FROM sq_is_e")), before * 2)
    # A subquery inside the source SELECT (recursion through the engine).
    engine.execute("CREATE TABLE sq_is_top (id INTEGER PRIMARY KEY, "
                   "nm TEXT)")
    engine.execute("INSERT INTO sq_is_top SELECT id, name FROM sq_is_e "
                   "WHERE sal = (SELECT MAX(sal) FROM sq_is_e)")
    _check("subquery inside the INSERT source",
           engine.execute("SELECT * FROM sq_is_top ORDER BY id"),
           [{"id": 2, "nm": "b"}, {"id": 12, "nm": "b"}])

    _expect_raises(
        "column-count mismatch",
        lambda: engine.execute(
            "INSERT INTO sq_is_arch SELECT id, name FROM sq_is_e"),
        ValueError)
    _expect_raises(
        "explicit column list arity mismatch",
        lambda: engine.execute(
            "INSERT INTO sq_is_arch (id, nm) SELECT id, name, sal "
            "FROM sq_is_e"),
        ValueError)


def verify_joins_and_indexes(engine):
    print("\nVerifying subqueries compose with JOINs and index scans...")
    _populate(engine, "sq_jx_e", "sq_jx_d")

    _check("IN-subquery in a JOIN query's WHERE",
           [r["nm"] for r in engine.execute(
               "SELECT e.name AS nm, d.budget AS b FROM sq_jx_e e "
               "JOIN sq_jx_d d ON e.dept = d.dname "
               "WHERE e.id IN (SELECT id FROM sq_jx_e WHERE sal > 120) "
               "ORDER BY e.id")],
           ["b", "c"])
    # An equality against a scalar subquery becomes `col = literal` after
    # materialization, which the optimizer may serve from a secondary
    # index -- the rows must be identical either way.
    engine.execute("CREATE INDEX sq_jx_sal_idx ON sq_jx_e (sal)")
    _check("indexed column = scalar subquery uses the rewrite correctly",
           _ids(engine, "SELECT id FROM sq_jx_e WHERE sal = "
                        "(SELECT MAX(sal) FROM sq_jx_e)"),
           [2])
    _check("indexed column IN subquery",
           _ids(engine, "SELECT id FROM sq_jx_e WHERE sal IN "
                        "(SELECT sal FROM sq_jx_e WHERE sal > 140) "
                        "ORDER BY id"),
           [2, 3])


def verify_fail_loud_regressions(engine):
    print("\nVerifying the silently-wrong constructs now fail loud...")
    _populate(engine, "sq_fl_e", "sq_fl_d")

    # Derived tables used to bind the *inner* table and drop the subquery's
    # WHERE/projection: `FROM (SELECT a FROM u WHERE a > 5) s` returned all
    # rows and columns of u.
    _expect_raises(
        "derived table in FROM",
        lambda: engine.execute(
            "SELECT * FROM (SELECT id FROM sq_fl_e WHERE sal > 100) s"),
        NotImplementedError)
    _expect_raises(
        "derived table in JOIN",
        lambda: engine.execute(
            "SELECT * FROM sq_fl_e e JOIN "
            "(SELECT dname FROM sq_fl_d) d ON e.dept = d.dname"),
        NotImplementedError)
    _expect_raises(
        "derived table inside a subquery",
        lambda: engine.execute(
            "SELECT id FROM sq_fl_e WHERE id IN "
            "(SELECT id FROM (SELECT id FROM sq_fl_e) x)"),
        NotImplementedError)
    # CTAS used to create an empty zero-column table and ignore the SELECT.
    _expect_raises(
        "CREATE TABLE ... AS SELECT",
        lambda: engine.execute(
            "CREATE TABLE sq_fl_ctas AS SELECT * FROM sq_fl_e"),
        NotImplementedError)
    _check("CTAS did not half-create the table",
           engine.catalog.get_table("sq_fl_ctas"), None)
    # WITH used to be silently dropped when the CTE went unused.
    _expect_raises(
        "WITH / CTE",
        lambda: engine.execute(
            "WITH c AS (SELECT 1 AS x FROM sq_fl_e) SELECT id FROM sq_fl_e"),
        NotImplementedError)
    # Set-operation subquery bodies are unsupported -- loudly.
    _expect_raises(
        "UNION inside a subquery",
        lambda: engine.execute(
            "SELECT id FROM sq_fl_e WHERE id IN "
            "(SELECT id FROM sq_fl_e UNION SELECT id FROM sq_fl_e)"),
        NotImplementedError)


def verify_python_backend():
    """Subqueries materialize above the storage layer, so they are
    backend-agnostic -- confirm parity on the pure-Python backend."""
    print("\nVerifying subqueries on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "sq_py_e", "sq_py_d")
    _check("python backend IN subquery",
           _ids(engine, "SELECT id FROM sq_py_e WHERE dept IN "
                        "(SELECT dname FROM sq_py_d WHERE budget > 800) "
                        "ORDER BY id"),
           [1, 2, 6])
    _check("python backend NOT IN with NULL matches nothing",
           _ids(engine, "SELECT id FROM sq_py_e WHERE dept NOT IN "
                        "(SELECT dept FROM sq_py_e)"),
           [])
    _check("python backend scalar + EXISTS",
           _ids(engine, "SELECT id FROM sq_py_e WHERE sal = "
                        "(SELECT MAX(sal) FROM sq_py_e) AND EXISTS "
                        "(SELECT 1 FROM sq_py_d)"),
           [2])
    engine.execute("CREATE TABLE sq_py_arch (id INTEGER PRIMARY KEY, "
                   "nm TEXT)")
    engine.execute("INSERT INTO sq_py_arch SELECT id, name FROM sq_py_e "
                   "WHERE sal >= 150")
    _check("python backend INSERT ... SELECT",
           engine.execute("SELECT * FROM sq_py_arch ORDER BY id"),
           [{"id": 2, "nm": "b"}, {"id": 3, "nm": "c"}])


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_in_subquery(shared)
    verify_in_dml(shared)
    verify_scalar_subquery(shared)
    verify_exists(shared)
    verify_quantified(shared)
    verify_correlation_fails_loud(shared)
    verify_insert_from_select(shared)
    verify_joins_and_indexes(shared)
    verify_fail_loud_regressions(shared)
    verify_python_backend()
    print("\nAll subquery checks passed.")
