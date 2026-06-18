"""
Verify JOIN support in the FDB-style SQL engine.

JOINs used to be silently wrong in several ways -- the regressions this suite
locks down:

  * Same-named columns from two tables (the ubiquitous ``id`` / ``id``)
    clobbered each other in the merged row, so ``SELECT *`` lost a column.
  * The ON clause only worked when written left-table = right-table; the
    reversed/either-way form returned zero rows.
  * Projection was ignored entirely -- every JOIN returned all columns.
  * LEFT / RIGHT / FULL OUTER joins silently behaved as INNER (unmatched rows
    vanished instead of being NULL-padded).
  * Non-equi / compound ON conditions degraded to a cross join.

Now: rows carry ``table.col`` keys so columns coexist; the full ON predicate
is evaluated (any operator, either direction); outer joins NULL-pad; and a
genuinely ambiguous unqualified reference fails loud rather than guessing a
side -- the same contract the rest of the engine follows.

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


def _tuples(rows, cols):
    """Order-independent comparison helper: project each row to a tuple of the
    given keys and sort by string form (so None and mixed types are safe)."""
    out = [tuple(r.get(c) for c in cols) for r in rows]
    return sorted(out, key=lambda t: tuple(str(x) for x in t))


def _expect_raise(engine, label, sql, exc):
    try:
        engine.execute(sql)
    except exc:
        print(f"{PASS}: {label} raises {exc.__name__}")
    except Exception as e:  # noqa: BLE001 - report the wrong exception type
        print(f"{FAIL}: {label} raised {type(e).__name__}, expected {exc.__name__}: {e}")
        sys.exit(1)
    else:
        print(f"{FAIL}: {label} did not raise (silently-wrong regression)")
        sys.exit(1)


def _users_orders(engine, u, o):
    """users(id, name) one-to-many orders(id, user_id, item); all orders valid."""
    engine.execute(f"CREATE TABLE {u} (id INTEGER PRIMARY KEY, name TEXT)")
    engine.execute(f"CREATE TABLE {o} (id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT)")
    for i, n in [(1, "Alice"), (2, "Bob"), (3, "Carol")]:   # Carol has no orders
        engine.execute(f"INSERT INTO {u} VALUES ({i}, '{n}')")
    for i, uid, it in [(10, 1, "book"), (11, 1, "pen"), (12, 2, "cup")]:
        engine.execute(f"INSERT INTO {o} VALUES ({i}, {uid}, '{it}')")


def verify_inner_basic(engine):
    print("Verifying INNER JOIN basics...")
    _users_orders(engine, "ib_u", "ib_o")
    rows = engine.execute(
        "SELECT ib_u.name, ib_o.item FROM ib_u JOIN ib_o "
        "ON ib_u.id = ib_o.user_id")
    _check("inner join rows", _tuples(rows, ["name", "item"]),
           [("Alice", "book"), ("Alice", "pen"), ("Bob", "cup")])

    # ON written the other way round must give the same result (was 0 rows).
    rows = engine.execute(
        "SELECT ib_u.name, ib_o.item FROM ib_u JOIN ib_o "
        "ON ib_o.user_id = ib_u.id")
    _check("inner join, reversed ON", _tuples(rows, ["name", "item"]),
           [("Alice", "book"), ("Alice", "pen"), ("Bob", "cup")])


def verify_star_keeps_both_collisions(engine):
    print("\nVerifying SELECT * keeps colliding columns (qualified keys)...")
    _users_orders(engine, "st_u", "st_o")
    rows = engine.execute(
        "SELECT * FROM st_u JOIN st_o ON st_u.id = st_o.user_id "
        "ORDER BY st_o.id")
    first = rows[0]
    # The colliding `id` survives on both sides under qualified keys; the
    # non-colliding columns keep their bare names.
    _check("st_u.id preserved", first.get("st_u.id"), 1)
    _check("st_o.id preserved", first.get("st_o.id"), 10)
    _check("bare name preserved", first.get("name"), "Alice")
    _check("bare item preserved", first.get("item"), "book")


def verify_aliases_and_projection(engine):
    print("\nVerifying table aliases + qualified/aliased projection...")
    _users_orders(engine, "al_u", "al_o")
    rows = engine.execute(
        "SELECT u.name AS who, o.item AS thing FROM al_u u JOIN al_o o "
        "ON u.id = o.user_id ORDER BY o.id")
    _check("aliased projection", [(r["who"], r["thing"]) for r in rows],
           [("Alice", "book"), ("Alice", "pen"), ("Bob", "cup")])
    # Projection actually restricts columns (it used to be ignored).
    _check("projection restricts columns", sorted(rows[0].keys()), ["thing", "who"])


def verify_left_right_full(engine):
    print("\nVerifying LEFT / RIGHT / FULL OUTER joins (NULL padding)...")
    # lu(id,name): Carol(3) has no orders. lo(id,uid,item): ghost(12->user 99)
    # has no matching user. So exactly one orphan on each side.
    engine.execute("CREATE TABLE lu (id INTEGER PRIMARY KEY, name TEXT)")
    engine.execute("CREATE TABLE lo (id INTEGER PRIMARY KEY, uid INTEGER, item TEXT)")
    for i, n in [(1, "Alice"), (2, "Bob"), (3, "Carol")]:
        engine.execute(f"INSERT INTO lu VALUES ({i}, '{n}')")
    for i, uid, it in [(10, 1, "book"), (11, 2, "cup"), (12, 99, "ghost")]:
        engine.execute(f"INSERT INTO lo VALUES ({i}, {uid}, '{it}')")

    inner = engine.execute("SELECT lu.name, lo.item FROM lu JOIN lo ON lu.id = lo.uid")
    _check("INNER drops both orphans", _tuples(inner, ["name", "item"]),
           [("Alice", "book"), ("Bob", "cup")])

    left = engine.execute("SELECT lu.name, lo.item FROM lu LEFT JOIN lo ON lu.id = lo.uid")
    _check("LEFT pads unmatched left (Carol)", _tuples(left, ["name", "item"]),
           [("Alice", "book"), ("Bob", "cup"), ("Carol", None)])

    right = engine.execute("SELECT lu.name, lo.item FROM lu RIGHT JOIN lo ON lu.id = lo.uid")
    _check("RIGHT pads unmatched right (ghost)", _tuples(right, ["name", "item"]),
           [("Alice", "book"), ("Bob", "cup"), (None, "ghost")])

    full = engine.execute("SELECT lu.name, lo.item FROM lu FULL OUTER JOIN lo ON lu.id = lo.uid")
    _check("FULL pads both orphans", _tuples(full, ["name", "item"]),
           [("Alice", "book"), ("Bob", "cup"), ("Carol", None), (None, "ghost")])


def verify_cross_and_comma(engine):
    print("\nVerifying CROSS JOIN and comma join (cartesian product)...")
    engine.execute("CREATE TABLE cx_a (id INTEGER PRIMARY KEY, x TEXT)")
    engine.execute("CREATE TABLE cx_b (id INTEGER PRIMARY KEY, y TEXT)")
    for i, v in [(1, "a1"), (2, "a2")]:
        engine.execute(f"INSERT INTO cx_a VALUES ({i}, '{v}')")
    for i, v in [(1, "b1"), (2, "b2"), (3, "b3")]:
        engine.execute(f"INSERT INTO cx_b VALUES ({i}, '{v}')")
    cross = engine.execute("SELECT cx_a.x, cx_b.y FROM cx_a CROSS JOIN cx_b")
    _check("CROSS JOIN is 2x3", len(cross), 6)
    comma = engine.execute("SELECT cx_a.x, cx_b.y FROM cx_a, cx_b")
    _check("comma join == cross", _tuples(comma, ["x", "y"]), _tuples(cross, ["x", "y"]))


def verify_non_equi_and_compound(engine):
    print("\nVerifying non-equi and compound ON conditions...")
    engine.execute("CREATE TABLE ne_a (id INTEGER PRIMARY KEY, v INTEGER)")
    engine.execute("CREATE TABLE ne_b (id INTEGER PRIMARY KEY, v INTEGER)")
    for i, v in [(1, 1), (2, 2), (3, 3)]:
        engine.execute(f"INSERT INTO ne_a VALUES ({i}, {v})")
    for i, v in [(1, 2), (2, 3)]:
        engine.execute(f"INSERT INTO ne_b VALUES ({i}, {v})")
    # a.v < b.v pairs: (1,2),(1,3),(2,3). Forces the nested-loop path.
    rows = engine.execute(
        "SELECT ne_a.v AS av, ne_b.v AS bv FROM ne_a JOIN ne_b ON ne_a.v < ne_b.v")
    _check("non-equi ON (<)", _tuples(rows, ["av", "bv"]),
           [(1, 2), (1, 3), (2, 3)])

    # Compound ON: equality AND inequality.
    engine.execute("CREATE TABLE cp_a (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
    engine.execute("CREATE TABLE cp_b (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
    for i, k, v in [(1, "x", 5), (2, "x", 1), (3, "y", 9)]:
        engine.execute(f"INSERT INTO cp_a VALUES ({i}, '{k}', {v})")
    for i, k, v in [(1, "x", 4), (2, "y", 9), (3, "y", 100)]:
        engine.execute(f"INSERT INTO cp_b VALUES ({i}, '{k}', {v})")
    # k equal AND a.v > b.v: ('x',5)>('x',4) yes; ('x',1)>('x',4) no;
    # ('y',9)>('y',9) no, ('y',9)>('y',100) no.  -> one row.
    rows = engine.execute(
        "SELECT cp_a.v AS av, cp_b.v AS bv FROM cp_a JOIN cp_b "
        "ON cp_a.k = cp_b.k AND cp_a.v > cp_b.v")
    _check("compound ON (= AND >)", _tuples(rows, ["av", "bv"]), [(5, 4)])


def verify_where_order_limit(engine):
    print("\nVerifying WHERE / ORDER BY / LIMIT over a JOIN...")
    _users_orders(engine, "wo_u", "wo_o")
    # WHERE over the merged row.
    rows = engine.execute(
        "SELECT wo_u.name AS n, wo_o.item AS it FROM wo_u JOIN wo_o "
        "ON wo_u.id = wo_o.user_id WHERE wo_u.name = 'Alice' ORDER BY wo_o.item")
    _check("WHERE + ORDER BY", [(r["n"], r["it"]) for r in rows],
           [("Alice", "book"), ("Alice", "pen")])
    # ORDER BY + LIMIT, including a positional key.
    rows = engine.execute(
        "SELECT wo_u.name AS n, wo_o.item AS it FROM wo_u JOIN wo_o "
        "ON wo_u.id = wo_o.user_id ORDER BY 2 DESC LIMIT 1")
    _check("ORDER BY position + LIMIT", [(r["n"], r["it"]) for r in rows],
           [("Alice", "pen")])

    # WHERE on the right side of a LEFT JOIN drops the NULL-padded row.
    engine.execute("CREATE TABLE wl_u (id INTEGER PRIMARY KEY, name TEXT)")
    engine.execute("CREATE TABLE wl_o (id INTEGER PRIMARY KEY, uid INTEGER, item TEXT)")
    for i, n in [(1, "Alice"), (2, "Carol")]:
        engine.execute(f"INSERT INTO wl_u VALUES ({i}, '{n}')")
    engine.execute("INSERT INTO wl_o VALUES (10, 1, 'book')")
    rows = engine.execute(
        "SELECT wl_u.name AS n FROM wl_u LEFT JOIN wl_o ON wl_u.id = wl_o.uid "
        "WHERE wl_o.item IS NOT NULL")
    _check("WHERE IS NOT NULL filters padded rows", _tuples(rows, ["n"]), [("Alice",)])


def verify_distinct_join(engine):
    print("\nVerifying SELECT DISTINCT over a JOIN...")
    _users_orders(engine, "ds_u", "ds_o")
    # Each user with >=1 order appears once, regardless of order count.
    rows = engine.execute(
        "SELECT DISTINCT ds_u.name AS n FROM ds_u JOIN ds_o "
        "ON ds_u.id = ds_o.user_id ORDER BY n")
    _check("DISTINCT over join", [r["n"] for r in rows], ["Alice", "Bob"])
    # ORDER BY a selected (qualified) column, descending.
    rows = engine.execute(
        "SELECT DISTINCT ds_u.name FROM ds_u JOIN ds_o "
        "ON ds_u.id = ds_o.user_id ORDER BY ds_u.name DESC")
    _check("DISTINCT ORDER BY selected col DESC", [r["name"] for r in rows],
           ["Bob", "Alice"])
    # ORDER BY a column that is NOT in the SELECT list is invalid for DISTINCT
    # (nothing to sort against post-projection) -- must fail loud, not silently
    # ignore the ORDER BY.
    _expect_raise(engine, "DISTINCT ORDER BY non-output column",
                  "SELECT DISTINCT ds_u.name FROM ds_u JOIN ds_o "
                  "ON ds_u.id = ds_o.user_id ORDER BY ds_u.id",
                  NotImplementedError)


def verify_group_by_join(engine):
    print("\nVerifying GROUP BY + aggregate over a JOIN...")
    _users_orders(engine, "gb_u", "gb_o")
    rows = engine.execute(
        "SELECT gb_u.name AS n, COUNT(*) AS c FROM gb_u JOIN gb_o "
        "ON gb_u.id = gb_o.user_id GROUP BY gb_u.name ORDER BY c DESC, n")
    _check("count orders per user", [(r["n"], r["c"]) for r in rows],
           [("Alice", 2), ("Bob", 1)])


def verify_three_way_join(engine):
    print("\nVerifying 3-table join...")
    engine.execute("CREATE TABLE tw_u (id INTEGER PRIMARY KEY, name TEXT)")
    engine.execute("CREATE TABLE tw_o (id INTEGER PRIMARY KEY, uid INTEGER, pid INTEGER)")
    engine.execute("CREATE TABLE tw_p (id INTEGER PRIMARY KEY, label TEXT)")
    engine.execute("INSERT INTO tw_u VALUES (1, 'Alice')")
    engine.execute("INSERT INTO tw_u VALUES (2, 'Bob')")
    engine.execute("INSERT INTO tw_o VALUES (10, 1, 100)")
    engine.execute("INSERT INTO tw_o VALUES (11, 2, 101)")
    engine.execute("INSERT INTO tw_p VALUES (100, 'book')")
    engine.execute("INSERT INTO tw_p VALUES (101, 'cup')")
    rows = engine.execute(
        "SELECT tw_u.name AS who, tw_p.label AS thing "
        "FROM tw_u JOIN tw_o ON tw_u.id = tw_o.uid "
        "JOIN tw_p ON tw_o.pid = tw_p.id ORDER BY who")
    _check("3-way join", [(r["who"], r["thing"]) for r in rows],
           [("Alice", "book"), ("Bob", "cup")])


def verify_self_join(engine):
    """A table joined to itself via two aliases: every column collides, so all
    references must be qualified (the bind-time ambiguity check enforces it)."""
    print("\nVerifying self-join...")
    engine.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, mgr_id INTEGER, name TEXT)")
    for i, m, n in [(1, 0, "CEO"), (2, 1, "VP"), (3, 1, "Dir")]:
        engine.execute(f"INSERT INTO emp VALUES ({i}, {m}, '{n}')")
    rows = engine.execute(
        "SELECT e.name AS emp, m.name AS mgr FROM emp e JOIN emp m "
        "ON e.mgr_id = m.id ORDER BY emp")
    # CEO's mgr_id (0) matches no id, so the CEO row is dropped by the inner join.
    _check("self-join to manager", [(r["emp"], r["mgr"]) for r in rows],
           [("Dir", "CEO"), ("VP", "CEO")])


def verify_join_key_coercion(engine):
    print("\nVerifying numeric-string coercion in equi-join keys...")
    engine.execute("CREATE TABLE jc_i (id INTEGER PRIMARY KEY, k INTEGER)")
    engine.execute("CREATE TABLE jc_s (id INTEGER PRIMARY KEY, k TEXT)")
    engine.execute("INSERT INTO jc_i VALUES (1, 7)")
    engine.execute("INSERT INTO jc_s VALUES (1, '7')")   # text "7" == int 7
    rows = engine.execute(
        "SELECT jc_i.id AS a, jc_s.id AS b FROM jc_i JOIN jc_s ON jc_i.k = jc_s.k")
    _check("int key matches numeric-string key", _tuples(rows, ["a", "b"]), [(1, 1)])


def verify_null_join_keys(engine):
    """NULL never equi-joins: ``NULL = NULL`` is UNKNOWN, not true. This must
    hold on both the hash-join fast path and the nested-loop path."""
    print("\nVerifying NULL join keys do not match...")
    engine.execute("CREATE TABLE nk_u (id INTEGER PRIMARY KEY, k INTEGER, name TEXT)")
    engine.execute("CREATE TABLE nk_o (id INTEGER PRIMARY KEY, k INTEGER, item TEXT)")
    engine.execute("INSERT INTO nk_u VALUES (1, 5, 'matched')")
    engine.execute("INSERT INTO nk_u (id, name) VALUES (2, 'null_key_u')")   # k NULL
    engine.execute("INSERT INTO nk_o VALUES (10, 5, 'hit')")
    engine.execute("INSERT INTO nk_o (id, item) VALUES (11, 'null_key_o')")  # k NULL
    inner = engine.execute("SELECT nk_u.name AS n, nk_o.item AS it FROM nk_u "
                           "JOIN nk_o ON nk_u.k = nk_o.k")
    _check("INNER: NULL keys excluded", _tuples(inner, ["n", "it"]),
           [("matched", "hit")])
    # LEFT JOIN: the NULL-key left row matches nothing -> one NULL-padded row.
    left = engine.execute("SELECT nk_u.name AS n, nk_o.item AS it FROM nk_u "
                          "LEFT JOIN nk_o ON nk_u.k = nk_o.k")
    _check("LEFT: NULL key row is padded, not matched", _tuples(left, ["n", "it"]),
           [("matched", "hit"), ("null_key_u", None)])


def verify_failures(engine):
    print("\nVerifying ambiguous / invalid joins fail loud...")
    _users_orders(engine, "fa_u", "fa_o")
    # Both tables have `id`: an unqualified reference is ambiguous.
    _expect_raise(engine, "ambiguous unqualified column",
                  "SELECT id FROM fa_u JOIN fa_o ON fa_u.id = fa_o.user_id",
                  ValueError)
    # Two projected columns landing on the same output key.
    _expect_raise(engine, "duplicate output column",
                  "SELECT fa_u.id, fa_o.id FROM fa_u JOIN fa_o ON fa_u.id = fa_o.user_id",
                  ValueError)
    # Unknown table.
    _expect_raise(engine, "unknown joined table",
                  "SELECT fa_u.name FROM fa_u JOIN nope ON fa_u.id = nope.x",
                  ValueError)


def verify_python_backend():
    """Join operators sit above the storage scan, so they are backend-agnostic
    -- confirm parity on the pure-Python backend too."""
    print("\nVerifying joins on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _users_orders(engine, "py_u", "py_o")
    rows = engine.execute(
        "SELECT py_u.name AS n, COUNT(*) AS c FROM py_u LEFT JOIN py_o "
        "ON py_u.id = py_o.user_id GROUP BY py_u.name ORDER BY n")
    # LEFT JOIN: Carol (no orders) contributes one NULL-padded row -> COUNT(*) 1.
    _check("python backend LEFT JOIN + GROUP BY",
           [(r["n"], r["c"]) for r in rows],
           [("Alice", 2), ("Bob", 1), ("Carol", 1)])


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_inner_basic(shared)
    verify_star_keeps_both_collisions(shared)
    verify_aliases_and_projection(shared)
    verify_left_right_full(shared)
    verify_cross_and_comma(shared)
    verify_non_equi_and_compound(shared)
    verify_where_order_limit(shared)
    verify_distinct_join(shared)
    verify_group_by_join(shared)
    verify_three_way_join(shared)
    verify_self_join(shared)
    verify_join_key_coercion(shared)
    verify_null_join_keys(shared)
    verify_failures(shared)
    verify_python_backend()
    print("\nAll JOIN checks passed.")
