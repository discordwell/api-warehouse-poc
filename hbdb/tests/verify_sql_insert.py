"""
Verify INSERT / UPDATE value handling and per-database read-cache scoping
in the FDB-style SQL engine.

Regression guards for four data-corruption bugs:

  1. Multi-row INSERT (``INSERT ... VALUES (..),(..),(..)``) bound only the
     first tuple, silently dropping every other row.
  2. INSERT literal coercion was ``int(x) if x.isdigit() else x``, which only
     survived ints and strings: floats became strings, ``-5`` lost its sign
     (stored as ``'5'``), ``TRUE`` became ``''`` and ``NULL`` became the
     string ``'NULL'``.
  3. UPDATE ``SET col = <literal>`` reused that same broken coercion, so
     ``SET price = 2.5`` stored the string ``'2.5'`` (while column
     expressions like ``balance + 10`` kept working and must stay working).
  4. The SQL read cache was a process-wide singleton keyed by storage key
     (/t/{table_id}/_r/{pk}). Table ids restart at 1 per database, so two
     HBDB instances in one process collided and one database served another's
     row. The cache is now scoped to each HBDB.

Note: every HBDB in one process+CWD shares the WAL, so a second instance
recovers the first one's catalog. Each scenario uses uniquely named tables;
the cross-instance cache test runs in its own temp CWD so both databases
start from an empty WAL and deterministically reuse table id 1.
"""
import os
import sys
import tempfile

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


def verify_multi_row(engine):
    print("Verifying multi-row INSERT...")
    engine.execute("CREATE TABLE mr (id INTEGER PRIMARY KEY, name TEXT)")
    res = engine.execute(
        "INSERT INTO mr (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    _check("INSERT reports 3 rows", res[0]["count"], 3)

    rows = {r["id"]: r["name"] for r in engine.execute("SELECT * FROM mr")}
    _check("all three rows present", sorted(rows), [1, 2, 3])
    _check("row values intact", [rows[1], rows[2], rows[3]], ["a", "b", "c"])


def verify_literal_types():
    """Float / negative / boolean / NULL literals must round-trip with their
    real Python types -- on both the native and pure-Python backends, since
    they go through different scan/decode paths."""
    for force_python in (False, True):
        mode = "python" if force_python else "native/default"
        t = "lit_py" if force_python else "lit_nat"
        print(f"\nVerifying INSERT literal types ({mode})...")
        engine = SQLEngine(HBDB(force_python=force_python))
        engine.execute(
            f"CREATE TABLE {t} (id INTEGER PRIMARY KEY, price REAL, "
            f"bal INTEGER, active BOOLEAN, note TEXT)")
        engine.execute(
            f"INSERT INTO {t} (id, price, bal, active, note) "
            f"VALUES (1, 3.14, -5, TRUE, NULL)")
        row = engine.execute(f"SELECT * FROM {t} WHERE id = 1")[0]

        _check(f"float stays float ({mode})", row["price"], 3.14)
        _check(f"negative keeps sign ({mode})", row["bal"], -5)
        _check(f"boolean stays bool ({mode})", row["active"], True)
        _check(f"NULL stays None ({mode})", row["note"], None)

        # The negative actually has to compare as a number, not sort as text.
        found = engine.execute(f"SELECT * FROM {t} WHERE bal < 0")
        _check(f"WHERE bal < 0 finds the row ({mode})",
               [r["id"] for r in found], [1])


def verify_update_set_types(engine):
    print("\nVerifying UPDATE SET value handling...")
    engine.execute(
        "CREATE TABLE us (id INTEGER PRIMARY KEY, price REAL, bal INTEGER)")
    engine.execute("INSERT INTO us (id, price, bal) VALUES (1, 1.0, 100)")

    engine.execute("UPDATE us SET price = 2.5 WHERE id = 1")
    _check("SET float literal", engine.execute(
        "SELECT * FROM us WHERE id = 1")[0]["price"], 2.5)

    # Column expressions must keep working (this path was never broken).
    engine.execute("UPDATE us SET bal = bal + 10 WHERE id = 1")
    _check("SET column expression", engine.execute(
        "SELECT * FROM us WHERE id = 1")[0]["bal"], 110)

    engine.execute("UPDATE us SET bal = -7 WHERE id = 1")
    _check("SET negative literal", engine.execute(
        "SELECT * FROM us WHERE id = 1")[0]["bal"], -7)


def verify_arity_mismatch(engine):
    print("\nVerifying value/column arity is checked...")
    engine.execute(
        "CREATE TABLE ar (a INTEGER PRIMARY KEY, b TEXT, c TEXT)")
    try:
        engine.execute("INSERT INTO ar (a, b, c) VALUES (1, 'x')")
    except ValueError:
        print(f"{PASS}: too few values raises ValueError")
    else:
        print(f"{FAIL}: arity mismatch silently accepted")
        sys.exit(1)


def verify_indexed_negative_lookup(engine):
    """An indexed point lookup must find the same rows a full scan would.

    The optimizer used to extract the lookup value with ``cond.right.this``,
    which yields the string ``'5'`` for the literal ``-5``; combined with the
    now-correctly-typed value INSERT writes into the index (``-5``), an
    indexed ``WHERE bal = -5`` silently returned nothing while an unindexed
    scan returned the row. Both must agree.
    """
    print("\nVerifying indexed lookup matches scan for typed values...")
    engine.execute("CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
    engine.execute("CREATE INDEX idx_bal ON acct (bal)")
    engine.execute("INSERT INTO acct (id, bal) VALUES (1, -5), (2, 10), (3, -5)")

    indexed = sorted(r["id"] for r in engine.execute(
        "SELECT * FROM acct WHERE bal = -5"))
    _check("indexed WHERE bal = -5", indexed, [1, 3])
    _check("indexed WHERE bal = 10",
           [r["id"] for r in engine.execute("SELECT * FROM acct WHERE bal = 10")],
           [2])

    # Same query against an unindexed twin table must give the same answer.
    engine.execute("CREATE TABLE acct_ni (id INTEGER PRIMARY KEY, bal INTEGER)")
    engine.execute("INSERT INTO acct_ni (id, bal) VALUES (1, -5), (2, 10), (3, -5)")
    _check("unindexed WHERE bal = -5 agrees",
           sorted(r["id"] for r in engine.execute(
               "SELECT * FROM acct_ni WHERE bal = -5")),
           [1, 3])


def verify_cache_isolation():
    """Two independent in-process databases must not share decoded rows.

    Run in a fresh CWD so both HBDBs start from an empty WAL and each assigns
    table id 1 -- the exact collision that made the global cache leak.
    """
    print("\nVerifying read cache is per-database...")
    prev_cwd = os.getcwd()
    with tempfile.TemporaryDirectory(prefix="hbdb_cache_") as tmp:
        os.chdir(tmp)
        try:
            a = SQLEngine(HBDB())
            b = SQLEngine(HBDB())
            _check("databases have distinct caches",
                   a.db.read_cache is b.db.read_cache, False)

            a.execute("CREATE TABLE ta (id INTEGER PRIMARY KEY, who TEXT)")
            b.execute("CREATE TABLE tb (id INTEGER PRIMARY KEY, who TEXT)")
            # Same table id -> same storage key in each backend.
            _check("both tables reuse id 1",
                   (a.catalog.get_table("ta").id, b.catalog.get_table("tb").id),
                   (1, 1))

            a.execute("INSERT INTO ta (id, who) VALUES (1, 'database-A')")
            b.execute("INSERT INTO tb (id, who) VALUES (1, 'database-B')")

            # Warm A's cache, then read B: B must see its own row.
            a.execute("SELECT * FROM ta WHERE id = 1")
            _check("B does not read A's cached row",
                   b.execute("SELECT * FROM tb WHERE id = 1")[0]["who"],
                   "database-B")
        finally:
            os.chdir(prev_cwd)


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_multi_row(shared)
    verify_literal_types()
    verify_update_set_types(shared)
    verify_arity_mismatch(shared)
    verify_indexed_negative_lookup(shared)
    verify_cache_isolation()
    print("\nAll INSERT/UPDATE/cache checks passed.")
