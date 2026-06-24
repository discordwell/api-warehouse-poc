"""
Verify the CASE expression (searched and simple) in the FDB-style SQL engine.

CASE resolves through the one shared operand resolver (``hbdb/sql/predicates.py``),
so -- like the other scalar functions -- it works in every clause that takes an
operand at once: SELECT projection, WHERE, ORDER BY, GROUP BY, HAVING and
``UPDATE ... SET``. This suite exercises each of those clauses and pins down the
SQL semantics that are easy to get subtly wrong:

  * Searched CASE WHENs use three-valued logic: only a *definite* True selects
    its result, so a WHEN that is UNKNOWN (touches NULL) falls through, and a
    row matching no WHEN takes the ELSE (or NULL when there is no ELSE).
  * Simple CASE compares the operand to each WHEN value with the engine's ``=``
    (numeric-string coercion included); because ``x = NULL`` is UNKNOWN, a NULL
    operand -- or a ``WHEN NULL`` -- never matches.
  * Only the selected branch is evaluated (lazy), so an erroring expression in
    a branch that is not taken is harmless -- but an unsupported function in the
    branch that *is* taken still fails loud.

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


def _vals(engine, sql, col):
    """Return the list of ``col`` values across the result rows, in order."""
    return [r[col] for r in engine.execute(sql)]


def _expect_raises(label, fn, exc):
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
    """A small table with NULLs in both a text (dept) and a numeric (age)
    column, so the NULL paths of both searched and simple CASE are exercised.
    Rows are inserted out of PK order so nothing depends on insertion order.

    By id:  1 Alice/30/eng  2 Bob/25/eng  3 Carol/17/sales
            4 Dave/NULL/sales  5 NULL/40/NULL
    """
    engine.execute(
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, "
        f"age INTEGER, dept TEXT)")
    engine.execute(f"INSERT INTO {table} VALUES (3, 'Carol', 17, 'sales')")
    engine.execute(f"INSERT INTO {table} VALUES (1, 'Alice', 30, 'eng')")
    engine.execute(f"INSERT INTO {table} VALUES (2, 'Bob', 25, 'eng')")
    engine.execute(f"INSERT INTO {table} (id, name, dept) "
                   f"VALUES (4, 'Dave', 'sales')")   # age NULL
    engine.execute(f"INSERT INTO {table} (id, age) VALUES (5, 40)")  # name/dept NULL


def verify_searched_case(engine):
    print("Verifying searched CASE (CASE WHEN ... THEN ... [ELSE ...] END)...")
    _populate(engine, "caset_s")

    # Multi-branch bucketing; a row matching no WHEN takes ELSE. NULL age (Dave)
    # makes every WHEN UNKNOWN, so it falls through to ELSE ('senior') too.
    _check("age buckets with ELSE",
           _vals(engine,
                 "SELECT CASE WHEN age < 18 THEN 'minor' "
                 "WHEN age < 30 THEN 'young' ELSE 'senior' END AS b "
                 "FROM caset_s ORDER BY id", "b"),
           ["senior", "young", "minor", "senior", "senior"])
    # An explicit IS NULL WHEN distinguishes the NULL-age row from the ELSE
    # bucket (proving the UNKNOWN-falls-through behavior above was real).
    _check("IS NULL WHEN distinguishes NULL age",
           _vals(engine,
                 "SELECT CASE WHEN age IS NULL THEN 'unknown' "
                 "WHEN age >= 30 THEN 'old' ELSE 'notold' END AS b "
                 "FROM caset_s ORDER BY id", "b"),
           ["old", "notold", "notold", "unknown", "old"])
    # No ELSE -> NULL for every unmatched row.
    _check("no ELSE yields NULL",
           _vals(engine,
                 "SELECT CASE WHEN age >= 40 THEN 'max' END AS b "
                 "FROM caset_s ORDER BY id", "b"),
           [None, None, None, None, "max"])


def verify_simple_case(engine):
    print("\nVerifying simple CASE (CASE x WHEN v THEN ... END)...")
    _populate(engine, "caset_p")

    # Operand compared to each WHEN with the engine's =; Eve's NULL dept matches
    # no WHEN and takes the ELSE (0).
    _check("dept mapped with ELSE",
           _vals(engine,
                 "SELECT CASE dept WHEN 'eng' THEN 1 WHEN 'sales' THEN 2 "
                 "ELSE 0 END AS d FROM caset_p ORDER BY id", "d"),
           [1, 1, 2, 2, 0])
    # No ELSE: unmatched (sales) rows and the NULL-operand row are NULL.
    _check("simple CASE without ELSE -> NULL",
           _vals(engine,
                 "SELECT CASE dept WHEN 'eng' THEN 'E' END AS d "
                 "FROM caset_p ORDER BY id", "d"),
           ["E", "E", None, None, None])
    # Numeric-string coercion: an INTEGER column equals a numeric *string*
    # literal, the same coercion WHERE / = use elsewhere in the engine.
    _check("simple CASE coerces numeric strings (age WHEN '30')",
           sorted(_vals(engine,
                        "SELECT id FROM caset_p "
                        "WHERE CASE age WHEN '30' THEN 1 ELSE 0 END = 1", "id")),
           [1])
    # The classic gotcha: WHEN NULL never matches (x = NULL is UNKNOWN), so even
    # a NULL operand takes the ELSE -- you cannot test for NULL with simple CASE.
    _check("WHEN NULL never matches (even a NULL operand)",
           _vals(engine,
                 "SELECT CASE age WHEN NULL THEN 'isnull' ELSE 'notnull' END "
                 "AS d FROM caset_p WHERE id = 4", "d"),
           ["notnull"])


def verify_case_in_clauses(engine):
    print("\nVerifying CASE in WHERE / ORDER BY / GROUP BY / HAVING...")
    _populate(engine, "caset_c")

    # WHERE: a CASE operand inside a comparison. The THEN (NULL dept -> 100) and
    # ELSE (-> age) branches are both exercised; Eve (NULL dept -> 100) and
    # Alice (30) pass >= 30.
    _check("WHERE over a CASE expression",
           sorted(_vals(engine,
                        "SELECT id FROM caset_c WHERE "
                        "(CASE WHEN dept IS NULL THEN 100 ELSE age END) >= 30",
                        "id")),
           [1, 5])
    # ORDER BY a CASE expression (eng rows sort last via key 1).
    _check("ORDER BY a CASE expression",
           _vals(engine,
                 "SELECT id FROM caset_c "
                 "ORDER BY CASE dept WHEN 'eng' THEN 1 ELSE 0 END, id", "id"),
           [3, 4, 5, 1, 2])
    # GROUP BY a CASE expression: the SELECT item is the same CASE, so it is a
    # valid grouping key. NULL age (Dave) -> ELSE 'ge30'.
    _check("GROUP BY a CASE expression",
           engine.execute(
               "SELECT CASE WHEN age < 30 THEN 'lt30' ELSE 'ge30' END AS b, "
               "COUNT(*) AS n FROM caset_c "
               "GROUP BY CASE WHEN age < 30 THEN 'lt30' ELSE 'ge30' END "
               "ORDER BY b"),
           [{"b": "ge30", "n": 3}, {"b": "lt30", "n": 2}])
    # HAVING with a CASE over an aggregate (keep groups of 2+).
    _check("HAVING with a CASE over an aggregate",
           engine.execute(
               "SELECT dept, COUNT(*) AS n FROM caset_c GROUP BY dept "
               "HAVING CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END = 1 "
               "ORDER BY dept"),
           [{"dept": "eng", "n": 2}, {"dept": "sales", "n": 2}])


def verify_case_in_update(engine):
    print("\nVerifying CASE in UPDATE ... SET...")
    _populate(engine, "caset_u")

    # Promote everyone 30+ to 'senior'; others keep their dept via a column
    # reference in the ELSE branch. NULL age (Dave) -> ELSE (unchanged 'sales').
    engine.execute(
        "UPDATE caset_u SET dept = CASE WHEN age >= 30 THEN 'senior' "
        "ELSE dept END")
    _check("SET dept = CASE ...",
           _vals(engine, "SELECT dept FROM caset_u ORDER BY id", "dept"),
           ["senior", "eng", "sales", "sales", "senior"])


def verify_case_lazy_and_nested(engine):
    print("\nVerifying CASE laziness + nesting...")
    _populate(engine, "caset_l")

    # Only the selected branch is evaluated: the non-taken THEN here would raise
    # (ABS of non-numeric text), but its WHEN is False, so ELSE is returned.
    _check("non-selected erroring branch is not evaluated",
           _vals(engine,
                 "SELECT CASE WHEN 1 = 0 THEN ABS('x') ELSE 'safe' END AS s "
                 "FROM caset_l WHERE id = 1", "s"),
           ["safe"])
    # A CASE nested in the ELSE branch.
    _check("nested CASE",
           _vals(engine,
                 "SELECT CASE WHEN age IS NULL THEN 'na' "
                 "ELSE CASE WHEN age >= 30 THEN 'old' ELSE 'young' END END AS k "
                 "FROM caset_l ORDER BY id", "k"),
           ["old", "young", "young", "na", "old"])


def verify_case_fail_loud(engine):
    print("\nVerifying CASE still fails loud where it must...")
    _populate(engine, "caset_f")

    # An unsupported function in the *selected* branch still raises -- the
    # branch is evaluated, so the fail-loud contract holds end-to-end (a CASE
    # must never become a way to smuggle in an unimplemented function silently).
    _expect_raises(
        "unsupported function in the selected THEN",
        lambda: engine.execute(
            "SELECT CASE WHEN 1 = 1 THEN SUBSTRING(name, 1, 2) ELSE 'x' END "
            "AS s FROM caset_f WHERE id = 1"),
        NotImplementedError)
    _expect_raises(
        "unsupported function in the selected ELSE",
        lambda: engine.execute(
            "SELECT CASE WHEN 1 = 0 THEN 'x' ELSE SUBSTRING(name, 1, 2) END "
            "AS s FROM caset_f WHERE id = 1"),
        NotImplementedError)


def verify_python_backend():
    """CASE lives in the resolver above the storage scan, so it is
    backend-agnostic -- confirm parity on the pure-Python backend."""
    print("\nVerifying CASE on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "caset_py")
    _check("python backend searched CASE",
           _vals(engine,
                 "SELECT CASE WHEN age < 30 THEN 'lt30' ELSE 'ge30' END AS b "
                 "FROM caset_py ORDER BY id", "b"),
           ["ge30", "lt30", "lt30", "ge30", "ge30"])
    _check("python backend simple CASE",
           _vals(engine,
                 "SELECT CASE dept WHEN 'eng' THEN 1 ELSE 0 END AS d "
                 "FROM caset_py ORDER BY id", "d"),
           [1, 1, 0, 0, 0])


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_searched_case(shared)
    verify_simple_case(shared)
    verify_case_in_clauses(shared)
    verify_case_in_update(shared)
    verify_case_lazy_and_nested(shared)
    verify_case_fail_loud(shared)
    verify_python_backend()
    print("\nAll CASE checks passed.")
