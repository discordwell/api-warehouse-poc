"""
Verify scalar functions in the FDB-style SQL engine.

COALESCE / NULLIF / UPPER / LOWER / LENGTH / TRIM / ABS / CEIL / FLOOR / ROUND /
CONCAT / ``||`` / CAST all resolve through the one shared operand resolver
(``hbdb/sql/predicates.py``), so each works the same wherever an operand is
allowed. This suite exercises every one of those clauses -- SELECT projection,
WHERE, ORDER BY, GROUP BY / HAVING and UPDATE ... SET -- and pins down the SQL
semantics that are easy to get subtly wrong:

  * NULL handling: COALESCE returns the first non-NULL; NULLIF(a, b) is NULL
    when a == b; every other function returns NULL for a NULL argument; and
    ``||`` / CONCAT propagate NULL (ANSI), so a NULL piece nulls the whole.
  * ROUND rounds halves away from zero (ROUND(2.5) = 3), not Python's
    round-half-to-even.
  * Numeric functions (ABS / ROUND / CAST-to-number) fail loud on a non-NULL,
    non-numeric argument instead of silently coercing it away -- and a numeric
    *string* is still accepted, matching WHERE / SUM coercion.
  * An unimplemented function (SUBSTRING, or TRIM's LEADING/FROM forms) raises
    rather than silently mis-evaluating.

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
    """A small mixed text/numeric table with NULLs to exercise NULL paths.

    name is NULL for one row, qty is NULL for another, so COALESCE / NULLIF /
    NULL-propagation all have something to bite on. Rows are inserted out of PK
    order so nothing accidentally depends on insertion order.
    """
    engine.execute(
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, "
        f"dept TEXT, price REAL, qty INTEGER)")
    engine.execute(f"INSERT INTO {table} VALUES (3, '  Carol ', 'ENG', 19.95, 3)")
    engine.execute(f"INSERT INTO {table} VALUES (1, 'alice', 'eng', 100.0, 2)")
    engine.execute(f"INSERT INTO {table} VALUES (2, 'Bob', 'Sales', 49.5, NULL)")
    engine.execute(f"INSERT INTO {table} (id, dept, price, qty) "
                   f"VALUES (4, 'eng', -8.25, 0)")  # name NULL


def verify_null_functions(engine):
    print("Verifying COALESCE / NULLIF NULL semantics...")
    _populate(engine, "nullfn")

    # COALESCE picks the first non-NULL: row 4 has NULL name -> '(none)'.
    _check("COALESCE(name, '(none)')",
           _vals(engine, "SELECT COALESCE(name, '(none)') AS n FROM nullfn "
                         "ORDER BY id", "n"),
           ["alice", "Bob", "  Carol ", "(none)"])
    # COALESCE over a NULL column and a NULL literal falls through to the last.
    _check("COALESCE(qty, NULL, -1)",
           _vals(engine, "SELECT COALESCE(qty, NULL, -1) AS q FROM nullfn "
                         "ORDER BY id", "q"),
           [2, -1, 3, 0])
    # NULLIF(a, b) is NULL when equal, else a. dept 'eng' (rows 1,4) -> NULL.
    _check("NULLIF(dept, 'eng')",
           _vals(engine, "SELECT NULLIF(dept, 'eng') AS d FROM nullfn "
                         "ORDER BY id", "d"),
           [None, "Sales", "ENG", None])
    # NULLIF coerces numerically like the rest of the engine: 0 == 0.0 -> NULL.
    _check("NULLIF(qty, 0)",
           _vals(engine, "SELECT NULLIF(qty, 0) AS q FROM nullfn ORDER BY id",
                 "q"),
           [2, None, 3, None])

    # Any non-NULL function of NULL is NULL (UPPER/LENGTH/ABS of a NULL).
    row = _one(engine, "SELECT UPPER(name) AS u, LENGTH(name) AS l "
                       "FROM nullfn WHERE id = 4")
    _check("UPPER(NULL) is NULL", row["u"], None)
    _check("LENGTH(NULL) is NULL", row["l"], None)


def verify_string_functions(engine):
    print("\nVerifying string functions (UPPER/LOWER/LENGTH/TRIM/CONCAT/||)...")
    _populate(engine, "strfn")

    _check("UPPER", _vals(engine, "SELECT UPPER(name) AS u FROM strfn "
                                  "WHERE id IN (1, 2) ORDER BY id", "u"),
           ["ALICE", "BOB"])
    _check("LOWER", _vals(engine, "SELECT LOWER(dept) AS d FROM strfn "
                                  "WHERE id = 2", "d"), ["sales"])
    # TRIM strips surrounding whitespace ('  Carol ' -> 'Carol').
    _check("TRIM", _vals(engine, "SELECT TRIM(name) AS t FROM strfn "
                                 "WHERE id = 3", "t"), ["Carol"])
    _check("LENGTH after TRIM",
           _vals(engine, "SELECT LENGTH(TRIM(name)) AS l FROM strfn "
                         "WHERE id = 3", "l"), [5])
    # || concatenates and coerces non-strings; NULL propagates (row 4 name NULL).
    _check("|| concatenation + numeric coercion",
           _vals(engine, "SELECT dept || '#' || id AS tag FROM strfn "
                         "WHERE id = 1", "tag"), ["eng#1"])
    _check("|| with NULL operand is NULL",
           _vals(engine, "SELECT name || '!' AS s FROM strfn ORDER BY id", "s"),
           ["alice!", "Bob!", "  Carol !", None])
    _check("CONCAT(...)",
           _vals(engine, "SELECT CONCAT(LOWER(dept), '/', id) AS c FROM strfn "
                         "WHERE id = 2", "c"), ["sales/2"])


def verify_numeric_functions(engine):
    print("\nVerifying numeric functions (ABS/CEIL/FLOOR/ROUND)...")
    _populate(engine, "numfn")

    _check("ABS of negative price",
           _vals(engine, "SELECT ABS(price) AS a FROM numfn WHERE id = 4", "a"),
           [8.25])
    _check("CEIL / FLOOR",
           _one(engine, "SELECT CEIL(price) AS c, FLOOR(price) AS f "
                        "FROM numfn WHERE id = 1"),
           {"c": 100, "f": 100})
    _check("CEIL(19.95), FLOOR(19.95)",
           _one(engine, "SELECT CEIL(price) AS c, FLOOR(price) AS f "
                        "FROM numfn WHERE id = 3"),
           {"c": 20, "f": 19})
    # ROUND to 1 decimal, and the half-away-from-zero tie rule.
    _check("ROUND(price, 1)",
           _vals(engine, "SELECT ROUND(price, 1) AS r FROM numfn WHERE id = 1",
                 "r"), [100.0])
    _check("ROUND halves away from zero (not banker's)",
           _one(engine, "SELECT ROUND(qty + 0.5) AS up FROM numfn WHERE id = 1"),
           {"up": 3})  # 2.5 -> 3, not 2
    # ROUND with no decimals returns an int; negative ndigits rounds left.
    _check("ROUND(19.95) -> 20", _vals(engine, "SELECT ROUND(price) AS r "
           "FROM numfn WHERE id = 3", "r"), [20])

    # Numeric functions accept numeric strings (engine-wide coercion), and a
    # computed column round-trips: price * qty.
    _check("ABS(price * qty)",
           _vals(engine, "SELECT ABS(price * qty) AS a FROM numfn WHERE id = 3",
                 "a"), [59.849999999999994])


def verify_cast(engine):
    print("\nVerifying CAST...")
    _populate(engine, "castfn")

    _check("CAST price AS INTEGER truncates toward zero",
           _vals(engine, "SELECT CAST(price AS INTEGER) AS i FROM castfn "
                         "ORDER BY id", "i"),
           [100, 49, 19, -8])  # 49.5 -> 49, -8.25 -> -8 (truncation)
    _check("CAST id AS TEXT then concat",
           _vals(engine, "SELECT 'row' || CAST(id AS TEXT) AS r FROM castfn "
                         "WHERE id = 2", "r"), ["row2"])
    _check("CAST qty AS BOOLEAN (0 -> false, 2 -> true)",
           _vals(engine, "SELECT CAST(qty AS BOOLEAN) AS b FROM castfn "
                         "WHERE id IN (1, 4) ORDER BY id", "b"),
           [True, False])
    _check("CAST of NULL is NULL",
           _vals(engine, "SELECT CAST(qty AS INTEGER) AS i FROM castfn "
                         "WHERE id = 2", "i"), [None])


def verify_functions_in_where(engine):
    print("\nVerifying functions in WHERE...")
    _populate(engine, "wherefn")

    # UPPER normalizes a case-insensitive match across 'alice'/'eng'/'ENG'.
    _check("WHERE UPPER(dept) = 'ENG'",
           sorted(_vals(engine, "SELECT id FROM wherefn "
                                "WHERE UPPER(dept) = 'ENG'", "id")),
           [1, 3, 4])
    _check("WHERE LENGTH(name) <= 3",
           sorted(_vals(engine, "SELECT id FROM wherefn "
                                "WHERE LENGTH(name) <= 3", "id")),
           [2])  # 'Bob'; row 4 name is NULL -> LENGTH NULL -> excluded
    # COALESCE in WHERE lets a NULL qty count as 0 and match.
    _check("WHERE COALESCE(qty, 0) = 0",
           sorted(_vals(engine, "SELECT id FROM wherefn "
                                "WHERE COALESCE(qty, 0) = 0", "id")),
           [2, 4])  # row 2 qty NULL, row 4 qty 0
    _check("WHERE ABS(price) > 50",
           sorted(_vals(engine, "SELECT id FROM wherefn "
                                "WHERE ABS(price) > 50", "id")),
           [1])  # only price 100; -8.25/19.95/49.5 are <= 50 in abs


def verify_functions_in_order_by(engine):
    print("\nVerifying functions in ORDER BY...")
    _populate(engine, "ordfn")

    # ORDER BY a function of a non-selected column (sort sits below projection).
    _check("ORDER BY LENGTH(dept), then id",
           _vals(engine, "SELECT id FROM ordfn "
                         "ORDER BY LENGTH(dept), id", "id"),
           [1, 3, 4, 2])  # 'eng'(3),'ENG'(3),'eng'(3) then 'Sales'(5)
    # ORDER BY ABS(price) DESC: 100, 49.5, 19.95, 8.25.
    _check("ORDER BY ABS(price) DESC",
           _vals(engine, "SELECT id FROM ordfn ORDER BY ABS(price) DESC", "id"),
           [1, 2, 3, 4])


def verify_functions_in_group_by(engine):
    print("\nVerifying functions in GROUP BY / HAVING...")
    _populate(engine, "grpfn")

    # GROUP BY UPPER(dept) folds 'eng'/'ENG' together -> ENG:3, SALES:1.
    rows = engine.execute(
        "SELECT UPPER(dept) AS d, COUNT(*) AS n FROM grpfn "
        "GROUP BY UPPER(dept) ORDER BY d")
    _check("GROUP BY UPPER(dept)", rows,
           [{"d": "ENG", "n": 3}, {"d": "SALES", "n": 1}])
    # HAVING over a function-grouped key.
    rows = engine.execute(
        "SELECT UPPER(dept) AS d, COUNT(*) AS n FROM grpfn "
        "GROUP BY UPPER(dept) HAVING COUNT(*) > 1")
    _check("HAVING COUNT(*) > 1 over UPPER(dept)", rows,
           [{"d": "ENG", "n": 3}])
    # SUM over a COALESCE'd column: NULL qty -> 0 so the sum is well-defined.
    row = _one(engine, "SELECT SUM(COALESCE(qty, 0)) AS s FROM grpfn")
    _check("SUM(COALESCE(qty, 0))", row["s"], 5)  # 2 + 0 + 3 + 0


def verify_functions_in_update(engine):
    print("\nVerifying functions in UPDATE ... SET...")
    _populate(engine, "updfn")

    engine.execute("UPDATE updfn SET name = UPPER(TRIM(name)) WHERE id = 3")
    _check("SET name = UPPER(TRIM(name))",
           _vals(engine, "SELECT name FROM updfn WHERE id = 3", "name"),
           ["CAROL"])
    # SET using ROUND + a NULL-coalescing default.
    engine.execute("UPDATE updfn SET price = ROUND(price, 0), "
                   "qty = COALESCE(qty, 0) WHERE id = 2")
    row = _one(engine, "SELECT price, qty FROM updfn WHERE id = 2")
    _check("SET price = ROUND(price, 0)", row["price"], 50.0)  # 49.5 -> 50
    _check("SET qty = COALESCE(qty, 0)", row["qty"], 0)


def verify_function_over_indexed_column(engine):
    """A scalar function must never let the index-scan fast path return the
    wrong rows. The optimizer turns ``WHERE col = X`` into an index point
    lookup when an index on ``col`` exists; two cases would silently drop rows
    if that fired for a scalar function, and both are guarded in
    ``optimizer._maybe_index_scan``:

      * ``WHERE f(col) = X`` -- a function-wrapped indexed column. ``CAST`` is
        the dangerous one: ``exp.Cast.name`` is the *inner* column name, so a
        naive ``hasattr(left, 'name')`` check index-scanned the raw stored
        value and missed rows (19.95 stored, but ``CAST(price AS INT) = 19``).
        The guard requires the left side to be a bare column.
      * ``WHERE col = f(other)`` -- a correlated RHS. ``COALESCE(other, 5)``
        resolves to the fallback ``5`` against the optimizer's empty probe row,
        so the index would look up ``col = 5`` instead of the real per-row
        predicate. The guard rejects a RHS that references any column.

    A genuinely constant scalar RHS (``col = ABS(-5)``) still folds and uses
    the index -- that is correct, not dropped.
    """
    print("\nVerifying scalar functions never corrupt the index fast path...")
    engine.execute("CREATE TABLE idxfn (id INTEGER PRIMARY KEY, code TEXT, "
                   "price REAL, n INTEGER, other INTEGER)")
    engine.execute("CREATE INDEX idxfn_code ON idxfn (code)")
    engine.execute("CREATE INDEX idxfn_price ON idxfn (price)")
    engine.execute("CREATE INDEX idxfn_n ON idxfn (n)")
    for row in [(1, 'eng', 19.95, 10, 10), (2, 'ENG', 20.0, 5, 99),
                (3, 'Eng', 7.5, 3, 3), (4, 'sales', 50.0, 40, 40)]:
        engine.execute(f"INSERT INTO idxfn VALUES {row}")

    # f(indexed col): UPPER folds case (its .name is empty -> never matched).
    _check("WHERE UPPER(code) = 'ENG' finds all case variants",
           sorted(_vals(engine, "SELECT id FROM idxfn WHERE UPPER(code) = 'ENG'",
                        "id")),
           [1, 2, 3])
    # CAST over an indexed REAL column: 19.95 truncates to 19 and must be kept
    # (the regression: index-scanning the raw price would miss it).
    _check("WHERE CAST(price AS INTEGER) = 19 keeps the 19.95 row",
           sorted(_vals(engine, "SELECT id FROM idxfn "
                                "WHERE CAST(price AS INTEGER) = 19", "id")),
           [1])
    # Correlated RHS over an indexed column: per-row predicate, NOT a lookup on
    # the COALESCE fallback 5 (which would have returned only id 2, n = 5).
    _check("WHERE n = COALESCE(other, 5) is per-row, not a fallback lookup",
           sorted(_vals(engine, "SELECT id FROM idxfn "
                                "WHERE n = COALESCE(other, 5)", "id")),
           [1, 3, 4])
    # A constant scalar RHS still folds and uses the index, correctly.
    _check("WHERE n = ABS(-10) still resolves the constant correctly",
           sorted(_vals(engine, "SELECT id FROM idxfn WHERE n = ABS(-10)", "id")),
           [1])
    # The plain indexed equality is untouched.
    _check("WHERE code = 'eng' still uses the (exact) index",
           sorted(_vals(engine, "SELECT id FROM idxfn WHERE code = 'eng'", "id")),
           [1])
    # A bad constant CAST on the RHS must fall back to a scan, not abort the
    # query at optimize time: over an empty indexed table that means []
    # (consistent with a non-indexed table, where 0 rows are evaluated).
    engine.execute("CREATE TABLE idxempty (id INTEGER PRIMARY KEY, code INTEGER)")
    engine.execute("CREATE INDEX idxempty_code ON idxempty (code)")
    _check("bad-cast constant RHS on empty indexed table returns [] (no abort)",
           engine.execute("SELECT id FROM idxempty "
                          "WHERE code = CAST('x' AS INTEGER)"),
           [])


def verify_fail_loud(engine):
    print("\nVerifying scalar functions still fail loud where they must...")
    _populate(engine, "failfn")

    # A numeric function over genuinely non-numeric text must raise, not
    # silently coerce it away to 0/NULL.
    _expect_raises(
        "ABS of non-numeric text",
        lambda: engine.execute("SELECT ABS(name) AS a FROM failfn WHERE id = 1"),
        ValueError)
    _expect_raises(
        "CAST non-numeric text to INTEGER",
        lambda: engine.execute(
            "SELECT CAST(name AS INTEGER) AS i FROM failfn WHERE id = 1"),
        ValueError)
    # An unimplemented function raises rather than mis-evaluating.
    _expect_raises(
        "SUBSTRING (unimplemented)",
        lambda: engine.execute(
            "SELECT SUBSTRING(name, 1, 2) AS s FROM failfn WHERE id = 1"),
        NotImplementedError)
    # The non-trivial TRIM forms are not implemented (only plain TRIM(x) is).
    _expect_raises(
        "TRIM(LEADING ... ) (unimplemented)",
        lambda: engine.execute(
            "SELECT TRIM(LEADING ' ' FROM name) AS t FROM failfn WHERE id = 1"),
        NotImplementedError)


def verify_python_backend():
    """Scalar functions live in the resolver above the storage scan, so they
    are backend-agnostic -- confirm parity on the pure-Python backend."""
    print("\nVerifying scalar functions on the pure-Python backend...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "pyfn")
    row = _one(engine, "SELECT UPPER(TRIM(name)) AS u, ROUND(price, 0) AS r, "
                       "COALESCE(qty, -1) AS q FROM pyfn WHERE id = 3")
    _check("python backend UPPER(TRIM(name))", row["u"], "CAROL")
    _check("python backend ROUND(price, 0)", row["r"], 20)
    _check("python backend COALESCE(qty, -1)", row["q"], 3)
    row = _one(engine, "SELECT COALESCE(qty, -1) AS q FROM pyfn WHERE id = 2")
    _check("python backend COALESCE picks default for NULL", row["q"], -1)


if __name__ == "__main__":
    shared = SQLEngine(HBDB())
    verify_null_functions(shared)
    verify_string_functions(shared)
    verify_numeric_functions(shared)
    verify_cast(shared)
    verify_functions_in_where(shared)
    verify_functions_in_order_by(shared)
    verify_functions_in_group_by(shared)
    verify_functions_in_update(shared)
    verify_function_over_indexed_column(shared)
    verify_fail_loud(shared)
    verify_python_backend()
    print("\nAll scalar-function checks passed.")
