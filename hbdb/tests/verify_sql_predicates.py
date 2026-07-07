"""
Verify WHERE-predicate evaluation and column projection in the FDB-style
SQL engine.

Regression for a data-loss bug: the executors only understood ``col =
literal`` and treated every other predicate as "matches all rows", so
``WHERE x > 5`` returned the whole table and -- far worse -- ``DELETE ...
WHERE x > 5`` wiped it. This exercises comparison operators, AND/OR/NOT,
IS [NOT] NULL, IN, [NOT] BETWEEN, [NOT] LIKE / ILIKE (with ESCAPE),
projection, and confirms unsupported predicates now fail loudly instead of
matching everything.

Note: every HBDB in one process+CWD shares the WAL, so a second instance
recovers the first one's catalog. Each scenario therefore uses a uniquely
named table (the same convention verify_sql_index.py relies on).
"""
import os
import sys
import time
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


def verify_like_between(engine):
    print("\nVerifying LIKE / ILIKE / BETWEEN...")

    def names(where):
        return sorted(r["name"] for r in engine.execute(f"SELECT * FROM cmp WHERE {where}"))

    # LIKE: % is any run of chars, _ is exactly one; matching is case-sensitive.
    _check("LIKE 'A%'", names("name LIKE 'A%'"), ["Alice"])
    _check("LIKE '%e'", names("name LIKE '%e'"), ["Alice", "Charlie", "Dave", "Eve"])
    _check("LIKE '_o_'", names("name LIKE '_o_'"), ["Bob"])
    _check("LIKE '%a%' is case-sensitive", names("name LIKE '%a%'"), ["Charlie", "Dave"])
    # ILIKE is the case-insensitive form.
    _check("ILIKE '%A%'", names("name ILIKE '%A%'"), ["Alice", "Charlie", "Dave"])
    # NOT LIKE parses as NOT(LIKE) and inverts; NULL stays UNKNOWN (no NULL name here).
    _check("NOT LIKE '%e'", names("name NOT LIKE '%e'"), ["Bob"])
    # Operands are coerced to text, so LIKE works on a numeric column too.
    _check("age LIKE '2%' (numeric coerced)", names("age LIKE '2%'"), ["Bob", "Dave"])
    # A NULL pattern (or value) is UNKNOWN -> matches nothing.
    _check("LIKE NULL matches nothing", names("name LIKE NULL"), [])

    # BETWEEN is inclusive and reuses the >=/<= coercion + three-valued logic.
    _check("age BETWEEN 25 AND 35", names("age BETWEEN 25 AND 35"),
           ["Alice", "Bob", "Charlie"])
    _check("age NOT BETWEEN 25 AND 35", names("age NOT BETWEEN 25 AND 35"), ["Dave"])
    _check("age BETWEEN 20 AND 20 (degenerate)", names("age BETWEEN 20 AND 20"), ["Dave"])
    _check("text BETWEEN 'A' AND 'C'", names("name BETWEEN 'A' AND 'C'"), ["Alice", "Bob"])
    # Eve's age is NULL; a NULL bound makes the whole comparison UNKNOWN.
    _check("BETWEEN with NULL bound matches nothing",
           names("age BETWEEN 25 AND NULL"), [])

    # ESCAPE: treat the next pattern char literally, so %/_ can be matched.
    engine.execute("CREATE TABLE esc (id INTEGER PRIMARY KEY, s TEXT)")
    for i, s in [(1, "10%"), (2, "100"), (3, "1_0"), (4, "1X0")]:
        engine.execute(f"INSERT INTO esc VALUES ({i}, '{s}')")

    def esc_ids(where):
        return sorted(r["id"] for r in engine.execute(f"SELECT id FROM esc WHERE {where}"))

    _check(r"LIKE '10\%' ESCAPE '\' (literal %)", esc_ids(r"s LIKE '10\%' ESCAPE '\'"), [1])
    _check(r"LIKE '1\_0' ESCAPE '\' (literal _)", esc_ids(r"s LIKE '1\_0' ESCAPE '\'"), [3])
    _check("LIKE '1_0' (wildcard _)", esc_ids("s LIKE '1_0'"), [2, 3, 4])


def verify_like_no_redos():
    """A LIKE pattern that alternates % and _ ('%_%_..x') used to translate to
    '.*..*..x' and backtrack catastrophically -- a single non-matching row could
    hang the whole scan. The translator now collapses each wildcard run into one
    quantifier, so matching stays linear. Guard it with a wall-clock budget."""
    print("\nVerifying LIKE has no catastrophic backtracking...")
    where = sqlglot.parse_one(
        "SELECT 1 WHERE x LIKE '" + "%_" * 16 + "z'").args["where"]
    row = {"x": "a" * 256}  # long, and never matches (no 'z')
    start = time.monotonic()
    result = evaluate(where, row)
    elapsed = time.monotonic() - start
    if result is not False:
        print(f"{FAIL}: pathological LIKE should not match, got {result!r}")
        sys.exit(1)
    if elapsed > 1.0:
        print(f"{FAIL}: pathological LIKE took {elapsed:.2f}s (ReDoS regression)")
        sys.exit(1)
    print(f"{PASS}: pathological LIKE evaluated in {elapsed * 1000:.1f}ms")


def verify_like_between_no_data_loss():
    """LIKE / BETWEEN go through the same shared evaluator as WHERE, so DELETE
    and UPDATE honor them too -- and must not over-match (the data-loss class)."""
    print("\nVerifying UPDATE/DELETE honor LIKE/BETWEEN (no over-match)...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "lb")

    def ids():
        return sorted(r["id"] for r in engine.execute("SELECT id FROM lb"))

    # DELETE only the names starting with a capital C..D via a range, plus a LIKE.
    engine.execute("DELETE FROM lb WHERE name LIKE 'Char%'")  # Charlie (id 3)
    _check("DELETE WHERE name LIKE 'Char%'", ids(), [1, 2, 4, 5])
    engine.execute("UPDATE lb SET name = 'HIT' WHERE age BETWEEN 25 AND 30")  # Alice, Bob
    hit = sorted(r["id"] for r in engine.execute("SELECT id FROM lb WHERE name = 'HIT'"))
    _check("UPDATE WHERE age BETWEEN 25 AND 30 touched only Alice+Bob", hit, [1, 2])


def verify_in_subquery_no_data_loss():
    """`IN` / `NOT IN` with an uncorrelated subquery must hit exactly the
    right rows -- the historic data-loss shape, now implemented.

    A subquery `IN (SELECT ...)` parses with an empty value list, and the
    original evaluator's loop never ran: `x IN (SELECT ...)` matched nothing
    while `x NOT IN (SELECT ...)` matched *everything*, so `DELETE ... WHERE
    id NOT IN (SELECT ...)` wiped the whole table. That was first fixed by
    failing loud; the engine now materializes uncorrelated subqueries
    (hbdb/sql/subqueries.py -- see verify_sql_subqueries.py for the full
    matrix), so the same statements must return / delete exactly the right
    rows. A *correlated* subquery still fails loud, and a plain value-list
    IN / NOT IN is unaffected."""
    print("\nVerifying IN-subquery hits exact rows (no silent table wipe)...")
    engine = SQLEngine(HBDB(force_python=True))
    _populate(engine, "insub")

    def ids():
        return sorted(r["id"] for r in engine.execute("SELECT id FROM insub"))

    _check("SELECT ... IN (subquery) matches the subquery's rows",
           sorted(r["id"] for r in engine.execute(
               "SELECT id FROM insub WHERE id IN "
               "(SELECT id FROM insub WHERE age >= 25)")),
           [1, 2, 3])
    _check("SELECT ... NOT IN (subquery) matches only the complement",
           sorted(r["id"] for r in engine.execute(
               "SELECT id FROM insub WHERE id NOT IN "
               "(SELECT id FROM insub WHERE age >= 25)")),
           [4, 5])
    # A correlated subquery cannot be materialized: executed standalone the
    # outer column would silently resolve to NULL, so it must still raise --
    # and the DELETE below proves a raising statement commits nothing.
    try:
        engine.execute("DELETE FROM insub WHERE id NOT IN "
                       "(SELECT id FROM cmp WHERE cmp.age = insub.age)")
    except NotImplementedError:
        print(f"{PASS}: DELETE ... NOT IN (correlated subquery) raises "
              f"NotImplementedError")
    else:
        print(f"{FAIL}: correlated NOT-IN DELETE did not raise "
              f"(silent-wrong / data-loss regression)")
        sys.exit(1)
    _check("table intact after rejected correlated DELETE", ids(), [1, 2, 3, 4, 5])

    # The historic wipe shape, live: delete the complement of a subset.
    engine.execute("DELETE FROM insub WHERE id NOT IN "
                   "(SELECT id FROM insub WHERE age >= 25)")
    _check("DELETE ... NOT IN (subquery) removed exactly the complement",
           ids(), [1, 2, 3])

    # A plain value-list IN / NOT IN is untouched.
    _check("value-list IN still works",
           sorted(r["id"] for r in engine.execute("SELECT id FROM insub WHERE id IN (1, 3)")),
           [1, 3])
    _check("value-list NOT IN still works",
           sorted(r["id"] for r in engine.execute("SELECT id FROM insub WHERE id NOT IN (1, 3)")),
           [2])


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
    # LIKE / ILIKE / BETWEEN are now implemented (verify_like_between); SIMILAR
    # TO and the % / MOD operator are still unsupported and must raise rather
    # than silently match. IN with a *subquery* parses with an empty value
    # list; the engine materializes uncorrelated subqueries before evaluation
    # ever sees them (verify_sql_subqueries.py), but *direct* evaluation of an
    # unmaterialized subquery -- this path bypasses the engine's rewrite --
    # must keep failing loud, or the old silent table-wipe returns for any
    # caller that skips the rewrite.
    cases = {
        "SIMILAR TO (predicate)": "SELECT * FROM t WHERE name SIMILAR TO 'A%'",
        "% / MOD (operand)": "SELECT * FROM t WHERE x = 10 % 3",
        "IN (subquery)": "SELECT * FROM t WHERE x IN (SELECT y FROM u)",
        "NOT IN (subquery)": "SELECT * FROM t WHERE x NOT IN (SELECT y FROM u)",
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
    verify_like_between(shared)
    verify_like_no_redos()
    verify_projection(shared)
    verify_no_data_loss()
    verify_like_between_no_data_loss()
    verify_in_subquery_no_data_loss()
    verify_unsupported_fails_loud()
    print("\nAll predicate/projection checks passed.")
