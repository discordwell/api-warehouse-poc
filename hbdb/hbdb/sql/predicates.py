"""
Shared WHERE/predicate evaluation for the FDB-style SQL engine.

The physical executors used to special-case only ``col = literal`` and
treat every other predicate as "matches", silently returning every row for
``WHERE x > 5`` -- and, far worse, making ``DELETE ... WHERE x > 5`` (or any
non-equality / compound predicate) wipe the whole table. Three near-identical
copies of that half-implemented logic lived in the Filter, Update and Delete
executors.

This module evaluates the predicate tree properly and is the single source
of truth for all three. Genuinely unsupported constructs raise instead of
matching everything, so an unhandled predicate fails loudly rather than
silently deleting/updating/returning every row.

Supported operators: =, !=/<>, <, <=, >, >=, AND, OR, NOT, parentheses,
IS [NOT] NULL, IN, [NOT] BETWEEN, [NOT] LIKE / ILIKE (with ``%`` / ``_``
wildcards and an optional ESCAPE character), and + - * / arithmetic on
operands. NULLs follow SQL's three-valued logic: a comparison touching NULL is
UNKNOWN and propagates through AND/OR/NOT; the top-level WHERE treats anything
that is not definitely True as not-matched.

Scalar functions (COALESCE, NULLIF, UPPER, LOWER, LENGTH, TRIM, ABS, CEIL,
FLOOR, ROUND, CONCAT / ``||``, CAST) and the CASE expression (searched and
simple) are resolved here too -- see the dispatch table at the bottom of the
file. Because every clause (WHERE, SELECT, ORDER BY, GROUP BY, HAVING, SET,
aggregate arguments) routes operands through this single ``_resolve``, a scalar
function or CASE works in all of them at once. An unimplemented function still
hits the fail-loud catch-all rather than being silently mis-evaluated.
"""
import math
import re
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache

from sqlglot import exp

_COMPARATORS = {
    exp.EQ: lambda a, b: a == b,
    exp.NEQ: lambda a, b: a != b,
    exp.LT: lambda a, b: a < b,
    exp.LTE: lambda a, b: a <= b,
    exp.GT: lambda a, b: a > b,
    exp.GTE: lambda a, b: a >= b,
}


def evaluate(condition, row) -> bool:
    """Return True iff ``row`` satisfies ``condition`` (a sqlglot expression).

    ``None`` (no WHERE clause) matches every row. Only a definite True
    matches: UNKNOWN (from NULL comparisons) and False both exclude the row.
    """
    if condition is None:
        return True
    if isinstance(condition, exp.Where):
        condition = condition.this
    return _eval(condition, row) is True


def resolve(node, row):
    """Resolve a scalar operand (column reference, literal, or arithmetic)
    to a Python value, given a row. Exposed for SET-clause evaluation."""
    return _resolve(node, row)


def _eval(node, row):
    """Three-valued evaluation: returns True, False, or None (UNKNOWN)."""
    if isinstance(node, exp.Paren):
        return _eval(node.this, row)
    if isinstance(node, exp.And):
        left, right = _eval(node.left, row), _eval(node.right, row)
        if left is False or right is False:
            return False
        if left is None or right is None:
            return None
        return True
    if isinstance(node, exp.Or):
        left, right = _eval(node.left, row), _eval(node.right, row)
        if left is True or right is True:
            return True
        if left is None or right is None:
            return None
        return False
    if isinstance(node, exp.Not):
        val = _eval(node.this, row)
        return None if val is None else (not val)
    if isinstance(node, exp.Is):
        return _eval_is(node, row)
    if isinstance(node, exp.In):
        return _eval_in(node, row)
    if isinstance(node, exp.Between):
        return _eval_between(node, row)
    if isinstance(node, (exp.Like, exp.ILike)):
        return _eval_like(node, row, case_insensitive=isinstance(node, exp.ILike))
    if isinstance(node, exp.Escape):
        return _eval_escape(node, row)
    if type(node) in _COMPARATORS:
        return _eval_comparison(node, row)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    raise NotImplementedError(
        f"Unsupported WHERE predicate: {type(node).__name__} ({node.sql()})"
    )


def _eval_comparison(node, row):
    return _compare_op(_resolve(node.left, row), _resolve(node.right, row),
                       _COMPARATORS[type(node)])


def _compare_op(left, right, op):
    """Three-valued comparison of two already-resolved values with ``op`` (a
    two-argument predicate from ``_COMPARATORS``).

    Shared by the comparison operators and BETWEEN so they agree on coercion
    and NULL handling. A NULL operand is UNKNOWN; operands that are genuinely
    incomparable for an ordering operator (e.g. a number vs a non-numeric
    string) are also UNKNOWN rather than inventing a lexicographic answer --
    both more defensible and safer for DELETE/UPDATE scope."""
    if left is None or right is None:
        return None
    a, b = _coerce_pair(left, right)
    try:
        return op(a, b)
    except TypeError:
        return None


def _eval_between(node, row):
    """``x BETWEEN low AND high`` is ``x >= low AND x <= high`` under SQL's
    three-valued logic, reusing the same comparison/coercion as ``>=`` / ``<=``
    (so ``NOT BETWEEN`` -- parsed as ``NOT (BETWEEN)`` -- inverts correctly)."""
    value = _resolve(node.this, row)
    low = _compare_op(value, _resolve(node.args["low"], row), _COMPARATORS[exp.GTE])
    high = _compare_op(value, _resolve(node.args["high"], row), _COMPARATORS[exp.LTE])
    if low is False or high is False:
        return False
    if low is None or high is None:
        return None
    return True


def _eval_like(node, row, case_insensitive=False, escape=None):
    """``LIKE`` / ``ILIKE`` with SQL ``%`` (any run of chars) and ``_`` (any
    single char) wildcards. A NULL value or pattern is UNKNOWN. Operands are
    coerced to text, so ``code LIKE '1%'`` works whether ``code`` is TEXT or
    INTEGER. ``escape`` (from an ESCAPE clause) turns the next pattern char into
    a literal."""
    value = _resolve(node.this, row)
    pattern = _resolve(node.expression, row)
    if value is None or pattern is None:
        return None
    flags = re.DOTALL | (re.IGNORECASE if case_insensitive else 0)
    return _compile_like(str(pattern), escape, flags).fullmatch(str(value)) is not None


@lru_cache(maxsize=512)
def _compile_like(pattern, escape, flags):
    """Compile a LIKE pattern to a regex once per ``(pattern, escape, flags)``.

    The predicate tree is evaluated once per row, so caching the compiled
    matcher keeps a table scan (or a DELETE/UPDATE) from rebuilding the same
    regex for every row."""
    return re.compile(_like_to_regex(pattern, escape), flags)


def _eval_escape(node, row):
    """``<expr> LIKE <pattern> ESCAPE <char>`` parses as ``Escape(Like, char)``;
    unwrap it and evaluate the LIKE with that escape character."""
    inner = node.this
    if not isinstance(inner, (exp.Like, exp.ILike)):
        raise NotImplementedError(
            f"ESCAPE is only supported on LIKE/ILIKE: {node.sql()}")
    esc = _resolve(node.expression, row)
    escape = None if esc is None else str(esc)
    if escape is not None and len(escape) != 1:
        raise ValueError("ESCAPE must be a single character")
    return _eval_like(inner, row,
                      case_insensitive=isinstance(inner, exp.ILike), escape=escape)


def _like_to_regex(pattern, escape):
    """Translate a SQL LIKE pattern into a (fullmatch) regex. ``%`` matches any
    run of characters, ``_`` any single one; every other character matches
    literally, and ``escape`` makes the following character literal.

    A maximal run of wildcards collapses into a *single* quantifier --
    ``.{k,}`` when the run contains a ``%`` (k = the number of ``_`` in it),
    or ``.{k}`` for a pure ``_`` run. That is what keeps matching linear: the
    naive ``%``->``.*`` / ``_``->``.`` mapping yields adjacent quantifiers like
    ``.*..*.`` for ``%_%_``, which backtrack catastrophically -- a pattern such
    as ``'%_%_%_...x'`` would otherwise hang a scan on a single row."""
    out = []
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if escape and ch == escape:
            # The next character is taken literally (a trailing escape is
            # itself literal).
            i += 1
            out.append(re.escape(pattern[i] if i < n else ch))
            i += 1
            continue
        if ch in "%_":
            underscores = 0
            has_pct = False
            while i < n and pattern[i] in "%_" and not (escape and pattern[i] == escape):
                if pattern[i] == "_":
                    underscores += 1
                else:
                    has_pct = True
                i += 1
            out.append(f".{{{underscores},}}" if has_pct else f".{{{underscores}}}")
            continue
        out.append(re.escape(ch))
        i += 1
    return "".join(out)


def _eval_is(node, row) -> bool:
    """IS NULL / IS TRUE etc. are always definite (never UNKNOWN)."""
    left = _resolve(node.left, row)
    right = node.right
    if isinstance(right, exp.Null):
        return left is None
    return left == _resolve(right, row)


def _eval_in(node, row):
    # Only a parenthesized value list (`x IN (1, 2, 3)`) is supported. A
    # subquery or UNNEST (`x IN (SELECT ...)`, `x IN UNNEST(...)`) parses with
    # an *empty* `expressions` list and the operand stashed under
    # `query`/`unnest`/`field`. The old loop iterated that empty list and
    # silently returned False for every row, so `x IN (SELECT ...)` matched
    # nothing and -- the data-loss case -- `x NOT IN (SELECT ...)` matched
    # *everything* (a DELETE/UPDATE with that predicate hit the whole table).
    # Fail loud instead, exactly like the other unsupported subquery forms
    # (a scalar `= (SELECT ...)`, EXISTS, `= ANY (...)`), so an unhandled
    # construct can never be silently mis-evaluated.
    if not node.expressions:
        raise NotImplementedError(
            f"IN requires a parenthesized value list; subqueries / UNNEST are "
            f"not supported: {node.sql()}")
    left = _resolve(node.this, row)
    if left is None:
        return None
    # IN is `left = v1 OR left = v2 OR ...` under three-valued logic: a
    # match wins outright, but if nothing matched and any candidate was
    # NULL the result is UNKNOWN (so `x NOT IN (1, NULL)` does not match).
    saw_unknown = False
    for candidate in node.expressions or []:
        right = _resolve(candidate, row)
        if right is None:
            saw_unknown = True
            continue
        a, b = _coerce_pair(left, right)
        if a == b:
            return True
    return None if saw_unknown else False


def _resolve(node, row):
    if isinstance(node, exp.Paren):
        return _resolve(node.this, row)
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        num = _as_number(node.this)
        return num if num is not None else node.this
    if isinstance(node, exp.Neg):
        val = _resolve(node.this, row)
        return -val if isinstance(val, (int, float)) and not isinstance(val, bool) else val
    if isinstance(node, exp.Add):
        return _arith(node, row, lambda a, b: a + b)
    if isinstance(node, exp.Sub):
        return _arith(node, row, lambda a, b: a - b)
    if isinstance(node, exp.Mul):
        return _arith(node, row, lambda a, b: a * b)
    if isinstance(node, exp.Div):
        return _arith(node, row, lambda a, b: a / b if b else None)
    if isinstance(node, exp.Column):
        # Honor a table qualifier when the row carries qualified keys (join
        # rows hold both "table.col" and the bare "col"). For single-table
        # rows the qualified key is absent, so this falls back to the bare
        # name -- identical to the pre-join behavior. Resolving the qualifier
        # is what lets `users.id` and `orders.id` coexist in one joined row.
        table = node.table
        if table:
            qualified = f"{table}.{node.name}"
            if qualified in row:
                return row[qualified]
        return row.get(node.name)
    if isinstance(node, exp.Identifier):
        return row.get(node.name)
    # Scalar functions (COALESCE, UPPER, ABS, CAST, ...) dispatch through the
    # table at the bottom of the file. Routing them through this shared
    # resolver is what makes them behave identically in WHERE, SELECT,
    # ORDER BY, GROUP BY, HAVING and SET without any per-clause special-casing.
    handler = _SCALAR_FUNCS.get(type(node))
    if handler is not None:
        return handler(node, row)
    # An unrecognized expression (an unimplemented function, SUBSTRING, %, ...):
    # fail loudly rather than silently mis-resolving it to a column lookup,
    # mirroring _eval's contract for predicate nodes.
    if isinstance(node, exp.Expression):
        raise NotImplementedError(
            f"Unsupported operand: {type(node).__name__} ({node.sql()})"
        )
    return node  # already a plain Python value


def _arith(node, row, op):
    left = _as_number(_resolve(node.left, row))
    right = _as_number(_resolve(node.right, row))
    if left is None or right is None:
        return None
    return op(left, right)


def _coerce_pair(a, b):
    """If both operands look numeric, compare them as numbers."""
    if isinstance(a, bool) or isinstance(b, bool):
        return a, b
    an, bn = _as_number(a), _as_number(b)
    if an is not None and bn is not None:
        return an, bn
    return a, b


def as_number(value):
    """Public alias of the numeric coercion used by WHERE/ORDER BY.

    Shared with the aggregate operators (``sql/aggregates.py``) so SUM/AVG add
    up numeric strings the same way a comparison would. Returns an int/float,
    or None when ``value`` is not numeric (booleans included)."""
    return _as_number(value)


def distinct_key(value):
    """A hashable identity for DISTINCT / de-duplication that matches the
    engine's value equality.

    Numeric values (including numeric strings) key on their number, so ``1``
    and ``1.0`` -- and ``10`` and ``"10"`` -- are one value, consistent with
    how WHERE/ORDER BY coerce them. Booleans stay distinct from ints
    (``TRUE`` is not ``1``), since ``as_number`` excludes bools. ``None`` keys
    on itself, so all NULLs collapse to one (SQL treats NULLs as equal for
    DISTINCT)."""
    n = _as_number(value)
    if n is not None:
        return ("n", n)
    return (type(value).__name__, value)


def compare_values(a, b) -> int:
    """Three-way compare (-1/0/1) using the WHERE/ORDER BY coercion rules.

    Both operands are coerced as a pair (numeric strings compare as numbers),
    then ordered. Genuinely incomparable values (e.g. a number vs non-numeric
    text) fall back to a stable string compare instead of raising -- the same
    contract ORDER BY relied on, now shared with MIN/MAX so every value
    ordering in the engine agrees."""
    a, b = _coerce_pair(a, b)
    try:
        return -1 if a < b else (1 if a > b else 0)
    except TypeError:
        a, b = str(a), str(b)
        return -1 if a < b else (1 if a > b else 0)


def _as_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            if any(c in value for c in ".eE"):
                return float(value)
            return int(value)
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Scalar functions
#
# Each handler takes the sqlglot node and the row and returns a plain Python
# value, recursing through ``_resolve`` for its operands. They are wired into
# ``_resolve`` via ``_SCALAR_FUNCS`` (defined at the very bottom, after every
# handler exists).
#
# NULL semantics follow SQL: unless a function is specifically about NULL
# (COALESCE / NULLIF), a NULL argument yields NULL. Numeric functions fail loud
# on a non-NULL, non-numeric argument rather than silently coercing it away
# (the same contract SUM / AVG use in aggregates.py). String concatenation
# (``||`` / CONCAT) propagates NULL the ANSI way, so use COALESCE to treat NULL
# as the empty string.
# ---------------------------------------------------------------------------

def _number_or_null(node, row, fname):
    """Resolve an operand that must be numeric. A NULL stays NULL; a non-NULL,
    non-numeric value fails loud instead of being silently dropped."""
    v = _resolve(node, row)
    if v is None:
        return None
    n = _as_number(v)
    if n is None:
        raise ValueError(f"{fname} requires a numeric argument; got {v!r}")
    return n


def _concat_str(v):
    """The string form of a value for concatenation / text CAST. Booleans
    render as SQL ``true`` / ``false``; everything else uses its plain str."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _fn_coalesce(node, row):
    """COALESCE(a, b, ...) -> the first non-NULL argument, else NULL."""
    for arg in [node.this, *(node.expressions or [])]:
        v = _resolve(arg, row)
        if v is not None:
            return v
    return None


def _fn_nullif(node, row):
    """NULLIF(a, b) -> NULL when a equals b (engine value-equality), else a."""
    a = _resolve(node.this, row)
    if a is None:
        return None
    b = _resolve(node.args.get("expression"), row)
    if b is None:
        return a  # a = NULL is UNKNOWN -> not equal -> yield a
    av, bv = _coerce_pair(a, b)
    return None if av == bv else a


def _fn_upper(node, row):
    v = _resolve(node.this, row)
    return None if v is None else str(v).upper()


def _fn_lower(node, row):
    v = _resolve(node.this, row)
    return None if v is None else str(v).lower()


def _fn_length(node, row):
    v = _resolve(node.this, row)
    return None if v is None else len(str(v))


def _fn_trim(node, row):
    """Plain TRIM(x): strip surrounding whitespace. The LEADING / TRAILING and
    TRIM(c FROM x) forms (which set ``position`` / ``expression``) are not
    implemented -- fail loud rather than silently ignore the trim spec."""
    if node.args.get("position") or node.args.get("expression"):
        raise NotImplementedError(
            f"Only plain TRIM(x) is supported: {node.sql()}")
    v = _resolve(node.this, row)
    return None if v is None else str(v).strip()


def _fn_abs(node, row):
    n = _number_or_null(node.this, row, "ABS")
    return None if n is None else abs(n)


def _fn_ceil(node, row):
    n = _number_or_null(node.this, row, "CEIL")
    return None if n is None else math.ceil(n)


def _fn_floor(node, row):
    n = _number_or_null(node.this, row, "FLOOR")
    return None if n is None else math.floor(n)


def _fn_round(node, row):
    """ROUND(x[, d]) rounding halves away from zero (ROUND(2.5) = 3), the SQL
    standard. Python's built-in round() uses banker's rounding (round half to
    even), so the work goes through Decimal to get the SQL answer
    deterministically; str(n) keeps binary-float noise out of the result."""
    n = _number_or_null(node.this, row, "ROUND")
    if n is None:
        return None
    ndigits = 0
    dec = node.args.get("decimals")
    if dec is not None:
        d = _number_or_null(dec, row, "ROUND")
        if d is None:
            return None
        ndigits = int(d)
    quantum = Decimal(1).scaleb(-ndigits)
    result = Decimal(str(n)).quantize(quantum, rounding=ROUND_HALF_UP)
    return int(result) if ndigits <= 0 else float(result)


def _fn_concat(node, row):
    """CONCAT(a, b, ...) with ANSI NULL propagation (any NULL -> NULL)."""
    parts = []
    for e in node.expressions or []:
        v = _resolve(e, row)
        if v is None:
            return None
        parts.append(_concat_str(v))
    return "".join(parts)


def _fn_dpipe(node, row):
    """``a || b`` string concatenation; ANSI NULL propagation. Chained
    ``a || b || c`` is nested DPipe, so the recursion handles it."""
    a = _resolve(node.this, row)
    b = _resolve(node.args.get("expression"), row)
    if a is None or b is None:
        return None
    return _concat_str(a) + _concat_str(b)


def _cast_bool(v):
    if isinstance(v, bool):
        return v
    n = _as_number(v)
    if n is not None:
        return n != 0
    s = str(v).strip().lower()
    if s in ("true", "t", "yes", "y"):
        return True
    if s in ("false", "f", "no", "n"):
        return False
    raise ValueError(f"cannot CAST {v!r} to boolean")


def _fn_cast(node, row):
    """CAST(x AS <type>) for the basic types. CAST to an integer truncates
    toward zero (CAST(3.7 AS INT) = 3); a value that is not numeric at all
    fails loud rather than casting to a silent 0/NULL."""
    v = _resolve(node.this, row)
    if v is None:
        return None
    target = node.to.this
    if target in _CAST_INT_TYPES:
        n = _as_number(v)
        if n is None:
            raise ValueError(f"cannot CAST {v!r} to integer")
        return int(n)
    if target in _CAST_FLOAT_TYPES:
        n = _as_number(v)
        if n is None:
            raise ValueError(f"cannot CAST {v!r} to a real number")
        return float(n)
    if target in _CAST_TEXT_TYPES:
        return _concat_str(v)
    if target in _CAST_BOOL_TYPES:
        return _cast_bool(v)
    raise NotImplementedError(f"Unsupported CAST target: {node.to.sql()}")


_T = exp.DataType.Type
_CAST_INT_TYPES = {_T.INT, _T.BIGINT, _T.SMALLINT, _T.TINYINT}
_CAST_FLOAT_TYPES = {_T.FLOAT, _T.DOUBLE, _T.DECIMAL}
_CAST_TEXT_TYPES = {_T.TEXT, _T.VARCHAR, _T.CHAR, _T.NCHAR, _T.NVARCHAR}
_CAST_BOOL_TYPES = {_T.BOOLEAN}


def _fn_case(node, row):
    """CASE expression -- searched or simple -- with three-valued logic and
    lazy branch evaluation (only the chosen result is resolved).

    Searched ``CASE WHEN cond THEN r [WHEN ...] [ELSE d] END``: each WHEN is a
    full predicate run through the shared ``_eval``, so only a *definite* True
    selects its result -- an UNKNOWN (NULL) condition does not match, per SQL.
    Simple ``CASE x WHEN v THEN r ... END`` compares ``x`` to each ``v`` with
    the engine's ``=`` (the same numeric-string coercion / NULL rules as
    everywhere else); because ``x = NULL`` is UNKNOWN, ``CASE NULL WHEN NULL``
    falls through rather than matching. The first matching THEN is returned;
    with no match the ELSE is returned, or NULL when there is no ELSE. Results
    route back through ``_resolve``, so a THEN/ELSE may itself be a column,
    arithmetic, or another function -- and a branch that is not taken is never
    evaluated (so an erroring expression in a non-selected branch is harmless).

    Routing CASE through this one resolver is what makes it work identically in
    SELECT, WHERE, ORDER BY, GROUP BY, HAVING and ``UPDATE ... SET`` at once.
    """
    operand = node.this  # the value matched by a simple CASE; None for searched
    has_operand = operand is not None
    op_val = _resolve(operand, row) if has_operand else None
    for branch in node.args.get("ifs") or []:
        if has_operand:
            matched = _compare_op(
                op_val, _resolve(branch.this, row), _COMPARATORS[exp.EQ]) is True
        else:
            matched = _eval(branch.this, row) is True
        if matched:
            return _resolve(branch.args.get("true"), row)
    default = node.args.get("default")
    return _resolve(default, row) if default is not None else None


# sqlglot node type -> handler. Looked up in _resolve; an absent type falls
# through to the fail-loud catch-all.
_SCALAR_FUNCS = {
    exp.Coalesce: _fn_coalesce,
    exp.Nullif: _fn_nullif,
    exp.Upper: _fn_upper,
    exp.Lower: _fn_lower,
    exp.Length: _fn_length,
    exp.Trim: _fn_trim,
    exp.Abs: _fn_abs,
    exp.Ceil: _fn_ceil,
    exp.Floor: _fn_floor,
    exp.Round: _fn_round,
    exp.Concat: _fn_concat,
    exp.DPipe: _fn_dpipe,
    exp.Cast: _fn_cast,
    exp.Case: _fn_case,
}
