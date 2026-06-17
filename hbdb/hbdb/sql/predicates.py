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
IS [NOT] NULL, IN, and + - * / arithmetic on operands. NULLs follow SQL's
three-valued logic: a comparison touching NULL is UNKNOWN and propagates
through AND/OR/NOT; the top-level WHERE treats anything that is not
definitely True as not-matched.
"""
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
    if type(node) in _COMPARATORS:
        return _eval_comparison(node, row)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    raise NotImplementedError(
        f"Unsupported WHERE predicate: {type(node).__name__} ({node.sql()})"
    )


def _eval_comparison(node, row):
    left = _resolve(node.left, row)
    right = _resolve(node.right, row)
    # SQL three-valued logic: a comparison touching NULL is UNKNOWN.
    if left is None or right is None:
        return None
    a, b = _coerce_pair(left, right)
    compare = _COMPARATORS[type(node)]
    try:
        return compare(a, b)
    except TypeError:
        # Operands are genuinely incomparable for an ordering operator
        # (e.g. a number vs a non-numeric string). Treat as UNKNOWN rather
        # than inventing a lexicographic answer -- which is both more
        # defensible and safer for DELETE/UPDATE scope.
        return None


def _eval_is(node, row) -> bool:
    """IS NULL / IS TRUE etc. are always definite (never UNKNOWN)."""
    left = _resolve(node.left, row)
    right = node.right
    if isinstance(right, exp.Null):
        return left is None
    return left == _resolve(right, row)


def _eval_in(node, row):
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
    if isinstance(node, (exp.Column, exp.Identifier)):
        return row.get(node.name)
    # An unrecognized expression (function call, CAST, %, ||, ...): fail
    # loudly rather than silently mis-resolving it to a column lookup,
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


def coerce_pair(a, b):
    """Public alias of the numeric-coercion helper.

    Shared with the ORDER BY comparator (``sql/executor.py``) so sorting uses
    the same number/string coercion as WHERE comparisons -- e.g. the string
    ``"9"`` sorts before ``"10"`` numerically rather than lexicographically.
    """
    return _coerce_pair(a, b)


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
