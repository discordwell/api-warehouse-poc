"""
Set-operation execution (UNION / UNION ALL / INTERSECT [ALL] / EXCEPT [ALL])
for the FDB-style SQL engine, by materialization.

A set operation combines whole query results, so -- like uncorrelated
subqueries (``subqueries.py``) -- it runs above the bind/optimize/execute
pipeline instead of inside it: the engine executes each side (inside the
statement's own transaction), reduces the rows to positional value tuples,
combines them with SQL's set semantics, and re-labels the result with the
FIRST side's output column names, per SQL. Sides recurse, so chains
(``a UNION b UNION c``) and parenthesized groups work; each side is a full
SELECT, so joins, aggregates, subqueries, and a side-level ORDER BY/LIMIT
(``(SELECT ... ORDER BY x LIMIT 2) UNION ALL ...``) all compose.

Semantics pinned here (``tests/verify_sql_setops.py``):

  * Columns are matched *positionally*; a side with a different column count
    fails loud (names need not match -- the first side's names win).
  * The DISTINCT forms de-duplicate with the engine's value equality
    (``distinct_key``: ``1`` matches ``"1"``; NULLs are equal to each other
    for set purposes, the standard's "not distinct" rule), keeping first-seen
    order. ``INTERSECT ALL`` / ``EXCEPT ALL`` use bag semantics: each right
    row consumes at most one matching left occurrence.
  * ``ORDER BY`` on a set operation may reference output columns only -- by
    name or by position, per the standard; anything else fails loud.
    ``LIMIT``/``OFFSET`` apply to the combined result.
  * sqlglot parses an unparenthesized mixed chain left-to-right, but standard
    SQL gives INTERSECT higher precedence than UNION/EXCEPT -- so
    ``a UNION b INTERSECT c`` would run as ``(a UNION b) INTERSECT c`` while
    every mainstream engine runs ``a UNION (b INTERSECT c)``. Executing the
    parsed shape would be *silently wrong*; that chain fails loud and asks
    for parentheses. (``a INTERSECT b UNION c`` parses to the same grouping
    the standard prescribes and is allowed.)
"""
from collections import Counter
from functools import cmp_to_key

from sqlglot import exp

from .parser import SQLParser
from .predicates import compare_rows, distinct_key


def run_set_operation(node, run_select):
    """Execute a set-operation tree; returns ``(rows, output_columns)``.

    ``run_select(select_ast) -> (rows, output_columns)`` executes one plain
    SELECT (the engine passes a closure bound to the current transaction).
    """
    if node.args.get("by_name"):
        raise NotImplementedError(
            "UNION BY NAME is not supported; columns are matched "
            "positionally")
    if isinstance(node, exp.Intersect) and isinstance(
            node.this, (exp.Union, exp.Except)):
        raise NotImplementedError(
            "Ambiguous set-operation chain: standard SQL gives INTERSECT "
            "higher precedence than UNION/EXCEPT, but this statement would "
            "execute left-to-right as written. Parenthesize the intended "
            f"grouping: {node.sql()}")

    left_rows, left_cols = _side(node.this, run_select)
    right_rows, right_cols = _side(node.expression, run_select)
    if len(left_cols) != len(right_cols):
        raise ValueError(
            f"Set operation sides have different column counts "
            f"({len(left_cols)} vs {len(right_cols)}): {node.sql()}")
    if len(set(left_cols)) != len(left_cols):
        # The first side names the output; duplicate names cannot survive a
        # dict row without one position silently clobbering the other.
        raise ValueError(
            f"Duplicate output column names in a set operation; alias them "
            f"apart: {left_cols}")

    left = [tuple(r.get(c) for c in left_cols) for r in left_rows]
    right = [tuple(r.get(c) for c in right_cols) for r in right_rows]
    combined = _combine(node, left, right)

    rows = [dict(zip(left_cols, t)) for t in combined]
    rows = _order_limit(node, rows, left_cols)
    return rows, left_cols


def _side(sub, run_select):
    """Rows and output columns of one side: a plain SELECT or a nested set
    operation, possibly parenthesized (``Subquery``-wrapped)."""
    node = sub
    while isinstance(node, exp.Subquery):
        if (node.args.get("order") or node.args.get("limit")
                or node.args.get("offset")):
            raise NotImplementedError(
                f"ORDER BY / LIMIT on a parenthesized set-operation side "
                f"wrapper is not supported: {node.sql()}")
        node = node.this
    if isinstance(node, exp.SetOperation):
        return run_set_operation(node, run_select)
    if isinstance(node, exp.Select):
        rows, cols = run_select(node)
        if cols is None:
            raise NotImplementedError(
                f"Cannot determine the output column order of a "
                f"set-operation side: {node.sql()}")
        return rows, cols
    raise NotImplementedError(
        f"Unsupported set-operation side (only SELECT or a nested set "
        f"operation): {sub.sql()}")


# ---------------------------------------------------------------------------
# Combining
# ---------------------------------------------------------------------------

def _key(values):
    """A row's set identity: per-value ``distinct_key``, so numeric coercion
    matches the engine's `=` / DISTINCT (``1`` is ``"1.0"``) and all NULLs
    are one value -- SQL treats rows as duplicates when the values are "not
    distinct", which unlike `=` pairs NULL with NULL."""
    return tuple(distinct_key(v) for v in values)


def _dedup(tuples):
    seen, out = set(), []
    for t in tuples:
        k = _key(t)
        if k not in seen:
            seen.add(k)
            out.append(t)
    return out


def _combine(node, left, right):
    distinct = bool(node.args.get("distinct"))
    if isinstance(node, exp.Union):
        return _dedup(left + right) if distinct else left + right
    if isinstance(node, exp.Intersect):
        if distinct:
            right_keys = {_key(t) for t in right}
            return [t for t in _dedup(left) if _key(t) in right_keys]
        # INTERSECT ALL: bag semantics -- keep min(left count, right count)
        # occurrences, in left order.
        remaining = Counter(_key(t) for t in right)
        out = []
        for t in left:
            k = _key(t)
            if remaining[k] > 0:
                remaining[k] -= 1
                out.append(t)
        return out
    if isinstance(node, exp.Except):
        if distinct:
            right_keys = {_key(t) for t in right}
            return [t for t in _dedup(left) if _key(t) not in right_keys]
        # EXCEPT ALL: each right occurrence cancels one left occurrence.
        remaining = Counter(_key(t) for t in right)
        out = []
        for t in left:
            k = _key(t)
            if remaining[k] > 0:
                remaining[k] -= 1
            else:
                out.append(t)
        return out
    raise NotImplementedError(f"Unsupported set operation: {node.key}")


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT over the combined result
# ---------------------------------------------------------------------------

def _col(name, row):
    """Accessor for ``compare_rows``: a set-op ORDER BY key is an output-column
    name, resolved by a bare lookup (the combined rows carry nothing else)."""
    return row.get(name)


def _order_limit(node, rows, cols):
    order = node.args.get("order")
    if order:
        keys = _sort_keys(order, cols)
        rows = sorted(rows, key=cmp_to_key(
            lambda a, b: compare_rows(a, b, keys, _col)))
    limit_node = node.args.get("limit")
    offset_node = node.args.get("offset")
    if limit_node or offset_node:
        limit = SQLParser._int_arg(limit_node.expression) if limit_node else None
        offset = SQLParser._int_arg(offset_node.expression) if offset_node else 0
        rows = rows[offset:] if limit is None else rows[offset:offset + limit]
    return rows


def _sort_keys(order, cols):
    """Bind a set operation's ORDER BY to ``(column, desc, nulls_first)``
    keys. Standard SQL restricts these to output columns, by name or 1-based
    position; an expression or a non-output name fails loud (the combined
    rows carry nothing else it could mean)."""
    keys = []
    for ordered in order.expressions:  # exp.Ordered
        expr = ordered.this
        desc = bool(ordered.args.get("desc"))
        nulls_first = ordered.args.get("nulls_first")
        if nulls_first is None:
            # SQL's "NULL is the smallest value" default, as in SortExecutor.
            nulls_first = not desc
        if isinstance(expr, exp.Literal) and not expr.is_string:
            pos = int(expr.this)
            if not 1 <= pos <= len(cols):
                raise ValueError(f"ORDER BY position {pos} is out of range")
            name = cols[pos - 1]
        elif (isinstance(expr, exp.Column) and not expr.table
                and expr.name in cols):
            name = expr.name
        else:
            raise NotImplementedError(
                f"ORDER BY on a set operation may only reference output "
                f"columns by name or position: {expr.sql()}")
        keys.append((name, desc, bool(nulls_first)))
    return keys
