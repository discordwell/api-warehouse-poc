"""
Uncorrelated-subquery materialization for the FDB-style SQL engine.

SQL's value-producing subquery forms -- a scalar ``(SELECT ...)``,
``[NOT] IN (SELECT ...)``, ``[NOT] EXISTS (...)`` and the quantified
comparisons ``<op> ANY / SOME / ALL (SELECT ...)`` -- used to fail loud in
every position. This module implements them for *uncorrelated* subqueries by
materialization: before a statement is bound, the engine executes each
subquery once (inside the same transaction, so it sees the statement's
snapshot) and splices the result back into the expression tree as plain
literal nodes. The rewritten tree then flows through the existing bind /
optimize / execute pipeline, so a subquery works in every clause an operand
or predicate works in (SELECT, WHERE, HAVING, ORDER BY, GROUP BY,
``UPDATE ... SET``, ``INSERT ... VALUES``, join ``ON``) with no per-clause
special-casing:

  * ``x IN (SELECT ...)``      -> ``x IN (v1, v2, ...)`` (de-duplicated; the
    value list keeps a NULL if the subquery produced one, preserving SQL's
    three-valued ``NOT IN`` behavior). An *empty* result cannot be written as
    a value list, so the In node is tagged (``meta``) and the predicate
    evaluator returns FALSE -- the SQL result for ``x IN (empty)`` even when
    ``x`` is NULL -- while still resolving the left operand so an unsupported
    expression there fails loud.
  * scalar ``(SELECT ...)``    -> its single value (NULL for an empty result;
    more than one row or column fails loud, per SQL).
  * ``EXISTS (...)``           -> TRUE / FALSE. The probe run is capped with
    ``LIMIT 1`` (only when the subquery has no LIMIT of its own -- an explicit
    ``LIMIT 0`` must stay an always-empty result).
  * ``<op> ANY / ALL (...)``   -> the same comparison against a materialized
    value tuple, evaluated with SQL's quantified three-valued semantics in
    ``predicates._eval_comparison``.

A *correlated* subquery -- one that references the enclosing query's tables
-- still fails loud, and the scope check here is what makes that safe: the
engine resolves an unknown column to NULL at runtime, so a correlated
subquery executed standalone would not error; it would silently compute the
wrong rows (`WHERE t2.y = t1.a` becoming `WHERE t2.y = NULL`), the exact
"silently wrong" failure the engine forbids. Every column reference in a
subquery must therefore resolve to the subquery's own FROM tables (or its
own SELECT aliases); anything else -- an outer reference or a typo --
raises ``NotImplementedError`` before the subquery runs.

Derived tables (``FROM (SELECT ...)``) are a relational, not value-producing,
position; they are not materialized here and the binder rejects them.
"""
from sqlglot import exp

from .predicates import distinct_key


def rewrite(statement, run_select, catalog):
    """Materialize every uncorrelated subquery in ``statement``, in place.

    ``run_select(query_ast) -> (rows, output_columns)`` executes one query
    body -- a plain SELECT or a set-operation tree (UNION / INTERSECT /
    EXCEPT), which is why a subquery body may be either. The engine passes a
    closure bound to the current transaction; it also re-enters this
    rewriter, which is what makes nested subqueries work -- each level is
    materialized just before the level above it runs.
    """
    _walk(statement, run_select, catalog)


def _walk(node, run_select, catalog):
    if not isinstance(node, exp.Expression):
        return
    if isinstance(node, exp.In) and node.args.get("query") is not None:
        _rewrite_in(node, run_select, catalog)
        return
    if isinstance(node, exp.Exists):
        _rewrite_exists(node, run_select, catalog)
        return
    if isinstance(node, (exp.Any, exp.All)):
        _rewrite_quantified(node, run_select, catalog)
        return
    if isinstance(node, exp.Subquery):
        if isinstance(node.parent, (exp.From, exp.Join)):
            # A derived table, not a value subquery. Leave it: the binder
            # rejects it loudly (it used to bind the *inner* table and
            # silently drop the subquery's WHERE/projection).
            return
        _rewrite_scalar(node, run_select, catalog)
        return
    # Snapshot the children: a rewrite replaces nodes in their parent.
    for child in list(node.iter_expressions()):
        _walk(child, run_select, catalog)


# ---------------------------------------------------------------------------
# Per-form rewrites
# ---------------------------------------------------------------------------

def _rewrite_in(node, run_select, catalog):
    """``x IN (SELECT ...)`` -> ``x IN (v1, v2, ...)`` (or the tagged
    empty-set form). The left operand may itself contain a subquery, so it
    is walked afterwards."""
    values = _column_values(node.args["query"], run_select, catalog)
    literals = _dedupe_literals(values)
    node.set("query", None)
    if literals:
        node.set("expressions", literals)
    else:
        # An empty value list is unrepresentable as SQL text (`IN ()`), and
        # an *untagged* empty `expressions` is exactly how an unsupported
        # IN-form parses -- predicates.py fails loud on it. The meta tag
        # tells the evaluator this one was genuinely materialized as empty.
        node.meta["materialized_empty_set"] = True
    _walk(node.this, run_select, catalog)


def _rewrite_exists(node, run_select, catalog):
    """``EXISTS (SELECT ...)`` -> TRUE/FALSE."""
    select = _select_of(node.this, node)
    _check_uncorrelated(select, catalog)
    # Existence only needs one row. Cap the probe -- but never override an
    # explicit LIMIT: `EXISTS (SELECT ... LIMIT 0)` must stay empty/FALSE.
    # .limit() copies, so the original tree (used in error messages) is
    # untouched. A set-operation body runs in full: its LIMIT applies to the
    # *combined* result, so capping a side could not be done blindly.
    if isinstance(select, exp.Select) and not select.args.get("limit"):
        probe = select.limit(1)
    else:
        probe = select
    rows, _ = run_select(probe)
    node.replace(exp.Boolean(this=bool(rows)))


def _rewrite_quantified(node, run_select, catalog):
    """``<op> ANY/SOME/ALL (SELECT ...)``: replace the subquery under the
    Any/All wrapper with a materialized value tuple. The comparison node
    itself survives; ``predicates._eval_comparison`` gives it SQL's
    quantified three-valued semantics (over an empty set: ANY -> FALSE,
    ALL -> TRUE)."""
    values = _column_values(node.this, run_select, catalog)
    node.set("this", exp.Tuple(expressions=_dedupe_literals(values)))


def _rewrite_scalar(node, run_select, catalog):
    """A scalar ``(SELECT ...)`` operand -> its single value (NULL when the
    result is empty; >1 row or >1 column fails loud, per SQL)."""
    select = _select_of(node.this, node)
    _check_uncorrelated(select, catalog)
    rows, cols = run_select(select)
    if cols is None or len(cols) != 1:
        raise ValueError(
            f"Scalar subquery must return exactly one column: {node.sql()}")
    if len(rows) > 1:
        raise ValueError(
            f"Scalar subquery returned {len(rows)} rows (expected at most "
            f"one): {node.sql()}")
    value = rows[0].get(cols[0]) if rows else None
    node.replace(_to_literal(value))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _select_of(inner, context_node):
    """Unwrap ``Subquery(this=Select)`` (sqlglot wraps some positions, not
    others) and require a query body the engine can run -- a plain SELECT or
    a set-operation tree (UNION / INTERSECT / EXCEPT, executed by
    ``setops.py``); any other form fails loud."""
    if isinstance(inner, exp.Subquery):
        inner = inner.this
    if not isinstance(inner, (exp.Select, exp.SetOperation)):
        raise NotImplementedError(
            f"Unsupported subquery form (only a SELECT or a set operation "
            f"is supported): {context_node.sql()}")
    return inner


def _column_values(sub, run_select, catalog):
    """Execute a subquery that must produce exactly one output column and
    return its values in row order (NULLs included)."""
    select = _select_of(sub, sub)
    _check_uncorrelated(select, catalog)
    rows, cols = run_select(select)
    if cols is None or len(cols) != 1:
        raise ValueError(
            f"Subquery must return exactly one column: {select.sql()}")
    name = cols[0]
    return [row.get(name) for row in rows]


def _dedupe_literals(values):
    """Literal nodes for ``values``, de-duplicated by the engine's value
    equality (``distinct_key``: 1 matches "1", TRUE stays distinct from 1).
    Safe for IN / ANY / ALL alike -- they fold candidates with OR/AND, so
    duplicates never change the outcome -- and one NULL survives when any
    was present, preserving three-valued NOT IN / ALL behavior."""
    literals, seen = [], set()
    for v in values:
        lit = _to_literal(v)  # fails loud on a non-scalar before keying
        key = distinct_key(v)
        if key in seen:
            continue
        seen.add(key)
        literals.append(lit)
    return literals


def _to_literal(value):
    """A Python value from a result row -> a sqlglot literal node the shared
    operand resolver reads back to the same value."""
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    if isinstance(value, str):
        return exp.Literal.string(value)
    raise NotImplementedError(
        f"Cannot materialize non-scalar subquery value {value!r} as a SQL "
        f"literal")


# ---------------------------------------------------------------------------
# Correlation / scope check
# ---------------------------------------------------------------------------

def _check_uncorrelated(select, catalog):
    """Reject a subquery whose column references do not all resolve within
    its own scope. A set-operation body is checked side by side -- each side
    is its own scope (one side can never reference another's tables).

    The subquery's scope is the set of tables in its own FROM/JOIN clauses
    (by alias or name) plus its own SELECT aliases (so
    ``(SELECT a AS z FROM t ORDER BY z ...)`` passes). Column references
    inside *nested* subqueries belong to those deeper scopes and are checked
    when their own level is materialized. Everything else -- a reference to
    an enclosing query's table, or a plain unknown name -- fails loud here,
    because executed standalone it would resolve to NULL and the subquery
    would silently return the wrong rows.
    """
    if isinstance(select, exp.SetOperation):
        _check_uncorrelated(_select_of(select.this, select.this), catalog)
        _check_uncorrelated(_select_of(select.expression, select.expression),
                            catalog)
        return

    frm = select.args.get("from") or select.args.get("from_")
    if frm is not None and not isinstance(frm.this, exp.Table):
        raise NotImplementedError(
            f"Derived tables / non-table FROM sources are not supported in "
            f"subqueries: {frm.sql()}")
    joins = select.args.get("joins") or []
    for join in joins:
        if not isinstance(join.this, exp.Table):
            raise NotImplementedError(
                f"Derived tables / non-table JOIN sources are not supported "
                f"in subqueries: {join.sql()}")

    sources = [t for t in select.find_all(exp.Table)
               if t.find_ancestor(exp.Select) is select]
    qualifiers = {t.alias_or_name for t in sources}
    known_cols = set()
    for t in sources:
        meta = catalog.get_table(t.name)
        if meta is None:
            # Unknown table: let execution raise its "Table not found",
            # which is the clearer error.
            return
        known_cols.update(c.name for c in meta.schema.columns)
    select_aliases = {p.alias for p in select.expressions
                      if isinstance(p, exp.Alias)}

    for col in select.find_all(exp.Column):
        if col.find_ancestor(exp.Select) is not select:
            continue  # belongs to a nested subquery's scope
        if isinstance(col.this, exp.Star):
            continue  # `t.*` -- the binder validates the qualifier
        if col.table:
            if col.table not in qualifiers:
                raise NotImplementedError(
                    f"Correlated subqueries are not supported: '{col.sql()}' "
                    f"references a table outside the subquery's FROM clause")
        elif col.name not in known_cols and col.name not in select_aliases:
            raise NotImplementedError(
                f"Column '{col.name}' does not exist in the subquery's "
                f"tables. Correlated references to the outer query are not "
                f"supported (and an unknown column would silently resolve "
                f"to NULL): {select.sql()}")
