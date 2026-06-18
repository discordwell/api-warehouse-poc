"""
Aggregate-function support for the FDB-style SQL engine.

This is the single source of truth for ``COUNT`` / ``SUM`` / ``AVG`` /
``MIN`` / ``MAX`` (and their ``DISTINCT`` variants). The parser turns each
aggregate call in a ``SELECT`` / ``HAVING`` into an :class:`AggSpec`; the
:class:`~hbdb.sql.executor.AggregateExecutor` computes one value per spec per
group via :func:`compute`.

Two design choices keep this consistent with the rest of the engine:

* Operands are resolved through the shared predicate resolver
  (``predicates.resolve``), so ``SUM(price * qty)`` and numeric-string columns
  behave exactly as they do in ``WHERE`` / ``ORDER BY``.
* A computed aggregate value is spliced back into ``HAVING`` / output /
  ``ORDER BY`` expressions by :func:`substitute_aggs`, which replaces every
  aggregate sub-tree with a literal and lets the ordinary resolver finish the
  job. So ``HAVING SUM(x) > 10`` and ``SELECT SUM(a) + SUM(b)`` reuse the same
  three-valued / numeric-coercion logic as everything else.

SQL semantics honored here:

* ``COUNT(*)`` counts rows; ``COUNT(expr)`` counts non-NULL values;
  ``COUNT`` never returns NULL (empty group -> 0).
* ``SUM`` / ``AVG`` / ``MIN`` / ``MAX`` ignore NULLs and return NULL when the
  group has no non-NULL input.
* ``DISTINCT`` (e.g. ``COUNT(DISTINCT v)``, ``SUM(DISTINCT v)``) de-duplicates
  the non-NULL operand values before aggregating.
"""
from dataclasses import dataclass
from typing import Any, List, Optional

from sqlglot import exp

from .predicates import resolve, as_number, compare_values, distinct_key

# sqlglot AggFunc subclass -> canonical function name we implement.
_FUNCS = {
    exp.Count: "COUNT",
    exp.Sum: "SUM",
    exp.Avg: "AVG",
    exp.Min: "MIN",
    exp.Max: "MAX",
}


@dataclass
class AggSpec:
    """One aggregate to compute, e.g. ``COUNT(*)`` or ``SUM(DISTINCT amount)``.

    ``key`` is the canonical SQL text (``node.sql()``) of the aggregate; it is
    how output / HAVING / ORDER BY expressions look the computed value back up,
    and how duplicate aggregates in one query collapse to a single computation.
    ``arg`` is the operand sqlglot node (None for ``COUNT(*)``).
    """
    key: str
    func: str
    arg: Optional[exp.Expression]
    distinct: bool
    star: bool


def agg_key(node: exp.AggFunc) -> str:
    """Canonical key for an aggregate node (used for dedup + cross-referencing)."""
    return node.sql()


def parse_agg(node: exp.AggFunc) -> AggSpec:
    """Build an :class:`AggSpec` from a sqlglot aggregate node.

    Raises ``NotImplementedError`` for aggregate functions the engine does not
    implement (e.g. ``GROUP_CONCAT``, ``STDDEV``) rather than silently treating
    them as something else -- the same fail-loud contract the rest of the SQL
    layer uses."""
    func = _FUNCS.get(type(node))
    if func is None:
        raise NotImplementedError(
            f"Aggregate function not supported: {node.sql()}")

    inner = node.this
    star = isinstance(inner, exp.Star)
    distinct = False
    arg: Optional[exp.Expression] = None

    if star:
        if func != "COUNT":
            # SUM(*), AVG(*), ... are not valid SQL.
            raise NotImplementedError(f"{func}(*) is not valid")
    elif isinstance(inner, exp.Distinct):
        distinct = True
        targets = inner.expressions
        if len(targets) != 1:
            raise NotImplementedError(
                f"DISTINCT aggregate needs exactly one argument: {node.sql()}")
        arg = targets[0]
    elif inner is None:
        # COUNT() with no argument -- treat like COUNT(*).
        if func != "COUNT":
            raise NotImplementedError(f"{func} requires an argument")
        star = True
    else:
        arg = inner

    return AggSpec(key=agg_key(node), func=func, arg=arg,
                   distinct=distinct, star=star)


def collect_aggregates(expressions) -> List[exp.AggFunc]:
    """Collect every distinct aggregate node found under ``expressions``.

    ``expressions`` is any iterable of sqlglot nodes (the SELECT list, plus the
    HAVING / ORDER BY clauses). Aggregates are de-duplicated by canonical key so
    ``SELECT COUNT(*) ... HAVING COUNT(*) > 1`` computes ``COUNT(*)`` once.
    Nested aggregates (``SUM(COUNT(x))``) are rejected -- they are only legal
    over a sub-query, which this engine does not support."""
    found = {}
    for root in expressions:
        if root is None:
            continue
        for agg in root.find_all(exp.AggFunc):
            if agg.find_ancestor(exp.AggFunc) is not None:
                raise NotImplementedError(
                    f"Nested aggregates are not supported: {root.sql()}")
            found.setdefault(agg_key(agg), agg)
    return list(found.values())


def compute(spec: AggSpec, rows: List[dict]) -> Any:
    """Compute one aggregate over a group's rows."""
    if spec.func == "COUNT" and spec.star:
        return len(rows)

    # Resolve the operand for each row, dropping NULLs (and, for DISTINCT,
    # duplicates). Resolution reuses the shared operand resolver so arithmetic
    # and numeric-string coercion match WHERE/ORDER BY.
    values = []
    seen = set()
    for row in rows:
        v = resolve(spec.arg, row)
        if v is None:
            continue
        if spec.distinct:
            marker = distinct_key(v)
            if marker in seen:
                continue
            seen.add(marker)
        values.append(v)

    if spec.func == "COUNT":
        return len(values)
    if not values:
        # SUM/AVG/MIN/MAX over an empty or all-NULL group is NULL.
        return None
    if spec.func == "SUM":
        return sum(_require_numbers(values, "SUM"))
    if spec.func == "AVG":
        nums = _require_numbers(values, "AVG")
        return sum(nums) / len(nums)
    if spec.func == "MIN":
        return _extreme(values, want_max=False)
    if spec.func == "MAX":
        return _extreme(values, want_max=True)
    raise NotImplementedError(f"Aggregate function not supported: {spec.func}")


def substitute_aggs(expr: exp.Expression, agg_values: dict) -> exp.Expression:
    """Return a copy of ``expr`` with every aggregate replaced by its value.

    ``agg_values`` maps an aggregate's canonical key to the value computed for
    the current group. After substitution the tree contains only literals,
    columns and operators, so ``predicates.resolve`` / ``evaluate`` can finish
    evaluating it against a representative row."""
    def _replace(node):
        if isinstance(node, exp.AggFunc):
            return exp.convert(agg_values[agg_key(node)])
        return node
    return expr.transform(_replace, copy=True)


def _require_numbers(values, func):
    """Coerce every (non-NULL) value to a number for SUM/AVG.

    Fails loud on a non-numeric operand rather than silently dropping it --
    ``SUM`` over a column of text would otherwise quietly return a partial
    total (or NULL), exactly the "silently wrong" behavior the rest of the SQL
    layer was hardened against. Booleans are deliberately not numbers here."""
    nums = []
    for v in values:
        n = as_number(v)
        if n is None:
            raise ValueError(
                f"{func} requires numeric operands; got non-numeric {v!r}")
        nums.append(n)
    return nums


def _extreme(values, want_max: bool):
    """MIN/MAX using the shared value comparator (so '9' < '10' numerically)."""
    best = values[0]
    for v in values[1:]:
        c = compare_values(v, best)
        if (want_max and c > 0) or (not want_max and c < 0):
            best = v
    return best
