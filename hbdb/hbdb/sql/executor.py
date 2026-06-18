import functools
from typing import Iterator, Any, Dict, List, Tuple
from abc import ABC, abstractmethod
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject,
                   LogicalProjectExprs, LogicalInsert, LogicalUpdate,
                   LogicalDelete, LogicalJoin, LogicalSort, LogicalLimit,
                   LogicalAggregate, LogicalDistinct)
from .encoding import KeyEncoder
from .predicates import (evaluate as eval_predicate, resolve as resolve_operand,
                         compare_values, distinct_key)
from .aggregates import compute as compute_agg, substitute_aggs
from ..core.proxy import Transaction
from ..core.cache import get_read_cache

class ExecutionContext:
    def __init__(self, txn: Transaction, cache=None):
        self.txn = txn
        # The read cache is scoped to one database: its keys are storage keys
        # like /t/{table_id}/_r/{pk}, and table ids restart at 1 for every
        # HBDB, so a process-wide singleton would collide across instances and
        # serve one database's row for another's. Callers pass the per-DB
        # cache; the global is only a fallback for ad-hoc construction.
        self.cache = cache if cache is not None else get_read_cache()

class PhysicalOperator(ABC):
    @abstractmethod
    def next(self) -> Iterator[Dict[str, Any]]:
        pass

class TableScanExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalScan):
        self.ctx = ctx
        self.node = node
        self.encoder = KeyEncoder()
        self._cache = ctx.cache

    def next(self) -> Iterator[Dict[str, Any]]:
        start = f"/t/{self.node.table_id}/_r/"
        end = f"/t/{self.node.table_id}/_r/~"
        
        kv_pairs = self.ctx.txn.scan(start, end)
        
        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"

        for key, val in kv_pairs:
            # Try cache first
            cached = self._cache.get(key)
            if cached is not None:
                row = cached.copy()
            else:
                # Decode and cache
                pk_val = self.encoder.decode_row_pk(key)
                row = self.encoder.decode_row_value(val)
                row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
                self._cache.put(key, row.copy())
            yield row


class FilterExecutor(PhysicalOperator):
    def __init__(self, child: PhysicalOperator, node: LogicalFilter):
        self.child = child
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        condition = self.node.condition
        for row in self.child.next():
            if eval_predicate(condition, row):
                yield row

class InsertExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalInsert, catalog=None):
        self.ctx = ctx
        self.node = node
        self.catalog = catalog
        self._cache = ctx.cache

    def next(self) -> Iterator[Dict[str, Any]]:
        pk_col = self.node.schema.get_pk_column()
        if not pk_col: raise ValueError("PK required")

        indexes = (self.catalog.get_indexes_for_table(self.node.table_id)
                   if self.catalog else [])

        count = 0
        for values in self.node.rows:
            # 1. Encode key + value and write the row
            pk_val = values.get(pk_col.name)
            key = KeyEncoder.encode_row(self.node.table_id, pk_val)
            self.ctx.txn.set(key, KeyEncoder.encode_row_value(values))
            # Drop any cached decode of this key (e.g. a re-inserted PK that
            # was previously read/deleted); the read cache is not write-through.
            self._cache.invalidate(key)

            # 2. Write to secondary indexes
            for idx in indexes:
                indexed_val = values.get(idx.column_name)
                if indexed_val is not None:
                    idx_key = KeyEncoder.encode_index(
                        self.node.table_id, idx.id, indexed_val, pk_val
                    )
                    # Index value is just the PK (for point lookup)
                    self.ctx.txn.set(idx_key, str(pk_val))
            count += 1

        yield {"count": count}

class UpdateExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalUpdate, catalog=None):
        self.ctx = ctx
        self.node = node
        self.catalog = catalog
        self._cache = ctx.cache

    def next(self) -> Iterator[Dict[str, Any]]:
        # 1. Scan all rows
        start = f"/t/{self.node.table_id}/_r/"
        end = f"/t/{self.node.table_id}/_r/~"
        kv_pairs = self.ctx.txn.scan(start, end)

        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"

        indexes = (self.catalog.get_indexes_for_table(self.node.table_id)
                   if self.catalog else [])

        count = 0
        for key, val in kv_pairs:
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val

            # 2. Filter by condition
            if eval_predicate(self.node.condition, row):
                # Capture indexed values before mutating so we can rewrite
                # any secondary-index entry whose key changed.
                old_indexed = {idx.id: row.get(idx.column_name) for idx in indexes}

                # 3. Apply SET clause. Every value is a sqlglot node; the
                # shared resolver handles literals of all types (int, float,
                # negative, bool, NULL) as well as column expressions like
                # balance + 10. (Plain Python values pass through unchanged.)
                for col, new_val in self.node.set_clause.items():
                    row[col] = resolve_operand(new_val, row)

                # 4. Write back
                new_val_enc = KeyEncoder.encode_row_value(row)
                self.ctx.txn.set(key, new_val_enc)
                # The read cache is not write-through; drop the stale decode.
                self._cache.invalidate(key)

                # 5. Maintain secondary indexes: tombstone the old entry and
                # add the new one for any indexed column that changed.
                self._maintain_indexes(indexes, pk_val, old_indexed, row)
                count += 1

        yield {"count": count}

    def _maintain_indexes(self, indexes, pk_val, old_indexed, row):
        for idx in indexes:
            old_v = old_indexed[idx.id]
            new_v = row.get(idx.column_name)
            if old_v == new_v:
                continue
            if old_v is not None:
                old_key = KeyEncoder.encode_index(
                    self.node.table_id, idx.id, old_v, pk_val)
                self.ctx.txn.set(old_key, None)  # tombstone
            if new_v is not None:
                new_key = KeyEncoder.encode_index(
                    self.node.table_id, idx.id, new_v, pk_val)
                self.ctx.txn.set(new_key, str(pk_val))

class DeleteExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalDelete, catalog=None):
        self.ctx = ctx
        self.node = node
        self.catalog = catalog
        self._cache = ctx.cache

    def next(self) -> Iterator[Dict[str, Any]]:
        start = f"/t/{self.node.table_id}/_r/"
        end = f"/t/{self.node.table_id}/_r/~"
        kv_pairs = self.ctx.txn.scan(start, end)

        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"

        indexes = (self.catalog.get_indexes_for_table(self.node.table_id)
                   if self.catalog else [])

        count = 0
        for key, val in kv_pairs:
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val

            if eval_predicate(self.node.condition, row):
                # Delete by writing a None tombstone; scans filter these out.
                self.ctx.txn.set(key, None)
                # The read cache is not write-through; drop the stale decode.
                self._cache.invalidate(key)

                # Tombstone this row's secondary-index entries too, so an
                # index scan can't resurrect a pointer to the deleted row.
                for idx in indexes:
                    idx_v = row.get(idx.column_name)
                    if idx_v is not None:
                        idx_key = KeyEncoder.encode_index(
                            self.node.table_id, idx.id, idx_v, pk_val)
                        self.ctx.txn.set(idx_key, None)
                count += 1

        yield {"count": count}

class QualifyExecutor(PhysicalOperator):
    """Add ``table.col`` keys to every row of a base scan feeding a JOIN.

    A row from the storage scan is keyed by bare column names. Two joined
    tables can share a column name (``id``), so the merged row would silently
    lose one side. Emitting each column under both its bare name and a
    ``qualify.col`` name lets ``users.id`` and ``orders.id`` coexist; the
    shared operand resolver honors the qualifier (see ``predicates._resolve``),
    and bare references to a colliding name are rejected at bind time. The
    bare keys are kept so non-colliding columns can still be referenced
    unqualified. Operates on a copy -- the scan's read cache only ever sees
    bare rows.

    A qualified key is emitted for *every* schema column, defaulting an absent
    (NULL/unset) column to None. This is essential: without it a missing
    column would have no ``table.col`` key, and a qualified reference would
    fall back to the bare name -- which on a collision holds the *other*
    table's value, silently matching rows that should not join.
    """
    def __init__(self, child: PhysicalOperator, qualify: str, columns: List[str]):
        self.child = child
        self.prefix = qualify + "."
        self.columns = columns

    def next(self) -> Iterator[Dict[str, Any]]:
        prefix = self.prefix
        for row in self.child.next():
            out = dict(row)
            for col in self.columns:
                out[prefix + col] = row.get(col)
            yield out


class JoinExecutor(PhysicalOperator):
    """INNER / LEFT / RIGHT / FULL OUTER / CROSS join over flat merged rows.

    Correctness over the old executors:
      * The full ON predicate is evaluated via the shared ``predicates``
        module (so non-equi and compound ``ON`` conditions work, and column
        references resolve by qualifier -- the join no longer depends on which
        side of ``=`` each column was written on).
      * Outer joins emit NULL-padded rows for unmatched tuples instead of
        silently degrading to an inner join.
      * Merged rows carry ``table.col`` keys (from ``QualifyExecutor``), so
        same-named columns from different tables no longer clobber each other.

    INNER equi-joins with classifiable key columns take a hash-join fast path
    (build on the right input, probe with the left); everything else uses a
    nested loop, which is always correct. Hash keys use the engine's value
    equality (``distinct_key``) so ``"1"`` and ``1`` match as they do under a
    nested-loop ``ON`` comparison.
    """
    def __init__(self, left: PhysicalOperator, right: PhysicalOperator, node: LogicalJoin):
        self.left = left
        self.right = right
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        jt = (self.node.join_type or "INNER").upper()
        cond = self.node.condition
        right_rows = list(self.right.next())

        # CROSS / comma join: cartesian product, no predicate.
        if cond is None:
            for l_row in self.left.next():
                for r_row in right_rows:
                    yield {**l_row, **r_row}
            return

        if jt == "INNER" and self.node.hash_keys is not None:
            yield from self._hash_inner(right_rows)
        else:
            yield from self._nested(right_rows, jt)

    def _hash_inner(self, right_rows) -> Iterator[Dict[str, Any]]:
        left_expr, right_expr = self.node.hash_keys
        table: Dict[Any, List[Dict[str, Any]]] = {}
        for r_row in right_rows:
            kv = resolve_operand(right_expr, r_row)
            # NULL never equi-joins: `NULL = NULL` is UNKNOWN, not true. Skip
            # NULL keys so the hash path matches the nested-loop ON evaluation
            # (which excludes them via three-valued logic) instead of bucketing
            # all NULLs together and joining them.
            if kv is None:
                continue
            table.setdefault(distinct_key(kv), []).append(r_row)
        for l_row in self.left.next():
            kv = resolve_operand(left_expr, l_row)
            if kv is None:
                continue
            for r_row in table.get(distinct_key(kv), []):
                yield {**l_row, **r_row}

    def _nested(self, right_rows, jt) -> Iterator[Dict[str, Any]]:
        cond = self.node.condition
        right_pad = self.node.right_pad or {}
        left_pad = self.node.left_pad or {}
        right_matched = [False] * len(right_rows)

        for l_row in self.left.next():
            matched = False
            for i, r_row in enumerate(right_rows):
                merged = {**l_row, **r_row}
                if eval_predicate(cond, merged) is True:
                    matched = True
                    right_matched[i] = True
                    yield merged
            if not matched and jt in ("LEFT", "FULL"):
                yield {**l_row, **right_pad}

        if jt in ("RIGHT", "FULL"):
            for i, r_row in enumerate(right_rows):
                if not right_matched[i]:
                    yield {**left_pad, **r_row}


class IndexScanExecutor(PhysicalOperator):
    """Uses secondary index for point lookups instead of full table scan."""
    def __init__(self, ctx: ExecutionContext, node: LogicalScan, index, lookup_value):
        self.ctx = ctx
        self.node = node
        self.index = index
        self.lookup_value = lookup_value

    def next(self) -> Iterator[Dict[str, Any]]:
        # Scan index range for this value
        prefix = KeyEncoder.encode_index_prefix(
            self.node.table_id, self.index.id, self.lookup_value
        )
        end = prefix + "~"
        
        idx_pairs = self.ctx.txn.scan(prefix, end)
        
        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"
        
        for idx_key, pk_val in idx_pairs:
            # Fetch the actual row by PK
            row_key = KeyEncoder.encode_row(self.node.table_id, pk_val)
            row_val = self.ctx.txn.get(row_key)
            if row_val:
                row = KeyEncoder.decode_row_value(row_val)
                row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
                yield row


class ProjectExecutor(PhysicalOperator):
    """Restrict each row to the projected columns (SELECT col list).

    Without this, ``SELECT name FROM users`` returned every column because
    projection was a no-op. ``SELECT *`` never builds a Project node, so it
    streams all columns through unchanged.
    """
    def __init__(self, child: PhysicalOperator, node: LogicalProject):
        self.child = child
        self.columns = node.column_names

    def next(self) -> Iterator[Dict[str, Any]]:
        for row in self.child.next():
            yield {col: row.get(col) for col in self.columns}


class ProjectExprsExecutor(PhysicalOperator):
    """Expression-based projection for the JOIN path (LogicalProjectExprs).

    Each projection is ``(out_name, expr)``; the value is resolved per row by
    the shared operand resolver, so qualified columns (``users.id``) and
    expressions (``price * qty``) project correctly. Output names are unique
    by construction (the parser rejects duplicates at bind time).
    """
    def __init__(self, child: PhysicalOperator, node: LogicalProjectExprs):
        self.child = child
        self.projections = node.projections

    def next(self) -> Iterator[Dict[str, Any]]:
        for row in self.child.next():
            yield {name: resolve_operand(expr, row) for name, expr in self.projections}


class SortExecutor(PhysicalOperator):
    """ORDER BY: materialize the child rows and sort by the key list.

    Each key is ``(expr, desc, nulls_first)``. ``expr`` is resolved per row
    through the shared operand resolver, so ``ORDER BY age``,
    ``ORDER BY price * qty`` and (after the parser rewrites it) positional
    ``ORDER BY 1`` all work. ``desc`` flips the value comparison.
    ``nulls_first`` is the absolute NULL position (sqlglot already folds the
    SQL "NULL is the smallest value" default and any explicit NULLS
    FIRST/LAST into it), so it is *not* re-inverted by ``desc``. Genuinely
    incomparable values (e.g. number vs non-numeric text) fall back to a
    stable string compare instead of raising.

    This sits below any projection so ORDER BY can reference columns that are
    not in the SELECT list (``SELECT name FROM t ORDER BY age``).
    """
    def __init__(self, child: PhysicalOperator, node: LogicalSort):
        self.child = child
        self.keys = node.keys

    def next(self) -> Iterator[Dict[str, Any]]:
        rows = list(self.child.next())
        rows.sort(key=functools.cmp_to_key(self._cmp))
        return iter(rows)

    def _cmp(self, a: Dict[str, Any], b: Dict[str, Any]) -> int:
        for expr, desc, nulls_first in self.keys:
            av = resolve_operand(expr, a)
            bv = resolve_operand(expr, b)
            if av is None or bv is None:
                if av is None and bv is None:
                    continue
                if av is None:
                    return -1 if nulls_first else 1
                return 1 if nulls_first else -1
            c = compare_values(av, bv)
            if c:
                return -c if desc else c
        return 0


class LimitExecutor(PhysicalOperator):
    """LIMIT / OFFSET: skip ``offset`` rows, then yield at most ``limit``.

    Lazy: stops pulling from the child once ``limit`` rows are emitted.
    ``limit`` of None caps nothing (a bare OFFSET).
    """
    def __init__(self, child: PhysicalOperator, node: LogicalLimit):
        self.child = child
        self.limit = node.limit
        self.offset = node.offset or 0

    def next(self) -> Iterator[Dict[str, Any]]:
        skipped = 0
        emitted = 0
        for row in self.child.next():
            if skipped < self.offset:
                skipped += 1
                continue
            if self.limit is not None and emitted >= self.limit:
                return
            yield row
            emitted += 1


class AggregateExecutor(PhysicalOperator):
    """GROUP BY + aggregate functions (with HAVING).

    Materializes the child rows, buckets them by the resolved GROUP BY key
    (first-appearance order, which is deterministic given the storage scan
    order), then emits one output row per surviving group. With no GROUP BY it
    forms a single global group -- emitted even for an empty input, so
    ``SELECT COUNT(*) FROM empty`` returns one row holding 0.

    Aggregate values are computed once per group, then spliced into the HAVING
    and output expressions via ``substitute_aggs`` so the shared resolver does
    the final arithmetic/comparison (``SUM(x) + 1``, ``HAVING COUNT(*) > 2``).
    Non-aggregated output columns are GROUP BY keys, so they are read from a
    representative row of the group (their value is constant within it).
    """
    def __init__(self, child: PhysicalOperator, node: LogicalAggregate):
        self.child = child
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        rows = list(self.child.next())
        node = self.node

        groups: Dict[Any, List[Dict[str, Any]]] = {}
        ordered_keys: List[Any] = []
        if node.group_keys:
            for row in rows:
                key = tuple(resolve_operand(g, row) for g in node.group_keys)
                bucket = groups.get(key)
                if bucket is None:
                    bucket = groups[key] = []
                    ordered_keys.append(key)
                bucket.append(row)
        else:
            groups[()] = rows
            ordered_keys.append(())

        for key in ordered_keys:
            grp = groups[key]
            rep = grp[0] if grp else {}
            agg_values = {spec.key: compute_agg(spec, grp) for spec in node.aggregates}

            if node.having is not None:
                cond = substitute_aggs(node.having, agg_values)
                if eval_predicate(cond, rep) is not True:
                    continue

            out = {}
            for source_expr, name in node.output:
                out[name] = resolve_operand(substitute_aggs(source_expr, agg_values), rep)
            yield out


class DistinctExecutor(PhysicalOperator):
    """SELECT DISTINCT: yield each distinct row once, in first-seen order."""
    def __init__(self, child: PhysicalOperator):
        self.child = child

    def next(self) -> Iterator[Dict[str, Any]]:
        seen = set()
        for row in self.child.next():
            # Per-column (name, distinct_key) so de-duplication uses the same
            # value equality as the rest of the engine (numeric coercion;
            # TRUE stays distinct from 1); sorted by name for a stable,
            # hashable key independent of dict ordering.
            key = tuple(sorted(
                (col, distinct_key(v)) for col, v in row.items()))
            if key in seen:
                continue
            seen.add(key)
            yield row


def build_physical_plan(ctx: ExecutionContext, logical: LogicalNode, catalog=None) -> PhysicalOperator:
    if isinstance(logical, LogicalScan):
        # Check if index scan is possible
        scan: PhysicalOperator
        if logical.index_id is not None and catalog:
            # Find the specific index object
            indexes = catalog.get_indexes_for_table(logical.table_id)
            target_idx = next((idx for idx in indexes if idx.id == logical.index_id), None)
            if target_idx:
                scan = IndexScanExecutor(ctx, logical, target_idx, logical.lookup_value)
            else:
                scan = TableScanExecutor(ctx, logical)
        else:
            scan = TableScanExecutor(ctx, logical)
        # In the JOIN path the scan is tagged with a qualifier so its rows
        # also carry table.col keys; single-table scans leave qualify None.
        if logical.qualify:
            columns = [c.name for c in logical.schema.columns]
            return QualifyExecutor(scan, logical.qualify, columns)
        return scan
    elif isinstance(logical, LogicalProject):
        return ProjectExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalProjectExprs):
        return ProjectExprsExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalAggregate):
        return AggregateExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalDistinct):
        return DistinctExecutor(build_physical_plan(ctx, logical.children[0], catalog))
    elif isinstance(logical, LogicalSort):
        return SortExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalLimit):
        return LimitExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalFilter):
        return FilterExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalInsert):
        return InsertExecutor(ctx, logical, catalog)
    elif isinstance(logical, LogicalUpdate):
        return UpdateExecutor(ctx, logical, catalog)
    elif isinstance(logical, LogicalDelete):
        return DeleteExecutor(ctx, logical, catalog)
    elif isinstance(logical, LogicalJoin):
        left_op = build_physical_plan(ctx, logical.left, catalog)
        right_op = build_physical_plan(ctx, logical.right, catalog)
        return JoinExecutor(left_op, right_op, logical)
    else:
        if logical.children:
            return build_physical_plan(ctx, logical.children[0], catalog)
        raise ValueError("Unknown Node")
