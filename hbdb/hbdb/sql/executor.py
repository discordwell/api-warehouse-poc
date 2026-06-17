from typing import Iterator, Any, Dict, List, Tuple
from abc import ABC, abstractmethod
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject,
                   LogicalInsert, LogicalUpdate, LogicalDelete, LogicalJoin)
from .encoding import KeyEncoder
from .predicates import evaluate as eval_predicate, resolve as resolve_operand
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

class NestedLoopJoinExecutor(PhysicalOperator):
    def __init__(self, left: PhysicalOperator, right: PhysicalOperator, node: LogicalJoin):
        self.left = left
        self.right = right
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        left_rows = list(self.left.next())
        right_rows = list(self.right.next())
        
        for l_row in left_rows:
            for r_row in right_rows:
                if self._matches_join(l_row, r_row, self.node.condition):
                    merged = {**l_row, **r_row}
                    yield merged

    def _matches_join(self, left, right, condition):
        if condition is None: return True
        from sqlglot import exp
        if isinstance(condition, exp.EQ):
            l_col = condition.left.name
            r_col = condition.right.name
            return left.get(l_col) == right.get(r_col)
        return True

class HashJoinExecutor(PhysicalOperator):
    """Hash Join - builds a hash table on the right input, probes with the left."""
    def __init__(self, left: PhysicalOperator, right: PhysicalOperator, node: LogicalJoin):
        self.left = left
        self.right = right
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        from sqlglot import exp

        # Get join columns
        condition = self.node.condition
        if not isinstance(condition, exp.EQ):
            # Fallback to nested loop
            for row in NestedLoopJoinExecutor(self.left, self.right, self.node).next():
                yield row
            return

        r_col = condition.right.name
        l_col = condition.left.name

        # Build phase: hash the right side
        hash_table = {}
        for r_row in self.right.next():
            key = r_row.get(r_col)
            hash_table.setdefault(key, []).append(r_row)

        # Probe phase: scan left and probe hash table
        for l_row in self.left.next():
            key = l_row.get(l_col)
            for r_row in hash_table.get(key, []):
                yield {**l_row, **r_row}


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


def build_physical_plan(ctx: ExecutionContext, logical: LogicalNode, catalog=None) -> PhysicalOperator:
    if isinstance(logical, LogicalScan):
        # Check if index scan is possible
        if logical.index_id is not None and catalog:
            # Find the specific index object
            indexes = catalog.get_indexes_for_table(logical.table_id)
            target_idx = next((idx for idx in indexes if idx.id == logical.index_id), None)
            if target_idx:
                return IndexScanExecutor(ctx, logical, target_idx, logical.lookup_value)
        return TableScanExecutor(ctx, logical)
    elif isinstance(logical, LogicalProject):
        return ProjectExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
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
        # Use Hash Join for equi-joins, Nested Loop otherwise
        from sqlglot import exp
        if isinstance(logical.condition, exp.EQ):
            return HashJoinExecutor(left_op, right_op, logical)
        return NestedLoopJoinExecutor(left_op, right_op, logical)
    else:
        if logical.children:
            return build_physical_plan(ctx, logical.children[0], catalog)
        raise ValueError("Unknown Node")
