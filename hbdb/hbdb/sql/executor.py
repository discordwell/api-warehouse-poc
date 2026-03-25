from typing import Iterator, Any, Dict, List, Tuple
from abc import ABC, abstractmethod
from functools import lru_cache
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject, 
                   LogicalInsert, LogicalUpdate, LogicalDelete, LogicalJoin)
from .encoding import KeyEncoder
from ..core.proxy import Transaction
from ..core.cache import get_read_cache

# Query result cache for expensive operations like JOINs
_query_cache: Dict[str, List[Dict[str, Any]]] = {}
_query_cache_max_size = 100

class ExecutionContext:
    def __init__(self, txn: Transaction):
        self.txn = txn

class PhysicalOperator(ABC):
    @abstractmethod
    def next(self) -> Iterator[Dict[str, Any]]:
        pass

class TableScanExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalScan):
        self.ctx = ctx
        self.node = node
        self.encoder = KeyEncoder()
        self._cache = get_read_cache()

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
        # For POC, condition is a sqlglot expression.
        # We need to evaluate it against the row.
        # Creating a simplified python eval shim.
        
        for row in self.child.next():
            if self._evaluate(condition, row):
                yield row

    def _evaluate(self, condition, row):
        # The condition is a Where node containing the actual predicate
        from sqlglot import exp
        try:
            # Unwrap Where if needed
            if isinstance(condition, exp.Where):
                condition = condition.this
            
            if isinstance(condition, exp.EQ):
                # Get column name from left side
                col = condition.left.name if hasattr(condition.left, 'name') else str(condition.left)
                
                # Get value from right side (Literal)
                right = condition.right
                if isinstance(right, exp.Literal):
                    val = right.this  # The actual value
                else:
                    val = right.name if hasattr(right, 'name') else str(right)
                
                row_val = row.get(col)
                # Type-aware comparison
                if isinstance(row_val, int):
                    try:
                        return row_val == int(val)
                    except:
                        return str(row_val) == str(val)
                return str(row_val) == str(val)
            return True  # Fallback for unsupported predicates
        except Exception as e:
            return True

class InsertExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalInsert, catalog=None):
        self.ctx = ctx
        self.node = node
        self.catalog = catalog

    def next(self) -> Iterator[Dict[str, Any]]:
        # 1. Encode Key
        pk_col = self.node.schema.get_pk_column()
        if not pk_col: raise ValueError("PK required")
        
        pk_val = self.node.values.get(pk_col.name)
        key = KeyEncoder.encode_row(self.node.table_id, pk_val)
        
        # 2. Encode Value
        val = KeyEncoder.encode_row_value(self.node.values)
        
        # 3. Write row
        self.ctx.txn.set(key, val)
        
        # 4. Write to secondary indexes
        if self.catalog:
            indexes = self.catalog.get_indexes_for_table(self.node.table_id)
            for idx in indexes:
                indexed_val = self.node.values.get(idx.column_name)
                if indexed_val is not None:
                    idx_key = KeyEncoder.encode_index(
                        self.node.table_id, idx.id, indexed_val, pk_val
                    )
                    # Index value is just the PK (for point lookup)
                    self.ctx.txn.set(idx_key, str(pk_val))
        
        yield {"count": 1}

class UpdateExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalUpdate):
        self.ctx = ctx
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        # 1. Scan all rows
        start = f"/t/{self.node.table_id}/_r/"
        end = f"/t/{self.node.table_id}/_r/~"
        kv_pairs = self.ctx.txn.scan(start, end)
        
        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"
        
        count = 0
        for key, val in kv_pairs:
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
            
            # 2. Filter by condition
            if self._matches(self.node.condition, row):
                # 3. Apply SET clause
                for col, new_val in self.node.set_clause.items():
                    if isinstance(new_val, (int, str)):
                        row[col] = new_val
                    else:
                        # Expression evaluation (e.g., balance + 10)
                        row[col] = self._eval_expr(new_val, row)
                
                # 4. Write back
                new_val_enc = KeyEncoder.encode_row_value(row)
                self.ctx.txn.set(key, new_val_enc)
                count += 1
        
        yield {"count": count}

    def _matches(self, condition, row):
        if condition is None: return True
        from sqlglot import exp
        if isinstance(condition, exp.Where):
            condition = condition.this
        if isinstance(condition, exp.EQ):
            col = condition.left.name
            val = condition.right.this if hasattr(condition.right, 'this') else str(condition.right)
            row_val = row.get(col)
            if isinstance(row_val, int):
                try: return row_val == int(val)
                except: pass
            return str(row_val) == str(val)
        return True

    def _eval_expr(self, expr, row):
        from sqlglot import exp
        if isinstance(expr, exp.Add):
            left = row.get(expr.left.name) if hasattr(expr.left, 'name') else int(expr.left.this)
            right = int(expr.right.this) if hasattr(expr.right, 'this') else row.get(expr.right.name)
            return left + right
        elif isinstance(expr, exp.Sub):
            left = row.get(expr.left.name) if hasattr(expr.left, 'name') else int(expr.left.this)
            right = int(expr.right.this) if hasattr(expr.right, 'this') else row.get(expr.right.name)
            return left - right
        return expr

class DeleteExecutor(PhysicalOperator):
    def __init__(self, ctx: ExecutionContext, node: LogicalDelete):
        self.ctx = ctx
        self.node = node

    def next(self) -> Iterator[Dict[str, Any]]:
        start = f"/t/{self.node.table_id}/_r/"
        end = f"/t/{self.node.table_id}/_r/~"
        kv_pairs = self.ctx.txn.scan(start, end)
        
        pk_col = self.node.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"
        
        count = 0
        for key, val in kv_pairs:
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
            
            if self._matches(self.node.condition, row):
                # Delete by setting to None (or use a tombstone)
                # For POC, we'll set to empty - real system uses tombstones
                self.ctx.txn.set(key, None)
                count += 1
        
        yield {"count": count}

    def _matches(self, condition, row):
        if condition is None: return True
        from sqlglot import exp
        if isinstance(condition, exp.Where):
            condition = condition.this
        if isinstance(condition, exp.EQ):
            col = condition.left.name
            val = condition.right.this if hasattr(condition.right, 'this') else str(condition.right)
            row_val = row.get(col)
            if isinstance(row_val, int):
                try: return row_val == int(val)
                except: pass
            return str(row_val) == str(val)
        return True

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
    """Hash Join - builds hash table on right, probes with left. Supports result caching."""
    def __init__(self, left: PhysicalOperator, right: PhysicalOperator, node: LogicalJoin):
        self.left = left
        self.right = right
        self.node = node
        # Generate cache key from node structure
        self._cache_key = f"join:{id(node.left)}:{id(node.right)}:{str(node.condition)}"

    def next(self) -> Iterator[Dict[str, Any]]:
        global _query_cache
        
        # Check cache first
        if self._cache_key in _query_cache:
            for row in _query_cache[self._cache_key]:
                yield row
            return
        
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
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(r_row)
        
        # Probe phase: scan left and probe hash table
        results = []
        for l_row in self.left.next():
            key = l_row.get(l_col)
            matches = hash_table.get(key, [])
            for r_row in matches:
                merged = {**l_row, **r_row}
                results.append(merged)
                yield merged
        
        # Cache results (LRU eviction if at capacity)
        if len(_query_cache) >= _query_cache_max_size:
            _query_cache.pop(next(iter(_query_cache)))
        _query_cache[self._cache_key] = results


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
    elif isinstance(logical, LogicalFilter):
        return FilterExecutor(build_physical_plan(ctx, logical.children[0], catalog), logical)
    elif isinstance(logical, LogicalInsert):
        return InsertExecutor(ctx, logical, catalog)
    elif isinstance(logical, LogicalUpdate):
        return UpdateExecutor(ctx, logical)
    elif isinstance(logical, LogicalDelete):
        return DeleteExecutor(ctx, logical)
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
