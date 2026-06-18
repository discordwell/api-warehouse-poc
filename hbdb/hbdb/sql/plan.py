from dataclasses import dataclass
from typing import List, Optional, Any, Dict
from .types import Schema

@dataclass
class LogicalNode:
    children: List['LogicalNode']
    schema: Schema

@dataclass
class LogicalScan(LogicalNode):
    table_name: str
    table_id: int
    index_id: Optional[int] = None
    lookup_value: Any = None
    # When set (only in the JOIN path), the scan's rows are emitted with both
    # their bare column names and "qualify.col" keys, so columns from
    # different tables can coexist in one merged row even when their names
    # collide. None for single-table queries -> bare keys only (unchanged).
    qualify: Optional[str] = None

@dataclass
class LogicalFilter(LogicalNode):
    # For POC, expression is just a callable lambda or dict
    # In real DB, it's an Expression Tree
    condition: Any 

@dataclass
class LogicalProject(LogicalNode):
    column_names: List[str]

@dataclass
class LogicalProjectExprs(LogicalNode):
    # Expression-based projection used by the JOIN path: each entry is
    # (out_name, expr) where expr is a sqlglot operand resolved per (merged)
    # row through the shared operand resolver. Unlike LogicalProject (a plain
    # column-name restriction for single-table SELECTs), this can pull
    # table-qualified columns (users.id vs orders.id) and evaluate
    # expressions, which a flat column-name list cannot express.
    projections: List[Any]

@dataclass
class LogicalSort(LogicalNode):
    # ORDER BY: each key is (expr, desc, nulls_first). `expr` is a sqlglot
    # operand node resolved per row by the shared operand resolver; `desc`
    # flips the comparison; `nulls_first` places NULLs (absolute position,
    # already accounting for direction).
    keys: List[Any]

@dataclass
class LogicalLimit(LogicalNode):
    # LIMIT / OFFSET. limit=None means "no row cap" (a bare OFFSET).
    limit: Optional[int]
    offset: int = 0

@dataclass
class LogicalAggregate(LogicalNode):
    # GROUP BY + aggregate functions (and HAVING).
    #   group_keys: list of sqlglot operand nodes to group by (the empty list
    #     means a single global group -- e.g. SELECT COUNT(*) FROM t).
    #   aggregates: list of aggregates.AggSpec to compute once per group.
    #   output: ordered list of (source_expr, out_name) -- source_expr is the
    #     SELECT item with its alias stripped (a group-key reference, an
    #     aggregate, or an expression mixing the two); out_name is the result
    #     column name (alias, column name, or the expression's SQL text).
    #   having: a sqlglot predicate evaluated per group after aggregation
    #     (None when there is no HAVING).
    group_keys: List[Any]
    aggregates: List[Any]
    output: List[Any]
    having: Any = None

@dataclass
class LogicalDistinct(LogicalNode):
    # SELECT DISTINCT: collapse duplicate rows emitted by the child. No extra
    # fields -- it de-duplicates whatever columns the child (a Project, or a
    # bare scan for SELECT DISTINCT *) produces.
    pass

@dataclass
class LogicalInsert(LogicalNode):
    table_name: str
    table_id: int
    rows: List[Dict[str, Any]]  # one Column -> Value dict per VALUES tuple

@dataclass
class LogicalUpdate(LogicalNode):
    table_name: str
    table_id: int
    set_clause: Dict[str, Any]  # Column -> New Value Expression
    condition: Any  # WHERE clause

@dataclass
class LogicalDelete(LogicalNode):
    table_name: str
    table_id: int
    condition: Any  # WHERE clause

@dataclass
class LogicalJoin(LogicalNode):
    left: 'LogicalNode'
    right: 'LogicalNode'
    join_type: str  # INNER, LEFT, RIGHT, FULL (CROSS/comma -> condition is None)
    condition: Any  # ON clause (None for CROSS / comma join)
    # (left_key_expr, right_key_expr) when the ON clause is a single equi-join
    # whose two columns can be assigned to the left and right inputs; enables
    # the hash-join fast path for INNER joins. None -> evaluate the full ON
    # predicate with a nested loop (handles non-equi / compound ON, and is
    # always correct).
    hash_keys: Any = None
    # Pre-computed NULL-padding dicts for outer joins: left_pad sets every
    # left-side key to None (used when a RIGHT/FULL row has no left match);
    # right_pad sets every right-side key to None (used when a LEFT/FULL row
    # has no right match). None for inner/cross joins, which never pad.
    left_pad: Any = None
    right_pad: Any = None

@dataclass
class LogicalCreateTable(LogicalNode):
    table_name: str
    columns: List[Any] # List[Column]

@dataclass
class LogicalCreateIndex(LogicalNode):
    index_name: str
    table_name: str
    column_name: str
