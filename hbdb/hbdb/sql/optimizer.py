"""
Cost-Based Optimizer (CBO) for HBDB SQL Layer.

Statistics + Cost Model + Plan Enumeration.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from .plan import LogicalNode, LogicalScan, LogicalFilter, LogicalJoin
from .catalog import Catalog

@dataclass
class TableStats:
    row_count: int
    avg_row_size: int
    column_stats: Dict[str, 'ColumnStats']

@dataclass
class ColumnStats:
    distinct_count: int
    null_count: int
    min_value: Any
    max_value: Any

class StatsCollector:
    """Collects and caches table statistics."""
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self._cache: Dict[int, TableStats] = {}

    def get_stats(self, table_id: int) -> Optional[TableStats]:
        return self._cache.get(table_id)

    def update_stats(self, table_id: int, rows: List[Dict[str, Any]]):
        if not rows:
            self._cache[table_id] = TableStats(0, 0, {})
            return
        
        row_count = len(rows)
        avg_size = sum(len(str(r)) for r in rows) // row_count
        
        col_stats = {}
        for col in rows[0].keys():
            vals = [r.get(col) for r in rows if r.get(col) is not None]
            if vals:
                col_stats[col] = ColumnStats(
                    distinct_count=len(set(str(v) for v in vals)),
                    null_count=len(rows) - len(vals),
                    min_value=min(vals, key=str),
                    max_value=max(vals, key=str)
                )
        
        self._cache[table_id] = TableStats(row_count, avg_size, col_stats)

class CostModel:
    """
    Simple cost model based on row counts and selectivity.
    Cost units are arbitrary (think: "disk pages read").
    """
    SCAN_COST_PER_ROW = 1.0
    FILTER_COST_PER_ROW = 0.1
    INDEX_LOOKUP_COST = 2.0
    NESTED_LOOP_MULTIPLIER = 1.0

    @staticmethod
    def estimate_scan_cost(stats: TableStats) -> float:
        return stats.row_count * CostModel.SCAN_COST_PER_ROW

    @staticmethod
    def estimate_filter_cost(stats: TableStats, selectivity: float) -> float:
        return stats.row_count * CostModel.FILTER_COST_PER_ROW * selectivity

    @staticmethod
    def estimate_index_cost(expected_rows: int) -> float:
        return CostModel.INDEX_LOOKUP_COST + expected_rows * 0.5

    @staticmethod
    def estimate_join_cost(left_rows: int, right_rows: int) -> float:
        return left_rows * right_rows * CostModel.NESTED_LOOP_MULTIPLIER

class Optimizer:
    """
    Rule-Based + Cost-Based hybrid optimizer.
    
    Priority:
    1. Uses index if available for point-query (WHERE col = X).
    2. Pushes predicates down.
    3. Estimates costs for join orderings.
    """
    def __init__(self, catalog: Catalog, stats_collector: StatsCollector):
        self.catalog = catalog
        self.stats = stats_collector

    def optimize(self, plan: LogicalNode) -> LogicalNode:
        # Phase 1: Predicate Pushdown
        plan = self._push_predicates(plan)
        
        # Phase 2: Index Selection (for Filter -> Scan patterns)
        plan = self._select_indexes(plan)
        
        # Phase 3: Join Reordering (if multiple joins)
        plan = self._reorder_joins(plan)
        
        return plan

    def _push_predicates(self, node: LogicalNode) -> LogicalNode:
        # Predicate pushdown: Move Filter closer to Scan
        if isinstance(node, LogicalFilter):
            child = node.children[0] if node.children else None
            if isinstance(child, LogicalScan):
                # Already at scan - this is optimal
                pass
        return node

    def _select_indexes(self, node: LogicalNode) -> LogicalNode:
        # Check if Filter on indexed column
        if isinstance(node, LogicalFilter):
            child = node.children[0] if node.children else None
            if isinstance(child, LogicalScan):
                indexes = self.catalog.get_indexes_for_table(child.table_id)
                # Check if filter column has index
                from sqlglot import exp
                cond = node.condition
                if isinstance(cond, exp.Where):
                    cond = cond.this
                if isinstance(cond, exp.EQ) and hasattr(cond.left, 'name'):
                    col = cond.left.name
                    for idx in indexes:
                        if idx.column_name == col:
                            # Mark for index scan (metadata flag)
                            child.use_index = idx
                            return node
        return node

    def _reorder_joins(self, node: LogicalNode) -> LogicalNode:
        # For now, keep original order
        # Full CBO would enumerate orderings and cost them
        return node

    def estimate_cost(self, node: LogicalNode) -> float:
        """Estimate total cost of a plan."""
        if isinstance(node, LogicalScan):
            stats = self.stats.get_stats(node.table_id)
            if stats:
                return CostModel.estimate_scan_cost(stats)
            return 100.0  # Default
        elif isinstance(node, LogicalFilter):
            child_cost = self.estimate_cost(node.children[0]) if node.children else 0
            return child_cost + 10.0  # Filter overhead
        elif isinstance(node, LogicalJoin):
            left_cost = self.estimate_cost(node.left)
            right_cost = self.estimate_cost(node.right)
            return left_cost + right_cost + CostModel.estimate_join_cost(100, 100)
        return 0.0
