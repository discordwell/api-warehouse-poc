from typing import List, Dict, Any, Optional
from ..core.proxy import Transaction
from ..db import HBDB
from .catalog import Catalog
from .parser import SQLParser
from .executor import build_physical_plan, ExecutionContext
from .optimizer import Optimizer, StatsCollector

class SQLEngine:
    def __init__(self, db: HBDB):
        self.db = db
        self.catalog = Catalog(db=self.db)
        self.parser = SQLParser(self.catalog)
        self.stats_collector = StatsCollector(self.catalog)
        self.optimizer = Optimizer(self.catalog, self.stats_collector)

    def execute(self, sql: str, txn: Optional[Transaction] = None) -> List[Dict[str, Any]]:
        # 1. Transaction Management
        # On error there is no rollback step: an uncommitted Transaction's
        # buffered writes are simply discarded with the object.
        local_txn = False
        if txn is None:
            txn = self.db.transaction()
            local_txn = True

        # 2. Parse & Bind
        logical_plan = self.parser.parse(sql)

        # Handle DDL
        from .plan import LogicalCreateTable, LogicalCreateIndex

        if isinstance(logical_plan, LogicalCreateTable):
            self.create_table_from_logical(logical_plan)
            return [{"status": "Table created"}]

        if isinstance(logical_plan, LogicalCreateIndex):
            self.create_index(logical_plan.index_name, logical_plan.table_name, logical_plan.column_name)
            return [{"status": "Index created"}]

        # 3. Optimize
        optimized_plan = self.optimizer.optimize(logical_plan)

        # 4. Execute
        ctx = ExecutionContext(txn, getattr(self.db, "read_cache", None))
        plan = build_physical_plan(ctx, optimized_plan, self.catalog)

        results = list(plan.next())

        # 5. Commit if local txn
        if local_txn:
            if not txn.commit():
                raise RuntimeError("Commit failed")

        return results

    def create_table(self, name: str, schema):
        """Create a table from a Schema object (programmatic DDL).

        Complements the SQL ``CREATE TABLE`` path so callers can build a
        schema directly without round-tripping through SQL text.
        """
        return self.catalog.create_table(name, schema)

    def create_table_from_logical(self, node):
        from .types import Schema as TableDef
        table_def = TableDef(columns=node.columns)
        self.create_table(node.table_name, table_def)

    def create_index(self, index_name: str, table_name: str, column_name: str):
        """Create a secondary index on a table column."""
        self.catalog.create_index(index_name, table_name, column_name)

    def analyze(self, table_name: str):
        """Collect statistics for a table (for CBO)."""
        table = self.catalog.get_table(table_name)
        if not table: return
        
        # Scan table and collect stats
        txn = self.db.transaction()
        start = f"/t/{table.id}/_r/"
        end = f"/t/{table.id}/_r/~"
        kv_pairs = txn.scan(start, end)
        
        from .encoding import KeyEncoder
        rows = []
        pk_col = table.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"
        
        for key, val in kv_pairs:
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
            rows.append(row)
        
        self.stats_collector.update_stats(table.id, rows)
