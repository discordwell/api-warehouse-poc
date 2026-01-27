from typing import List, Dict, Any, Optional
from ..core.proxy import Transaction
from ..db import HBDB
from .catalog import Catalog, Schema
from .parser import SQLParser
from .executor import build_physical_plan, ExecutionContext
from .optimizer import Optimizer, StatsCollector

class SQLEngine:
    def __init__(self, db: HBDB):
        self.db = db
        self.catalog = Catalog()
        self.parser = SQLParser(self.catalog)
        self.stats_collector = StatsCollector(self.catalog)
        self.optimizer = Optimizer(self.catalog, self.stats_collector)

    def execute(self, sql: str, txn: Optional[Transaction] = None) -> List[Dict[str, Any]]:
        # 1. Transaction Management
        local_txn = False
        if txn is None:
            txn = self.db.transaction()
            local_txn = True

        try:
            # 2. Parse & Bind
            logical_plan = self.parser.parse(sql)
            
            # 3. Optimize
            optimized_plan = self.optimizer.optimize(logical_plan)
            
            # 4. Execute
            ctx = ExecutionContext(txn)
            plan = build_physical_plan(ctx, optimized_plan, self.catalog)
            
            results = list(plan.next())
            
            # 5. Commit if local txn
            if local_txn:
                if not txn.commit():
                    raise RuntimeError("Commit failed")
                    
            return results
            
        except Exception as e:
            if local_txn:
                pass
            raise e

    def create_table(self, name: str, schema: Schema):
        self.catalog.create_table(name, schema)

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
