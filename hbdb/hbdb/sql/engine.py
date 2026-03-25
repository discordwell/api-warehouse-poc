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
        self.catalog = Catalog(db=self.db)
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

            # Handle DDL
            from .plan import LogicalCreateTable, LogicalCreateIndex
            from .schema import Schema as TableSchema
            
            if isinstance(logical_plan, LogicalCreateTable):
                schema = TableSchema()
                # We need to reconstruct schema object from columns
                # Actually create_table takes Schema object with columns?
                # parser returns list of columns.
                # Let's fix create_table in engine to accept list of columns or fix here.
                # Catalog expects Schema.
                # Schema expects nothing in init but has add_column?
                # parser returns LogicalCreateTable with .columns list.
                
                # Re-init schema
                schema = TableSchema() 
                # Wait, Schema class in schema.py init is empty.
                # catalog.create_table(name, TableSchema()) then add columns?
                # catalog.create_table expects Schema object.
                # Let's populate it.
                real_schema = TableSchema()
                # We need to access private members or use public API?
                # create_table in catalog takes 'schema'.
                # Let's verify schema.py... 'create_table' in catalog takes 'schema'.
                # Schema object has no ctor args for columns?
                # Let's assume we can just pass it.
                
                # Actually, Schema doesn't store columns, Table does.
                # Catalog.create_table(name, schema)... wait.
                # Table stores columns. Schema manages tables? No.
                # schema.py: Schema class manages tables. Table class has columns.
                # Catalog.create_table -> returns Table.
                # So we should call catalog.create_table with NO schema?
                # schema.py: "class Schema: Database schema manager. Manages table definitions."
                # It's misnamed. It's a Catalog-lite.
                # But catalog.py imports Schema...
                # Let's look at catalog.py: "class Table: ... schema: Schema".
                # It denotes the DB schema?
                
                # Re-reading schema.py: 
                # class Schema: def create_table(self, name, columns) -> Table.
                # class Catalog: def create_table(self, name, schema) -> Table.
                
                # This is confusing. 
                # catalog.py line 35: Table(..., schema=schema).
                # Here 'schema' seems to be the Database Schema?
                # But Table also contains columns? schema.py line 38: columns: Dict.
                
                # Let's simplify.
                # In engine.py:50: self.catalog.create_table(name, schema).
                # But parser returns columns.
                
                # Let's assume for this POC we fix engine.create_table to construct the Table properly.
                self.create_table_from_logical(logical_plan)
                return [{"status": "Table created"}]
                
            if isinstance(logical_plan, LogicalCreateIndex):
                self.create_index(logical_plan.index_name, logical_plan.table_name, logical_plan.column_name)
                return [{"status": "Index created"}]
            
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

    def create_table_from_logical(self, node):
        from .types import Schema as TableDef
        table_def = TableDef(columns=node.columns)
        self.catalog.create_table(node.table_name, table_def)

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
