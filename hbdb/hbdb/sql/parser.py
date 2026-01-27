import sqlglot
from sqlglot import exp
from typing import Any, Dict, List
from .catalog import Catalog
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject, 
                   LogicalInsert, LogicalUpdate, LogicalDelete, LogicalJoin)
from .types import Schema, Column

class SQLParser:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def parse(self, sql: str) -> LogicalNode:
        parsed = sqlglot.parse_one(sql)
        
        if isinstance(parsed, exp.Select):
            return self._bind_select(parsed)
        elif isinstance(parsed, exp.Insert):
            return self._bind_insert(parsed)
        elif isinstance(parsed, exp.Update):
            return self._bind_update(parsed)
        elif isinstance(parsed, exp.Delete):
            return self._bind_delete(parsed)
        else:
            raise ValueError(f"Unsupported statement: {parsed.key}")

    def _bind_select(self, node: exp.Select) -> LogicalNode:
        # Check for JOINs
        joins = node.args.get("joins")
        
        if joins:
            # Handle JOIN
            return self._bind_select_with_join(node, joins)
        
        # Simple SELECT (no JOIN)
        table_exp = node.find(exp.Table)
        if not table_exp:
            raise ValueError("SELECT without FROM not supported")
            
        table_name = table_exp.name
        table = self.catalog.get_table(table_name)
        if not table:
            raise ValueError(f"Table {table_name} not found")
            
        root = LogicalScan(children=[], schema=table.schema, table_name=table.name, table_id=table.id)
        
        # Bind WHERE (Filter)
        if node.args.get("where"):
            root = LogicalFilter(children=[root], schema=root.schema, condition=node.args.get("where"))

        # Bind SELECT (Project)
        projections = node.expressions
        if not (len(projections) == 1 and isinstance(projections[0], exp.Star)):
            col_names = [p.name for p in projections]
            root = LogicalProject(children=[root], schema=root.schema, column_names=col_names)
            
        return root

    def _bind_select_with_join(self, node: exp.Select, joins) -> LogicalNode:
        # Get all tables - first one is the left (FROM) table
        all_tables = list(node.find_all(exp.Table))
        if not all_tables:
            raise ValueError("JOIN without tables not supported")
        
        left_table_exp = all_tables[0]
        left_table = self.catalog.get_table(left_table_exp.name)
        if not left_table:
            raise ValueError(f"Table {left_table_exp.name} not found")
        
        left_scan = LogicalScan(children=[], schema=left_table.schema, 
                                table_name=left_table.name, table_id=left_table.id)
        
        # Process each JOIN
        root = left_scan
        for join in joins:
            right_table_exp = join.find(exp.Table)
            right_table = self.catalog.get_table(right_table_exp.name)
            if not right_table:
                raise ValueError(f"Table {right_table_exp.name} not found")
            
            right_scan = LogicalScan(children=[], schema=right_table.schema,
                                     table_name=right_table.name, table_id=right_table.id)
            
            # Get ON condition
            on_condition = join.args.get("on")
            
            # Determine join type
            join_type = "INNER"
            if isinstance(join, exp.Join):
                if join.args.get("side"):
                    join_type = str(join.args.get("side")).upper()
            
            # Merge schemas for result
            from .types import Schema
            merged_schema = Schema(columns=left_table.schema.columns + right_table.schema.columns)
            
            root = LogicalJoin(
                children=[],
                schema=merged_schema,
                left=root,
                right=right_scan,
                join_type=join_type,
                condition=on_condition
            )
        
        # Apply WHERE if present
        if node.args.get("where"):
            root = LogicalFilter(children=[root], schema=root.schema, condition=node.args.get("where"))
        
        return root

    def _bind_insert(self, node: exp.Insert) -> LogicalNode:
        # Table is inside a Schema node for INSERT
        table_exp = node.find(exp.Table)
        if not table_exp:
            raise ValueError("INSERT without table not supported")
        table_name = table_exp.name
        table = self.catalog.get_table(table_name)
        if not table: raise ValueError(f"Table {table_name} not found")
        
        # Parse column names from the Schema node
        schema_node = node.this
        if schema_node and schema_node.expressions:
            cols = [c.name for c in schema_node.expressions]
        else:
            # If no columns specified, use schema order
            cols = [c.name for c in table.schema.columns]
            
        # Parse VALUES
        values_clause = node.expression
        if not values_clause:
            raise ValueError("INSERT without VALUES not supported")
            
        first_tuple = values_clause.expressions[0]
        vals = []
        for v in first_tuple.expressions:
            # Handle different literal types
            if isinstance(v, exp.Literal):
                if v.is_string:
                    vals.append(v.this)
                else:
                    vals.append(int(v.this) if v.this.isdigit() else v.this)
            elif hasattr(v, 'name'):
                vals.append(v.name)
            else:
                vals.append(str(v.this))
        
        # Build dict
        row_map = dict(zip(cols, vals))
            
        return LogicalInsert(
            children=[], 
            schema=table.schema, 
            table_name=table.name, 
            table_id=table.id,
            values=row_map
        )

    def _bind_update(self, node: exp.Update) -> LogicalNode:
        table_exp = node.find(exp.Table)
        if not table_exp:
            raise ValueError("UPDATE without table not supported")
        table_name = table_exp.name
        table = self.catalog.get_table(table_name)
        if not table: raise ValueError(f"Table {table_name} not found")
        
        # Parse SET clause
        set_clause = {}
        for eq in node.expressions:
            if isinstance(eq, exp.EQ):
                col = eq.left.name
                val_node = eq.right
                if isinstance(val_node, exp.Literal):
                    val = int(val_node.this) if val_node.this.isdigit() else val_node.this
                else:
                    val = val_node  # Expression (e.g., col + 1)
                set_clause[col] = val
        
        condition = node.args.get("where")
        
        return LogicalUpdate(
            children=[],
            schema=table.schema,
            table_name=table.name,
            table_id=table.id,
            set_clause=set_clause,
            condition=condition
        )

    def _bind_delete(self, node: exp.Delete) -> LogicalNode:
        table_exp = node.find(exp.Table)
        if not table_exp:
            raise ValueError("DELETE without table not supported")
        table_name = table_exp.name
        table = self.catalog.get_table(table_name)
        if not table: raise ValueError(f"Table {table_name} not found")
        
        condition = node.args.get("where")
        
        return LogicalDelete(
            children=[],
            schema=table.schema,
            table_name=table.name,
            table_id=table.id,
            condition=condition
        )
