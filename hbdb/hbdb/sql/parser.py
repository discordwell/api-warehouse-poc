import sqlglot
from sqlglot import exp
from typing import Any, Dict, List
from .catalog import Catalog
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject,
                   LogicalInsert, LogicalUpdate, LogicalDelete, LogicalJoin,
                   LogicalCreateTable, LogicalCreateIndex, LogicalSort,
                   LogicalLimit)
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
        elif isinstance(parsed, exp.Create):
            return self._bind_create(parsed)
        else:
            raise ValueError(f"Unsupported statement: {parsed.key}")

    def _bind_create(self, node: exp.Create) -> LogicalNode:
        kind = node.args.get("kind")
        if kind == "TABLE":
            return self._bind_create_table(node)
        elif kind == "INDEX":
            return self._bind_create_index(node)
        else:
            raise ValueError(f"Unsupported CREATE kind: {kind}")

    def _bind_create_table(self, node: exp.Create) -> LogicalNode:
        table_exp = node.this
        table_name = table_exp.this.name
        
        # Parse columns
        # sqlglot structures create table columns in node.this.expressions? No, node.this is Schema usually
        schema_node = node.this
        if not isinstance(schema_node, exp.Schema):
             # Try finding schema in expressions if not directly there
             pass

        cols = []
        from .types import Column, DataType
        
        # For 'CREATE TABLE x (id INT, name TEXT)'
        # node.this is a Schema object
        for col_def in schema_node.expressions:
            if isinstance(col_def, exp.ColumnDef):
                col_name = col_def.this.name
                col_type_str = col_def.kind.this.name.upper()
                col_pk = False
                
                # Check for constraints (PRIMARY KEY)
                for constraint in col_def.args.get("constraints", []):
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        col_pk = True

                # Map type
                dtype = DataType.STRING
                if col_type_str in ("INT", "INTEGER"): dtype = DataType.INTEGER
                elif col_type_str == "TEXT": dtype = DataType.STRING
                elif col_type_str == "REAL": dtype = DataType.FLOAT
                elif col_type_str == "BOOLEAN": dtype = DataType.BOOLEAN
                
                cols.append(Column(name=col_name, type=dtype, primary_key=col_pk))
        
        return LogicalCreateTable(children=[], schema=None, table_name=table_name, columns=cols)

    def _bind_create_index(self, node: exp.Create) -> LogicalNode:
        # CREATE INDEX idx ON users (age)
        index_name = node.this.name # "idx"
        
        # Table is usually in properties or findable
        # sqlglot: node.this is the index name.
        # Check 'properties' or args
        # Example: CREATE INDEX x ON y (z)
        # properties: IndexParameters
        
        # Actually sqlglot parse tree for CREATE INDEX is complex.
        # Let's inspect node.args
        pass 
        # Debugging sqlglot structure is hard without running it. 
        # Attempt standard structure:
        table_exp = node.find(exp.Table)
        table_name = table_exp.name
        
        # Columns?
        # Usually inside the IndexParameters or just expressions?
        # Let's look for identifiers or columns
        cols = [c.name for c in node.find_all(exp.Column)]
        if not cols:
             # Might be Identifiers
             cols = [c.name for c in node.find_all(exp.Identifier) if c.name != index_name and c.name != table_name]

        return LogicalCreateIndex(children=[], schema=None, index_name=index_name, table_name=table_name, column_name=cols[0])

    def _bind_select(self, node: exp.Select) -> LogicalNode:
        # Check for JOINs
        joins = node.args.get("joins")

        if joins:
            # Handle JOIN
            return self._bind_select_with_join(node, joins)

        # Reject clauses we cannot honor *before* building a plan, so an
        # unsupported query fails loudly rather than silently returning the
        # wrong rows (the same fail-loud contract predicates.py enforces).
        self._reject_unsupported(node)

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

        # Decide projection. Only project plain column lists; SELECT *,
        # functions and aliased expressions stream through with all columns
        # (projecting them by `.name` would mangle the result). `select_cols`
        # is the projected name list, or None for "all columns".
        projections = node.expressions
        is_star = len(projections) == 1 and isinstance(projections[0], exp.Star)
        bare_cols = (not is_star and bool(projections) and all(
            isinstance(p, (exp.Column, exp.Identifier)) for p in projections))
        select_cols = [p.name for p in projections] if bare_cols else None

        # Bind ORDER BY *below* the projection so it can sort on columns that
        # are not in the SELECT list (SELECT name FROM t ORDER BY age).
        order = node.args.get("order")
        if order:
            keys = self._build_sort_keys(order, select_cols, table.schema)
            root = LogicalSort(children=[root], schema=root.schema, keys=keys)

        # Bind SELECT (Project).
        if select_cols is not None:
            root = LogicalProject(children=[root], schema=root.schema, column_names=select_cols)

        # Bind LIMIT / OFFSET at the very top (applied after sort + project).
        root = self._maybe_limit(node, root)

        return root

    @staticmethod
    def _reject_unsupported(node: exp.Select):
        """Fail loudly on SELECT features the engine does not implement.

        Without this, GROUP BY / HAVING / DISTINCT / aggregates were silently
        dropped and the query returned raw rows -- e.g. ``SELECT COUNT(*)``
        returned every row instead of a count. Raising keeps the engine
        honest until these are actually supported.
        """
        if node.args.get("distinct"):
            raise NotImplementedError("SELECT DISTINCT is not supported")
        if node.args.get("group"):
            raise NotImplementedError("GROUP BY is not supported")
        if node.args.get("having"):
            raise NotImplementedError("HAVING is not supported")
        for proj in node.expressions:
            if proj.find(exp.AggFunc):
                raise NotImplementedError(
                    f"Aggregate functions are not supported: {proj.sql()}")

    def _build_sort_keys(self, order: exp.Order, select_cols, schema):
        """Turn an sqlglot ORDER BY clause into (expr, desc, nulls_first) keys."""
        keys = []
        for ordered in order.expressions:  # exp.Ordered
            expr = ordered.this
            desc = bool(ordered.args.get("desc"))
            nulls_first = ordered.args.get("nulls_first")
            if nulls_first is None:
                # sqlglot fills this in per direction, but be defensive:
                # default to SQL's "NULL is the smallest value" (NULLs first
                # ascending, last descending).
                nulls_first = not desc
            # Positional ORDER BY (ORDER BY 1) -> the matching output column.
            if isinstance(expr, exp.Literal) and not expr.is_string:
                expr = self._positional_to_column(expr, select_cols, schema)
            keys.append((expr, desc, bool(nulls_first)))
        return keys

    @staticmethod
    def _positional_to_column(lit: exp.Literal, select_cols, schema):
        try:
            pos = int(lit.this)
        except (TypeError, ValueError):
            return lit  # not an integer position; leave as a constant operand
        cols = select_cols if select_cols is not None else [c.name for c in schema.columns]
        if 1 <= pos <= len(cols):
            return exp.column(cols[pos - 1])
        raise ValueError(f"ORDER BY position {pos} is out of range")

    def _maybe_limit(self, node: exp.Select, root: LogicalNode) -> LogicalNode:
        limit_node = node.args.get("limit")
        offset_node = node.args.get("offset")
        if not limit_node and not offset_node:
            return root
        limit = self._int_arg(limit_node.expression) if limit_node else None
        offset = self._int_arg(offset_node.expression) if offset_node else 0
        return LogicalLimit(children=[root], schema=root.schema, limit=limit, offset=offset)

    @staticmethod
    def _int_arg(expr) -> int:
        """Coerce a LIMIT/OFFSET operand (an integer literal) to a non-negative int."""
        from .predicates import resolve
        try:
            val = int(resolve(expr, {}))
        except (TypeError, ValueError, NotImplementedError):
            raise ValueError(f"LIMIT/OFFSET requires an integer, got {expr.sql()}")
        return max(0, val)

    def _bind_select_with_join(self, node: exp.Select, joins) -> LogicalNode:
        self._reject_unsupported(node)

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

        # ORDER BY / LIMIT / OFFSET. The join path projects nothing, so sort
        # keys resolve against the merged schema and positional refs index it.
        order = node.args.get("order")
        if order:
            keys = self._build_sort_keys(order, None, root.schema)
            root = LogicalSort(children=[root], schema=root.schema, keys=keys)
        root = self._maybe_limit(node, root)

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

        # Parse VALUES. A single statement can carry several tuples
        # (INSERT ... VALUES (..), (..), ..); bind every one of them, not
        # just the first -- silently dropping the rest was data loss.
        values_clause = node.expression
        if not values_clause or not values_clause.expressions:
            raise ValueError("INSERT without VALUES not supported")

        rows = []
        for tup in values_clause.expressions:
            vals = [self._literal_value(v) for v in tup.expressions]
            if len(vals) != len(cols):
                raise ValueError(
                    f"INSERT has {len(vals)} values for {len(cols)} columns")
            rows.append(dict(zip(cols, vals)))

        return LogicalInsert(
            children=[],
            schema=table.schema,
            table_name=table.name,
            table_id=table.id,
            rows=rows
        )

    @staticmethod
    def _literal_value(node):
        """Convert a VALUES literal node to a Python value.

        Delegates to the predicate operand resolver, the single source of
        truth for operand -> value coercion, so floats, negative numbers,
        booleans and NULL convert correctly. The old ad-hoc
        ``int(x) if x.isdigit() else x`` mangled everything but ints and
        strings: floats became strings, ``-5`` lost its sign, ``TRUE``
        became ``''`` and ``NULL`` became the string ``'NULL'``.
        """
        from .predicates import resolve
        return resolve(node, {})

    def _bind_update(self, node: exp.Update) -> LogicalNode:
        table_exp = node.find(exp.Table)
        if not table_exp:
            raise ValueError("UPDATE without table not supported")
        table_name = table_exp.name
        table = self.catalog.get_table(table_name)
        if not table: raise ValueError(f"Table {table_name} not found")
        
        # Parse SET clause. Keep the raw sqlglot node for every assignment
        # and let the executor resolve it per row: that path already handles
        # column expressions (e.g. balance + 10) and, via the shared operand
        # resolver, literals of every type. Pre-converting literals here used
        # the same broken int/str-only logic as INSERT, so `SET price = 2.5`
        # stored the string '2.5'.
        set_clause = {}
        for eq in node.expressions:
            if isinstance(eq, exp.EQ):
                set_clause[eq.left.name] = eq.right
        
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
