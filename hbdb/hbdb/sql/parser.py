import sqlglot
from sqlglot import exp
from collections import Counter
from typing import Any, Dict, List
from .catalog import Catalog
from .plan import (LogicalNode, LogicalScan, LogicalFilter, LogicalProject,
                   LogicalProjectExprs, LogicalInsert, LogicalUpdate,
                   LogicalDelete, LogicalJoin, LogicalCreateTable,
                   LogicalCreateIndex, LogicalSort, LogicalLimit,
                   LogicalAggregate, LogicalDistinct)
from .aggregates import collect_aggregates, parse_agg
from .types import Schema, Column


def _is_star_column(node) -> bool:
    """True for a *qualified* star like ``t.*`` -- sqlglot parses it as a Column
    whose ``this`` is a Star (with ``.name == '*'``), not as a bare
    ``exp.Star``. Treating it as an ordinary column projects a bogus ``'*'``
    output key (``{'*': None}``); callers expand it to the table's columns
    instead."""
    return isinstance(node, exp.Column) and isinstance(node.this, exp.Star)


class SQLParser:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def parse(self, sql: str) -> LogicalNode:
        return self.bind(sqlglot.parse_one(sql))

    def bind(self, parsed: exp.Expression) -> LogicalNode:
        """Bind an already-parsed statement AST to a logical plan.

        Split out of ``parse`` so the engine can parse once, materialize
        uncorrelated subqueries against the live transaction (see
        ``subqueries.py``), and then bind the rewritten tree.
        """
        # WITH / CTEs are not implemented on any statement kind. They used
        # to be silently dropped from the args -- an unused CTE "worked" and
        # a used one failed with a misleading "Table not found".
        if parsed.args.get("with") or parsed.args.get("with_"):
            raise NotImplementedError(
                "WITH / common table expressions are not supported")

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

    @staticmethod
    def _require_table_sources(node: exp.Select):
        """Fail loud when a FROM or JOIN source is not a plain table.

        A derived table (``FROM (SELECT ...) sub``) used to bind *silently
        wrong*: ``find(exp.Table)`` descended into the subquery and bound its
        inner table, dropping the subquery's WHERE and projection entirely --
        ``SELECT * FROM (SELECT a FROM u WHERE a > 5) sub`` returned every
        row and every column of ``u``.
        """
        frm = node.args.get("from") or node.args.get("from_")
        if frm is not None and not isinstance(frm.this, exp.Table):
            raise NotImplementedError(
                f"Derived tables / non-table FROM sources are not "
                f"supported: {frm.sql()}")
        for join in node.args.get("joins") or []:
            if not isinstance(join.this, exp.Table):
                raise NotImplementedError(
                    f"Derived tables / non-table JOIN sources are not "
                    f"supported: {join.sql()}")

    def _bind_create(self, node: exp.Create) -> LogicalNode:
        kind = node.args.get("kind")
        if kind == "TABLE":
            return self._bind_create_table(node)
        elif kind == "INDEX":
            return self._bind_create_index(node)
        else:
            raise ValueError(f"Unsupported CREATE kind: {kind}")

    def _bind_create_table(self, node: exp.Create) -> LogicalNode:
        # CREATE TABLE ... AS SELECT parses with the target as a bare Table
        # (no column-def Schema) and the SELECT under `expression`. It used
        # to fall through and silently create an *empty, zero-column* table,
        # ignoring the SELECT entirely.
        if node.args.get("expression") is not None:
            raise NotImplementedError(
                "CREATE TABLE ... AS SELECT is not supported; create the "
                "table with explicit columns, then INSERT INTO ... SELECT")

        # For 'CREATE TABLE x (id INT, name TEXT)' node.this is a Schema
        # wrapping the table and its column definitions.
        schema_node = node.this
        if not isinstance(schema_node, exp.Schema) or not schema_node.expressions:
            raise ValueError(
                f"CREATE TABLE requires a column list: {node.sql()}")
        table_name = schema_node.this.name

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
        # Every FROM/JOIN source must be a plain table -- a derived table
        # would otherwise bind to its *inner* table and silently drop the
        # subquery's own clauses.
        self._require_table_sources(node)

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

        # Bind WHERE (Filter) -- WHERE filters input rows *before* grouping.
        if node.args.get("where"):
            root = LogicalFilter(children=[root], schema=root.schema, condition=node.args.get("where"))

        # GROUP BY / aggregates produce their own output rows (one per group),
        # so that path handles its own projection, ORDER BY and LIMIT.
        if self._is_aggregate_query(node):
            return self._bind_aggregate_query(node, root)

        # Decide projection. Three shapes:
        #   * SELECT *            -> stream every column (no projection node).
        #   * plain column list   -> LogicalProject (a fast name restriction).
        #   * aliases/expressions -> LogicalProjectExprs (expression projection,
        #     the same path the JOIN code uses).
        # The last case is the fix for a silently-wrong bug: an aliased or
        # computed SELECT item (`name AS who`, `age * 2 AS d`) used to fall
        # through with no projection at all, so the engine streamed *every*
        # column and the result row had no `who`/`d` key -- exactly the
        # "silently return the wrong rows" failure the rest of the engine
        # forbids. `select_cols` is the bare-name list, `expr_specs` the
        # (out_name, expr) list; at most one is set.
        projections = node.expressions
        # A lone `*` or `t.*` streams every column; for a single table both mean
        # "all columns", so no projection node is needed.
        is_star = len(projections) == 1 and (
            isinstance(projections[0], exp.Star) or _is_star_column(projections[0]))
        # A bare column list excludes qualified stars (`t.*` is a Column but must
        # expand, not project a literal `'*'` key).
        bare_cols = (not is_star and bool(projections) and all(
            isinstance(p, (exp.Column, exp.Identifier)) and not _is_star_column(p)
            for p in projections))
        select_cols = [p.name for p in projections] if bare_cols else None
        expr_specs = (self._projection_specs(projections, table.schema)
                      if projections and not is_star and not bare_cols else None)

        order = node.args.get("order")

        if node.args.get("distinct"):
            # SELECT DISTINCT projects first, then de-duplicates; ORDER BY then
            # sorts the distinct rows (so it may only reference output columns).
            if expr_specs is not None:
                root = LogicalProjectExprs(children=[root], schema=root.schema, projections=expr_specs)
            elif select_cols is not None:
                root = LogicalProject(children=[root], schema=root.schema, column_names=select_cols)
            root = LogicalDistinct(children=[root], schema=root.schema)
            if order:
                keys = (self._build_projected_sort_keys(order, expr_specs, projected=True)
                        if expr_specs is not None
                        else self._build_sort_keys(order, select_cols, table.schema))
                root = LogicalSort(children=[root], schema=root.schema, keys=keys)
            return self._maybe_limit(node, root)

        # Bind ORDER BY *below* the projection so it can sort on columns that
        # are not in the SELECT list (SELECT name FROM t ORDER BY age).
        if order:
            keys = (self._build_projected_sort_keys(order, expr_specs, projected=False)
                    if expr_specs is not None
                    else self._build_sort_keys(order, select_cols, table.schema))
            root = LogicalSort(children=[root], schema=root.schema, keys=keys)

        # Bind SELECT (Project).
        if expr_specs is not None:
            root = LogicalProjectExprs(children=[root], schema=root.schema, projections=expr_specs)
        elif select_cols is not None:
            root = LogicalProject(children=[root], schema=root.schema, column_names=select_cols)

        # Bind LIMIT / OFFSET at the very top (applied after sort + project).
        root = self._maybe_limit(node, root)

        return root

    @staticmethod
    def _reject_unsupported(node: exp.Select):
        """Fail loudly on SELECT features the engine still does not implement.

        GROUP BY / HAVING / aggregate functions and DISTINCT are now honored
        (see ``_bind_aggregate_query`` and ``LogicalDistinct``); what remains
        unimplemented is window/analytic functions. Raising keeps the engine
        honest -- an unhandled clause must never be silently dropped and let a
        query return the wrong rows.
        """
        for proj in node.expressions:
            if proj.find(exp.Window):
                raise NotImplementedError(
                    f"Window functions are not supported: {proj.sql()}")

    @staticmethod
    def _is_aggregate_query(node: exp.Select) -> bool:
        """True if this SELECT aggregates: it has GROUP BY/HAVING, or any
        aggregate function in its projection list."""
        if node.args.get("group") or node.args.get("having"):
            return True
        return any(p.find(exp.AggFunc) for p in node.expressions)

    def _bind_aggregate_query(self, node: exp.Select, root: LogicalNode) -> LogicalNode:
        """Build the Aggregate (+ Sort + Limit) plan for a GROUP BY / aggregate
        SELECT. ``root`` is the input pipeline (Scan, optionally with a WHERE
        Filter, or a Join tree)."""
        group = node.args.get("group")
        group_exprs = self._group_exprs(group, node) if group else []
        group_col_names = {g.name for g in group_exprs if isinstance(g, exp.Column)}
        group_sqls = {g.sql() for g in group_exprs}

        having = node.args.get("having")
        having_cond = having.this if having else None
        # A bare column in HAVING must also be a GROUP BY key, exactly like a
        # SELECT item -- otherwise its value is undefined across the group and
        # we would silently filter on an arbitrary row.
        if having_cond is not None:
            self._validate_agg_projection(having_cond, group_col_names, group_sqls)

        # Collect aggregates from the SELECT list *and* HAVING, so HAVING can
        # reference an aggregate that is not in the SELECT list
        # (GROUP BY k HAVING SUM(x) > 10 while selecting only k, COUNT(*)).
        agg_sources = list(node.expressions)
        if having_cond is not None:
            agg_sources.append(having_cond)
        specs = [parse_agg(a) for a in collect_aggregates(agg_sources)]

        output = self._aggregate_output(node.expressions, group_col_names, group_sqls)

        result = LogicalAggregate(
            children=[root], schema=root.schema,
            group_keys=group_exprs, aggregates=specs,
            output=output, having=having_cond)

        # ORDER BY / LIMIT apply to the aggregated output rows.
        order = node.args.get("order")
        if order:
            keys = self._build_sort_keys_for_aggregate(order, output)
            result = LogicalSort(children=[result], schema=root.schema, keys=keys)
        return self._maybe_limit(node, result)

    def _group_exprs(self, group: exp.Group, node: exp.Select):
        """Resolve GROUP BY items to operand expressions, mapping positional
        ``GROUP BY 1`` to the matching SELECT item."""
        exprs = []
        for g in group.expressions:
            if isinstance(g, exp.Literal) and not g.is_string:
                g = self._positional_select_expr(g, node)
            exprs.append(g)
        return exprs

    @staticmethod
    def _positional_select_expr(lit: exp.Literal, node: exp.Select):
        pos = int(lit.this)
        projs = node.expressions
        if 1 <= pos <= len(projs):
            p = projs[pos - 1]
            return p.this if isinstance(p, exp.Alias) else p
        raise ValueError(f"GROUP BY position {pos} is out of range")

    def _aggregate_output(self, projections, group_col_names, group_sqls):
        """Map each SELECT item to an ``(source_expr, out_name)`` pair and
        validate it against the GROUP BY keys.

        Every non-aggregated column must be a grouping key, otherwise its value
        is undefined across the group -- we reject that rather than pick an
        arbitrary row (fail-loud, like the WHERE evaluator)."""
        output = []
        for p in projections:
            if isinstance(p, exp.Star):
                raise NotImplementedError(
                    "SELECT * with GROUP BY / aggregates is not supported; "
                    "list the grouped columns explicitly")
            expr, name = self._output_name(p)
            self._validate_agg_projection(expr, group_col_names, group_sqls)
            output.append((expr, name))
        return output

    @staticmethod
    def _output_name(p):
        """Output column name for a SELECT item: its alias, else the column
        name for a bare column, else the expression's SQL text (e.g. the
        unaliased ``COUNT(*)`` becomes the column ``"COUNT(*)"``)."""
        if isinstance(p, exp.Alias):
            return p.this, p.alias
        if isinstance(p, exp.Column):
            return p, p.name
        return p, p.sql()

    @staticmethod
    def _validate_agg_projection(expr, group_col_names, group_sqls):
        if expr.sql() in group_sqls:
            return  # the whole projection is itself a grouping key
        for col in expr.find_all(exp.Column):
            if col.find_ancestor(exp.AggFunc) is not None:
                continue  # inside an aggregate -- fine
            if col.name in group_col_names or col.sql() in group_sqls:
                continue  # references a GROUP BY key -- fine
            raise ValueError(
                f"Column '{col.sql()}' must appear in GROUP BY or be used in "
                f"an aggregate function")

    def _build_sort_keys_for_aggregate(self, order: exp.Order, output):
        """ORDER BY for an aggregate query: keys are rewritten to reference the
        aggregated output columns (by position, by matching SELECT expression,
        or by output name). An ORDER BY aggregate that is not in the SELECT
        list has nothing to sort against, so it fails loudly."""
        name_by_sql = {}
        for src, name in output:
            name_by_sql.setdefault(src.sql(), name)
        names = [name for _, name in output]
        output_names = set(names)

        keys = []
        for ordered in order.expressions:
            expr = ordered.this
            desc = bool(ordered.args.get("desc"))
            nulls_first = ordered.args.get("nulls_first")
            if nulls_first is None:
                nulls_first = not desc
            if isinstance(expr, exp.Literal) and not expr.is_string:
                pos = int(expr.this)
                if not (1 <= pos <= len(names)):
                    raise ValueError(f"ORDER BY position {pos} is out of range")
                expr = exp.column(names[pos - 1])
            elif expr.sql() in name_by_sql:
                # Matches a SELECT expression (e.g. ORDER BY COUNT(*) or a group
                # column) -> sort by its output column.
                expr = exp.column(name_by_sql[expr.sql()])
            elif isinstance(expr, exp.Column) and expr.name in output_names:
                # Already an output column name (typically an alias) -- keep it;
                # it resolves against the aggregated rows.
                pass
            else:
                # Anything else (a bare aggregate not in SELECT, or a column
                # that is neither grouped nor projected) has nothing to sort
                # against once rows are aggregated. Fail loud rather than
                # silently ignore the ORDER BY.
                raise NotImplementedError(
                    f"ORDER BY {expr.sql()} must reference a SELECT output "
                    f"column (by name, position, or the same expression)")
            keys.append((expr, desc, bool(nulls_first)))
        return keys

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
        """Bind a JOIN query.

        Each base table is scanned with a ``qualify`` tag so its rows carry
        ``table.col`` keys (``QualifyExecutor``); the join then merges rows
        without same-named columns clobbering one another. The full ON
        predicate is kept and evaluated per row, so it works regardless of
        which side of ``=`` a column sits on and supports non-equi/compound
        conditions. Outer joins (LEFT/RIGHT/FULL) carry pre-computed NULL-pad
        dicts. Anything genuinely ambiguous (an unqualified reference to a
        name present in two tables) fails loud rather than silently choosing a
        side -- the same contract the rest of the engine follows.
        """
        self._reject_unsupported(node)

        # find_all(exp.Table) yields the FROM table first, then each join's
        # right table in order (confirmed against sqlglot's parse tree).
        all_table_exps = list(node.find_all(exp.Table))
        if not all_table_exps:
            raise ValueError("JOIN without tables not supported")

        left_exp = all_table_exps[0]
        left_meta = self.catalog.get_table(left_exp.name)
        if not left_meta:
            raise ValueError(f"Table {left_exp.name} not found")
        left_qualify = left_exp.alias_or_name
        left_cols = [c.name for c in left_meta.schema.columns]

        root = LogicalScan(children=[], schema=left_meta.schema,
                           table_name=left_meta.name, table_id=left_meta.id,
                           qualify=left_qualify)

        # Ordered (qualify, [bare cols]) per table -- drives ambiguity
        # detection, SELECT * expansion and NULL-pad construction.
        table_specs = [(left_qualify, left_cols)]
        # Accumulates everything on the left of the *next* join.
        left_attr = [(left_qualify, left_cols)]

        for join in joins:
            right_exp = join.find(exp.Table)
            right_meta = self.catalog.get_table(right_exp.name)
            if not right_meta:
                raise ValueError(f"Table {right_exp.name} not found")
            right_qualify = right_exp.alias_or_name
            right_cols = [c.name for c in right_meta.schema.columns]
            right_scan = LogicalScan(children=[], schema=right_meta.schema,
                                     table_name=right_meta.name, table_id=right_meta.id,
                                     qualify=right_qualify)
            table_specs.append((right_qualify, right_cols))

            on = join.args.get("on")
            join_type = self._join_type(join)
            left_bare = [c for _, cols in left_attr for c in cols]
            collision = set(left_bare) & set(right_cols)

            merged_schema = Schema(columns=root.schema.columns + right_meta.schema.columns)
            root = LogicalJoin(
                children=[], schema=merged_schema, left=root, right=right_scan,
                join_type=join_type, condition=on,
                hash_keys=self._classify_hash_keys(
                    on, join_type, {q for q, _ in left_attr}, set(left_bare),
                    right_qualify, set(right_cols)),
                left_pad=self._pad_dict(left_attr, collision),
                right_pad=self._pad_dict([(right_qualify, right_cols)], collision))
            left_attr = left_attr + [(right_qualify, right_cols)]

        # An unqualified reference to a name present in 2+ tables is ambiguous.
        name_counts = Counter(c for _, cols in table_specs for c in cols)
        ambiguous = {n for n, k in name_counts.items() if k > 1}
        self._reject_ambiguous(node, ambiguous)

        # WHERE filters merged rows before any grouping.
        if node.args.get("where"):
            root = LogicalFilter(children=[root], schema=root.schema,
                                 condition=node.args.get("where"))

        # GROUP BY / aggregates own their projection, ORDER BY and LIMIT.
        if self._is_aggregate_query(node):
            return self._bind_aggregate_query(node, root)

        # Projection specs: (out_name, expr). SELECT * expands to one entry per
        # joined column, qualified only where the bare name collides.
        specs = self._join_projection_specs(node, table_specs, ambiguous)
        order = node.args.get("order")

        if node.args.get("distinct"):
            # Project, de-duplicate, then ORDER BY over the output columns.
            root = LogicalProjectExprs(children=[root], schema=root.schema, projections=specs)
            root = LogicalDistinct(children=[root], schema=root.schema)
            if order:
                keys = self._build_projected_sort_keys(order, specs, projected=True)
                root = LogicalSort(children=[root], schema=root.schema, keys=keys)
            return self._maybe_limit(node, root)

        # ORDER BY sits below projection so it can sort on any joined column.
        if order:
            keys = self._build_projected_sort_keys(order, specs, projected=False)
            root = LogicalSort(children=[root], schema=root.schema, keys=keys)
        root = LogicalProjectExprs(children=[root], schema=root.schema, projections=specs)
        return self._maybe_limit(node, root)

    @staticmethod
    def _join_type(join: exp.Join) -> str:
        """INNER / LEFT / RIGHT / FULL. CROSS and comma joins have side=None
        and a NULL ON clause, which the executor treats as a cross product."""
        side = join.args.get("side")
        return str(side).upper() if side else "INNER"

    @staticmethod
    def _pad_dict(attr, collision):
        """NULL-pad keys for one side of an outer join: every ``table.col`` (and
        the bare ``col`` for names that don't collide with the other side, so
        padding never overwrites a real bare value the present row holds)."""
        pad = {}
        for qualify, cols in attr:
            for c in cols:
                pad[f"{qualify}.{c}"] = None
                if c not in collision:
                    pad[c] = None
        return pad

    @staticmethod
    def _classify_hash_keys(on, join_type, left_tables, left_bare, right_qualify, right_cols):
        """For an INNER equi-join ``a = b`` of two columns, return
        ``(left_key_expr, right_key_expr)`` so the executor can hash-join;
        None otherwise (a nested loop then evaluates the full ON predicate)."""
        if join_type != "INNER" or not isinstance(on, exp.EQ):
            return None
        l, r = on.left, on.right
        if not (isinstance(l, exp.Column) and isinstance(r, exp.Column)):
            return None

        def side(col):
            if col.table == right_qualify or (
                    not col.table and col.name in right_cols and col.name not in left_bare):
                return "R"
            if col.table in left_tables or (
                    not col.table and col.name in left_bare and col.name not in right_cols):
                return "L"
            return None

        sl, sr = side(l), side(r)
        if sl == "L" and sr == "R":
            return (l, r)
        if sl == "R" and sr == "L":
            return (r, l)
        return None

    @staticmethod
    def _reject_ambiguous(node: exp.Select, ambiguous):
        """Fail loud on an unqualified reference to a column present in more
        than one joined table, instead of silently resolving to one side."""
        if not ambiguous:
            return
        for col in node.find_all(exp.Column):
            if not col.table and col.name in ambiguous:
                raise ValueError(
                    f"Column '{col.name}' is ambiguous across the joined "
                    f"tables; qualify it (e.g. table.{col.name})")

    @staticmethod
    def _projection_specs(projections, schema):
        """Map a single-table SELECT list to ordered ``(out_name, expr)`` specs
        for expression projection.

        Used when the list is neither a lone ``*`` nor a plain column list --
        i.e. it has an alias or a computed expression (``name AS who``,
        ``age * 2 AS d``). Each spec's value is resolved per row through the
        shared operand resolver (``predicates.resolve``), the same machinery the
        JOIN path uses, so genuinely unsupported operands (``UPPER(x)``) still
        fail loud instead of silently streaming the raw row. ``SELECT *`` mixed
        with other items (``SELECT *, age * 2``) expands the star to every
        column. Output names must be unique -- two items landing on the same key
        fail loud, since a result row is a dict and cannot hold both."""
        specs = []
        seen = set()

        def add(out_name, expr):
            if out_name in seen:
                raise ValueError(
                    f"Duplicate output column '{out_name}'; alias one of them "
                    f"(e.g. {out_name} AS {out_name}2)")
            seen.add(out_name)
            specs.append((out_name, expr))

        for p in projections:
            if isinstance(p, exp.Star) or _is_star_column(p):
                # `*` and (for a single table) `t.*` both expand to all columns.
                for col in schema.columns:
                    add(col.name, exp.column(col.name))
            elif isinstance(p, exp.Alias):
                add(p.alias, p.this)
            elif isinstance(p, (exp.Column, exp.Identifier)):
                add(p.name, p)
            else:
                add(p.sql(), p)
        return specs

    def _join_projection_specs(self, node: exp.Select, table_specs, ambiguous):
        """Map the SELECT list to ordered ``(out_name, expr)`` projection specs.

        ``SELECT *`` expands to every joined column (bare name, or ``table.col``
        when the name collides); ``SELECT t.*`` expands to just table ``t``'s
        columns. Output names must be unique -- two columns that would land on
        the same key (``SELECT a.id, b.id``) fail loud, since a result row is a
        dict and cannot hold both."""
        specs = []
        seen = set()

        def add(out_name, expr):
            if out_name in seen:
                raise ValueError(
                    f"Duplicate output column '{out_name}'; alias one of them "
                    f"(e.g. {out_name} AS {out_name}2)")
            seen.add(out_name)
            specs.append((out_name, expr))

        def emit(qualify, cols):
            for c in cols:
                out_name = c if c not in ambiguous else f"{qualify}.{c}"
                add(out_name, exp.column(c, table=qualify))

        for p in node.expressions:
            if isinstance(p, exp.Star):
                for qualify, cols in table_specs:
                    emit(qualify, cols)
            elif _is_star_column(p):
                # t.* -> only table t's columns (p.table is its name/alias).
                matched = [(q, cols) for q, cols in table_specs if q == p.table]
                if not matched:
                    raise ValueError(f"Unknown table '{p.table}' in {p.sql()}")
                for qualify, cols in matched:
                    emit(qualify, cols)
            elif isinstance(p, exp.Alias):
                add(p.alias, p.this)
            elif isinstance(p, exp.Column):
                add(p.name, p)
            else:
                add(p.sql(), p)
        return specs

    def _build_projected_sort_keys(self, order: exp.Order, specs, projected: bool):
        """ORDER BY keys for any expression-projection path (JOINs and the
        single-table alias/expression case).

        Positional ``ORDER BY n`` -> the n-th SELECT item. When ``projected``
        (the DISTINCT case, where rows hold only output columns) keys reference
        the output column name; otherwise they reference the item's source
        expression, resolved against the full (pre-projection) row -- so a
        non-DISTINCT ORDER BY may also sort on columns that are not in the
        SELECT list."""
        out_names = [name for name, _ in specs]
        out_name_set = set(out_names)
        src_by_name = {name: expr for name, expr in specs}
        name_by_src_sql = {}
        for name, expr in specs:
            name_by_src_sql.setdefault(expr.sql(), name)
        keys = []
        for ordered in order.expressions:
            expr = ordered.this
            desc = bool(ordered.args.get("desc"))
            nulls_first = ordered.args.get("nulls_first")
            if nulls_first is None:
                nulls_first = not desc
            if isinstance(expr, exp.Literal) and not expr.is_string:
                pos = int(expr.this)
                if not (1 <= pos <= len(out_names)):
                    raise ValueError(f"ORDER BY position {pos} is out of range")
                name = out_names[pos - 1]
                expr = exp.column(name) if projected else src_by_name[name]
            elif projected:
                # SELECT DISTINCT: the rows now hold only the output columns, so
                # ORDER BY must reference one of them (by name/alias, or by the
                # same expression a SELECT item used). Anything else has nothing
                # to sort against post-projection -- fail loud rather than
                # silently sort by NULL (matching the aggregate ORDER BY rule
                # and SQL's "ORDER BY must appear in the select list for
                # DISTINCT").
                if expr.sql() in name_by_src_sql:
                    expr = exp.column(name_by_src_sql[expr.sql()])
                elif isinstance(expr, exp.Column) and not expr.table and expr.name in out_name_set:
                    pass  # already an output column name / alias
                else:
                    raise NotImplementedError(
                        f"ORDER BY {expr.sql()} must reference a SELECT output "
                        f"column for SELECT DISTINCT")
            elif (isinstance(expr, exp.Column) and not expr.table
                  and expr.name in src_by_name):
                # Non-DISTINCT: ORDER BY sits below the projection, so an output
                # alias is not a real column -- sort by its source expression
                # (e.g. `... u.name AS who ... ORDER BY who`). A bare name that
                # is not an output column is left as-is to resolve against the
                # merged row, so ORDER BY can still use non-selected columns.
                expr = src_by_name[expr.name]
            keys.append((expr, desc, bool(nulls_first)))
        return keys

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
        if isinstance(values_clause, (exp.Select, exp.Subquery, exp.Union)):
            # INSERT ... SELECT needs the source rows materialized, which
            # takes a live transaction; the engine handles it before binding
            # (SQLEngine._insert_from_select). Reaching here means a direct
            # parse() call -- fail with the real reason, not a bogus
            # "0 values for N columns".
            raise NotImplementedError(
                "INSERT INTO ... SELECT is executed by the engine and "
                "cannot be bound standalone")
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
