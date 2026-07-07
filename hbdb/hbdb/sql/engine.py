from typing import List, Dict, Any, Optional, Tuple

import sqlglot
from sqlglot import exp

from ..core.proxy import Transaction
from ..db import HBDB
from .catalog import Catalog
from .parser import SQLParser
from .executor import build_physical_plan, ExecutionContext
from .optimizer import Optimizer, StatsCollector
from .plan import (LogicalCreateTable, LogicalCreateIndex, LogicalInsert,
                   output_columns)
from .subqueries import rewrite as rewrite_subqueries

class SQLEngine:
    def __init__(self, db: HBDB):
        self.db = db
        self.catalog = Catalog(db=self.db)
        self.parser = SQLParser(self.catalog)
        self.stats_collector = StatsCollector(self.catalog)
        self.optimizer = Optimizer(self.catalog, self.stats_collector)

    def execute(self, sql: str, txn: Optional[Transaction] = None) -> List[Dict[str, Any]]:
        # 1. Parse. The engine owns the parse step (rather than handing the
        # SQL text to the parser) because uncorrelated subqueries must be
        # materialized against the live transaction *before* binding -- see
        # _execute_statement.
        ast = sqlglot.parse_one(sql)

        # WITH / CTEs are unsupported on every statement kind. The binder
        # also rejects them, but the check must run before the subquery
        # rewrite, or `WITH c AS (...) ... IN (SELECT ... FROM c)` would
        # fail with a misleading "Table c not found".
        if ast.args.get("with") or ast.args.get("with_"):
            raise NotImplementedError(
                "WITH / common table expressions are not supported")

        # 2. DDL: binds and applies through the catalog (db.set_sync), no
        # transaction involved -- unchanged behavior.
        if isinstance(ast, exp.Create):
            logical_plan = self.parser.bind(ast)
            if isinstance(logical_plan, LogicalCreateTable):
                self.create_table_from_logical(logical_plan)
                return [{"status": "Table created"}]
            if isinstance(logical_plan, LogicalCreateIndex):
                self.create_index(logical_plan.index_name,
                                  logical_plan.table_name,
                                  logical_plan.column_name)
                return [{"status": "Index created"}]
            raise ValueError(f"Unsupported CREATE statement: {sql}")

        # 3. Transaction management. On error there is no rollback step: an
        # uncommitted Transaction's buffered writes are simply discarded
        # with the object.
        local_txn = txn is None
        if local_txn:
            txn = self.db.transaction()

        results = self._execute_statement(ast, txn)

        # 4. Commit if local txn
        if local_txn:
            if not txn.commit():
                raise RuntimeError("Commit failed")

        return results

    def _execute_statement(self, ast: exp.Expression, txn: Transaction) -> List[Dict[str, Any]]:
        """Rewrite, bind, optimize and run one DML/query statement in ``txn``."""
        # INSERT ... SELECT materializes its source rows, which no
        # bind-time-only path can do; handle it before binding.
        if isinstance(ast, exp.Insert) and isinstance(
                ast.expression, (exp.Select, exp.Subquery, exp.Union)):
            return self._insert_from_select(ast, txn)

        # Materialize uncorrelated subqueries in place (each runs inside
        # this same transaction, so it sees the statement's snapshot), then
        # bind the rewritten tree through the unchanged pipeline.
        rewrite_subqueries(ast, lambda sel: self._run_select(sel, txn),
                           self.catalog)
        logical_plan = self.parser.bind(ast)
        optimized_plan = self.optimizer.optimize(logical_plan)
        ctx = ExecutionContext(txn, getattr(self.db, "read_cache", None))
        plan = build_physical_plan(ctx, optimized_plan, self.catalog)
        return list(plan.next())

    def _run_select(self, select_ast: exp.Expression, txn: Transaction
                    ) -> Tuple[List[Dict[str, Any]], Optional[List[str]]]:
        """Execute one SELECT AST inside ``txn``.

        Returns ``(rows, output_column_names)`` -- the names carry the
        SELECT's output *order*, which dict rows alone cannot guarantee.
        Re-enters the subquery rewriter first, so arbitrarily nested
        subqueries materialize innermost-first.
        """
        if not isinstance(select_ast, exp.Select):
            raise NotImplementedError(
                f"Unsupported subquery statement: {select_ast.key}")
        rewrite_subqueries(select_ast, lambda sel: self._run_select(sel, txn),
                           self.catalog)
        logical_plan = self.parser.bind(select_ast)
        optimized_plan = self.optimizer.optimize(logical_plan)
        ctx = ExecutionContext(txn, getattr(self.db, "read_cache", None))
        plan = build_physical_plan(ctx, optimized_plan, self.catalog)
        return list(plan.next()), output_columns(optimized_plan)

    def _insert_from_select(self, ast: exp.Insert, txn: Transaction) -> List[Dict[str, Any]]:
        """``INSERT INTO t [(cols)] SELECT ...``: run the SELECT (inside the
        same transaction), map its output columns onto the target columns
        *positionally* -- SQL's rule; names need not match -- and insert
        through the regular InsertExecutor (so secondary indexes and cache
        invalidation behave exactly like INSERT ... VALUES).

        The source rows are fully materialized before the first write, so
        ``INSERT INTO t SELECT ... FROM t`` reads a stable snapshot of ``t``
        rather than chasing its own inserts.
        """
        target = ast.this
        if isinstance(target, exp.Schema):
            table_name = target.this.name
            explicit_cols = [c.name for c in target.expressions]
        elif isinstance(target, exp.Table):
            table_name = target.name
            explicit_cols = None
        else:
            raise ValueError(f"Unsupported INSERT target: {ast.sql()}")
        table = self.catalog.get_table(table_name)
        if not table:
            raise ValueError(f"Table {table_name} not found")
        target_cols = (explicit_cols if explicit_cols is not None
                       else [c.name for c in table.schema.columns])

        source = ast.expression
        select_ast = source.this if isinstance(source, exp.Subquery) else source
        if not isinstance(select_ast, exp.Select):
            raise NotImplementedError(
                f"Unsupported INSERT source (only SELECT or VALUES): "
                f"{source.sql()}")
        rows, out_cols = self._run_select(select_ast, txn)
        if out_cols is None:
            raise NotImplementedError(
                f"Cannot determine the SELECT's output column order for "
                f"INSERT: {select_ast.sql()}")
        if len(out_cols) != len(target_cols):
            raise ValueError(
                f"INSERT ... SELECT has {len(out_cols)} source columns for "
                f"{len(target_cols)} target columns")

        insert_rows = [
            {col: row.get(src) for col, src in zip(target_cols, out_cols)}
            for row in rows]
        node = LogicalInsert(children=[], schema=table.schema,
                             table_name=table.name, table_id=table.id,
                             rows=insert_rows)
        ctx = ExecutionContext(txn, getattr(self.db, "read_cache", None))
        plan = build_physical_plan(ctx, node, self.catalog)
        return list(plan.next())

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
        """Create a secondary index on a table column and backfill it from
        the table's existing rows.

        The DML executors maintain index entries from the moment an index
        exists, but an index created on an already-populated table must also
        cover the rows inserted *before* it -- without this backfill, a
        subsequent index scan silently dropped every pre-existing row
        (`CREATE TABLE` -> `INSERT` x N -> `CREATE INDEX` -> `WHERE col = x`
        found nothing).
        """
        idx = self.catalog.create_index(index_name, table_name, column_name)
        table = self.catalog.get_table(table_name)
        pk_col = table.schema.get_pk_column()
        pk_name = pk_col.name if pk_col else "id"

        from .encoding import KeyEncoder
        txn = self.db.transaction()
        wrote = False
        for key, val in txn.scan(f"/t/{table.id}/_r/", f"/t/{table.id}/_r/~"):
            row = KeyEncoder.decode_row_value(val)
            pk_val = KeyEncoder.decode_row_pk(key)
            # Overlay the PK from the key, exactly like every scan path, so
            # an index on the PK column itself backfills correctly too.
            row[pk_name] = int(pk_val) if pk_val.isdigit() else pk_val
            indexed_val = row.get(column_name)
            if indexed_val is not None:
                txn.set(
                    KeyEncoder.encode_index(table.id, idx.id, indexed_val,
                                            pk_val),
                    str(pk_val))
                wrote = True
        if wrote and not txn.commit():
            raise RuntimeError("Index backfill commit failed")

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
