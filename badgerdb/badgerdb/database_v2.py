"""
BadgerDB v2 - Sophisticated Distributed SQL Database

Combines:
- Parallel sequencers (BOHM-style) with Raft HA
- Aria-style speculative execution
- Detock-style fast path
- Aurora-style disaggregated storage
"""

from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional

from .config import Config
from .types import Timestamp, TxnId, Transaction, Operation, ReadWriteSet
from .coordinator import Coordinator
from .sql.parser import SQLParser
from .sql.schema import Schema, Table, Column, DataType
from .sql.executor import QueryResult
from .execution.aria import ExecutionResult, SpeculativeContext


class BadgerDBV2:
    """
    BadgerDB v2 - Sophisticated distributed SQL database.

    Improvements over v1:
    1. No SPOF - parallel sequencers with Raft
    2. Higher throughput - Aria speculative execution
    3. Lower latency - fast path for non-conflicting txns
    4. Better scalability - disaggregated storage

    Usage:
        db = BadgerDBV2()
        db.start()

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        result = db.execute("SELECT * FROM users WHERE id = 1")

        db.stop()
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()

        # Schema management
        self.schema = Schema()

        # SQL parser
        self.parser = SQLParser()

        # Coordinator (handles everything)
        self.coordinator = Coordinator(
            num_sequencer_partitions=self.config.num_shards,
            num_storage_servers=self.config.num_shards,
            num_compute_nodes=4,
            epoch_duration_ms=10,
            enable_fast_path=True
        )

        self._running = False
        self._lock = threading.RLock()

    def start(self):
        """Start the database."""
        self.coordinator.start()
        self._running = True

    def stop(self):
        """Stop the database."""
        self._running = False
        self.coordinator.stop()

    def execute(self, sql: str) -> QueryResult:
        """
        Execute a SQL statement.

        Supports:
        - CREATE TABLE
        - DROP TABLE
        - INSERT
        - SELECT
        - UPDATE
        - DELETE
        """
        if not self._running:
            return QueryResult(success=False, error="Database not started")

        try:
            # Parse SQL
            stmt = self.parser.parse(sql)

            # Handle DDL separately (not transactional)
            from .sql.parser import CreateTableStmt, DropTableStmt
            if isinstance(stmt, CreateTableStmt):
                return self._execute_create_table(stmt)
            elif isinstance(stmt, DropTableStmt):
                return self._execute_drop_table(stmt)

            # DML goes through coordinator
            return self._execute_dml(sql, stmt)

        except Exception as e:
            return QueryResult(success=False, error=str(e))

    def _execute_create_table(self, stmt) -> QueryResult:
        """Execute CREATE TABLE (DDL)."""
        columns = []
        for col_name, col_type, is_pk in stmt.columns:
            dtype = DataType.TEXT
            if col_type == 'INTEGER':
                dtype = DataType.INTEGER
            elif col_type == 'REAL':
                dtype = DataType.REAL
            elif col_type == 'BOOLEAN':
                dtype = DataType.BOOLEAN

            columns.append(Column(
                name=col_name,
                data_type=dtype,
                primary_key=is_pk
            ))

        try:
            self.schema.create_table(stmt.table, columns)
            return QueryResult(success=True)
        except ValueError as e:
            return QueryResult(success=False, error=str(e))

    def _execute_drop_table(self, stmt) -> QueryResult:
        """Execute DROP TABLE (DDL)."""
        try:
            self.schema.drop_table(stmt.table)
            return QueryResult(success=True)
        except ValueError as e:
            return QueryResult(success=False, error=str(e))

    def _execute_dml(self, sql: str, stmt) -> QueryResult:
        """Execute DML (INSERT, SELECT, UPDATE, DELETE)."""
        from .sql.parser import SelectStmt, InsertStmt, UpdateStmt, DeleteStmt

        # Build transaction
        txn = Transaction(txn_id=TxnId.generate())

        # Analyze read/write set
        table = self.schema.get_table(stmt.table)
        if not table:
            return QueryResult(success=False, error=f"Table {stmt.table} does not exist")

        if isinstance(stmt, SelectStmt):
            return self._execute_select(stmt, table)

        elif isinstance(stmt, InsertStmt):
            return self._execute_insert(stmt, table, txn)

        elif isinstance(stmt, UpdateStmt):
            return self._execute_update(stmt, table, txn)

        elif isinstance(stmt, DeleteStmt):
            return self._execute_delete(stmt, table, txn)

        return QueryResult(success=False, error="Unknown statement type")

    def _execute_select(self, stmt, table: Table) -> QueryResult:
        """Execute SELECT."""
        timestamp = Timestamp.now()
        rows = []

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point lookup
            key = f"{stmt.table}:{stmt.where.value}"
            value = self.coordinator._compute.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                row = self._filter_columns(value, stmt.columns, table)
                rows.append(row)
        else:
            # Scan
            results = self.coordinator._compute.scan(f"{stmt.table}:", timestamp, limit=10000)
            for key, value in results:
                if stmt.where is None or self._matches_where(value, stmt.where):
                    row = self._filter_columns(value, stmt.columns, table)
                    rows.append(row)

        return QueryResult(success=True, rows=rows)

    def _execute_insert(self, stmt, table: Table, txn: Transaction) -> QueryResult:
        """Execute INSERT."""
        if not table.primary_key:
            return QueryResult(success=False, error=f"Table {stmt.table} has no primary key")

        # Build row
        if stmt.columns:
            row = dict(zip(stmt.columns, stmt.values))
        else:
            col_names = list(table.columns.keys())
            row = dict(zip(col_names, stmt.values))

        pk_value = row.get(table.primary_key)
        if pk_value is None:
            return QueryResult(success=False, error="Primary key value required")

        key = f"{stmt.table}:{pk_value}"

        # Check if exists
        timestamp = Timestamp.now()
        existing = self.coordinator._compute.read(key, timestamp)
        if existing:
            return QueryResult(success=False, error=f"Row with key {pk_value} already exists")

        # Add operation to transaction
        txn.rw_set.add_write(key)
        txn.add_operation(Operation(
            op_type='write',
            table=stmt.table,
            key=str(pk_value),
            value=row
        ))

        # Execute
        result = self.coordinator.execute(txn)

        if result.success:
            return QueryResult(success=True, affected_rows=1)
        else:
            return QueryResult(success=False, error=result.error)

    def _execute_update(self, stmt, table: Table, txn: Transaction) -> QueryResult:
        """Execute UPDATE."""
        timestamp = Timestamp.now()
        affected = 0

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point update
            key = f"{stmt.table}:{stmt.where.value}"
            value = self.coordinator._compute.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                new_value = {**value, **stmt.assignments}
                txn.rw_set.add_read(key)
                txn.rw_set.add_write(key)
                txn.add_operation(Operation(
                    op_type='write',
                    table=stmt.table,
                    key=str(stmt.where.value),
                    value=new_value
                ))
                affected = 1
        else:
            # Scan and update
            results = self.coordinator._compute.scan(f"{stmt.table}:", timestamp, limit=10000)
            for key, value in results:
                if stmt.where is None or self._matches_where(value, stmt.where):
                    new_value = {**value, **stmt.assignments}
                    txn.rw_set.add_read(key)
                    txn.rw_set.add_write(key)
                    # Extract pk from key
                    pk = key.split(":", 1)[1]
                    txn.add_operation(Operation(
                        op_type='write',
                        table=stmt.table,
                        key=pk,
                        value=new_value
                    ))
                    affected += 1

        if affected == 0:
            return QueryResult(success=True, affected_rows=0)

        result = self.coordinator.execute(txn)
        if result.success:
            return QueryResult(success=True, affected_rows=affected)
        else:
            return QueryResult(success=False, error=result.error)

    def _execute_delete(self, stmt, table: Table, txn: Transaction) -> QueryResult:
        """Execute DELETE."""
        timestamp = Timestamp.now()
        affected = 0

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point delete
            key = f"{stmt.table}:{stmt.where.value}"
            value = self.coordinator._compute.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                txn.rw_set.add_write(key)
                txn.add_operation(Operation(
                    op_type='delete',
                    table=stmt.table,
                    key=str(stmt.where.value)
                ))
                affected = 1
        else:
            # Scan and delete
            results = self.coordinator._compute.scan(f"{stmt.table}:", timestamp, limit=10000)
            for key, value in results:
                if stmt.where is None or self._matches_where(value, stmt.where):
                    txn.rw_set.add_write(key)
                    pk = key.split(":", 1)[1]
                    txn.add_operation(Operation(
                        op_type='delete',
                        table=stmt.table,
                        key=pk
                    ))
                    affected += 1

        if affected == 0:
            return QueryResult(success=True, affected_rows=0)

        result = self.coordinator.execute(txn)
        if result.success:
            return QueryResult(success=True, affected_rows=affected)
        else:
            return QueryResult(success=False, error=result.error)

    def _matches_where(self, row: Dict[str, Any], where) -> bool:
        """Check if row matches WHERE clause."""
        if where is None:
            return True

        value = row.get(where.column)
        target = where.value

        if where.operator == '=':
            return value == target
        elif where.operator == '!=':
            return value != target
        elif where.operator == '<':
            return value < target
        elif where.operator == '<=':
            return value <= target
        elif where.operator == '>':
            return value > target
        elif where.operator == '>=':
            return value >= target

        return False

    def _filter_columns(
        self,
        row: Dict[str, Any],
        columns: List[str],
        table: Table
    ) -> Dict[str, Any]:
        """Filter row to requested columns."""
        if columns == ['*']:
            return row
        return {col: row.get(col) for col in columns if col in row}

    # Convenience methods

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute query and return rows (raises on error)."""
        result = self.execute(sql)
        if result.success:
            return result.rows
        raise Exception(result.error)

    def insert(self, table: str, data: Dict[str, Any]) -> bool:
        """Insert a row."""
        columns = ', '.join(data.keys())
        values = ', '.join(
            f"'{v}'" if isinstance(v, str) else str(v)
            for v in data.values()
        )
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        result = self.execute(sql)
        return result.success

    def select(
        self,
        table: str,
        columns: List[str] = None,
        where: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """Select rows."""
        cols = ', '.join(columns) if columns else '*'
        sql = f"SELECT {cols} FROM {table}"

        if where:
            conditions = ' AND '.join(
                f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                for k, v in where.items()
            )
            sql += f" WHERE {conditions}"

        result = self.execute(sql)
        return result.rows if result.success else []

    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any]
    ) -> int:
        """Update rows."""
        assignments = ', '.join(
            f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
            for k, v in data.items()
        )
        conditions = ' AND '.join(
            f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
            for k, v in where.items()
        )
        sql = f"UPDATE {table} SET {assignments} WHERE {conditions}"
        result = self.execute(sql)
        return result.affected_rows if result.success else 0

    def delete(self, table: str, where: Dict[str, Any]) -> int:
        """Delete rows."""
        conditions = ' AND '.join(
            f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
            for k, v in where.items()
        )
        sql = f"DELETE FROM {table} WHERE {conditions}"
        result = self.execute(sql)
        return result.affected_rows if result.success else 0

    def get_stats(self) -> dict:
        """Get database statistics."""
        return {
            "coordinator": self.coordinator.get_stats(),
            "schema": {
                "tables": list(self.schema._tables.keys())
            }
        }

    # Context manager

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
