"""
SQL Executor

Executes parsed SQL statements against storage.
"""

from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ...types import Key, Value, Timestamp, Operation, Transaction, TxnId
from ...storage.shard import ShardManager
from ..schema import Schema, Table, Column, DataType
from .parser import (
    Statement, SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt, WhereClause
)


@dataclass
class QueryResult:
    """Result of a query execution."""
    success: bool
    rows: List[Dict[str, Any]] = None
    affected_rows: int = 0
    error: Optional[str] = None

    def __post_init__(self):
        if self.rows is None:
            self.rows = []


class Executor:
    """
    Executes SQL statements.

    Translates SQL operations to key-value operations on storage.
    """

    def __init__(self, storage: ShardManager, schema: Schema):
        self.storage = storage
        self.schema = schema

    def execute(
        self,
        stmt: Statement,
        timestamp: Timestamp,
        txn_id: Optional[TxnId] = None
    ) -> QueryResult:
        """Execute a SQL statement."""
        try:
            if isinstance(stmt, SelectStmt):
                return self._execute_select(stmt, timestamp)
            elif isinstance(stmt, InsertStmt):
                return self._execute_insert(stmt, timestamp, txn_id)
            elif isinstance(stmt, UpdateStmt):
                return self._execute_update(stmt, timestamp, txn_id)
            elif isinstance(stmt, DeleteStmt):
                return self._execute_delete(stmt, timestamp, txn_id)
            elif isinstance(stmt, CreateTableStmt):
                return self._execute_create_table(stmt)
            elif isinstance(stmt, DropTableStmt):
                return self._execute_drop_table(stmt)
            else:
                return QueryResult(success=False, error=f"Unknown statement type: {type(stmt)}")
        except Exception as e:
            return QueryResult(success=False, error=str(e))

    def _make_key(self, table: str, pk_value: Any) -> Key:
        """Create a storage key from table and primary key."""
        return f"{table}:{pk_value}"

    def _execute_select(self, stmt: SelectStmt, timestamp: Timestamp) -> QueryResult:
        """Execute SELECT statement."""
        table = self.schema.get_table(stmt.table)
        if not table:
            return QueryResult(success=False, error=f"Table {stmt.table} does not exist")

        rows = []

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point lookup by primary key
            key = self._make_key(stmt.table, stmt.where.value)
            value = self.storage.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                row = self._filter_columns(value, stmt.columns, table)
                rows.append(row)
        else:
            # Full table scan (inefficient but works)
            # In production, would use indexes
            for shard in self.storage.get_all_shards():
                # Scan all keys in shard that match table prefix
                results = shard.scan(
                    f"{stmt.table}:",
                    f"{stmt.table}:\xff",
                    timestamp,
                    limit=10000
                )
                for key, value in results:
                    if stmt.where is None or self._matches_where(value, stmt.where):
                        row = self._filter_columns(value, stmt.columns, table)
                        rows.append(row)

        return QueryResult(success=True, rows=rows)

    def _execute_insert(
        self,
        stmt: InsertStmt,
        timestamp: Timestamp,
        txn_id: Optional[TxnId]
    ) -> QueryResult:
        """Execute INSERT statement."""
        table = self.schema.get_table(stmt.table)
        if not table:
            return QueryResult(success=False, error=f"Table {stmt.table} does not exist")

        if not table.primary_key:
            return QueryResult(success=False, error=f"Table {stmt.table} has no primary key")

        # Build row data
        if stmt.columns:
            row = dict(zip(stmt.columns, stmt.values))
        else:
            # Assume values match column order
            col_names = list(table.columns.keys())
            row = dict(zip(col_names, stmt.values))

        # Get primary key value
        pk_value = row.get(table.primary_key)
        if pk_value is None:
            return QueryResult(success=False, error="Primary key value required")

        key = self._make_key(stmt.table, pk_value)

        # Check if exists
        existing = self.storage.read(key, timestamp)
        if existing:
            return QueryResult(success=False, error=f"Row with key {pk_value} already exists")

        # Write
        self.storage.write(key, row, timestamp, txn_id)

        return QueryResult(success=True, affected_rows=1)

    def _execute_update(
        self,
        stmt: UpdateStmt,
        timestamp: Timestamp,
        txn_id: Optional[TxnId]
    ) -> QueryResult:
        """Execute UPDATE statement."""
        table = self.schema.get_table(stmt.table)
        if not table:
            return QueryResult(success=False, error=f"Table {stmt.table} does not exist")

        affected = 0

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point update by primary key
            key = self._make_key(stmt.table, stmt.where.value)
            value = self.storage.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                # Apply updates
                new_value = {**value, **stmt.assignments}
                self.storage.write(key, new_value, timestamp, txn_id)
                affected = 1
        else:
            # Scan and update (expensive)
            for shard in self.storage.get_all_shards():
                results = shard.scan(
                    f"{stmt.table}:",
                    f"{stmt.table}:\xff",
                    timestamp,
                    limit=10000
                )
                for key, value in results:
                    if stmt.where is None or self._matches_where(value, stmt.where):
                        new_value = {**value, **stmt.assignments}
                        self.storage.write(key, new_value, timestamp, txn_id)
                        affected += 1

        return QueryResult(success=True, affected_rows=affected)

    def _execute_delete(
        self,
        stmt: DeleteStmt,
        timestamp: Timestamp,
        txn_id: Optional[TxnId]
    ) -> QueryResult:
        """Execute DELETE statement."""
        table = self.schema.get_table(stmt.table)
        if not table:
            return QueryResult(success=False, error=f"Table {stmt.table} does not exist")

        affected = 0

        if stmt.where and table.primary_key and stmt.where.column == table.primary_key:
            # Point delete by primary key
            key = self._make_key(stmt.table, stmt.where.value)
            value = self.storage.read(key, timestamp)

            if value and self._matches_where(value, stmt.where):
                self.storage.delete(key, timestamp, txn_id)
                affected = 1
        else:
            # Scan and delete (expensive)
            keys_to_delete = []
            for shard in self.storage.get_all_shards():
                results = shard.scan(
                    f"{stmt.table}:",
                    f"{stmt.table}:\xff",
                    timestamp,
                    limit=10000
                )
                for key, value in results:
                    if stmt.where is None or self._matches_where(value, stmt.where):
                        keys_to_delete.append(key)

            for key in keys_to_delete:
                self.storage.delete(key, timestamp, txn_id)
                affected += 1

        return QueryResult(success=True, affected_rows=affected)

    def _execute_create_table(self, stmt: CreateTableStmt) -> QueryResult:
        """Execute CREATE TABLE statement."""
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

    def _execute_drop_table(self, stmt: DropTableStmt) -> QueryResult:
        """Execute DROP TABLE statement."""
        try:
            self.schema.drop_table(stmt.table)
            return QueryResult(success=True)
        except ValueError as e:
            return QueryResult(success=False, error=str(e))

    def _matches_where(self, row: Dict[str, Any], where: WhereClause) -> bool:
        """Check if a row matches a WHERE clause."""
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
        """Filter row to only requested columns."""
        if columns == ['*']:
            return row

        return {col: row.get(col) for col in columns if col in row}
