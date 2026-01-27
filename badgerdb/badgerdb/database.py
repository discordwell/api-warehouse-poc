"""
BadgerDB - Main Database Interface

Ties together all components into a simple interface.
"""

from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional

from .config import Config
from .types import Timestamp, TxnId
from .storage import ShardManager
from .sequencer import Sequencer
from .sql import SQLParser, Executor, Schema
from .sql.executor import QueryResult
from .txn import TransactionManager, CalvinExecutor


class BadgerDB:
    """
    BadgerDB - A distributed SQL database with deterministic transactions.

    Usage:
        db = BadgerDB()
        db.start()

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        result = db.execute("SELECT * FROM users WHERE id = 1")
        print(result.rows)

        db.stop()
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()

        # Core components
        self.schema = Schema()
        self.storage = ShardManager(num_shards=self.config.num_shards)
        self.sequencer = Sequencer(self.config)
        self.executor = CalvinExecutor(
            self.sequencer,
            self.storage,
            self.schema
        )
        self.txn_manager = TransactionManager(self.sequencer, self.storage)

        self._running = False

    def start(self):
        """Start the database."""
        self.sequencer.start()
        self.executor.start()
        self._running = True

    def stop(self):
        """Stop the database."""
        self._running = False
        self.executor.stop()
        self.sequencer.stop()

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

        return self.executor.execute_sql(sql)

    def execute_many(self, statements: List[str]) -> List[QueryResult]:
        """Execute multiple SQL statements."""
        return [self.execute(sql) for sql in statements]

    # Transaction API

    def begin(self) -> TxnId:
        """Begin a transaction."""
        return self.txn_manager.begin()

    def commit(self, txn_id: TxnId):
        """Commit a transaction."""
        return self.txn_manager.commit(txn_id)

    def abort(self, txn_id: TxnId):
        """Abort a transaction."""
        return self.txn_manager.abort(txn_id)

    # Convenience methods

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a query and return rows."""
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

    # Stats

    def get_stats(self) -> dict:
        """Get database statistics."""
        return {
            "sequencer": self.sequencer.get_stats(),
            "executor": self.executor.get_stats(),
            "storage": self.storage.get_stats(),
            "txn_manager": self.txn_manager.get_stats(),
        }

    # Context manager

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
