"""
Transaction Manager

Manages transaction lifecycle from creation to commit/abort.
"""

from __future__ import annotations
import threading
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field

from ..types import Transaction, TxnId, TxnStatus, Operation, ReadWriteSet, Timestamp
from ..sequencer import Sequencer
from ..storage import ShardManager
from ..sql.legacy.parser import SQLParser, Statement, SelectStmt, InsertStmt, UpdateStmt, DeleteStmt


@dataclass
class ActiveTransaction:
    """An in-progress transaction."""
    txn: Transaction
    statements: List[Statement] = field(default_factory=list)
    auto_commit: bool = True


class TransactionManager:
    """
    Manages transactions through their lifecycle.

    In Calvin architecture:
    1. Collect all statements in a transaction
    2. Analyze read/write sets
    3. Send to sequencer for ordering
    4. Execute deterministically
    """

    def __init__(self, sequencer: Sequencer, storage: ShardManager):
        self.sequencer = sequencer
        self.storage = storage
        self.parser = SQLParser()

        self._active_txns: Dict[TxnId, ActiveTransaction] = {}
        self._lock = threading.RLock()

        # Stats
        self._stats = {
            "transactions_started": 0,
            "transactions_committed": 0,
            "transactions_aborted": 0,
        }

    def begin(self) -> TxnId:
        """Begin a new transaction."""
        txn_id = TxnId.generate()
        txn = Transaction(txn_id=txn_id)

        with self._lock:
            self._active_txns[txn_id] = ActiveTransaction(
                txn=txn,
                auto_commit=False
            )
            self._stats["transactions_started"] += 1

        return txn_id

    def add_statement(self, txn_id: TxnId, sql: str) -> Statement:
        """Add a SQL statement to a transaction."""
        stmt = self.parser.parse(sql)

        with self._lock:
            active = self._active_txns.get(txn_id)
            if not active:
                raise ValueError(f"Transaction {txn_id} not found")

            active.statements.append(stmt)

            # Analyze read/write set
            self._analyze_statement(active.txn, stmt)

        return stmt

    def commit(self, txn_id: TxnId) -> Transaction:
        """
        Commit a transaction.

        Sends to sequencer for global ordering, then executes.
        """
        with self._lock:
            active = self._active_txns.pop(txn_id, None)
            if not active:
                raise ValueError(f"Transaction {txn_id} not found")

            txn = active.txn

        # Send to sequencer for global ordering
        entry = self.sequencer.submit(txn)

        # Mark as committed
        txn.status = TxnStatus.COMMITTED
        self._stats["transactions_committed"] += 1

        return txn

    def abort(self, txn_id: TxnId):
        """Abort a transaction."""
        with self._lock:
            active = self._active_txns.pop(txn_id, None)
            if active:
                active.txn.status = TxnStatus.ABORTED
                self._stats["transactions_aborted"] += 1

    def execute_auto_commit(self, sql: str) -> Transaction:
        """
        Execute a single statement with auto-commit.

        For simple queries that don't need explicit transaction.
        """
        txn_id = TxnId.generate()
        txn = Transaction(txn_id=txn_id)

        stmt = self.parser.parse(sql)
        self._analyze_statement(txn, stmt)

        # Submit to sequencer
        entry = self.sequencer.submit(txn)
        txn.status = TxnStatus.COMMITTED

        self._stats["transactions_started"] += 1
        self._stats["transactions_committed"] += 1

        return txn

    def _analyze_statement(self, txn: Transaction, stmt: Statement):
        """
        Analyze a statement to extract read/write sets.

        Calvin requires knowing these upfront for deterministic execution.
        """
        if isinstance(stmt, SelectStmt):
            # SELECT reads from the table
            # For point queries, we know the exact key
            if stmt.where and stmt.where.column:
                key = f"{stmt.table}:{stmt.where.value}"
                txn.rw_set.add_read(key)
            else:
                # Full scan - mark table as read
                txn.rw_set.add_read(f"{stmt.table}:*")

            txn.add_operation(Operation(
                op_type='read',
                table=stmt.table,
                key=stmt.where.value if stmt.where else '*',
                columns=stmt.columns
            ))

        elif isinstance(stmt, InsertStmt):
            # INSERT writes to specific key
            # Need to figure out the primary key value
            if stmt.columns and stmt.values:
                # Assume first column is PK for now
                pk_idx = 0
                if 'id' in stmt.columns:
                    pk_idx = stmt.columns.index('id')
                pk_value = stmt.values[pk_idx]
            else:
                pk_value = stmt.values[0] if stmt.values else 'unknown'

            key = f"{stmt.table}:{pk_value}"
            txn.rw_set.add_write(key)

            txn.add_operation(Operation(
                op_type='write',
                table=stmt.table,
                key=str(pk_value),
                value=dict(zip(stmt.columns, stmt.values)) if stmt.columns else None
            ))

        elif isinstance(stmt, UpdateStmt):
            # UPDATE reads then writes
            if stmt.where:
                key = f"{stmt.table}:{stmt.where.value}"
                txn.rw_set.add_read(key)
                txn.rw_set.add_write(key)
            else:
                txn.rw_set.add_read(f"{stmt.table}:*")
                txn.rw_set.add_write(f"{stmt.table}:*")

            txn.add_operation(Operation(
                op_type='write',
                table=stmt.table,
                key=stmt.where.value if stmt.where else '*',
                value=stmt.assignments
            ))

        elif isinstance(stmt, DeleteStmt):
            # DELETE writes (tombstone)
            if stmt.where:
                key = f"{stmt.table}:{stmt.where.value}"
                txn.rw_set.add_write(key)
            else:
                txn.rw_set.add_write(f"{stmt.table}:*")

            txn.add_operation(Operation(
                op_type='delete',
                table=stmt.table,
                key=stmt.where.value if stmt.where else '*'
            ))

    def get_active_transactions(self) -> List[TxnId]:
        """Get list of active transaction IDs."""
        with self._lock:
            return list(self._active_txns.keys())

    def get_stats(self) -> dict:
        """Get transaction manager statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_transactions": len(self._active_txns),
            }
