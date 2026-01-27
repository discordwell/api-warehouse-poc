"""
Calvin-Style Deterministic Executor

The key innovation: transactions are ordered BEFORE execution,
so all nodes can execute independently without coordination.
"""

from __future__ import annotations
import threading
from typing import Dict, List, Optional, Callable
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

from ..types import Transaction, LogEntry, TxnId, TxnStatus, Timestamp
from ..sequencer import Sequencer, TransactionLog
from ..storage import ShardManager
from ..sql.schema import Schema
from ..sql.executor import Executor, QueryResult
from ..sql.parser import SQLParser


class CalvinExecutor:
    """
    Calvin-style deterministic transaction executor.

    Key insight: if all nodes execute transactions in the same order,
    they will all reach the same state - no coordination needed during execution!

    Flow:
    1. Sequencer assigns global order to transactions
    2. Executor processes transactions in sequence order
    3. Non-conflicting transactions can execute in parallel
    4. Conflicting transactions execute serially in order
    """

    def __init__(
        self,
        sequencer: Sequencer,
        storage: ShardManager,
        schema: Schema,
        num_workers: int = 4
    ):
        self.sequencer = sequencer
        self.storage = storage
        self.schema = schema
        self.sql_executor = Executor(storage, schema)
        self.parser = SQLParser()

        # Track execution progress
        self._last_executed: int = 0
        self._lock = threading.RLock()

        # Pending results
        self._results: Dict[TxnId, QueryResult] = {}
        self._result_events: Dict[TxnId, threading.Event] = {}

        # Worker pool for parallel execution
        self._executor = ThreadPoolExecutor(max_workers=num_workers)

        # Background executor thread
        self._running = False
        self._exec_thread: Optional[threading.Thread] = None

        # Subscribe to sequencer
        self.sequencer.on_sequenced(self._on_transaction_sequenced)

        # Stats
        self._stats = {
            "transactions_executed": 0,
            "parallel_executions": 0,
            "serial_executions": 0,
        }

    def start(self):
        """Start the executor."""
        self._running = True
        self._exec_thread = threading.Thread(target=self._executor_loop, daemon=True)
        self._exec_thread.start()

    def stop(self):
        """Stop the executor."""
        self._running = False
        if self._exec_thread:
            self._exec_thread.join(timeout=1.0)
        self._executor.shutdown(wait=False)

    def execute_sql(self, sql: str, wait: bool = True) -> QueryResult:
        """
        Execute a SQL statement.

        1. Parse SQL
        2. Create transaction
        3. Submit to sequencer
        4. Wait for execution
        5. Return result
        """
        # Parse
        stmt = self.parser.parse(sql)

        # Create transaction
        txn = Transaction(txn_id=TxnId.generate())

        # Store the parsed statement for execution
        txn.operations = []  # We'll use the raw SQL instead

        # Create result event
        event = threading.Event()
        with self._lock:
            self._result_events[txn.txn_id] = event

        # Store SQL in transaction for later execution
        txn._sql = sql
        txn._stmt = stmt

        # Submit to sequencer
        entry = self.sequencer.submit(txn)

        if wait:
            # Wait for execution
            event.wait(timeout=5.0)

            with self._lock:
                result = self._results.pop(txn.txn_id, None)
                self._result_events.pop(txn.txn_id, None)

            if result is None:
                return QueryResult(success=False, error="Execution timeout")

            return result
        else:
            return QueryResult(success=True)  # Async, result will come later

    def _on_transaction_sequenced(self, entry: LogEntry):
        """Called when a transaction is sequenced."""
        # The executor loop will pick it up
        pass

    def _executor_loop(self):
        """Background loop that executes sequenced transactions."""
        while self._running:
            try:
                self._execute_pending()
            except Exception as e:
                print(f"Executor error: {e}")

            # Small sleep to avoid busy-waiting
            threading.Event().wait(0.001)

    def _execute_pending(self):
        """Execute any pending sequenced transactions."""
        # Get entries we haven't executed yet
        entries = self.sequencer.get_entries_since(self._last_executed)

        for entry in entries:
            self._execute_entry(entry)

            with self._lock:
                self._last_executed = entry.sequence_number

    def _execute_entry(self, entry: LogEntry):
        """Execute a single log entry."""
        txn = entry.txn
        timestamp = entry.timestamp

        # Get the SQL to execute
        sql = getattr(txn, '_sql', None)
        stmt = getattr(txn, '_stmt', None)

        if stmt is None:
            # No statement to execute
            result = QueryResult(success=True)
        else:
            # Execute the statement
            result = self.sql_executor.execute(stmt, timestamp, txn.txn_id)

        # Mark transaction as committed
        txn.status = TxnStatus.COMMITTED
        self._stats["transactions_executed"] += 1

        # Store result and signal waiter
        with self._lock:
            self._results[txn.txn_id] = result
            event = self._result_events.get(txn.txn_id)
            if event:
                event.set()

    def execute_batch_parallel(self, entries: List[LogEntry]):
        """
        Execute a batch of non-conflicting transactions in parallel.

        Calvin insight: if transactions don't conflict, they can run in parallel
        while still maintaining deterministic results.
        """
        # Group by conflict
        # For simplicity, we execute serially here
        # Full implementation would build conflict graph
        for entry in entries:
            self._execute_entry(entry)

    def get_stats(self) -> dict:
        """Get executor statistics."""
        return {
            **self._stats,
            "last_executed_sequence": self._last_executed,
        }
