"""
Sequencer

The central component that orders all transactions.
In a real system, this would be replicated via Raft.
"""

from __future__ import annotations
import threading
import time
from typing import List, Optional, Dict, Callable
from queue import Queue, Empty
from dataclasses import dataclass

from ..types import Transaction, LogEntry, TxnId, TxnStatus
from ..config import Config
from .log import TransactionLog


@dataclass
class BatchedTransactions:
    """A batch of transactions to be sequenced together."""
    transactions: List[Transaction]
    created_at: float


class Sequencer:
    """
    Global transaction sequencer (Calvin-style).

    All transactions must go through the sequencer to get
    a global ordering before execution.

    Features:
    - Batches transactions for efficiency
    - Assigns monotonic sequence numbers
    - Replicates log (simplified - single node in POC)
    """

    def __init__(self, config: Config):
        self.config = config
        self.log = TransactionLog()
        self._lock = threading.RLock()

        # Batching
        self._pending_queue: Queue[Transaction] = Queue()
        self._batch_size = config.batch_size
        self._batch_timeout_ms = config.batch_timeout_ms

        # Background batcher thread
        self._running = False
        self._batcher_thread: Optional[threading.Thread] = None

        # Callbacks for when transactions are sequenced
        self._on_sequenced: List[Callable[[LogEntry], None]] = []

        # Stats
        self._stats = {
            "transactions_sequenced": 0,
            "batches_processed": 0,
        }

    def start(self):
        """Start the sequencer."""
        self._running = True
        self._batcher_thread = threading.Thread(target=self._batcher_loop, daemon=True)
        self._batcher_thread.start()

    def stop(self):
        """Stop the sequencer."""
        self._running = False
        if self._batcher_thread:
            self._batcher_thread.join(timeout=1.0)

    def submit(self, txn: Transaction) -> LogEntry:
        """
        Submit a transaction for sequencing.

        For simplicity, this is synchronous in the POC.
        In production, would be async with callback.
        """
        # Direct sequencing (bypass batching for simplicity)
        with self._lock:
            entry = self.log.append(txn)
            self._stats["transactions_sequenced"] += 1

            # Notify listeners
            for callback in self._on_sequenced:
                callback(entry)

            return entry

    def submit_batch(self, txns: List[Transaction]) -> List[LogEntry]:
        """Submit a batch of transactions."""
        with self._lock:
            entries = self.log.append_batch(txns)
            self._stats["transactions_sequenced"] += len(txns)
            self._stats["batches_processed"] += 1

            for entry in entries:
                for callback in self._on_sequenced:
                    callback(entry)

            return entries

    def _batcher_loop(self):
        """Background thread that batches transactions."""
        batch: List[Transaction] = []
        batch_start = time.time()

        while self._running:
            try:
                # Try to get a transaction
                txn = self._pending_queue.get(timeout=0.001)
                batch.append(txn)

                # Check if batch is full
                if len(batch) >= self._batch_size:
                    self._process_batch(batch)
                    batch = []
                    batch_start = time.time()

            except Empty:
                # Check if batch timeout exceeded
                if batch and (time.time() - batch_start) * 1000 >= self._batch_timeout_ms:
                    self._process_batch(batch)
                    batch = []
                    batch_start = time.time()

        # Process remaining
        if batch:
            self._process_batch(batch)

    def _process_batch(self, batch: List[Transaction]):
        """Process a batch of transactions."""
        if not batch:
            return

        with self._lock:
            entries = self.log.append_batch(batch)
            self._stats["transactions_sequenced"] += len(batch)
            self._stats["batches_processed"] += 1

            for entry in entries:
                for callback in self._on_sequenced:
                    callback(entry)

    def get_entry(self, sequence_number: int) -> Optional[LogEntry]:
        """Get a log entry by sequence number."""
        return self.log.get_entry(sequence_number)

    def get_entries_since(self, after_sequence: int) -> List[LogEntry]:
        """Get all entries after a sequence number."""
        return self.log.get_entries_after(after_sequence)

    def get_latest_sequence(self) -> int:
        """Get the latest sequence number."""
        return self.log.get_latest_sequence()

    def on_sequenced(self, callback: Callable[[LogEntry], None]):
        """Register callback for when transactions are sequenced."""
        self._on_sequenced.append(callback)

    def get_stats(self) -> dict:
        """Get sequencer statistics."""
        return {
            **self._stats,
            "log_size": len(self.log),
            "pending_queue_size": self._pending_queue.qsize(),
        }
