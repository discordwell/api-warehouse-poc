"""
Transaction Log

The ordered log of all transactions.
This is the source of truth for transaction ordering.
"""

from __future__ import annotations
import threading
from typing import List, Optional, Dict, Callable
from collections import deque

from ..types import Transaction, LogEntry, Timestamp, TxnId, TxnStatus


class TransactionLog:
    """
    Append-only transaction log.

    In Calvin architecture, this log determines the global order
    of all transactions. All nodes execute transactions in this order.
    """

    def __init__(self):
        self._entries: List[LogEntry] = []
        self._by_txn_id: Dict[TxnId, LogEntry] = {}
        self._sequence_counter: int = 0
        self._lock = threading.RLock()

        # For subscribers
        self._subscribers: List[Callable[[LogEntry], None]] = []

        # Current timestamp
        self._timestamp = Timestamp.now()

    def append(self, txn: Transaction) -> LogEntry:
        """
        Append a transaction to the log.

        Returns the log entry with assigned sequence number.
        """
        with self._lock:
            self._sequence_counter += 1
            self._timestamp = self._timestamp.next()

            entry = LogEntry(
                sequence_number=self._sequence_counter,
                txn=txn,
                timestamp=self._timestamp
            )

            txn.sequence_number = self._sequence_counter
            txn.timestamp = self._timestamp
            txn.status = TxnStatus.SEQUENCED

            self._entries.append(entry)
            self._by_txn_id[txn.txn_id] = entry

            # Notify subscribers
            for sub in self._subscribers:
                sub(entry)

            return entry

    def append_batch(self, txns: List[Transaction]) -> List[LogEntry]:
        """Append multiple transactions atomically."""
        entries = []
        with self._lock:
            for txn in txns:
                entry = self.append(txn)
                entries.append(entry)
        return entries

    def get_entry(self, sequence_number: int) -> Optional[LogEntry]:
        """Get entry by sequence number."""
        with self._lock:
            if 1 <= sequence_number <= len(self._entries):
                return self._entries[sequence_number - 1]
            return None

    def get_by_txn_id(self, txn_id: TxnId) -> Optional[LogEntry]:
        """Get entry by transaction ID."""
        with self._lock:
            return self._by_txn_id.get(txn_id)

    def get_entries_after(self, after_sequence: int, limit: int = 100) -> List[LogEntry]:
        """Get entries after a sequence number."""
        with self._lock:
            start = after_sequence
            end = min(start + limit, len(self._entries))
            return self._entries[start:end]

    def get_latest_sequence(self) -> int:
        """Get the latest sequence number."""
        with self._lock:
            return self._sequence_counter

    def get_current_timestamp(self) -> Timestamp:
        """Get current log timestamp."""
        with self._lock:
            return self._timestamp

    def subscribe(self, callback: Callable[[LogEntry], None]):
        """Subscribe to new log entries."""
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[LogEntry], None]):
        """Unsubscribe from log entries."""
        with self._lock:
            self._subscribers.remove(callback)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
