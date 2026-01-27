"""
Aria-Style Deterministic Execution Engine

Implements speculative parallel execution with deterministic conflict resolution.
Based on the Aria paper (Lu et al., 2020).

Key insight: Execute speculatively in parallel, then deterministically
reorder/retry only the conflicting transactions.
"""

from __future__ import annotations
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from enum import Enum

from ..types import Transaction, TxnId, Timestamp, Key, Value
from ..sequencer.simple_parallel import EpochBatch


class ExecutionPhase(Enum):
    """Aria execution phases."""
    SPECULATIVE = "speculative"
    CONFLICT_DETECTION = "conflict_detection"
    REORDER = "reorder"
    COMMIT = "commit"


@dataclass
class ReadSet:
    """Reads performed during speculative execution."""
    reads: Dict[Key, Tuple[Value, Timestamp]] = field(default_factory=dict)

    def add(self, key: Key, value: Value, timestamp: Timestamp):
        self.reads[key] = (value, timestamp)

    def keys(self) -> Set[Key]:
        return set(self.reads.keys())


@dataclass
class WriteSet:
    """Writes performed during speculative execution."""
    writes: Dict[Key, Value] = field(default_factory=dict)

    def add(self, key: Key, value: Value):
        self.writes[key] = value

    def keys(self) -> Set[Key]:
        return set(self.writes.keys())


@dataclass
class ExecutionResult:
    """Result of executing a transaction."""
    txn_id: TxnId
    success: bool
    read_set: ReadSet = field(default_factory=ReadSet)
    write_set: WriteSet = field(default_factory=WriteSet)
    result: Any = None
    error: Optional[str] = None
    needs_retry: bool = False


@dataclass
class ConflictInfo:
    """Information about a conflict between transactions."""
    txn1_id: TxnId
    txn2_id: TxnId
    conflict_type: str  # 'ww', 'rw', 'wr'
    key: Key


class AriaExecutor:
    """
    Aria-style speculative execution engine.

    Phases:
    1. Speculative: Execute all transactions in parallel
    2. Conflict Detection: Find conflicting transactions
    3. Reorder: Re-execute conflicts in deterministic order
    4. Commit: Apply all writes
    """

    def __init__(
        self,
        storage_read: Callable[[Key, Timestamp], Value],
        storage_write: Callable[[Key, Value, Timestamp, TxnId], None],
        execute_txn: Callable[[Transaction, 'SpeculativeContext'], ExecutionResult],
        num_workers: int = 8
    ):
        self.storage_read = storage_read
        self.storage_write = storage_write
        self.execute_txn = execute_txn
        self.num_workers = num_workers

        self._executor = ThreadPoolExecutor(max_workers=num_workers)
        self._lock = threading.RLock()

        # Stats
        self._stats = {
            "epochs_executed": 0,
            "transactions_executed": 0,
            "speculative_successes": 0,
            "conflicts_detected": 0,
            "retries": 0,
        }

    def execute_epoch(self, epoch: EpochBatch) -> Dict[TxnId, ExecutionResult]:
        """
        Execute an entire epoch using Aria protocol.

        Returns results dict mapping TxnId to ExecutionResult.
        """
        transactions = epoch.transactions

        if not transactions:
            return {}

        # Phase 1: Speculative Execution
        speculative_results = self._phase_speculative(transactions, epoch.timestamp)

        # Phase 2: Conflict Detection
        conflicts, conflict_txns = self._phase_conflict_detection(
            transactions, speculative_results
        )

        # Phase 3: Reorder and Re-execute conflicts
        if conflict_txns:
            final_results = self._phase_reorder(
                transactions, speculative_results, conflict_txns, epoch.timestamp
            )
        else:
            final_results = speculative_results

        # Phase 4: Commit
        self._phase_commit(transactions, final_results, epoch.timestamp)

        self._stats["epochs_executed"] += 1
        self._stats["transactions_executed"] += len(transactions)

        return final_results

    def _phase_speculative(
        self,
        transactions: List[Transaction],
        timestamp: Timestamp
    ) -> Dict[TxnId, ExecutionResult]:
        """
        Phase 1: Execute all transactions speculatively in parallel.

        Each transaction sees a snapshot of the database at epoch start.
        Writes are buffered, not applied yet.
        """
        results = {}

        # Create speculative contexts for each transaction
        contexts = {
            txn.txn_id: SpeculativeContext(
                txn_id=txn.txn_id,
                read_timestamp=timestamp,
                storage_read=self.storage_read
            )
            for txn in transactions
        }

        # Execute in parallel
        futures: Dict[Future, TxnId] = {}
        for txn in transactions:
            ctx = contexts[txn.txn_id]
            future = self._executor.submit(self.execute_txn, txn, ctx)
            futures[future] = txn.txn_id

        # Collect results
        for future in as_completed(futures):
            txn_id = futures[future]
            try:
                result = future.result()
                results[txn_id] = result
            except Exception as e:
                results[txn_id] = ExecutionResult(
                    txn_id=txn_id,
                    success=False,
                    error=str(e)
                )

        return results

    def _phase_conflict_detection(
        self,
        transactions: List[Transaction],
        results: Dict[TxnId, ExecutionResult]
    ) -> Tuple[List[ConflictInfo], Set[TxnId]]:
        """
        Phase 2: Detect conflicts between transactions.

        Conflicts:
        - Write-Write: Two transactions write the same key
        - Read-Write: T1 reads a key that T2 writes (T1 < T2 in order)
        - Write-Read: T1 writes a key that T2 reads (T1 < T2 in order)
        """
        conflicts = []
        conflict_txns = set()

        # Build index of reads and writes
        read_index: Dict[Key, List[Tuple[int, TxnId]]] = {}  # key -> [(order, txn_id)]
        write_index: Dict[Key, List[Tuple[int, TxnId]]] = {}

        for i, txn in enumerate(transactions):
            result = results.get(txn.txn_id)
            if not result or not result.success:
                continue

            for key in result.read_set.keys():
                if key not in read_index:
                    read_index[key] = []
                read_index[key].append((i, txn.txn_id))

            for key in result.write_set.keys():
                if key not in write_index:
                    write_index[key] = []
                write_index[key].append((i, txn.txn_id))

        # Detect Write-Write conflicts
        for key, writers in write_index.items():
            if len(writers) > 1:
                # All but the last writer conflict
                for i in range(len(writers) - 1):
                    _, txn1_id = writers[i]
                    _, txn2_id = writers[i + 1]
                    conflicts.append(ConflictInfo(
                        txn1_id=txn1_id,
                        txn2_id=txn2_id,
                        conflict_type='ww',
                        key=key
                    ))
                    conflict_txns.add(txn1_id)
                    self._stats["conflicts_detected"] += 1

        # Detect Read-Write conflicts (anti-dependency)
        for key in set(read_index.keys()) & set(write_index.keys()):
            readers = read_index[key]
            writers = write_index[key]

            for r_order, r_txn in readers:
                for w_order, w_txn in writers:
                    if r_order < w_order:
                        # Reader comes before writer - potential stale read
                        conflicts.append(ConflictInfo(
                            txn1_id=r_txn,
                            txn2_id=w_txn,
                            conflict_type='rw',
                            key=key
                        ))
                        conflict_txns.add(r_txn)
                        self._stats["conflicts_detected"] += 1
                    elif w_order < r_order:
                        # Writer comes before reader - reader should see write
                        conflicts.append(ConflictInfo(
                            txn1_id=w_txn,
                            txn2_id=r_txn,
                            conflict_type='wr',
                            key=key
                        ))
                        conflict_txns.add(r_txn)
                        self._stats["conflicts_detected"] += 1

        return conflicts, conflict_txns

    def _phase_reorder(
        self,
        transactions: List[Transaction],
        speculative_results: Dict[TxnId, ExecutionResult],
        conflict_txns: Set[TxnId],
        timestamp: Timestamp
    ) -> Dict[TxnId, ExecutionResult]:
        """
        Phase 3: Re-execute conflicting transactions in order.

        Non-conflicting transactions keep their speculative results.
        Conflicting transactions are re-executed serially in sequence order.
        """
        final_results = {}

        # Build write buffer from non-conflicting transactions
        write_buffer: Dict[Key, Value] = {}
        for txn in transactions:
            if txn.txn_id not in conflict_txns:
                result = speculative_results.get(txn.txn_id)
                if result and result.success:
                    final_results[txn.txn_id] = result
                    write_buffer.update(result.write_set.writes)
                    self._stats["speculative_successes"] += 1

        # Re-execute conflicting transactions in order
        for txn in transactions:
            if txn.txn_id in conflict_txns:
                # Create context that sees previous writes
                ctx = SpeculativeContext(
                    txn_id=txn.txn_id,
                    read_timestamp=timestamp,
                    storage_read=self.storage_read,
                    write_buffer=write_buffer.copy()
                )

                # Re-execute
                result = self.execute_txn(txn, ctx)
                final_results[txn.txn_id] = result

                # Update write buffer
                if result.success:
                    write_buffer.update(result.write_set.writes)

                self._stats["retries"] += 1

        return final_results

    def _phase_commit(
        self,
        transactions: List[Transaction],
        results: Dict[TxnId, ExecutionResult],
        timestamp: Timestamp
    ):
        """
        Phase 4: Commit all writes to storage.

        Writes are applied in transaction order for determinism.
        """
        for txn in transactions:
            result = results.get(txn.txn_id)
            if result and result.success:
                for key, value in result.write_set.writes.items():
                    self.storage_write(key, value, timestamp, txn.txn_id)

    def get_stats(self) -> dict:
        """Get executor statistics."""
        return {
            **self._stats,
            "conflict_rate": (
                self._stats["conflicts_detected"] / max(1, self._stats["transactions_executed"])
            ),
            "speculative_success_rate": (
                self._stats["speculative_successes"] / max(1, self._stats["transactions_executed"])
            ),
        }


class SpeculativeContext:
    """
    Execution context for speculative transaction execution.

    Tracks reads and buffers writes without modifying storage.
    """

    def __init__(
        self,
        txn_id: TxnId,
        read_timestamp: Timestamp,
        storage_read: Callable[[Key, Timestamp], Value],
        write_buffer: Optional[Dict[Key, Value]] = None
    ):
        self.txn_id = txn_id
        self.read_timestamp = read_timestamp
        self._storage_read = storage_read
        self._write_buffer = write_buffer or {}

        self.read_set = ReadSet()
        self.write_set = WriteSet()

    def read(self, key: Key) -> Value:
        """
        Read a value.

        First checks local write buffer, then previous transactions' writes,
        then storage snapshot.
        """
        # Check our own writes first
        if key in self.write_set.writes:
            return self.write_set.writes[key]

        # Check write buffer (previous transactions in epoch)
        if key in self._write_buffer:
            value = self._write_buffer[key]
            self.read_set.add(key, value, self.read_timestamp)
            return value

        # Read from storage snapshot
        value = self._storage_read(key, self.read_timestamp)
        self.read_set.add(key, value, self.read_timestamp)
        return value

    def write(self, key: Key, value: Value):
        """Buffer a write (not applied to storage yet)."""
        self.write_set.add(key, value)

    def delete(self, key: Key):
        """Buffer a delete (tombstone)."""
        self.write_set.add(key, None)  # None represents tombstone

    def to_result(self, success: bool, result: Any = None, error: str = None) -> ExecutionResult:
        """Convert context to execution result."""
        return ExecutionResult(
            txn_id=self.txn_id,
            success=success,
            read_set=self.read_set,
            write_set=self.write_set,
            result=result,
            error=error
        )
