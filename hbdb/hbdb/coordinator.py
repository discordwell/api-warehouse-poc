"""
Transaction Coordinator

Routes transactions and implements fast-path optimization.
Based on Detock's hybrid deterministic/optimistic approach.
"""

from __future__ import annotations
import threading
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future

from .types import Transaction, TxnId, Timestamp, Key
from .sequencer.simple_parallel import SimpleParallelSequencerCluster, EpochBatch
from .execution.aria import AriaExecutor, ExecutionResult, SpeculativeContext
from .storage.disaggregated import ComputeCluster, StorageCluster


@dataclass
class FastPathResult:
    """Result of fast-path execution."""
    success: bool
    result: Any = None
    error: Optional[str] = None
    took_fast_path: bool = True


class BloomFilter:
    """
    Simple Bloom filter for fast-path conflict detection.

    Tracks recently written keys to detect potential conflicts.
    """

    def __init__(self, size: int = 10000, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bits = [False] * size
        self._lock = threading.Lock()

    def _hashes(self, key: str) -> List[int]:
        """Generate hash positions for a key."""
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.md5(f"{key}:{i}".encode()).hexdigest()
            positions.append(int(h, 16) % self.size)
        return positions

    def add(self, key: str):
        """Add a key to the filter."""
        with self._lock:
            for pos in self._hashes(key):
                self.bits[pos] = True

    def might_contain(self, key: str) -> bool:
        """Check if key might be in the filter (may have false positives)."""
        with self._lock:
            return all(self.bits[pos] for pos in self._hashes(key))

    def clear(self):
        """Clear the filter."""
        with self._lock:
            self.bits = [False] * self.size


class RecentWriteTracker:
    """
    Tracks recent writes for fast-path conflict detection.

    Uses a sliding window bloom filter approach.
    """

    def __init__(self, window_ms: int = 100, num_filters: int = 10):
        self.window_ms = window_ms
        self.num_filters = num_filters

        # Rotating bloom filters
        self._filters = [BloomFilter() for _ in range(num_filters)]
        self._current_filter = 0
        self._last_rotation = time.time()

        self._lock = threading.Lock()

    def record_write(self, key: Key):
        """Record a write to a key."""
        self._maybe_rotate()
        with self._lock:
            self._filters[self._current_filter].add(key)

    def has_recent_write(self, key: Key) -> bool:
        """Check if there was a recent write to this key."""
        self._maybe_rotate()
        with self._lock:
            return any(f.might_contain(key) for f in self._filters)

    def _maybe_rotate(self):
        """Rotate to a new filter if window expired."""
        now = time.time()
        if (now - self._last_rotation) * 1000 >= self.window_ms:
            with self._lock:
                self._current_filter = (self._current_filter + 1) % self.num_filters
                self._filters[self._current_filter].clear()
                self._last_rotation = now


class Coordinator:
    """
    Transaction coordinator with fast-path optimization.

    Fast Path (Detock-style):
    - Check if transaction's keys have recent writes
    - If no conflicts likely, execute immediately without sequencer
    - If conflicts possible, route through sequencer

    Slow Path:
    - Full Calvin-style deterministic execution
    - Goes through parallel sequencers
    - Epoch-based batching
    - Aria execution
    """

    def __init__(
        self,
        num_sequencer_partitions: int = 4,
        num_storage_servers: int = 4,
        num_compute_nodes: int = 4,
        epoch_duration_ms: int = 10,
        enable_fast_path: bool = True
    ):
        # Storage layer (disaggregated)
        self._storage = StorageCluster(num_servers=num_storage_servers)
        self._compute = ComputeCluster(self._storage, num_nodes=num_compute_nodes)

        # Sequencer cluster (simplified for demo - production would use full Raft)
        self._sequencers = SimpleParallelSequencerCluster(
            num_partitions=num_sequencer_partitions,
            epoch_duration_ms=epoch_duration_ms
        )

        # Aria executor
        self._executor = AriaExecutor(
            storage_read=self._compute.read,
            storage_write=self._compute.write,
            execute_txn=self._execute_transaction,
            num_workers=num_compute_nodes * 2
        )

        # Fast path tracking
        self._enable_fast_path = enable_fast_path
        self._write_tracker = RecentWriteTracker()

        # Pending results
        self._results: Dict[TxnId, ExecutionResult] = {}
        self._result_events: Dict[TxnId, threading.Event] = {}
        self._lock = threading.RLock()

        # Background execution
        self._running = False
        self._epoch_thread: Optional[threading.Thread] = None

        # Stats
        self._stats = {
            "transactions_submitted": 0,
            "fast_path_attempts": 0,
            "fast_path_successes": 0,
            "slow_path_executions": 0,
        }

    def start(self):
        """Start the coordinator."""
        self._running = True
        self._sequencers.start()
        self._sequencers.on_epoch_ready(self._on_epoch_ready)
        self._epoch_thread = threading.Thread(target=self._epoch_loop, daemon=True)
        self._epoch_thread.start()

    def stop(self):
        """Stop the coordinator."""
        self._running = False
        self._sequencers.stop()
        if self._epoch_thread:
            self._epoch_thread.join(timeout=1.0)

    def execute(self, txn: Transaction, timeout: float = 5.0) -> ExecutionResult:
        """
        Execute a transaction.

        Tries fast path first, falls back to slow path if needed.
        """
        self._stats["transactions_submitted"] += 1

        # Try fast path
        if self._enable_fast_path and self._can_fast_path(txn):
            result = self._try_fast_path(txn)
            if result.success:
                return result

        # Slow path - through sequencer
        return self._slow_path(txn, timeout)

    def _can_fast_path(self, txn: Transaction) -> bool:
        """Check if transaction can take fast path."""
        # Check if any keys have recent writes
        all_keys = txn.rw_set.reads | txn.rw_set.writes

        for key in all_keys:
            if self._write_tracker.has_recent_write(key):
                return False

        return True

    def _try_fast_path(self, txn: Transaction) -> ExecutionResult:
        """
        Try to execute via fast path (immediate, no sequencer).
        """
        self._stats["fast_path_attempts"] += 1

        # Create execution context
        timestamp = Timestamp.now()
        ctx = SpeculativeContext(
            txn_id=txn.txn_id,
            read_timestamp=timestamp,
            storage_read=self._compute.read
        )

        # Execute
        result = self._execute_transaction(txn, ctx)

        if result.success:
            # Commit writes immediately
            for key, value in result.write_set.writes.items():
                self._compute.write(key, value, timestamp, txn.txn_id)
                self._write_tracker.record_write(key)

            self._stats["fast_path_successes"] += 1

        return result

    def _slow_path(self, txn: Transaction, timeout: float) -> ExecutionResult:
        """
        Execute via slow path (sequencer + Aria).
        """
        self._stats["slow_path_executions"] += 1

        # Create result event
        event = threading.Event()
        with self._lock:
            self._result_events[txn.txn_id] = event

        # Submit to sequencer
        epoch = self._sequencers.submit(txn)

        # Wait for result
        if event.wait(timeout):
            with self._lock:
                result = self._results.pop(txn.txn_id, None)
                self._result_events.pop(txn.txn_id, None)

            if result:
                return result

        return ExecutionResult(
            txn_id=txn.txn_id,
            success=False,
            error="Execution timeout"
        )

    def _epoch_loop(self):
        """Background loop to check for ready epochs."""
        # Epochs are processed via callback
        while self._running:
            time.sleep(0.001)

    def _on_epoch_ready(self, epoch: EpochBatch):
        """Handle a ready epoch (callback from sequencer)."""
        # Execute epoch using Aria
        results = self._executor.execute_epoch(epoch)

        # results is Dict[TxnId, ExecutionResult]
        # Record writes for fast-path tracking
        for txn_id, result in results.items():
            if result.success:
                for key in result.write_set.keys():
                    self._write_tracker.record_write(key)

        # Notify waiters
        with self._lock:
            for txn_id, result in results.items():
                self._results[txn_id] = result
                if txn_id in self._result_events:
                    self._result_events[txn_id].set()

    def _execute_transaction(
        self,
        txn: Transaction,
        ctx: SpeculativeContext
    ) -> ExecutionResult:
        """
        Execute a single transaction.

        This is the transaction execution logic that interprets operations.
        """
        try:
            # Execute each operation
            for op in txn.operations:
                if op.op_type == 'read':
                    key = f"{op.table}:{op.key}"
                    value = ctx.read(key)
                    # Store result for SELECT

                elif op.op_type == 'write':
                    key = f"{op.table}:{op.key}"
                    ctx.write(key, op.value)

                elif op.op_type == 'delete':
                    key = f"{op.table}:{op.key}"
                    ctx.delete(key)

            return ctx.to_result(success=True)

        except Exception as e:
            return ctx.to_result(success=False, error=str(e))

    def get_stats(self) -> dict:
        """Get coordinator statistics."""
        fast_path_rate = (
            self._stats["fast_path_successes"] /
            max(1, self._stats["fast_path_attempts"])
        )

        return {
            **self._stats,
            "fast_path_success_rate": fast_path_rate,
            "sequencer": self._sequencers.get_stats(),
            "executor": self._executor.get_stats(),
            "storage": self._compute.get_stats(),
        }
