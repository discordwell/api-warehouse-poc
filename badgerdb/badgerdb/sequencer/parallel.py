"""
Parallel Sequencer (BOHM-style)

Multiple sequencers partition the key space for parallel ordering.
Each sequencer runs Raft internally for HA.
"""

from __future__ import annotations
import threading
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, Future

from ..types import Transaction, TxnId, Timestamp, LogEntry
from ..consensus.raft import RaftNode, RaftCluster


@dataclass
class SequencedBatch:
    """A batch of sequenced transactions for an epoch."""
    epoch: int
    partition: int
    transactions: List[Tuple[int, Transaction]]  # (sequence_num, txn)
    timestamp: Timestamp


@dataclass
class EpochBatch:
    """Complete epoch with all partition batches merged."""
    epoch: int
    transactions: List[Transaction]  # Deterministically ordered
    timestamp: Timestamp


class PartitionedSequencer:
    """
    A single partition sequencer with Raft HA.

    Handles ordering for a subset of the key space.
    """

    def __init__(
        self,
        partition_id: int,
        num_partitions: int,
        node_id: str,
        peer_ids: List[str],
        epoch_duration_ms: int = 10
    ):
        self.partition_id = partition_id
        self.num_partitions = num_partitions
        self.epoch_duration_ms = epoch_duration_ms

        # Raft for HA
        self.raft = RaftNode(
            node_id=node_id,
            peers=peer_ids,
            apply_callback=self._apply_sequenced
        )

        # Current epoch state
        self._current_epoch = 0
        self._epoch_start = time.time()
        self._epoch_buffer: List[Transaction] = []
        self._sequence_counter = 0

        # Committed batches
        self._committed_batches: Dict[int, SequencedBatch] = {}
        self._batch_events: Dict[int, threading.Event] = {}

        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._epoch_thread: Optional[threading.Thread] = None

        # Callbacks
        self._on_batch_ready: Optional[Callable[[SequencedBatch], None]] = None

    def start(self):
        """Start the sequencer."""
        self._running = True
        self.raft.start()
        self._epoch_thread = threading.Thread(target=self._epoch_loop, daemon=True)
        self._epoch_thread.start()

    def stop(self):
        """Stop the sequencer."""
        self._running = False
        self.raft.stop()
        if self._epoch_thread:
            self._epoch_thread.join(timeout=1.0)

    def submit(self, txn: Transaction) -> int:
        """
        Submit a transaction for sequencing.

        Returns the epoch number.
        """
        with self._lock:
            epoch = self._current_epoch
            self._epoch_buffer.append(txn)
            return epoch

    def _epoch_loop(self):
        """Background loop that closes epochs."""
        while self._running:
            time.sleep(self.epoch_duration_ms / 1000.0)
            self._close_epoch()

    def _close_epoch(self):
        """Close the current epoch and submit batch to Raft."""
        with self._lock:
            if not self._epoch_buffer:
                self._current_epoch += 1
                self._epoch_start = time.time()
                return

            epoch = self._current_epoch
            transactions = self._epoch_buffer
            self._epoch_buffer = []
            self._current_epoch += 1
            self._epoch_start = time.time()

            # Create event for this epoch
            self._batch_events[epoch] = threading.Event()

        # Submit to Raft (if leader)
        if self.raft.is_leader():
            try:
                # Sequence each transaction
                sequenced = []
                for txn in transactions:
                    seq_num = self._sequence_counter
                    self._sequence_counter += 1
                    sequenced.append((seq_num, txn))

                batch = SequencedBatch(
                    epoch=epoch,
                    partition=self.partition_id,
                    transactions=sequenced,
                    timestamp=Timestamp.now()
                )

                # Propose to Raft
                self.raft.propose(batch)
            except Exception as e:
                # Not leader or timeout
                pass

    def _apply_sequenced(self, batch: SequencedBatch) -> SequencedBatch:
        """Apply a committed batch (Raft callback)."""
        with self._lock:
            self._committed_batches[batch.epoch] = batch

            # Signal waiters
            if batch.epoch in self._batch_events:
                self._batch_events[batch.epoch].set()

            # Notify callback
            if self._on_batch_ready:
                self._on_batch_ready(batch)

        return batch

    def get_batch(self, epoch: int, timeout: float = 5.0) -> Optional[SequencedBatch]:
        """Get the batch for a specific epoch."""
        with self._lock:
            if epoch in self._committed_batches:
                return self._committed_batches[epoch]

            if epoch not in self._batch_events:
                self._batch_events[epoch] = threading.Event()
            event = self._batch_events[epoch]

        if event.wait(timeout):
            with self._lock:
                return self._committed_batches.get(epoch)
        return None

    def on_batch_ready(self, callback: Callable[[SequencedBatch], None]):
        """Register callback for when batches are committed."""
        self._on_batch_ready = callback

    def owns_key(self, key: str) -> bool:
        """Check if this partition owns the given key."""
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return key_hash % self.num_partitions == self.partition_id


class ParallelSequencerCluster:
    """
    Cluster of partitioned sequencers.

    Routes transactions to appropriate partition(s).
    """

    def __init__(
        self,
        num_partitions: int = 4,
        replicas_per_partition: int = 3,
        epoch_duration_ms: int = 10
    ):
        self.num_partitions = num_partitions
        self.replicas_per_partition = replicas_per_partition
        self.epoch_duration_ms = epoch_duration_ms

        # Sequencers per partition (Raft groups)
        self.partitions: Dict[int, List[PartitionedSequencer]] = {}
        self.raft_clusters: Dict[int, RaftCluster] = {}

        # Epoch assembler
        self._current_epoch = 0
        self._epoch_batches: Dict[int, Dict[int, SequencedBatch]] = {}  # epoch -> partition -> batch
        self._assembled_epochs: Dict[int, EpochBatch] = {}
        self._epoch_events: Dict[int, threading.Event] = {}

        self._lock = threading.RLock()
        self._on_epoch_ready: Optional[Callable[[EpochBatch], None]] = None

        self._setup_partitions()

    def _setup_partitions(self):
        """Create sequencer partitions with Raft groups."""
        for p in range(self.num_partitions):
            cluster = RaftCluster()
            sequencers = []

            # Create replicas for this partition
            node_ids = [f"seq_{p}_{r}" for r in range(self.replicas_per_partition)]

            for r in range(self.replicas_per_partition):
                node_id = node_ids[r]
                peers = [nid for nid in node_ids if nid != node_id]

                seq = PartitionedSequencer(
                    partition_id=p,
                    num_partitions=self.num_partitions,
                    node_id=node_id,
                    peer_ids=peers,
                    epoch_duration_ms=self.epoch_duration_ms
                )
                seq.on_batch_ready(lambda batch, p=p: self._on_partition_batch(p, batch))

                cluster.add_node(seq.raft)
                sequencers.append(seq)

            self.partitions[p] = sequencers
            self.raft_clusters[p] = cluster

    def start(self):
        """Start all sequencers."""
        for sequencers in self.partitions.values():
            for seq in sequencers:
                seq.start()

    def stop(self):
        """Stop all sequencers."""
        for sequencers in self.partitions.values():
            for seq in sequencers:
                seq.stop()

    def submit(self, txn: Transaction) -> int:
        """
        Submit a transaction for sequencing.

        Routes to appropriate partition(s) based on read/write set.
        Returns epoch number.
        """
        # Determine partitions touched by this transaction
        touched_partitions = self._get_touched_partitions(txn)

        epoch = None
        for p in touched_partitions:
            # Find leader for this partition
            leader = self._get_partition_leader(p)
            if leader:
                epoch = leader.submit(txn)

        return epoch or self._current_epoch

    def _get_touched_partitions(self, txn: Transaction) -> Set[int]:
        """Get partitions touched by a transaction's read/write set."""
        partitions = set()

        for key in txn.rw_set.reads | txn.rw_set.writes:
            key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
            partitions.add(key_hash % self.num_partitions)

        # If no keys specified, assign to partition 0
        if not partitions:
            partitions.add(0)

        return partitions

    def _get_partition_leader(self, partition: int) -> Optional[PartitionedSequencer]:
        """Get the leader sequencer for a partition."""
        for seq in self.partitions[partition]:
            if seq.raft.is_leader():
                return seq
        return self.partitions[partition][0]  # Fallback

    def _on_partition_batch(self, partition: int, batch: SequencedBatch):
        """Handle a committed batch from a partition."""
        with self._lock:
            epoch = batch.epoch

            if epoch not in self._epoch_batches:
                self._epoch_batches[epoch] = {}
                self._epoch_events[epoch] = threading.Event()

            self._epoch_batches[epoch][partition] = batch

            # Check if epoch is complete
            if len(self._epoch_batches[epoch]) == self.num_partitions:
                self._assemble_epoch(epoch)

    def _assemble_epoch(self, epoch: int):
        """Assemble a complete epoch from all partition batches."""
        with self._lock:
            batches = self._epoch_batches[epoch]

            # Deterministic merge: sort by (partition_id, sequence_number)
            all_txns = []
            for p in sorted(batches.keys()):
                batch = batches[p]
                for seq_num, txn in batch.transactions:
                    all_txns.append((p, seq_num, txn))

            # Sort deterministically
            all_txns.sort(key=lambda x: (x[0], x[1]))

            # Extract ordered transactions
            ordered = [txn for _, _, txn in all_txns]

            epoch_batch = EpochBatch(
                epoch=epoch,
                transactions=ordered,
                timestamp=Timestamp.now()
            )

            self._assembled_epochs[epoch] = epoch_batch
            self._epoch_events[epoch].set()

            # Notify callback
            if self._on_epoch_ready:
                self._on_epoch_ready(epoch_batch)

    def get_epoch(self, epoch: int, timeout: float = 5.0) -> Optional[EpochBatch]:
        """Get a complete assembled epoch."""
        with self._lock:
            if epoch in self._assembled_epochs:
                return self._assembled_epochs[epoch]

            if epoch not in self._epoch_events:
                self._epoch_events[epoch] = threading.Event()
            event = self._epoch_events[epoch]

        if event.wait(timeout):
            with self._lock:
                return self._assembled_epochs.get(epoch)
        return None

    def on_epoch_ready(self, callback: Callable[[EpochBatch], None]):
        """Register callback for when epochs are assembled."""
        self._on_epoch_ready = callback

    def get_stats(self) -> dict:
        """Get cluster statistics."""
        return {
            "num_partitions": self.num_partitions,
            "replicas_per_partition": self.replicas_per_partition,
            "assembled_epochs": len(self._assembled_epochs),
        }
