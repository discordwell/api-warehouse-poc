"""
Core types for HBDB
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from enum import Enum
import time
import uuid


class TxnStatus(Enum):
    """Transaction status."""
    PENDING = "pending"
    SEQUENCED = "sequenced"
    EXECUTING = "executing"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class Timestamp:
    """
    Hybrid logical timestamp.

    Combines physical time with logical counter for ordering.
    """
    physical: int  # Milliseconds since epoch
    logical: int   # Counter for same-millisecond ordering

    @classmethod
    def now(cls) -> Timestamp:
        return cls(physical=int(time.time() * 1000), logical=0)

    @classmethod
    def zero(cls) -> Timestamp:
        return cls(physical=0, logical=0)

    def next(self) -> Timestamp:
        """Get next timestamp (increment logical)."""
        return Timestamp(self.physical, self.logical + 1)

    def advance(self, other: Timestamp) -> Timestamp:
        """Advance past another timestamp."""
        if other.physical > self.physical:
            return Timestamp(other.physical, other.logical + 1)
        elif other.physical == self.physical:
            return Timestamp(self.physical, max(self.logical, other.logical) + 1)
        else:
            return self.next()

    def __lt__(self, other: Timestamp) -> bool:
        if self.physical != other.physical:
            return self.physical < other.physical
        return self.logical < other.logical

    def __le__(self, other: Timestamp) -> bool:
        return self == other or self < other

    def __gt__(self, other: Timestamp) -> bool:
        return other < self

    def __ge__(self, other: Timestamp) -> bool:
        return self == other or self > other

    def __str__(self) -> str:
        return f"{self.physical}.{self.logical}"


@dataclass(frozen=True)
class TxnId:
    """Unique transaction identifier."""
    id: str

    @classmethod
    def generate(cls) -> TxnId:
        return cls(id=str(uuid.uuid4())[:8])

    def __str__(self) -> str:
        return self.id


# Type aliases
Key = str
Value = Any


@dataclass
class Row:
    """A row in a table."""
    data: Dict[str, Any]

    def get(self, column: str) -> Any:
        return self.data.get(column)

    def __getitem__(self, column: str) -> Any:
        return self.data[column]


@dataclass
class MVCCValue:
    """A versioned value with timestamp and tombstone support."""
    value: Value
    timestamp: Timestamp
    deleted: bool = False
    txn_id: Optional[TxnId] = None

    def is_visible_at(self, read_ts: Timestamp) -> bool:
        """Check if this version is visible at the given timestamp."""
        return self.timestamp <= read_ts and not self.deleted


@dataclass
class WriteIntent:
    """
    A pending write (lock) from an uncommitted transaction.

    Calvin-style: these are resolved deterministically, not with locks.
    """
    key: Key
    value: Value
    txn_id: TxnId
    timestamp: Timestamp


@dataclass
class ReadWriteSet:
    """
    The read and write sets for a transaction.

    Calvin requires knowing these upfront for deterministic execution.
    """
    reads: Set[Key] = field(default_factory=set)
    writes: Set[Key] = field(default_factory=set)

    def conflicts_with(self, other: ReadWriteSet) -> bool:
        """Check if this transaction conflicts with another."""
        # Write-write conflict
        if self.writes & other.writes:
            return True
        # Read-write conflict
        if self.reads & other.writes:
            return True
        if self.writes & other.reads:
            return True
        return False

    def add_read(self, key: Key):
        self.reads.add(key)

    def add_write(self, key: Key):
        self.writes.add(key)


@dataclass
class Operation:
    """A single operation within a transaction."""
    op_type: str  # 'read', 'write', 'delete'
    table: str
    key: Key
    value: Optional[Value] = None
    columns: Optional[List[str]] = None


@dataclass
class Transaction:
    """
    A transaction with its operations and metadata.
    """
    txn_id: TxnId
    status: TxnStatus = TxnStatus.PENDING
    operations: List[Operation] = field(default_factory=list)
    rw_set: ReadWriteSet = field(default_factory=ReadWriteSet)
    sequence_number: Optional[int] = None  # Assigned by sequencer
    timestamp: Optional[Timestamp] = None

    def add_operation(self, op: Operation):
        self.operations.append(op)
        if op.op_type == 'read':
            self.rw_set.add_read(f"{op.table}:{op.key}")
        elif op.op_type in ('write', 'delete'):
            self.rw_set.add_write(f"{op.table}:{op.key}")


@dataclass
class LogEntry:
    """
    An entry in the sequencer's transaction log.
    """
    sequence_number: int
    txn: Transaction
    timestamp: Timestamp

    def __lt__(self, other: LogEntry) -> bool:
        return self.sequence_number < other.sequence_number
