"""
BadgerDB - A distributed SQL database with deterministic transactions

Key innovations over CockroachDB:
- Disaggregated compute/storage (Aurora-style)
- Deterministic transactions (Calvin-style)
- Simplified consensus for higher throughput
"""

__version__ = "0.1.0"

from .types import Timestamp, TxnId, Key, Value
from .config import Config
