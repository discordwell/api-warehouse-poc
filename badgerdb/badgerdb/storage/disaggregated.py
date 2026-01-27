"""
Disaggregated Storage Layer (Aurora-style)

Separates compute and storage for independent scaling and fast failover.
Storage nodes own data and handle durability.
Compute nodes are stateless and can fail/recover instantly.
"""

from __future__ import annotations
import threading
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, Future

from ..types import Key, Value, Timestamp, TxnId, MVCCValue


@dataclass
class PageId:
    """Identifier for a storage page."""
    page_num: int

    def __hash__(self):
        return hash(self.page_num)


@dataclass
class Page:
    """A storage page containing key-value pairs."""
    page_id: PageId
    data: Dict[Key, List[MVCCValue]] = field(default_factory=dict)
    dirty: bool = False
    last_modified: float = field(default_factory=time.time)

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read value visible at timestamp."""
        if key not in self.data:
            return None

        versions = self.data[key]
        # Find latest version <= timestamp
        for v in reversed(versions):
            if v.timestamp <= timestamp and not v.deleted:
                return v.value
        return None

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: TxnId):
        """Write a new version."""
        if key not in self.data:
            self.data[key] = []

        self.data[key].append(MVCCValue(
            value=value,
            timestamp=timestamp,
            deleted=False,
            txn_id=txn_id
        ))
        self.dirty = True
        self.last_modified = time.time()

    def delete(self, key: Key, timestamp: Timestamp, txn_id: TxnId):
        """Write a tombstone."""
        if key not in self.data:
            self.data[key] = []

        self.data[key].append(MVCCValue(
            value=None,
            timestamp=timestamp,
            deleted=True,
            txn_id=txn_id
        ))
        self.dirty = True
        self.last_modified = time.time()


@dataclass
class WriteLogEntry:
    """Entry in the shared write-ahead log."""
    lsn: int  # Log Sequence Number
    page_id: PageId
    key: Key
    value: Value
    timestamp: Timestamp
    txn_id: TxnId
    is_delete: bool = False


class PageServer:
    """
    A storage node that owns and serves pages.

    Handles durability and replication for its pages.
    """

    def __init__(self, server_id: int, num_servers: int):
        self.server_id = server_id
        self.num_servers = num_servers

        # Pages owned by this server
        self._pages: Dict[PageId, Page] = {}
        self._lock = threading.RLock()

        # Write-ahead log (would be replicated in production)
        self._wal: List[WriteLogEntry] = []
        self._lsn_counter = 0

        # Stats
        self._stats = {
            "reads": 0,
            "writes": 0,
            "pages": 0,
        }

    def owns_page(self, page_id: PageId) -> bool:
        """Check if this server owns the given page."""
        return page_id.page_num % self.num_servers == self.server_id

    def read(self, page_id: PageId, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read a value from a page."""
        with self._lock:
            self._stats["reads"] += 1

            if page_id not in self._pages:
                return None

            return self._pages[page_id].read(key, timestamp)

    def write(
        self,
        page_id: PageId,
        key: Key,
        value: Value,
        timestamp: Timestamp,
        txn_id: TxnId
    ):
        """Write a value to a page."""
        with self._lock:
            self._stats["writes"] += 1

            # Log first (WAL)
            self._lsn_counter += 1
            entry = WriteLogEntry(
                lsn=self._lsn_counter,
                page_id=page_id,
                key=key,
                value=value,
                timestamp=timestamp,
                txn_id=txn_id
            )
            self._wal.append(entry)

            # Apply to page
            if page_id not in self._pages:
                self._pages[page_id] = Page(page_id=page_id)
                self._stats["pages"] += 1

            self._pages[page_id].write(key, value, timestamp, txn_id)

    def delete(
        self,
        page_id: PageId,
        key: Key,
        timestamp: Timestamp,
        txn_id: TxnId
    ):
        """Delete a value from a page (write tombstone)."""
        with self._lock:
            self._stats["writes"] += 1

            # Log first
            self._lsn_counter += 1
            entry = WriteLogEntry(
                lsn=self._lsn_counter,
                page_id=page_id,
                key=key,
                value=None,
                timestamp=timestamp,
                txn_id=txn_id,
                is_delete=True
            )
            self._wal.append(entry)

            # Apply tombstone
            if page_id not in self._pages:
                self._pages[page_id] = Page(page_id=page_id)
                self._stats["pages"] += 1

            self._pages[page_id].delete(key, timestamp, txn_id)

    def get_page(self, page_id: PageId) -> Optional[Page]:
        """Get a page (for caching by compute nodes)."""
        with self._lock:
            return self._pages.get(page_id)

    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "server_id": self.server_id,
            **self._stats,
            "wal_size": len(self._wal),
        }


class StorageCluster:
    """
    Cluster of page servers providing disaggregated storage.

    Routes requests to appropriate page server based on key hash.
    """

    def __init__(self, num_servers: int = 4, pages_per_server: int = 1024):
        self.num_servers = num_servers
        self.pages_per_server = pages_per_server
        self.total_pages = num_servers * pages_per_server

        # Create page servers
        self.servers: List[PageServer] = [
            PageServer(i, num_servers)
            for i in range(num_servers)
        ]

        self._lock = threading.RLock()

    def _key_to_page(self, key: Key) -> PageId:
        """Map a key to a page."""
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        page_num = key_hash % self.total_pages
        return PageId(page_num=page_num)

    def _get_server(self, page_id: PageId) -> PageServer:
        """Get the server that owns a page."""
        server_idx = page_id.page_num % self.num_servers
        return self.servers[server_idx]

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read a value."""
        page_id = self._key_to_page(key)
        server = self._get_server(page_id)
        return server.read(page_id, key, timestamp)

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: TxnId):
        """Write a value."""
        page_id = self._key_to_page(key)
        server = self._get_server(page_id)
        server.write(page_id, key, value, timestamp, txn_id)

    def delete(self, key: Key, timestamp: Timestamp, txn_id: TxnId):
        """Delete a value."""
        page_id = self._key_to_page(key)
        server = self._get_server(page_id)
        server.delete(page_id, key, timestamp, txn_id)

    def scan(
        self,
        prefix: str,
        timestamp: Timestamp,
        limit: int = 1000
    ) -> List[Tuple[Key, Value]]:
        """Scan keys with prefix (expensive - hits all servers)."""
        results = []

        for server in self.servers:
            for page in server._pages.values():
                for key in page.data.keys():
                    if key.startswith(prefix):
                        value = page.read(key, timestamp)
                        if value is not None:
                            results.append((key, value))
                            if len(results) >= limit:
                                return results

        return results

    def get_stats(self) -> dict:
        """Get cluster statistics."""
        server_stats = [s.get_stats() for s in self.servers]
        return {
            "num_servers": self.num_servers,
            "total_reads": sum(s["reads"] for s in server_stats),
            "total_writes": sum(s["writes"] for s in server_stats),
            "total_pages": sum(s["pages"] for s in server_stats),
            "servers": server_stats,
        }


class ComputeNode:
    """
    A stateless compute node.

    Executes queries using the storage cluster.
    Can cache pages locally for read performance.
    """

    def __init__(self, node_id: int, storage: StorageCluster, cache_size: int = 100):
        self.node_id = node_id
        self.storage = storage
        self.cache_size = cache_size

        # Page cache (LRU)
        self._cache: Dict[PageId, Page] = {}
        self._cache_order: List[PageId] = []
        self._lock = threading.RLock()

        # Stats
        self._stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "reads": 0,
            "writes": 0,
        }

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read with caching."""
        self._stats["reads"] += 1

        # For now, bypass cache and read from storage
        # (Cache would need invalidation protocol)
        return self.storage.read(key, timestamp)

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: TxnId):
        """Write through to storage."""
        self._stats["writes"] += 1
        self.storage.write(key, value, timestamp, txn_id)

    def delete(self, key: Key, timestamp: Timestamp, txn_id: TxnId):
        """Delete through to storage."""
        self._stats["writes"] += 1
        self.storage.delete(key, timestamp, txn_id)

    def scan(
        self,
        prefix: str,
        timestamp: Timestamp,
        limit: int = 1000
    ) -> List[Tuple[Key, Value]]:
        """Scan keys with prefix."""
        return self.storage.scan(prefix, timestamp, limit)

    def get_stats(self) -> dict:
        """Get node statistics."""
        total_accesses = self._stats["cache_hits"] + self._stats["cache_misses"]
        return {
            "node_id": self.node_id,
            **self._stats,
            "cache_hit_rate": (
                self._stats["cache_hits"] / max(1, total_accesses)
            ),
        }


class ComputeCluster:
    """
    Cluster of stateless compute nodes.

    Routes requests to compute nodes for load balancing.
    """

    def __init__(self, storage: StorageCluster, num_nodes: int = 4):
        self.storage = storage
        self.num_nodes = num_nodes

        self.nodes: List[ComputeNode] = [
            ComputeNode(i, storage)
            for i in range(num_nodes)
        ]

        self._request_counter = 0
        self._lock = threading.Lock()

    def _get_node(self) -> ComputeNode:
        """Get next compute node (round-robin)."""
        with self._lock:
            node = self.nodes[self._request_counter % self.num_nodes]
            self._request_counter += 1
            return node

    def read(self, key: Key, timestamp: Timestamp) -> Optional[Value]:
        """Read via a compute node."""
        return self._get_node().read(key, timestamp)

    def write(self, key: Key, value: Value, timestamp: Timestamp, txn_id: TxnId):
        """Write via a compute node."""
        self._get_node().write(key, value, timestamp, txn_id)

    def delete(self, key: Key, timestamp: Timestamp, txn_id: TxnId):
        """Delete via a compute node."""
        self._get_node().delete(key, timestamp, txn_id)

    def scan(
        self,
        prefix: str,
        timestamp: Timestamp,
        limit: int = 1000
    ) -> List[Tuple[Key, Value]]:
        """Scan via a compute node."""
        return self._get_node().scan(prefix, timestamp, limit)

    def get_stats(self) -> dict:
        """Get cluster statistics."""
        return {
            "num_nodes": self.num_nodes,
            "total_requests": self._request_counter,
            "nodes": [n.get_stats() for n in self.nodes],
            "storage": self.storage.get_stats(),
        }
