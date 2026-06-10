"""
Bloom Filter for HBDB.
Per-table Bloom filter to skip empty key ranges during scans.
"""
from pybloom_live import BloomFilter
from threading import RLock
from typing import Dict

class TableBloomFilter:
    """Manages Bloom filters per table prefix."""
    
    def __init__(self, capacity: int = 100000, error_rate: float = 0.001):
        self.capacity = capacity
        self.error_rate = error_rate
        self._filters: Dict[str, BloomFilter] = {}
        self._lock = RLock()

    def _get_table_prefix(self, key: str) -> str:
        """Extract table prefix from key like /t/1/_r/123."""
        parts = key.split("/")
        if len(parts) >= 4:
            return f"/t/{parts[2]}/"
        return key

    def add(self, key: str):
        """Add key to appropriate Bloom filter."""
        prefix = self._get_table_prefix(key)
        with self._lock:
            if prefix not in self._filters:
                self._filters[prefix] = BloomFilter(
                    capacity=self.capacity, 
                    error_rate=self.error_rate
                )
            self._filters[prefix].add(key)

    def might_contain(self, key: str) -> bool:
        """Check if key might exist (no false negatives)."""
        prefix = self._get_table_prefix(key)
        with self._lock:
            if prefix not in self._filters:
                return False  # No filter = no data
            return key in self._filters[prefix]

    def table_has_data(self, table_prefix: str) -> bool:
        """Check if any data exists for table prefix."""
        with self._lock:
            return table_prefix in self._filters

    def reset(self):
        """Drop all filters (used when rebuilding from a snapshot)."""
        with self._lock:
            self._filters.clear()

# Global bloom filter instance
_bloom_filter = TableBloomFilter()

def get_bloom_filter() -> TableBloomFilter:
    return _bloom_filter
