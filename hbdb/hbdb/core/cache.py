"""
Read Cache (LRU Buffer Pool) for HBDB.
Caches decoded row values to avoid repeated JSON parsing.
"""
from collections import OrderedDict
from threading import RLock
from typing import Any, Optional

class LRUCache:
    """Thread-safe LRU cache with configurable size."""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._lock = RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache. Returns None if not found."""
        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            return None

    def put(self, key: str, value: Any):
        """Put value in cache, evicting LRU if necessary."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._cache[key] = value
            else:
                if len(self._cache) >= self.max_size:
                    # Evict oldest
                    self._cache.popitem(last=False)
                self._cache[key] = value

    def invalidate(self, key: str):
        """Remove key from cache."""
        with self._lock:
            self._cache.pop(key, None)

    def invalidate_prefix(self, prefix: str):
        """Remove all keys starting with prefix."""
        with self._lock:
            keys_to_remove = [k for k in self._cache if k.startswith(prefix)]
            for k in keys_to_remove:
                del self._cache[k]

    def clear(self):
        """Clear the entire cache."""
        with self._lock:
            self._cache.clear()

    @property
    def hit_ratio(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    @property
    def stats(self) -> dict:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio": self.hit_ratio,
            "size": len(self._cache)
        }

# Global read cache instance
_read_cache = LRUCache(max_size=10000)

def get_read_cache() -> LRUCache:
    return _read_cache
