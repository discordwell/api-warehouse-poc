from abc import ABC, abstractmethod
from typing import Any, Dict, List

class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""

    @abstractmethod
    def connect(self):
        """Establish connection."""
        pass

    @abstractmethod
    def close(self):
        """Close connection."""
        pass

    @abstractmethod
    def setup_schema(self):
        """Create necessary tables."""
        pass

    @abstractmethod
    def execute(self, sql: str) -> bool:
        """Execute a raw SQL statement."""
        pass

    @abstractmethod
    def clear_data(self):
        """Truncate tables."""
        pass
