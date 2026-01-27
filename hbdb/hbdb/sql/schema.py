"""
Schema Management

Defines tables, columns, and types.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from enum import Enum
import threading


class DataType(Enum):
    """SQL data types."""
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    JSON = "JSON"


@dataclass
class Column:
    """A column definition."""
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    default: Any = None


@dataclass
class Table:
    """A table definition."""
    name: str
    columns: Dict[str, Column] = field(default_factory=dict)
    primary_key: Optional[str] = None

    def add_column(self, column: Column):
        self.columns[column.name] = column
        if column.primary_key:
            self.primary_key = column.name

    def get_column(self, name: str) -> Optional[Column]:
        return self.columns.get(name)

    def validate_row(self, row: Dict[str, Any]) -> bool:
        """Validate a row against the schema."""
        for col_name, col in self.columns.items():
            if col_name not in row:
                if not col.nullable and col.default is None:
                    return False
        return True


class Schema:
    """
    Database schema manager.

    Manages table definitions.
    """

    def __init__(self):
        self._tables: Dict[str, Table] = {}
        self._lock = threading.RLock()

    def create_table(self, name: str, columns: List[Column]) -> Table:
        """Create a new table."""
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Table {name} already exists")

            table = Table(name=name)
            for col in columns:
                table.add_column(col)

            self._tables[name] = table
            return table

    def drop_table(self, name: str):
        """Drop a table."""
        with self._lock:
            if name not in self._tables:
                raise ValueError(f"Table {name} does not exist")
            del self._tables[name]

    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name."""
        with self._lock:
            return self._tables.get(name)

    def table_exists(self, name: str) -> bool:
        """Check if a table exists."""
        with self._lock:
            return name in self._tables

    def list_tables(self) -> List[str]:
        """List all table names."""
        with self._lock:
            return list(self._tables.keys())
