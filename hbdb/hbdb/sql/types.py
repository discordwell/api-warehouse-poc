from enum import Enum, auto
from typing import Any, List, Dict, Optional
from dataclasses import dataclass

class DataType(Enum):
    INTEGER = auto()
    STRING = auto()
    BOOLEAN = auto()
    FLOAT = auto()

@dataclass
class Column:
    name: str
    type: DataType
    primary_key: bool = False
    nullable: bool = True

@dataclass
class Schema:
    columns: List[Column]
    
    def get_pk_column(self) -> Optional[Column]:
        for col in self.columns:
            if col.primary_key: return col
        return None
    
    def get_column_index(self, name: str) -> int:
        for i, col in enumerate(self.columns):
            if col.name == name: return i
        return -1
