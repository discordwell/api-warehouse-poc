from dataclasses import dataclass
from typing import List, Optional, Any, Dict
from .types import Schema

@dataclass
class LogicalNode:
    children: List['LogicalNode']
    schema: Schema

@dataclass
class LogicalScan(LogicalNode):
    table_name: str
    table_id: int

@dataclass
class LogicalFilter(LogicalNode):
    # For POC, expression is just a callable lambda or dict
    # In real DB, it's an Expression Tree
    condition: Any 

@dataclass
class LogicalProject(LogicalNode):
    column_names: List[str]

@dataclass
class LogicalInsert(LogicalNode):
    table_name: str
    table_id: int
    values: Dict[str, Any] # Column -> Value

@dataclass
class LogicalUpdate(LogicalNode):
    table_name: str
    table_id: int
    set_clause: Dict[str, Any]  # Column -> New Value Expression
    condition: Any  # WHERE clause

@dataclass
class LogicalDelete(LogicalNode):
    table_name: str
    table_id: int
    condition: Any  # WHERE clause

@dataclass
class LogicalJoin(LogicalNode):
    left: 'LogicalNode'
    right: 'LogicalNode'
    join_type: str  # INNER, LEFT, etc.
    condition: Any  # ON clause
