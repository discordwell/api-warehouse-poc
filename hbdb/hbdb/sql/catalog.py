from typing import Dict, Optional, List
from dataclasses import dataclass
from .types import Schema, Column, DataType

@dataclass
class Table:
    id: int
    name: str
    schema: Schema

@dataclass
class Index:
    id: int
    name: str
    table_id: int
    column_name: str

class Catalog:
    """
    In-memory catalog for POC. 
    Real system would store this in the KV store key range /meta/
    """
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.tables_by_id: Dict[int, Table] = {}
        self.indexes: Dict[str, Index] = {}  # index_name -> Index
        self.indexes_by_table: Dict[int, List[Index]] = {}  # table_id -> [Index]
        self._next_id = 1
        self._next_index_id = 1

    def create_table(self, name: str, schema: Schema) -> Table:
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
        
        t = Table(id=self._next_id, name=name, schema=schema)
        self.tables[name] = t
        self.tables_by_id[t.id] = t
        self.indexes_by_table[t.id] = []
        self._next_id += 1
        return t

    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)

    def get_table_by_id(self, tid: int) -> Optional[Table]:
        return self.tables_by_id.get(tid)

    def create_index(self, name: str, table_name: str, column_name: str) -> Index:
        table = self.get_table(table_name)
        if not table: raise ValueError(f"Table {table_name} not found")
        
        idx = Index(id=self._next_index_id, name=name, table_id=table.id, column_name=column_name)
        self.indexes[name] = idx
        self.indexes_by_table[table.id].append(idx)
        self._next_index_id += 1
        return idx

    def get_indexes_for_table(self, table_id: int) -> List[Index]:
        return self.indexes_by_table.get(table_id, [])
