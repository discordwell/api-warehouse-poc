import json
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
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
    Catalog with persistence to HBDB.
    Keyspace:
    /meta/sys/tables/{name} -> table_id
    /meta/sys/defs/{table_id} -> JSON(schema)
    /meta/sys/next_id -> int
    /meta/sys/indexes/{name} -> JSON(index_def)
    """
    def __init__(self, db=None):
        self.db = db
        self.tables: Dict[str, Table] = {}
        self.tables_by_id: Dict[int, Table] = {}
        self.indexes: Dict[str, Index] = {}
        self.indexes_by_table: Dict[int, List[Index]] = {}
        self._next_id = 1
        self._next_index_id = 1
        
        if self.db:
            self._recover()

    def _recover(self):
        # Recover next IDs
        self._next_id = self.db.get_sync("/meta/sys/next_id") or 1
        self._next_index_id = self.db.get_sync("/meta/sys/next_index_id") or 1
        
        # Scan tables
        # Since we don't have a reliable scan for metadata with current backend scan?
        # Backend scan works on prefix.
        # Scan /meta/sys/defs/
        txn = self.db.transaction()
        defs = txn.scan("/meta/sys/defs/", "/meta/sys/defs/~")
        for key, val_json in defs:
            # Key: /meta/sys/defs/{tid}
            parts = key.split("/")
            tid = int(parts[-1])
            data = json.loads(val_json)
            # data: {name: str, columns: [...]}
            
            # Reconstruct Schema
            cols = []
            for c in data['columns']:
                cols.append(Column(
                    name=c['name'],
                    type=DataType[c['type']],
                    primary_key=c['primary_key'],
                    nullable=c['nullable']
                ))
            
            t = Table(id=tid, name=data['name'], schema=Schema(columns=cols))
            self.tables[t.name] = t
            self.tables_by_id[t.id] = t
            self.indexes_by_table[t.id] = []
            
        # Scan indexes
        # TODO: Persist indexes properly. For now simple.
        
    def create_table(self, name: str, schema: Schema) -> Table:
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
        
        t = Table(id=self._next_id, name=name, schema=schema)
        self.tables[name] = t
        self.tables_by_id[t.id] = t
        self.indexes_by_table[t.id] = []
        self._next_id += 1
        
        if self.db:
            self._persist_table(t)
            # Persist next_id
            self.db.set_sync("/meta/sys/next_id", self._next_id)
            
        return t

    def _persist_table(self, t: Table):
        # Save ID mapping (optional, lookup by id is enough)
        # self.db.set_sync(f"/meta/sys/tables/{t.name}", t.id)
        
        # Save Definition
        cols_data = []
        for c in t.schema.columns:
            cols_data.append({
                "name": c.name,
                "type": c.type.name, # Enum name
                "primary_key": c.primary_key,
                "nullable": c.nullable
            })
        
        data = {
            "name": t.name,
            "columns": cols_data
        }
        self.db.set_sync(f"/meta/sys/defs/{t.id}", json.dumps(data))

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
        
        # TODO: Persist indexes
        
        return idx

    def get_indexes_for_table(self, table_id: int) -> List[Index]:
        return self.indexes_by_table.get(table_id, [])
