"""Legacy SQL layer for the Calvin-style engine (hbdb.database).

This is the original statement-based parser/executor pair used by
HBDB's deterministic Calvin stack (database.py, txn/calvin.py). The
top-level hbdb.sql package now hosts the newer plan-based engine
(parser -> optimizer -> physical operators) used by hbdb.db; both
share sql/schema.py.
"""

from .parser import (
    SQLParser, Statement, SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt, WhereClause,
)
from .executor import Executor, QueryResult
