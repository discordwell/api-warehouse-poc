"""SQL layer - parsing, planning, and execution.

Layout:
- engine.py / parser.py / optimizer.py / executor.py: the plan-based
  engine used by the FDB-style stack (hbdb.db). Import these via their
  submodules (e.g. ``from hbdb.sql.engine import SQLEngine``); they pull
  in sqlglot and the core storage layer.
- legacy/: the original statement-based parser/executor used by the
  Calvin engine (hbdb.database).
- schema.py / types.py: shared table-schema definitions.

Nothing is re-exported here so that importing one side of the SQL layer
does not drag in the other side's dependencies (the Calvin engine works
without sqlglot or the bloom-filter package).
"""
