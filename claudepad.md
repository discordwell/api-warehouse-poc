# Claudepad (Session Memory)

## Session Summaries (most recent first; keep 20)

### 2026-06-24T04:15Z — hbdb/sql: expression projection + LIKE/BETWEEN (+ review fixes)
Two improvements to the FDB-style SQL engine, continuing the "fail loud, never
silently wrong" trajectory, plus fixes from a self-review pass.
- **Silent-wrong projection bug (fixed).** Single-table `SELECT name AS who` /
  `SELECT age * 2 AS d` used to fall through with *no* projection, streaming
  every column (result row had no `who`/`d` key) — the JOIN path projected
  expressions correctly, the single-table path didn't. Now both route through
  `LogicalProjectExprs` + the shared operand resolver via the new
  `_projection_specs`; an unsupported scalar fn (`UPPER(x)`) raises instead of
  streaming the raw row. `_build_join_sort_keys` → `_build_projected_sort_keys`
  (now shared by both paths). New `tests/verify_sql_project.py`.
- **LIKE/ILIKE/BETWEEN (added).** New branches in the shared `predicates._eval`,
  so they work in WHERE *and* DELETE/UPDATE/HAVING/JOIN-ON for free.
  `%`/`_` wildcards, optional `ESCAPE`, three-valued NULL logic, text coercion.
  `verify_sql_predicates.py` extended (and its "LIKE fails loud" assertion
  swapped for `SIMILAR TO`, which is still unsupported).
- **Review fixes (this session):** (1) LIKE ReDoS — `%_%_…x` translated to
  `.*..*..x` and backtracked catastrophically (a single row could hang a scan);
  `_like_to_regex` now collapses each wildcard run into one quantifier
  (`.{k,}`/`.{k}`) and `_compile_like` is `lru_cache`d. (2) `t.*` qualified star
  used to project a bogus `{'*': None}`; now expands to the table's columns on
  both the single-table and JOIN paths (unknown qualifier → fail loud).
Full suite: 19/19 (incl. cluster). No C++ changes (pure Python).

### 2026-06-18T10:42Z — hbdb/sql: correct, tested JOIN support
Implemented real `JOIN` in the FDB-style SQL engine (`hbdb/hbdb/sql/`),
continuing the recent "fail loud, never silently wrong" trajectory. The
pre-existing join path was reachable but silently broken in 5+ ways
(column-name collisions clobbered a side; reversed `ON` returned 0 rows;
projection ignored; LEFT/RIGHT/FULL ran as INNER; non-equi `ON` became a cross
join). Now:
- `predicates._resolve` honors a column's table qualifier (`users.id`) via
  `table.col` keys, falling back to the bare name (single-table path
  unchanged → all prior tests still green).
- `QualifyExecutor` tags each base scan's rows with `table.col` keys for
  *every* schema column (absent→None — critical, else a qualified ref to a
  NULL column falls back to the other table's value).
- `JoinExecutor` evaluates the full `ON` predicate (any operator/direction),
  INNER/LEFT/RIGHT/FULL/CROSS, NULL-pads outer rows, hash-join fast path for
  classifiable INNER equi-joins (NULL keys skipped so `NULL=NULL` ≠ match).
- Parser binds joins: ambiguity check (unqualified ref to a name in 2+ tables
  → fail loud), projection (`SELECT *` keeps colliding cols under qualified
  names; duplicate output names fail loud), multi-table + self-joins,
  WHERE/ORDER BY/LIMIT/DISTINCT/GROUP-BY over joins.
- New `tests/verify_sql_join.py` (~30 checks) wired into `run_all.py`; README
  SQL Support section updated. Full suite: 18/18 (incl. cluster).
Two subtle silent-wrong bugs were caught during testing/review and fixed:
(1) absent (NULL) join-key columns matched the wrong table; (2) `SELECT
DISTINCT ... ORDER BY <non-output-col>` silently sorted by NULL — now fails
loud like the aggregate ORDER BY rule.

## Key Findings (permanent)

- **Two SQL engines coexist.** `hbdb.database.HBDB` (Calvin/deterministic) and
  `hbdb.db.HBDB` (FoundationDB-style OCC). The actively-developed SQL layer is
  `hbdb/hbdb/sql/` (sqlglot parser → logical plan → physical operators), used
  via `hbdb.sql.engine.SQLEngine(HBDB(...))`. There is also a dormant
  `hbdb/hbdb/sql/legacy/`.
- **Engine philosophy: fail loud.** Unsupported/ambiguous SQL must raise
  (`ValueError`/`NotImplementedError`), never silently drop a clause and return
  wrong rows. `predicates.py` is the single operand/predicate resolver shared
  by WHERE/SET/ORDER BY/JOIN/aggregates.
- **Rows are flat `dict[col -> value]`.** Joins add `table.col` keys on top to
  disambiguate; the read cache only ever holds bare rows (scoped per `HBDB`).
- **Test env:** `hbdb/venv/bin/python tests/run_all.py [--skip-cluster]`. Each
  verify script runs in an isolated temp CWD; every `HBDB` in one process+CWD
  shares the WAL, so scenarios use uniquely named tables. Rebuild the native
  `.so` only after C++ edits (none this session — change was pure Python).
