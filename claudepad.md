# Claudepad (Session Memory)

## Session Summaries (most recent first; keep 20)

### 2026-06-24T15:12Z — hbdb/sql: CASE expressions (+ IN-subquery data-loss fix)
Continued the "fail loud, never silently wrong" trajectory on the FDB-style SQL
engine with one silent-wrong fix and one feature, both centered on the shared
operand resolver (`predicates.py`).
- **IN-subquery silent-wrong fix (data loss).** `WHERE x IN (SELECT ...)` /
  `x IN UNNEST(...)` parse with an *empty* `expressions` list (the operand sits
  under `query`/`unnest`/`field`), so the old `_eval_in` loop never ran and
  returned False for every row: `x IN (SELECT ...)` matched **nothing** and
  `x NOT IN (SELECT ...)` matched **everything** — so
  `DELETE FROM t WHERE id NOT IN (SELECT ...)` silently **wiped the whole
  table** (confirmed: count=N, 0 rows left). Now `_eval_in` fails loud
  (`NotImplementedError`) when there is no value list, matching how every other
  subquery form already failed (scalar `= (SELECT ...)`, `EXISTS`, `= ANY`).
  Guard is `if not node.expressions` — robust for any non-value-list IN; a
  value list (incl. single-element `IN (5)`) always populates `expressions`.
- **CASE expressions (added).** Searched (`CASE WHEN cond THEN r ... [ELSE d]
  END`) and simple (`CASE x WHEN v THEN r END`) via a new `_fn_case` in
  `_SCALAR_FUNCS` (keyed on `exp.Case`). Because it lives in the one shared
  `_resolve`, CASE works in SELECT/WHERE/ORDER BY/GROUP BY/HAVING/`UPDATE SET`
  at once, nests, and composes with aggregates (`SUM(CASE ...)`,
  `COUNT(CASE WHEN ... THEN 1 END)`). SQL semantics: searched WHEN uses
  three-valued `_eval` (UNKNOWN/NULL WHEN doesn't match → ELSE/NULL); simple
  CASE compares with the engine's `=` so a NULL operand / `WHEN NULL` never
  matches; no-ELSE and explicit `ELSE NULL` both yield NULL; only the selected
  branch is evaluated (lazy), but an unsupported fn in the *taken* branch still
  fails loud. A bare CASE used directly as a WHERE predicate is unsupported
  (fails loud) — CASE is an operand, not a predicate node.
- **Tests/docs.** New `tests/verify_sql_case.py` (~20 checks across all clauses,
  NULL paths, laziness, fail-loud, pure-Python parity) wired into `run_all.py`;
  IN-subquery regression added to `verify_sql_predicates.py` (low-level +
  end-to-end "table not wiped" guard). README SQL Support updated (CASE listed;
  IN documented as value-list-only with subqueries failing loud; subqueries
  added to the "not implemented" list). Full suite **21/21** (incl. cluster).
  Pure-Python change — no `.so` rebuild. A high-effort multi-agent code review
  (2 finders, 24+ end-to-end edge-case probes) found no correctness issues.

### 2026-06-24T09:45Z — hbdb/sql: scalar functions (+ index-path silent-wrong fix)
Added scalar functions to the FDB-style SQL engine, then a self-review caught
and fixed two silent-wrong regressions the feature exposed in the optimizer.
- **Scalar functions (added).** `COALESCE`, `NULLIF`, `UPPER`, `LOWER`,
  `LENGTH`, `TRIM`, `ABS`, `CEIL`, `FLOOR`, `ROUND`, `CONCAT`/`||`, `CAST`.
  All live in the one shared operand resolver (`predicates._resolve`) via a
  `_SCALAR_FUNCS` dispatch table, so they work in WHERE, SELECT, ORDER BY,
  GROUP BY, HAVING and `UPDATE ... SET` at once. Purely additive — these nodes
  used to hit the fail-loud catch-all. SQL NULL rules honored (COALESCE/NULLIF;
  every other fn NULL-in→NULL-out; `||`/CONCAT propagate NULL the ANSI way).
  Numeric fns fail loud on a non-NULL non-numeric arg (don't silently coerce);
  numeric *strings* still accepted (engine-wide coercion). `ROUND` rounds halves
  away from zero (Decimal/`ROUND_HALF_UP`), not Python's banker's rounding.
  `CAST`-to-int truncates toward zero. `SUBSTRING` and the `TRIM(... FROM ...)`
  forms stay unimplemented (fail loud). New `tests/verify_sql_functions.py`
  (~45 checks incl. pure-Python parity) wired into `run_all.py`;
  `verify_sql_project.py`'s fail-loud case moved `UPPER`→`SUBSTRING`.
- **Index-path silent-wrong fixes (this session, found in review).** The new
  fns broke two latent assumptions in `optimizer._maybe_index_scan`:
  (1) `WHERE CAST(col AS INT) = 19` index-scanned `col`'s *raw* stored value
  (19.95) and dropped the row — `exp.Cast.name` is the inner column name, so the
  `hasattr(left,'name')` check was fooled. (2) `WHERE col = COALESCE(other, 5)`
  resolved to the fallback `5` on the optimizer's empty probe row and looked up
  `col = 5` instead of the per-row predicate. Fix (right altitude, not a CAST
  special-case): require the LHS to be a bare `exp.Column` and the RHS to have
  *no* column reference (`cond.right.find(exp.Column) is None`); a truly
  constant scalar RHS (`col = ABS(-5)`) still folds and uses the index. Also
  broadened the resolve `except` to `ValueError` so a constant bad cast falls
  back to a scan instead of aborting at optimize time. Regression cases added to
  `verify_sql_functions.py`. Full suite: 20/20 (incl. cluster).

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
