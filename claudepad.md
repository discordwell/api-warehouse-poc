# Claudepad (Session Memory)

## Session Summaries (most recent first; keep 20)

### 2026-07-08T05:51Z — hbdb/sql: set operations UNION/INTERSECT/EXCEPT (+ parenthesized-query fix, shared row comparator)
Landed the in-progress **set-operations** feature and completed it end to end.
- **Set ops by materialization** (new `hbdb/sql/setops.py`, `run_set_operation`).
  Like subqueries, they run *above* bind/optimize: each side runs as its own
  SELECT in the statement's txn, rows reduce to positional value tuples,
  combine with SQL set semantics, and re-label with the **first** side's output
  column names (positional matching; count mismatch fails loud). `UNION [ALL]`,
  `INTERSECT [ALL]`, `EXCEPT [ALL]`; DISTINCT forms dedupe via `distinct_key`
  (NULLs equal for set purposes; `10` == `"10"`), `*_ALL` use `Counter` bag
  semantics. Sides recurse (chains, parenthesized groups), each side is a full
  SELECT (joins/aggregates/subqueries/side ORDER BY+LIMIT). ORDER BY on the
  combined result is output-columns-only (name/position, else fails loud);
  LIMIT/OFFSET apply after combining. **Precedence guard:** sqlglot parses
  `a UNION b INTERSECT c` left-to-right, but the standard binds INTERSECT
  tighter — that bare chain fails loud until parenthesized (caught at top level
  and via `_side` recursion). Set ops work as subquery bodies (IN/EXISTS/
  scalar/ANY/ALL) and as `INSERT … SELECT` sources; correlated leaves stay loud.
  Engine plumbing: `_run_query` dispatches SetOperation vs SELECT and is the
  closure the subquery rewriter now uses (so a subquery body may be a set op);
  `subqueries._check_uncorrelated` recurses per side; EXISTS probe runs a set-op
  body in full (its LIMIT is on the combined result, can't cap a side).
- **Fixed a real gap (this session):** a fully parenthesized top-level query —
  `(a UNION b) ORDER BY c LIMIT n`, `(SELECT …)`, nested `((…))` — parses as a
  `Subquery` *wrapper*, so the engine misrouted it to the scalar-subquery path
  and reported a misleading "scalar subquery returned N rows". New
  `engine._unwrap_parenthesized_query` unwraps it and moves the trailing
  ORDER BY/LIMIT/OFFSET onto the body (fails loud on a wrapper-vs-body clash),
  reused at both the top-level dispatch and the INSERT source. Standard SQL now
  works instead of erroring.
- **Dedup (review finding):** `setops` had its own copy of the ORDER BY row
  comparator, a near-verbatim mirror of `SortExecutor._cmp` that had to stay
  byte-for-byte in sync. Extracted `predicates.compare_rows(a,b,keys,get)` (an
  accessor-parameterized comparator) and pointed both `SortExecutor._cmp` and
  `setops._order_limit` at it, so ORDER BY NULL/coercion semantics live in one
  place. (Left the parser's 3 sort-*key-binding* copies alone — different
  concern, more invasive.)
- **Tests/docs:** new `tests/verify_sql_setops.py` (UNION/INTERSECT/EXCEPT,
  NULL/coercion identity, bag semantics, precedence, ORDER/LIMIT fail-loud,
  subquery+INSERT positions, pure-Python parity, plus a
  `verify_toplevel_parenthesized` block for the paren fix) wired into
  `run_all.py`; `verify_sql_subqueries.py`'s old "UNION-in-subquery must raise"
  flipped to a positive test + a narrower VALUES-side fail-loud. README SQL
  section updated. Reviewed via 3 finder agents (0 correctness/regression
  findings) + hands-on edge probing. Full suite **23/23** (incl. cluster). Pure
  Python — no `.so` rebuild.

### 2026-07-02T03:55Z — hbdb/sql: uncorrelated subqueries, INSERT…SELECT (+ 3 silent-wrong fixes)
The big missing SQL feature landed: **uncorrelated subqueries in every
expression/predicate position**, plus `INSERT INTO … SELECT`, plus fixes for
three constructs that were *silently wrong* (worse than the fail-loud gaps).
- **Subqueries by materialization** (new `hbdb/sql/subqueries.py`). The engine
  now parses once, and before binding runs each uncorrelated subquery inside
  the statement's own txn, splicing results back as literal nodes: scalar
  `(SELECT …)` → literal/NULL (>1 row/col fails loud), `IN (SELECT …)` →
  deduped value list (empty → meta-tagged In; `_eval_in` resolves the LHS then
  returns FALSE — SQL's empty-set rule, even for NULL x), `EXISTS` →
  TRUE/FALSE (probe capped `LIMIT 1` only when no explicit LIMIT — `LIMIT 0`
  stays FALSE), `op ANY/SOME/ALL` → comparison vs a materialized `Tuple`
  evaluated three-valued in `predicates._eval_quantified` (empty: ANY→FALSE,
  ALL→TRUE). Nesting works via recursion (`engine._run_select` re-enters the
  rewriter). NULL semantics all pinned by tests (`NOT IN` w/ NULL matches
  nothing, etc.).
- **Correlation = fail loud, and it MUST be.** The engine resolves unknown
  columns to NULL at runtime, so a correlated subquery executed standalone
  would silently return wrong rows. `subqueries._check_uncorrelated` rejects
  any column not in the subquery's own FROM tables/aliases (typos included)
  before execution. Scoping via `col.find_ancestor(exp.Select) is select`;
  nested levels are checked when their level materializes.
- **INSERT INTO … SELECT** (`engine._insert_from_select`): source rows fully
  materialized first (self-insert reads its snapshot), mapped positionally via
  new `plan.output_columns(plan)` (dict rows don't carry order); arity
  mismatch fails loud; goes through InsertExecutor so indexes/cache upkeep are
  identical to VALUES.
- **Silent-wrong fixes:** (1) derived tables `FROM (SELECT …) s` used to bind
  the *inner* table and drop the subquery's WHERE/projection — now
  `parser._require_table_sources` fails loud; (2) `CREATE TABLE … AS SELECT`
  silently created an empty zero-column table — fails loud; (3) `WITH`/CTEs
  were silently dropped — fails loud (checked in engine before rewrite AND in
  `parser.bind`). (4) Bonus, found by the new suite: **`CREATE INDEX` on a
  populated table never backfilled** — index scans silently missed every
  pre-existing row; `engine.create_index` now backfills (regression scenario
  in `verify_sql_index.py`).
- **Plumbing:** `parser.parse(sql)` → `parse_one` + `parser.bind(ast)` (engine
  rewrites between); sqlglot 28.6 arg keys are `from_`/`with_` (code checks
  both spellings). `In.meta["materialized_empty_set"]` lives in `_meta` (not
  args) — survives `.copy()`/`transform(copy=True)` (verified), invisible to
  `.sql()`.
- **Tests/docs:** new `tests/verify_sql_subqueries.py` (~60 checks incl.
  pure-Python parity) wired into `run_all.py`;
  `verify_sql_predicates.py`'s IN-subquery block rewritten from "must raise"
  to "must hit exact rows" (correlated DELETE still must raise + table
  intact); direct `predicates.evaluate` of an *unmaterialized* subquery still
  fails loud (the old wipe can't return through paths that skip the rewrite).
  README SQL Support rewritten. Full suite **22/22** (incl. cluster). Pure
  Python — no `.so` rebuild.

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
