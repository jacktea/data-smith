# DataSmith Remediation Handoff

Latest completed phase: Session 4, issue [#5](https://github.com/jacktea/data-smith/issues/5), on 2026-09-18.

## Session 4 summary

SQL identifiers now flow through `pkg/sql/ident`: PostgreSQL uses doubled double quotes, MySQL uses doubled backticks, and schema-qualified names are built consistently. PostgreSQL and MySQL data DML plus adapter batch/range/stats/hash reads use the qualified names, including PostgreSQL schemas outside `public`.

Schema comparison and generation are deterministic. Tables, columns, indexes, foreign keys, and emitted phases have stable ordering. TABLE↔VIEW changes are explicit drop + create operations. New tables are created without foreign keys, all table creates finish before any foreign key is added, table deletion is dependency ordered after owned constraints are removed, and views are created in dependency order and dropped in reverse order. Mutual table references are supported by the two-phase create/add-FK model. Unexecutable view dependency cycles return an error from additive `GenerateSchemaSQLSafe`; the schema CLI uses that error-aware API.

Both adapters now read view dependencies from `information_schema.view_table_usage`. Golden coverage exercises a non-public schema with an embedded quote, mixed-case/reserved names, mutually referencing tables, a three-view chain, TABLE→VIEW, VIEW→TABLE, and rollback. Forward and rollback SQL are regenerated 30 times and must remain byte-for-byte identical.

Committed dependency behavior remains intact: issue #2 commit `697b398`, issue #3 commit `ed79f25`, and issue #4 commit `0e846c2`. No commit, branch switch, worktree, push, or pull request was created in Session 4.

## Changed files

- `pkg/sql/ident/ident.go`, `pkg/sql/ident/ident_test.go` — centralized dialect delimiters, embedded-delimiter escaping, qualified names, identifier-list quoting, and regressions.
- `pkg/sql/postgres/postgre.go`, `pkg/sql/mysql/mysql.go` — qualified/escaped DML and DDL; deterministic columns, indexes, and foreign keys.
- `pkg/sql/postgres/convert.go`, `pkg/sql/mysql/convert.go` — column DDL uses centralized identifier escaping.
- `pkg/db/postgres/postgres.go`, `pkg/db/mysql/mysql.go` — schema-qualified batch/chunk/stat/hash reads and view dependency extraction.
- `pkg/db/postgres/data_safety_test.go`, `pkg/db/mysql/data_safety_test.go` — schema-qualified SQL expectations while retaining cursor/range/hash safety coverage.
- `pkg/conn/db.go` — stable column name and position ordering with name tie-breaks.
- `pkg/diff/schema.go` — stable schema diff ordering and TABLE↔VIEW drop/create modeling.
- `pkg/diff/data.go` — stable data column order and deterministic unique-index fallback selection.
- `pkg/sql/generator.go` — phased safe generator, deferred FKs, dependency ordering, cycle detection, and additive error-returning API.
- `pkg/sql/generator_test.go`, `pkg/sql/generator_golden_test.go`, `pkg/sql/testdata/issue5_forward.golden`, `pkg/sql/testdata/issue5_rollback.golden` — phase assertions, deterministic golden SQL, mutual references, view chains, transitions, rollback, and cycle regressions.
- `pkg/sql/postgres/postgres_test.go` — qualified PostgreSQL data DML contract.
- `internal/datasmith/diff/diff_schema.go` — uses `GenerateSchemaSQLSafe` and stops on generation/cycle errors.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — Session 4 evidence, risks, remaining order, and Session 5 prompt.

Unrelated and preserved: tracked `datasmith` exists, is zero bytes, and remains status `M`; `CODE_REVIEW_REPORT.md` remains untracked at exactly 21194 bytes. Neither artifact is staged or part of Session 4.

## Dependency and ordering decisions

- Object identity remains the schema reader's table-map key within its configured single schema. SQL output always uses the `Table.Schema` metadata (PostgreSQL defaults an empty value to `public`).
- TABLE↔VIEW is never treated as an in-place modification. The source object is dropped before the target object is created, in both forward and rollback generation.
- A new table is shallow-copied with `ForeignKeys=nil` for `CREATE TABLE`. Its foreign keys are emitted later, after every new table exists. Direct dialect `GenerateTableDDL` calls still include foreign keys for source compatibility.
- Table-drop ordering uses foreign-key dependencies, but owned foreign keys are removed first. Cyclic/mutual table relationships therefore remain executable and are ordered deterministically.
- Foreign-key additions use dependency ranks (referenced table before dependent where acyclic). Mutual cycles are explicitly detected by the ordering helper and resolved deterministically because all tables already exist.
- View creation uses dependency-first topological ordering; view deletion reverses it. A view cycle is not executable without a separate replacement strategy, so it is an error rather than arbitrary SQL.
- View dependency metadata is collected in sorted schema/name order. Dependencies outside the changed view set are treated as already existing and do not participate in the local topological graph.

## Per-criterion acceptance evidence

- PostgreSQL outside `public`: `TestPostgresDataSQLUsesQualifiedEscapedNames` asserts `"sales""Ops"."Order"` DML and escaped mixed-case/reserved identifiers. SQL-mock adapter tests assert configured schema qualification for reads and hash/range paths.
- Map-order independence: `TestDialectDDLMapFieldsAreStable` regenerates map-backed columns/indexes/FKs 30 times and asserts stable sorted order. Schema comparison sorts all map keys.
- Byte-for-byte repetition: `TestIssue5GoldenForwardRollbackDeterministic` regenerates both directions 30 times and compares every byte to checked-in golden fixtures.
- Related new tables: the forward golden creates `Alpha` and `Beta` without inline FKs, then adds both mutual constraints only after all table creates.
- Object transitions: the forward golden drops table `Convert` then creates a view, and drops view `Rebuild` then creates a table; rollback proves the inverse operations.
- View dependencies/cycles: the three-level `BaseView → Convert → Summary` chain is created dependency-first and dropped reverse; `TestGenerateSchemaSQLSafeRejectsViewDependencyCycle` checks the actionable error.
- Rollback: `issue5_rollback.golden` covers reverse view ordering, mutual constraint removal, table deletion, and reverse type transitions.
- Preserved #4 semantics: focused tests still pass for cursor errors, exact comparison packages, verified hash stats, NULL markers, and unbounded first ranges; full repository test/race/vet passes.

## Exact verification results

Baseline before Session 4 changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final Session 4 verification:

- `go test ./pkg/sql/... ./pkg/diff ./pkg/db/postgres ./pkg/db/mysql ./internal/datasmith/diff -count=1` — PASS
- `go test ./pkg/sql ./pkg/sql/ident ./pkg/db/postgres ./pkg/db/mysql ./pkg/diff -run 'Issue5|Qualified|Deterministic|Stable|Transition|ChunkRanges|CursorError' -count=1 -v` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS

Protected-file evidence after final verification:

- `datasmith` — present, size `0`, tracked status `M`.
- `CODE_REVIEW_REPORT.md` — present, size `21194`, untracked status `??`.
- `git diff --cached --name-only` — empty; nothing is staged.

## Compatibility notes and residual risks

- Existing `GenerateSchemaSQL` remains available and source-compatible. It cannot expose errors and therefore returns no statements when safe generation rejects a cycle. The CLI and error-aware integrations use additive `GenerateSchemaSQLSafe`.
- Direct dialect `GenerateTableDDL` retains complete-DDL behavior; only the phased schema generator removes FKs from the create step.
- PostgreSQL now explicitly qualifies `public` instead of relying on `search_path`. MySQL qualifies a table when schema metadata is present. This is an intentional correctness change for ambiguous/reserved/mixed-case names.
- Complete PostgreSQL expression-index SQL returned by `pg_get_indexdef` remains opaque and is preserved verbatim; parsing/reformatting it is outside this issue.
- Dependency discovery is limited to metadata visible to the configured database user. Missing privileges can hide view dependencies; live database permission/E2E coverage remains #9.
- Session 4 uses deterministic golden and SQL-mock tests. Live PostgreSQL/MySQL apply → empty diff → rollback is still #9.
- No blocker remains for issue #5. General CLI fail-fast behavior, `--best-effort`, and atomic output replacement remain exclusively issue #6.

## Copy/paste prompt for independent Session 5 / Issue #6

```text
You are the only code execution session for DataSmith remediation Session 5. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed dependency #2: https://github.com/jacktea/data-smith/issues/2
- Completed dependency #3: https://github.com/jacktea/data-smith/issues/3
- Completed dependency #4: https://github.com/jacktea/data-smith/issues/4
- Completed dependency #5: https://github.com/jacktea/data-smith/issues/5
- This task #6: https://github.com/jacktea/data-smith/issues/6
- Next task #7: https://github.com/jacktea/data-smith/issues/7

Committed baselines:
- Issue #2: 697b398 fix: harden migration execution
- Issue #3: ed79f25 fix: make exec sql handling safe
- Issue #4: 0e846c2 fix: make data comparison exact
Session 4 / Issue #5 changes may still be uncommitted when you start. Preserve them exactly; inspect the working tree and this handoff rather than assuming commit state.

Required baseline and preservation rules:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #6, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md before editing.
2. Preserve all completed #2–#5 behavior. Do not revert or weaken migration discovery/ledger/locking/checksum, exec-sql scanner/transaction/dry-run, exact numeric comparison, verified count/min/max chunk-hash gating, unbounded-first-range behavior, cursor errors, preflight validation, schema qualification/identifier escaping, deterministic ordering, deferred foreign keys, view dependency sorting/cycle detection, or TABLE↔VIEW transitions.
3. The tracked datasmith file must remain present, zero bytes, and status M. CODE_REVIEW_REPORT.md must remain untracked at exactly 21194 bytes. Do not delete, overwrite, stage, or commit either user artifact. If a full-suite command overwrites datasmith, restore only its exact protected state: present, zero bytes, M.
4. Do not create a worktree, switch branches, commit, push, or create a pull request. The scheduler will inspect and decide later Git operations.
5. Establish go test, race, and vet baselines before changing code and distinguish pre-existing failures from regressions.

Issue #6 complete scope:
- Convert Cobra business handlers to RunE; only the process root may map returned errors to exit status.
- Fail the entire diff on any table error by default.
- Add an explicit --best-effort mode whose report enumerates every failed table and cannot be mistaken for complete success.
- Write diff and rollback output through same-directory temporary files.
- Check every write, flush, sync, and close error; atomically rename only after complete success.
- On any failure, leave any previous final-path file unchanged and remove only the session's temporary file.
- Preserve unrelated user files and existing CLI defaults except the intentional fail-fast behavior.

Acceptance criteria to prove individually:
- Table failures return non-zero unless --best-effort is explicitly enabled.
- Best-effort output/report lists failed tables and remains unambiguous.
- Disk/write/flush/sync/close errors propagate.
- Failed generation leaves previous final diff and rollback files byte-for-byte unchanged.
- No business command calls os.Exit; process-root exit mapping remains correct.
- Focused failure-mode tests pass.
- go test ./... passes.
- go test -race ./... passes.
- go vet ./... passes.
- git diff --check passes.
- datasmith and CODE_REVIEW_REPORT.md retain their exact protected states.

Non-goals and constraints:
- Do not start issue #7 streaming/performance work or issue #8 connection/reset work.
- Do not use real credentials. If completion requires a live database, external permission, a product decision, or a destructive action, stop and request the user's decision.
- Preserve exported APIs where practical through additive/error-returning replacements and compatibility wrappers.

Session handoff requirements:
- Update docs/remediation-plan.md with Session 5 status, per-criterion evidence, risks, and remaining #7→#9 order.
- Replace docs/remediation-handoff.md with the latest Session 5 summary, changed files, exact commands/results, CLI/error/atomic-file compatibility decisions, blockers/risks, protected-file evidence, and a complete copy/paste prompt for independent Issue #7.
- Post a concise but complete GitHub Issue #6 update with failure-mode evidence, exact test commands/results, remaining limitations/blockers, and the Issue #7 prompt. Close #6 only if every acceptance criterion is satisfied; otherwise leave it open and state the gaps.
- Final response must list completed work, changed files, per-criterion evidence, test results, residual risks, Issue #6 status, issue-update link, and the fallback Issue #7 prompt.
```
