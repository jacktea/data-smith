# DataSmith Remediation Plan

Parent epic: [#1](https://github.com/jacktea/data-smith/issues/1)

Execution order is sequential in the shared checkout. Each session must preserve unrelated user changes, finish in a buildable/tested state, update this checklist and `docs/remediation-handoff.md`, and post evidence to its GitHub issue. No commit, push, worktree, or pull request is implied.

## Phase checklist

- [x] [#2](https://github.com/jacktea/data-smith/issues/2) — Session 1: migration discovery, ledger, and execution (completed 2026-09-18)
- [x] [#3](https://github.com/jacktea/data-smith/issues/3) — Session 2: `exec-sql` parsing, transactions, and dry-run (completed 2026-09-18)
- [x] [#4](https://github.com/jacktea/data-smith/issues/4) — Session 3: exact data comparison and safe chunk filtering (completed 2026-09-18)
- [x] [#5](https://github.com/jacktea/data-smith/issues/5) — Session 4: deterministic, dependency-safe, schema-qualified SQL (completed 2026-09-18)
- [ ] [#6](https://github.com/jacktea/data-smith/issues/6) — Session 5: fail-fast CLI and atomic output files
- [ ] [#7](https://github.com/jacktea/data-smith/issues/7) — Session 6: streaming diff and database performance
- [ ] [#8](https://github.com/jacktea/data-smith/issues/8) — Session 7: configuration, SSH, connections, and reset safety
- [ ] [#9](https://github.com/jacktea/data-smith/issues/9) — Session 8: dual-database E2E, CI, coverage, and documentation

## Session 1 acceptance evidence

- Upgrade discovery includes `.up.sql` and directionless `.sql` (normalized to `up`) and excludes `.down.sql`; `TestScanMigrationsUpgradeOnly` covers a mixed directory.
- Parseable JSON migrations fail during directory scanning, before `EnsureVersionTable`; direct API use also rejects JSON in `PrepareMigrationFiles`. `TestScanMigrationsRejectsJSON` and `TestApplyMigrationsPreflightBeforeDatabaseMutation` cover both paths.
- Every file is read and checksummed before the first database mutation. `MigrationFile.ReadContent() (string, error)` preserves read failures; deprecated `GetContent()` remains for compatibility. Missing files are covered by `TestMigrationFileReadContentReportsMissingFile` and the preflight test.
- Duplicate normalized versions fail with both paths in the error. Stored checksum drift fails with the database/file checksums and file path. Tests: `TestScanMigrationsRejectsDuplicateVersions`, `TestPrepareMigrationFilesRejectsDuplicateVersion`, and `TestApplyMigrationsRejectsChecksumDrift`.
- The ledger has a unique version index plus checksum, `running/success/failed`, execution milliseconds, and an error summary. Existing ledgers are upgraded in place; unique-index creation reports that duplicate versions must be removed first.
- Pending selection uses only the applied-success set, not a highest-version shortcut. `ApplyMigrations` repeats the status/checksum check while holding the database lock.
- PostgreSQL uses a dedicated connection advisory lock and one transaction for migration SQL plus the success update. `TestApplyPostgresMigrationAndSuccessAreAtomic` asserts the transaction boundary.
- MySQL uses `GET_LOCK`, only `?` placeholders, and `running → success/failed`. `TestApplyMySQLUsesQuestionMarkPlaceholdersAndStateMachine`, `TestMySQLFailureIsRecorded`, and `TestApplyMigrationsRejectsConcurrentRunner` cover these behaviors.
- MySQL migration dry-run is rejected because DDL can auto-commit and cannot provide a rollback guarantee.

## Session 1 verification

Baseline before changes:

- `go test ./...` — PASS
- `go test -race ./...` — PASS
- `go vet ./...` — PASS

## Session 2 acceptance evidence

- `ExecuteSQL` submits a script at most once. Failure diagnostics no longer begin probe transactions or replay individual statements; `TestExecuteSQLFailureDoesNotReexecuteSQL` asserts one driver call and preservation of the original driver error.
- `runExecSQL`, PostgreSQL dry-run, and `--tx` preserve the original SQL bytes. `TestExecuteSQLPostgresDryRunExecutesOriginalSQLAndRollsBack` and `TestExecuteSQLTransactionExecutesOriginalSQLAndCommits` match the exact input, including leading/trailing whitespace and comments.
- The lightweight scanner recognizes single-quoted strings, double-quoted identifiers/strings, backticks, dialect-specific line comments, nested PostgreSQL/non-nested MySQL block comments, semicolons, parentheses, PostgreSQL `$$`/tagged dollar quotes, and PostgreSQL `E'...'` strings. Table-driven tests cover inert `BEGIN`/semicolons in every quoted/comment form and dialect-specific ambiguity cases.
- `--tx` and dry-run reject top-level `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, savepoint controls, PostgreSQL aliases/prepared transactions, and MySQL XA controls before `Begin`. Mode-dependent backslash quoting and MySQL executable comments are conservatively rejected rather than guessed.
- PostgreSQL dry-run executes the unmodified script in a transaction and explicitly rolls it back. MySQL dry-run preflights the whole script and admits only `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, or `WITH` resolving to one of those commands; DDL and every other command class fail before `Begin`.
- Non-transaction execution remains a single raw driver call. PostgreSQL character positions are mapped to Unicode-safe statement/line/column locations; without a driver position, diagnostics report an exact single statement or a safe candidate range and keep the wrapped driver error.
- `FuzzScanSQL` covers both PostgreSQL-style and MySQL-style scanner options and validates monotonic in-bounds statement spans.

## Session 2 verification

Baseline before changes:

- `go test ./...` — PASS
- `go test -race ./...` — PASS
- `go vet ./...` — PASS

Final verification:

- `go test ./internal/datasmith/exec -count=1 -run '^(TestScanSQL|TestScanSQLReportsUnterminatedConstructs|TestScanSQLUsesDialectSpecificLineComments|TestScanSQLUsesDialectSpecificBackslashEscapes|TestValidateNoTransactionControl|TestValidateMySQLDryRun|TestExecuteSQL)' -v` — PASS
- `go test ./internal/datasmith/exec -run '^$' -fuzz '^FuzzScanSQL$' -fuzztime=2s` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS

Final verification:

- `go test ./pkg/migrate ./internal/datasmith/migrate/... -count=1 -v` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

## Session 3 acceptance evidence

- Integer and `BIGINT` comparison now uses `math/big.Int`; decimal/numeric comparison uses `math/big.Rat`; only float/double/real uses the existing absolute tolerance. Tests cover adjacent values above 2^53, `uint64` maximum values, high-precision decimals, trailing-zero normalization, NaN, positive/negative infinity, and the absence of decimal tolerance.
- `TestStreamCompareDataReportsZeroPrimaryKey` proves source `{0,1}` versus target `{1}` reports dropped primary key `0`. Existing and new tests cover composite keys, NULL versus empty string, and batch boundaries.
- Every MySQL and PostgreSQL database cursor loop checks `Rows.Err()`. SQL-mock interrupted-cursor tests prove `GetTableDataBatch` returns the original cursor error; the `diff-data` command now returns comparison errors through Cobra `RunE` instead of logging and continuing.
- Batch size is checked before schema/database work in streaming APIs and before config/rules file reads in the CLI. Chunk size is checked before hash/database work. Table, column, primary-key, and composite last-key inputs are validated before adapter queries.
- Hash ranges are `[nil, first-boundary) ... [last-boundary, nil)`, so the first range is unbounded below. Hash skipping requires exact source/target count, minimum key, and maximum key equality before every probabilistic chunk fingerprint matches. NULL and empty values have distinct length-prefixed hash encodings.
- `--chunk-hash` remains disabled by default; its help text identifies the optimization as probabilistic and conditional on exact count/min/max checks. Existing `ChunkHasher` implementations remain source-compatible; only implementations of the additive `VerifiedChunkHasher` interface can take the skip path.

## Session 3 verification

Baseline before changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final verification:

- `GOCACHE=/tmp/data-smith-go-cache go test ./pkg/diff ./pkg/db/postgres ./pkg/db/mysql ./internal/datasmith/diff -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go test ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go test -race ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go vet ./...` — PASS
- `git diff --check` — PASS

## Session 4 acceptance evidence

- `pkg/sql/ident` centralizes PostgreSQL double-quote and MySQL backtick quoting, doubles embedded delimiters, and builds schema-qualified names. Both data DML generators and every adapter data-read/hash/range query now use it; PostgreSQL defaults an absent schema to `public`.
- Schema comparison sorts table, column, index, and foreign-key keys before producing diffs. `Table.GetColumns` and position ordering are deterministic, including name tie-breaking, and unique-index fallback selection is sorted.
- TABLE↔VIEW changes are represented as a source-object drop plus target-object create. The generator drops old views/tables before any same-name create, making both transition directions executable.
- New tables are generated from foreign-key-free copies. All tables are created first; only then are new/modified foreign keys emitted. Mutual references are therefore valid and deterministic instead of depending on Go map iteration.
- Table drops are dependency ordered after owned foreign keys are removed; foreign-key additions place referenced tables before dependents where acyclic and use deterministic ordering inside a cycle; view creation is topological and view deletion reverses that order. View dependency cycles return an actionable error through `GenerateSchemaSQLSafe`, which the schema CLI uses.
- MySQL and PostgreSQL schema readers populate `ViewDefinition.Dependencies` from `information_schema.view_table_usage`, enabling real view-chain ordering.
- Golden fixtures cover a non-public schema containing an embedded quote, mixed-case/reserved names, mutual foreign keys, a three-view chain, TABLE↔VIEW in both directions, and rollback. The test repeats forward and rollback generation 30 times and requires byte-for-byte identity. Separate tests cover embedded quote/backtick escaping, map-backed column/index/foreign-key stability, and view-cycle rejection.

## Session 4 verification

Baseline before changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final verification:

- `go test ./pkg/sql/... ./pkg/diff ./pkg/db/postgres ./pkg/db/mysql ./internal/datasmith/diff -count=1` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS
- Protected artifacts: `datasmith` exists, size `0`, status `M`; `CODE_REVIEW_REPORT.md` exists, size `21194`, status `??`; staged diff empty.

## Open risks and later work

- Session 1 uses deterministic SQL-mock regression tests; live PostgreSQL/MySQL migration E2E remains for #9.
- Historical success rows from the old ledger may have a null checksum. They remain treated as applied for compatibility, so drift cannot be proven for those pre-upgrade rows. New/retried rows always receive a SHA-256 checksum.
- Adding the unique version index to an old ledger intentionally fails if duplicate versions already exist; the actionable error requires an operator to reconcile those historical duplicates.
- MySQL DDL is not transactional. If DDL succeeds but the success-ledger update fails, the command reports that the DDL may already be committed and leaves a recoverable `running` record; operators must inspect the schema before retrying.
- The Session 2 scanner is intentionally lexical, not a full SQL grammar. Client-side directives and payload formats such as MySQL `DELIMITER` and PostgreSQL `COPY ... FROM STDIN` are not modeled; transaction/dry-run mode may conservatively reject ambiguous scripts.
- MySQL dry-run validates command classes, but rollback still requires transactional table engines. It rejects executable comments and mode-dependent backslash quoting; use doubled quotes for portable transactional scripts.
- Live PostgreSQL/MySQL `exec-sql` E2E remains part of #9; Session 2 uses deterministic SQL-mock transaction and execution-count tests.
- Chunk fingerprints remain probabilistic: PostgreSQL uses nested MD5 and MySQL uses XOR-combined CRC32. Matching hashes are not an equivalence proof, even after exact count/min/max validation; concurrent writes outside a shared snapshot can also invalidate a comparison. `--chunk-hash` therefore remains opt-in.
- Session 3 adapter coverage is deterministic SQL-mock plus in-memory comparison tests. Live cross-database comparison and concurrent-mutation E2E remain deferred to #9.
- Session 4 validates SQL generation with deterministic unit/golden tests, not live PostgreSQL/MySQL execution. Dual-database apply → empty diff → rollback remains #9.
- View ordering depends on dependency metadata visible through `information_schema.view_table_usage`; objects hidden by database permissions cannot be ordered from metadata that the connection cannot see.
- PostgreSQL expression-index definitions returned as complete `pg_get_indexdef` SQL remain preserved verbatim for compatibility rather than parsed and rewritten.
- `GenerateSchemaSQL` remains source-compatible and returns no statements when safe generation rejects a cycle; CLI and error-aware callers use additive `GenerateSchemaSQLSafe` to receive the error.
- General Cobra `RunE` conversion, fail-fast table handling, best-effort reporting, and atomic output replacement are intentionally deferred to #6.
- The original modified, zero-byte `datasmith` binary and untracked `CODE_REVIEW_REPORT.md` are unrelated user changes and must remain untouched in later sessions. Full-suite tests can overwrite `datasmith`; truncate only that newly generated binary afterward to restore the user's pre-session modified state.

## Next action

Run Session 5 for issue #6 using the complete prompt in `docs/remediation-handoff.md`. Preserve migration behavior from #2, `exec-sql` safety from #3, exact comparison/safe hash behavior from #4, and deterministic dependency-safe SQL behavior from #5. Remaining order is #6 → #7 → #8 → #9.
