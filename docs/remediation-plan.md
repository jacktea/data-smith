# DataSmith Remediation Plan

Parent epic: [#1](https://github.com/jacktea/data-smith/issues/1)

Execution order is sequential in the shared checkout. Each session must preserve unrelated user changes, finish in a buildable/tested state, update this checklist and `docs/remediation-handoff.md`, and post evidence to its GitHub issue. No commit, push, worktree, or pull request is implied.

## Phase checklist

- [x] [#2](https://github.com/jacktea/data-smith/issues/2) — Session 1: migration discovery, ledger, and execution (completed 2026-09-18)
- [ ] [#3](https://github.com/jacktea/data-smith/issues/3) — Session 2: `exec-sql` parsing, transactions, and dry-run
- [ ] [#4](https://github.com/jacktea/data-smith/issues/4) — Session 3: exact data comparison and safe chunk filtering
- [ ] [#5](https://github.com/jacktea/data-smith/issues/5) — Session 4: deterministic, dependency-safe, schema-qualified SQL
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

Final verification:

- `go test ./pkg/migrate ./internal/datasmith/migrate/... -count=1 -v` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

## Open risks and later work

- Session 1 uses deterministic SQL-mock regression tests; live PostgreSQL/MySQL migration E2E remains for #9.
- Historical success rows from the old ledger may have a null checksum. They remain treated as applied for compatibility, so drift cannot be proven for those pre-upgrade rows. New/retried rows always receive a SHA-256 checksum.
- Adding the unique version index to an old ledger intentionally fails if duplicate versions already exist; the actionable error requires an operator to reconcile those historical duplicates.
- MySQL DDL is not transactional. If DDL succeeds but the success-ledger update fails, the command reports that the DDL may already be committed and leaves a recoverable `running` record; operators must inspect the schema before retrying.
- The original modified, zero-byte `datasmith` binary and untracked `CODE_REVIEW_REPORT.md` are unrelated user changes and must remain untouched in later sessions. Full-suite tests can overwrite `datasmith`; truncate only that newly generated binary afterward to restore the user's pre-session modified state.

## Next action

Run Session 2 for issue #3 using the complete prompt in `docs/remediation-handoff.md`. Do not change the migration-ledger behavior established by #2.
