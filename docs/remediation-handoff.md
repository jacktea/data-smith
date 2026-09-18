# DataSmith Remediation Handoff

Latest completed session: Session 1 / [Issue #2](https://github.com/jacktea/data-smith/issues/2)  
Next session: Session 2 / [Issue #3](https://github.com/jacktea/data-smith/issues/3)

## Session 1 summary

Upgrade discovery now admits only `.up.sql` and directionless `.sql`, treats directionless SQL as `up`, excludes down scripts, and rejects migration JSON before ledger creation. All migration files are pre-read and SHA-256 checksummed before database mutation. `MigrationFile.ReadContent() (string, error)` is the internal API; deprecated `GetContent()` remains source-compatible.

The ledger now includes unique `version`, `checksum`, `running/success/failed`, execution milliseconds, and an error summary, including in-place column/index upgrades for existing ledgers. Pending selection comes from successful ledger rows. A dedicated-connection advisory lock serializes runners and state is rechecked under that lock. PostgreSQL executes migration SQL and its success update in one transaction. MySQL records running before SQL, then success/failure, uses only `?` placeholders, and documents its non-transactional DDL ambiguity. MySQL migration dry-run is rejected.

## Changed files

- `pkg/migrate/model.go` — added `ReadContent`, retained deprecated `GetContent`, added checksum field.
- `pkg/migrate/migrate.go` — preflight/checksum, ledger schema upgrades, applied-success lookup, advisory locks, PostgreSQL atomic path, MySQL state machine, dialect placeholders.
- `internal/datasmith/migrate/local/parser.go` — upgrade-only discovery, directionless normalization, JSON and duplicate-version errors.
- `internal/datasmith/migrate/migrate_script.go` — pre-mutation validation and applied-success pending selection.
- `pkg/migrate/migrate_test.go` — ledger/execution/preflight/concurrency regression tests.
- `internal/datasmith/migrate/local/parser_test.go` — mixed direction, JSON, and duplicate discovery tests.
- `go.mod`, `go.sum` — `go-sqlmock` test dependency.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — persistent plan and latest handoff.

Unrelated and preserved: modified, zero-byte tracked `datasmith` binary and untracked `CODE_REVIEW_REPORT.md`.

## Compatibility notes

- Directionless `.sql` remains a valid upgrade.
- `MigrationFile.GetContent() string` remains exported and behaves as before, but is deprecated; new code must use `ReadContent() (string, error)`.
- `CurrentVersion` remains exported, although the CLI no longer uses it to decide pending migrations.
- Legacy successful ledger rows with null checksums remain applied. Drift checks become enforceable for rows written by the new implementation.
- Existing ledgers are upgraded in place. Duplicate historical versions must be reconciled before the new unique index can be created.
- MySQL DDL cannot be atomically coupled to its ledger update; an explicit error identifies the possible-commit case. MySQL migration dry-run is deliberately unsupported.

## Exact verification results

Baseline before changes:

- `go test ./...` — PASS (exit 0)
- `go test -race ./...` — PASS (exit 0)
- `go vet ./...` — PASS (exit 0)

Final:

- `go test ./pkg/migrate ./internal/datasmith/migrate/... -count=1 -v` — PASS (exit 0)
- `go test ./... -count=1` — PASS (exit 0)
- `go test -race ./... -count=1` — PASS (exit 0)
- `go vet ./...` — PASS (exit 0)

## Blockers and residual risks

No blocker remains for #2. No live database or credentials were used; dual-database E2E is intentionally deferred to #9. See `docs/remediation-plan.md` for legacy-checksum, historical-duplicate, and MySQL non-transactional DDL risks.

## Copy/paste prompt for independent Session 2

```text
You are the only code execution session for DataSmith remediation Session 2. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed dependency #2: https://github.com/jacktea/data-smith/issues/2
- This task #3: https://github.com/jacktea/data-smith/issues/3
- Next task #4: https://github.com/jacktea/data-smith/issues/4

Before editing:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #3, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md.
2. Treat the uncommitted Issue #2 implementation and both remediation docs as required existing work. Do not revert, overwrite, or broaden it.
3. Preserve unrelated user changes exactly: the tracked datasmith binary intentionally exists as a modified zero-byte file and CODE_REVIEW_REPORT.md is untracked. Full-suite tests may overwrite datasmith; truncate only that newly generated binary afterward so the original modified zero-byte state remains. Do not stage or commit either user artifact.
4. Do not create a worktree, switch branches, commit, push, or create a PR.

Issue #3 scope:
- Remove all diagnostic SQL re-execution; a submitted script must execute at most once.
- Implement a lightweight scanner for strings, quoted identifiers, backticks, line/block comments, semicolons, and PostgreSQL dollar quotes.
- Detect explicit top-level transaction control; --tx and dry-run must reject it without rewriting SQL.
- PostgreSQL dry-run executes the original script inside a transaction and rolls back.
- MySQL dry-run rejects DDL before execution and permits transactional DML only.
- Preserve single-shot non-transaction execution and report the driver error plus a safely derived statement location.
- Add table-driven scanner tests and fuzz coverage.

Non-goals:
- Do not implement a full SQL grammar.
- Do not change migration discovery, locking, checksums, or ledger behavior delivered by #2.
- Do not begin Issue #4 work.

Acceptance criteria to prove individually:
- Failure diagnostics execute no additional SQL.
- Transaction mode never modifies SQL text.
- Semicolons/BEGIN inside comments, strings, quoted identifiers, or dollar quotes are handled safely.
- MySQL DDL dry-run fails before database mutation.
- Focused tests pass.
- go test ./... passes.
- go test -race ./... passes.
- go vet ./... passes.

Execution requirements:
- Establish a test/race/vet baseline first and distinguish pre-existing unrelated failures from regressions.
- Implement only Issue #3 with automated regression tests and preserve existing exported APIs where practical.
- Use no real credentials. If completion requires a live database, external permission, product decision, or destructive action, stop and request the user's decision.
- Update docs/remediation-plan.md with Session 2 status, acceptance evidence, risks, and the remaining #4→#9 order.
- Replace docs/remediation-handoff.md with a concise Session 2 handoff containing the summary, changed files, exact commands/results, parser limitations, blockers/risks, compatibility notes, and a complete copy/paste prompt for independent Issue #4.
- Post a concise but complete update to GitHub Issue #3 with changed files, parser limitations, exact commands/results, blockers, and the Issue #4 prompt. Close #3 only if every acceptance criterion is met; otherwise leave it open and state the gaps.
- Final response must list completed work, changed files, acceptance evidence, test results, residual risks, Issue #3 status, issue-update link if available, and the fallback Issue #4 prompt.
```
