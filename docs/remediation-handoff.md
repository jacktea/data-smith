# DataSmith Remediation Handoff

Latest completed phase: Session 2, issue [#3](https://github.com/jacktea/data-smith/issues/3), on 2026-09-18.

## Session 2 summary

`exec-sql` now scans SQL without rewriting it. Transaction and dry-run paths reject explicit top-level transaction control, then pass the original script bytes to the driver exactly once. PostgreSQL dry-run executes inside a transaction and explicitly rolls back. MySQL dry-run validates the complete script before `Begin`, permits only transactional DML command classes, and rejects DDL, unsupported commands, executable comments, and mode-dependent backslash quoting.

Non-transaction execution remains a single raw `Exec`. On failure it wraps the original driver error and reports a PostgreSQL driver position as a Unicode-safe statement/line/column; when no exact position exists it reports a safe statement or statement range without replaying SQL.

The migration discovery, ledger, locking, checksum, and execution behavior delivered by commit `697b398` for issue #2 was not changed.

## Changed files

- `internal/datasmith/exec/exec_sql.go` — raw-script execution, preflight validation, PostgreSQL/MySQL dry-run behavior, single-shot diagnostics, and Unicode-safe PostgreSQL error positions.
- `internal/datasmith/exec/sql_scanner.go` — lightweight dialect-aware lexical scanner and transaction/MySQL DML validation.
- `internal/datasmith/exec/exec_sql_safety_test.go` — SQL-mock execution-count, exact-text, transaction, rollback, pre-mutation rejection, and error-location tests.
- `internal/datasmith/exec/sql_scanner_test.go` — table-driven scanner/validator cases plus fuzz coverage.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — Session 2 evidence, risks, and Session 3 prompt.

Unrelated and preserved: tracked `datasmith` exists as a modified zero-byte file, and `CODE_REVIEW_REPORT.md` remains untracked. Neither is staged or included in Session 2 work.

## Acceptance evidence

- No diagnostic replay: `TestExecuteSQLFailureDoesNotReexecuteSQL` expects exactly one `Exec`, no diagnostic `Begin`, and verifies `errors.Is` against the original driver error.
- No SQL rewriting: exact SQL-mock expectations in `TestExecuteSQLPostgresDryRunExecutesOriginalSQLAndRollsBack` and `TestExecuteSQLTransactionExecutesOriginalSQLAndCommits` include original whitespace and comments.
- Safe lexical boundaries: `TestScanSQL`, `TestScanSQLUsesDialectSpecificLineComments`, and `TestScanSQLUsesDialectSpecificBackslashEscapes` cover strings, quoted identifiers, backticks, comments, nested/non-nested block-comment rules, semicolons, tagged/untagged dollar quotes, and transaction words inside inert regions.
- Explicit transactions rejected before database work: `TestValidateNoTransactionControl` and `TestExecuteSQLRejectsExplicitTransactionBeforeBegin` cover standard, savepoint, PostgreSQL alias/prepared, and MySQL XA controls.
- PostgreSQL dry-run: one exact-text `tx.Exec`, followed by rollback.
- MySQL dry-run: `TestExecuteSQLMySQLDryRunRejectsDDLBeforeMutation` proves the full script is rejected before `Begin`; the allowlist test proves transactional DML executes once and rolls back. Executable comments and SQL-mode-dependent quote ambiguity are separately rejected before `Begin`.
- Safe failure location: exact PostgreSQL character positions map across multibyte text; generic errors retain the driver error and report a safe candidate range. If lexical scanning fails, non-transaction mode still submits the raw script once and explicitly reports that a safe location is unavailable.

## Exact verification results

Baseline before Session 2 changes:

- `go test ./...` — PASS
- `go test -race ./...` — PASS
- `go vet ./...` — PASS

Final Session 2 verification:

- `go test ./internal/datasmith/exec -count=1 -run '^(TestScanSQL|TestScanSQLReportsUnterminatedConstructs|TestScanSQLUsesDialectSpecificLineComments|TestScanSQLUsesDialectSpecificBackslashEscapes|TestValidateNoTransactionControl|TestValidateMySQLDryRun|TestExecuteSQL)' -v` — PASS
- `go test ./internal/datasmith/exec -run '^$' -fuzz '^FuzzScanSQL$' -fuzztime=2s` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS

## Scanner limits and compatibility

- This is a lexical safety scanner, not a full SQL grammar. It does not model client-side `DELIMITER`, PostgreSQL `COPY ... FROM STDIN` payloads, or every procedural dialect; transaction/dry-run mode rejects ambiguous input rather than altering it.
- PostgreSQL dollar quotes support `$$` and identifier-like tags. PostgreSQL nested block comments and `E'...'` escapes are recognized.
- MySQL uses its own `#`, whitespace-sensitive `--`, non-nested block-comment, and backslash rules. Executable version comments are rejected in transactional modes. Because server `NO_BACKSLASH_ESCAPES` cannot be inferred safely without a live session query, mode-dependent quote forms are rejected; doubled quotes remain supported.
- MySQL dry-run permits only `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, and `WITH` whose top-level command resolves to one of those. Rollback also requires transactional table engines; the command does not inspect table engines.
- `ExecuteSQL` keeps its exported signature. Non-transaction scripts that the scanner cannot fully classify are still sent once to the driver; only transaction/dry-run requires successful safety scanning.
- No issue #2 migration API or behavior changed.

## Blockers and residual risks

- No blocker remains for issue #3.
- Live PostgreSQL/MySQL coverage is deferred to issue #9; Session 2 evidence is deterministic SQL-mock plus full repository test/race/vet.
- MySQL storage-engine transactional capability cannot be proven from SQL text alone. Operators must use transactional engines for dry-run rollback guarantees.

## Copy/paste prompt for independent Session 3

```text
You are the only code execution session for DataSmith remediation Session 3. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed dependency #2: https://github.com/jacktea/data-smith/issues/2
- Completed dependency #3: https://github.com/jacktea/data-smith/issues/3
- This task #4: https://github.com/jacktea/data-smith/issues/4
- Next task #5: https://github.com/jacktea/data-smith/issues/5

Required baseline and preservation rules:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #4, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md before editing.
2. Preserve all completed #2 and #3 work. Do not revert, rewrite, or broaden migration discovery/ledger/locking/checksum behavior or exec-sql scanner/transaction/dry-run behavior.
3. The tracked datasmith file must remain present, zero bytes, and status M. CODE_REVIEW_REPORT.md must remain untracked. Do not delete, overwrite, stage, or commit either user artifact. If a full-suite command overwrites datasmith, restore it only to the exact pre-session state: present, zero bytes, M.
4. Do not create a worktree, switch branches, commit, push, or create a pull request. The scheduler will inspect and decide later Git operations.
5. Establish go test, race, and vet baselines before changing code and distinguish pre-existing failures from regressions.

Issue #4 scope:
- Compare integer/BIGINT exactly, decimal/numeric with arbitrary precision, and reserve tolerances for float/double.
- Cover values beyond 2^53, unsigned integers, high-precision decimals, NaN, and Inf.
- Check rows.Err() after every database cursor loop.
- Validate batch size, chunk size, and key inputs before database or file work.
- Make the first hash chunk unbounded below and validate count/min/max before hash-based skipping.
- Keep --chunk-hash opt-in and document its probabilistic fingerprint behavior.
- Add boundary, NULL/empty, composite-key, interrupted-cursor, and hash-range regression tests.

Acceptance criteria to prove individually:
- Adjacent BIGINT values beyond 2^53 never compare equal.
- Source {0,1} versus target {1} reports primary key 0.
- Cursor failures propagate as command errors.
- Invalid batch sizes fail before comparison/database/file work.
- Focused tests pass.
- go test ./... passes.
- go test -race ./... passes.
- go vet ./... passes.
- git diff --check passes.
- datasmith and CODE_REVIEW_REPORT.md retain their exact protected states.

Non-goals and constraints:
- Do not start issue #5.
- Keep --chunk-hash opt-in; do not represent probabilistic hashes as proof of equality without count/min/max validation.
- Preserve exported APIs where practical and use no real credentials.
- If completion requires a live database, external permission, a product decision, or a destructive action, stop and request the user's decision.

Session handoff requirements:
- Update docs/remediation-plan.md with Session 3 status, acceptance evidence, risks, and remaining #5→#9 order.
- Replace docs/remediation-handoff.md with the latest Session 3 summary, changed files, exact commands/results, comparison/hash limitations, compatibility notes, blockers/risks, protected-file evidence, and a complete copy/paste prompt for independent Issue #5.
- Post a concise but complete GitHub Issue #4 update with correctness cases, changed files, exact test commands/results, remaining hash limitations, blockers, and the Issue #5 prompt. Close #4 only if every acceptance criterion is satisfied; otherwise leave it open and state the gaps.
- Final response must list completed work, changed files, per-criterion evidence, test results, residual risks, Issue #4 status, issue-update link, and the fallback Issue #5 prompt.
```
