# DataSmith Remediation Handoff

Latest completed phase: Session 5, issue [#6](https://github.com/jacktea/data-smith/issues/6), on 2026-09-18.

## Session 5 summary

- Every Cobra business handler now uses `RunE` and returns errors. `internal/datasmith/root.go` is the only process boundary that calls `os.Exit(1)`.
- `diff-data` now fails on the first table extraction/comparison error by default. The additive `--best-effort` flag is false by default; when selected, comparison continues through all rules and both SQL outputs plus logs enumerate every failed table under an explicit `INCOMPLETE` marker.
- Data and schema forward/rollback files are generated in same-directory temporary files. Every SQL write, buffered flush, file sync, file close, and directory sync is checked before success.
- Forward and rollback publication is paired. Existing finals are backed up; a reported failure during either final rename or directory sync restores both previous files byte-for-byte and removes only session-owned temporary artifacts.
- Focused fault injection covers direct write, flush, sync, close, callback/generation, and second-final-rename failures, plus the successful pair replacement path.
- Committed dependency behavior remains intact: issue #2 commit `697b398`, issue #3 commit `ed79f25`, issue #4 commit `0e846c2`, and issue #5 commit `f0e45c4`.
- No branch switch, worktree, commit, push, or pull request was created in Session 5.

## Changed files

- `internal/datasmith/diff/atomic_output.go` — shared staged output, checked flush/sync/close, paired publish, rollback, directory sync, and cleanup.
- `internal/datasmith/diff/atomic_output_test.go` — injected write/flush/sync/close/rename/generation failures and byte-preservation assertions.
- `internal/datasmith/diff/diff_data.go` — default fail-fast table handling, additive `--best-effort`, complete/incomplete reports, checked output writes, and atomic paired files.
- `internal/datasmith/diff/diff_data_validation_test.go` — default fail-fast and all-failed-table best-effort report tests.
- `internal/datasmith/diff/diff_schema.go` — `RunE`, returned errors, and paired atomic schema/rollback output; `writeSqlFile` remains as an error-returning atomic compatibility wrapper.
- `internal/datasmith/exec/exec_sql.go` — `exec-sql` business handler now directly returns `runExecSQL` through `RunE`.
- `internal/datasmith/migrate/migrate_script.go` — migration command now returns wrapped errors through `RunE`.
- `internal/datasmith/migrate/reset.go` — reset command now returns wrapped errors through `RunE`.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — Session 5 evidence, corrected protected-file state, residual risk, remaining order, and Session 6 prompt.

Unrelated protected files were not edited, staged, or committed:

- `datasmith` — present executable, size `7301362`, mode `-rwxr-xr-x`, SHA-256 `efadb73998d2e845b88a5d3465b12a3b21e3c4d82d59c74a1f1202cc7dd56253`, tracked status `M`.
- `CODE_REVIEW_REPORT.md` — present, size `21194`, mode `-rw-r--r--`, SHA-256 `9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b`, untracked status `??`.
- `git diff --cached --name-only` is empty.

The earlier zero-byte description of `datasmith` was obsolete and is superseded by the exact executable state above. A preservation backup was created at `/tmp/datasmith-session5.VQf6OJ/datasmith`; it has the same size, mode, and SHA-256.

## Per-criterion acceptance evidence

- **Table error is non-zero by default:** `TestGenerateDataDiffOutputsFailsFastByDefault` injects a first-table failure, requires an error naming that table, and proves the second table is not attempted. The enclosing atomic callback leaves final files untouched.
- **Best-effort is explicit and unambiguous:** `--best-effort` defaults false. `TestGenerateDataDiffOutputsBestEffortListsEveryFailedTable` proves all rules are attempted and both outputs contain `DATASMITH RESULT: INCOMPLETE (--best-effort)` plus every failed table/error. Runtime logs repeat the full list.
- **Write/flush/sync/close errors propagate:** `TestAtomicPairPropagatesImmediateWriteError` and `TestAtomicPairPropagatesOutputFailuresAndPreservesFinals` inject each failure class and require a returned error.
- **Old finals survive failure:** the write, flush, sync, close, callback, and second-rename tests seed old forward/rollback bytes and require them to remain byte-for-byte unchanged with no `.tmp-*` or `.backup-*` artifacts.
- **Paired publication:** `TestAtomicPairSecondRenameFailureRestoresBothFinals` proves a forward publish followed by a rollback publish failure compensates the first rename and restores both old files. `TestAtomicPairSuccessReplacesBothFinals` proves success replaces both.
- **Only process root exits:** repository audit finds no `Run:` business handler and finds `os.Exit` only in `internal/datasmith/root.go`.
- **Compatibility:** command flag defaults are unchanged except intentional default fail-fast behavior. `--best-effort` is additive. Existing schema SQL safety and error-aware `GenerateSchemaSQLSafe` behavior remain in place.

## Exact verification results

Baseline before Session 5 changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final Session 5 verification:

- `go test ./internal/datasmith/diff -run 'AtomicPair|GenerateDataDiffOutputs|Validate' -count=1 -v` — PASS
- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS
- `rg -n '\bRun:' internal cmd --glob '*.go'` — no matches
- `rg -n 'os\.Exit' internal cmd --glob '*.go'` — only `internal/datasmith/root.go`

Every full-suite command was followed by a protected-file fingerprint check; `datasmith` stayed at the exact size/mode/SHA above. `CODE_REVIEW_REPORT.md` stayed at the exact size/SHA/status above. Nothing is staged.

## CLI, error, and atomic-file compatibility decisions

- Business handlers return contextual errors and leave exit-code mapping at the root. Cobra/root logging remains responsible for user-visible failures.
- Default diff behavior intentionally changed from skipping target-table extraction errors to fail-fast. Compare errors were already returned; they now participate in the same no-publication guarantee.
- Best-effort is an opt-in completion policy, not a silent success mode. It exits successfully only after all processable tables and both durable outputs complete, and its artifacts state that the result is incomplete and list every failure.
- Forward/rollback temporary files are created in each final file's directory so the final rename stays on the same filesystem. Existing finals are backed up and restored on any reported paired-publish failure.
- Two unrelated filesystem paths cannot be replaced by a single cross-path atomic primitive. The implementation compensates every returned rename/sync failure; an OS/process crash between the two final renames remains a narrow residual crash-consistency window.
- Session 6 must preserve the atomic output callback boundary while changing data generation to streaming/spooling. Do not reintroduce unchecked writes or write directly to final paths.

## Remaining risks and order

- Issue #7 remains responsible for batch-bounded memory, streamed forward SQL, rollback spooling, multi-row DML, keyset chunk boundaries, metadata caching, bounded deterministic concurrency, and benchmarks.
- Issue #8 remains responsible for config/proxy normalization, safe DSNs, SSH verification/lifecycle, context/pool controls, credential cleanup, and guarded reset.
- Issue #9 remains responsible for live MySQL/PostgreSQL E2E, CI gates, coverage thresholds, operator docs, and final epic evidence.
- Remaining order is **#7 → #8 → #9**. Do not start #8 while executing #7.

## Copy/paste prompt for independent Session 6 / Issue #7

```text
You are the only code execution session for DataSmith remediation Session 6. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed dependency #2: https://github.com/jacktea/data-smith/issues/2
- Completed dependency #3: https://github.com/jacktea/data-smith/issues/3
- Completed dependency #4: https://github.com/jacktea/data-smith/issues/4
- Completed dependency #5: https://github.com/jacktea/data-smith/issues/5
- Completed dependency #6: https://github.com/jacktea/data-smith/issues/6
- This task #7: https://github.com/jacktea/data-smith/issues/7
- Later #8: https://github.com/jacktea/data-smith/issues/8
- Later #9: https://github.com/jacktea/data-smith/issues/9

Committed baselines:
- Issue #2: 697b398 fix: harden migration execution
- Issue #3: ed79f25 fix: make exec sql handling safe
- Issue #4: 0e846c2 fix: make data comparison exact
- Issue #5: f0e45c4 fix: generate deterministic schema sql
Session 5 / Issue #6 changes may still be uncommitted when you start. Preserve them exactly; inspect the current working tree and this handoff rather than assuming commit state.

Required baseline and preservation rules:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #7, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md before editing.
2. Preserve all completed #2-#6 behavior. Do not weaken migration discovery/ledger/locking/checksum, exec-sql scanner/transaction/dry-run, exact numeric comparison, verified count/min/max hash gating, schema qualification/identifier escaping, deterministic ordering, deferred foreign keys, view dependency/cycle handling, TABLE↔VIEW transitions, RunE/root-only exit semantics, default fail-fast, explicit incomplete best-effort reports, checked write/flush/sync/close, or atomic paired forward/rollback publication and restoration.
3. Protected user artifacts must remain exact: datasmith is present, executable, size 7301362, mode -rwxr-xr-x, SHA-256 efadb73998d2e845b88a5d3465b12a3b21e3c4d82d59c74a1f1202cc7dd56253, status M. CODE_REVIEW_REPORT.md is present, size 21194, SHA-256 9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b, status ??. Do not delete, overwrite, truncate, restore, stage, or commit either file. Avoid every build command that emits ./datasmith. If an accident changes datasmith, restore only from the verified backup /tmp/datasmith-session5.VQf6OJ/datasmith and report it.
4. Do not create a worktree, switch branches, commit, push, or create a pull request. The scheduler will inspect and decide later Git operations.
5. Establish go test, race, and vet baselines before changing code and distinguish pre-existing failures from regressions.
6. Do not start Issue #8 connection/reset/security work or Issue #9 E2E/CI work.

Issue #7 complete scope:
- Stream forward SQL directly from comparison callbacks without accumulating DataDiff rows as differing-row count grows.
- Spool rollback fragments per table to bounded temporary storage, then merge them in safe reverse dependency/rule order without retaining all rollback rows in memory.
- Preserve Session 5's same-directory temporary-file, checked write/flush/sync/close, fail-fast/best-effort, and paired final publication semantics. All spool files must be session-owned, checked, and cleaned on every path.
- Add bounded multi-row INSERT and DELETE generation while retaining precise changed-column UPDATE statements. Preserve dialect correctness, quoting, schema qualification, deterministic row/order semantics, and configurable batch limits.
- Replace deep OFFSET and whole-table ROW_NUMBER chunk boundary discovery with keyset boundary discovery for MySQL and PostgreSQL. Preserve unbounded first/last ranges and exact count/min/max verification before probabilistic hash skipping.
- Cache table models and batch metadata extraction within one run without leaking stale metadata across runs.
- Add only bounded per-table concurrency where deterministic output, connection/resource caps, fail-fast cancellation, and best-effort failure collection are preserved. A sequential default is acceptable unless the issue explicitly requires a new nonzero default.
- Add representative low-difference and high-difference benchmarks. Record before/after peak memory, query count, SQL output size, and elapsed time with reproducible parameters.

Acceptance criteria to prove individually:
- Peak diff memory remains batch-bounded as total differing-row count grows; benchmarks/tests must demonstrate the bound rather than only report elapsed time.
- Forward SQL streams and rollback fragments spool/merge without retaining all differing rows.
- Chunk discovery performs keyset progression and no progressively deeper OFFSET/whole-table ROW_NUMBER scans.
- Multi-row DML obeys configured bounds, remains deterministic, keeps precise changed-column UPDATEs, and generates safe rollback ordering.
- Metadata extraction is cached per run and query-count evidence shows the improvement.
- Any concurrency is bounded, deterministic, race-free, and respects database/resource limits.
- Default fail-fast and explicit best-effort semantics from #6 remain unchanged and covered.
- Atomic output failure-mode tests from #6 still pass.
- Focused performance/correctness tests and benchmarks pass.
- go test ./... passes.
- go test -race ./... passes.
- go vet ./... passes.
- git diff --check passes.
- datasmith and CODE_REVIEW_REPORT.md retain the exact protected state above; staged diff stays empty.

Compatibility and implementation guidance:
- Prefer additive/error-returning interfaces and compatibility wrappers for exported APIs.
- Keep public DataDiff-returning helpers for compatibility if needed, but route the CLI through new streaming handlers.
- Make batch sizes/resource limits explicit, validated, and deterministic. Do not use unbounded goroutines, channels, in-memory rollback slices, or whole-result buffers.
- Reuse Session 5 atomic output callbacks; do not bypass them by opening final files directly.
- Do not use real credentials. If live databases, external permissions, destructive actions, or product decisions are required, stop and request user direction.

Handoff requirements:
- Update docs/remediation-plan.md with Session 6 status, per-criterion benchmark/test evidence, limits, risks, and remaining #8 → #9 order.
- Replace docs/remediation-handoff.md with the latest Session 6 summary, changed files, exact commands/results, streaming/spooling/batching/cache/concurrency compatibility decisions, benchmark before/after data, protected-file evidence, and a complete copy/paste prompt for independent Issue #8.
- Post a complete GitHub Issue #7 update with benchmark tables, query/memory evidence, exact verification commands/results, remaining limitations/blockers, and the Issue #8 prompt. Close #7 only if every acceptance criterion is satisfied; otherwise leave it open and state gaps.
- Final response must list completed work, changed files, per-criterion evidence, benchmark and test results, residual risks, Issue #7 status, issue-update link, and fallback Issue #8 prompt.
```
