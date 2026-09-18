# DataSmith Remediation Handoff

Latest completed phase: Session 6, issue [#7](https://github.com/jacktea/data-smith/issues/7), on 2026-09-18.

## Session 6 summary

- `diff-data` now routes through error-aware streaming comparison callbacks with caller-supplied cached table models. The compatibility helpers that return `DataDiff` remain, but the CLI no longer retains all differing rows.
- Forward SQL streams into the Issue #6 atomic staged writer in default fail-fast mode. Each table writes rollback SQL to a session-owned, checked spool and successful spools merge in reverse rule order. Best-effort adds a bounded table-local forward spool so a failed table cannot publish partial SQL.
- MySQL and PostgreSQL support deterministic bounded multi-row INSERT/DELETE through additive `IDataBatchDialect`; UPDATE continues to write only actual changed columns. `--dml-batch-size` defaults to 1000 and rejects values above 10000.
- MySQL deep `OFFSET` and PostgreSQL whole-table `ROW_NUMBER()` range discovery were replaced with bounded `chunkSize+1` keyset pages. The first/last range semantics and exact count/min/max gate remain. `StatsAwareChunkRanger` reuses verified target statistics and avoids a duplicate stats query.
- Per-run source/target table-model caches reduce repeated-rule extraction calls from 15 to 2 in the query-count regression. Cache state does not escape the command invocation.
- Per-table concurrency remains one by design: output stays deterministic, no additional connections/goroutines exist, fail-fast stops immediately, and best-effort collects every failure.
- No live credentials or destructive database actions were used. No branch switch, worktree, commit, push, or pull request was created.

## Changed files

- `internal/datasmith/diff/diff_data.go` — CLI streaming path, per-run table caches, `--dml-batch-size`, and retained atomic pair boundary.
- `internal/datasmith/diff/streaming_data.go` — bounded DML stream, checked per-table rollback spools, best-effort forward isolation, reverse merge, cleanup, and hard resource limit.
- `internal/datasmith/diff/streaming_data_test.go` — memory bound, query count, DML batching, precise UPDATE, rollback order, spool cleanup, best-effort, and fail-fast atomic-pair tests.
- `internal/datasmith/diff/streaming_data_benchmark_test.go` — reproducible low/high-difference accumulated-before versus streaming-after benchmarks.
- `pkg/diff/data.go` — additive error-returning streaming APIs with caller-provided table metadata and callback-error cancellation.
- `pkg/diff/exact_comparison_test.go` — stats reuse query counts and callback-stop regression.
- `pkg/chunk/chunk.go` — additive `StatsAwareChunkRanger` interface.
- `pkg/db/mysql/mysql.go`, `pkg/db/mysql/data_safety_test.go` — bounded keyset boundary discovery and SQL-mock evidence.
- `pkg/db/postgres/postgres.go`, `pkg/db/postgres/data_safety_test.go` — bounded keyset boundary discovery and SQL-mock evidence.
- `pkg/sql/dialect.go` — additive batch-DML capability.
- `pkg/sql/mysql/mysql.go`, `pkg/sql/mysql/mysql_test.go` — deterministic multi-row INSERT/DELETE.
- `pkg/sql/postgres/postgre.go`, `pkg/sql/postgres/postgres_test.go` — deterministic schema-qualified multi-row INSERT/DELETE.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — Session 6 evidence, benchmark data, risks, remaining order, and Issue #8 prompt.

Unrelated protected files remain exact and unstaged:

- `datasmith` — executable, size `7301362`, mode `-rwxr-xr-x`, SHA-256 `efadb73998d2e845b88a5d3465b12a3b21e3c4d82d59c74a1f1202cc7dd56253`, tracked status `M`.
- `CODE_REVIEW_REPORT.md` — size `21194`, mode `-rw-r--r--`, SHA-256 `9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b`, untracked status `??`.
- `git diff --cached --name-only` is empty.
- The verified emergency backup `/tmp/datasmith-session5.VQf6OJ/datasmith` was not needed.

## Per-criterion acceptance evidence

- **Batch-bounded diff memory:** `TestStreamingBuffersStayBatchBoundedAsDifferencesGrow` emits 100000 differences with a 1000-row DML batch and proves at most 2000 row references are live across forward and rollback buffers. The high-difference benchmark reports 100000 buffered rows before versus 2000 after.
- **Forward streaming and rollback spooling:** the CLI calls `generateStreamingDataDiffOutputs`; it does not call `StreamCompareDataToDiff*`. Default forward writes originate in the compare callback. Rollback is per-table disk spool, reverse-merged, synced/closed, and cleaned on success/failure. Best-effort forward spooling is intentionally table-local and bounded for partial-table safety.
- **Keyset range discovery:** SQL-mock tests require fixed-size `LIMIT chunkSize+1` pages followed by `WHERE pk >= lastBoundary`. Static audit finds no `OFFSET`, `ROW_NUMBER`, or `row_number` in MySQL/PostgreSQL adapter Go code. Unbounded first/last ranges remain covered.
- **Exact hash gate and query count:** source and target count/min/max still match before hashing. `StatsAwareChunkRanger` receives the already verified target stats; the test records one stats call per side, one stats-aware range call, and zero legacy range calls.
- **Bounded deterministic DML:** both dialects implement multi-row INSERT and OR-composed primary-key DELETE with stable table/column/key order. Batch size defaults to 1000 with a hard 10000 cap. Tests prove three rows at limit two produce two statements, precise UPDATE omits unchanged columns, and rollback tables merge in reverse rule order.
- **Metadata cache:** for five repeated rules on one table, query-count evidence is 15 old extraction calls versus 2 after (one source and one target model for the run). Comparison accepts the cached target model and does not re-extract it.
- **Concurrency/resource limits:** table concurrency is intentionally one; there are no new goroutines/channels or connection consumers. Race tests pass. This is the safest bounded setting and preserves deterministic output/failure behavior.
- **Issue #6 behavior:** new-path tests prove fail-fast preserves old final files and best-effort attempts all rules, lists all failures, omits failed-table partial SQL, and leaks no spool. Existing write/flush/sync/close/rename fault injection continues to pass.
- **Compatibility:** old exported `DataDiff` helpers and `IDialect` remain source-compatible. New capabilities are additive. Migration, exec-sql, exact comparison, deterministic schema SQL, `RunE`, root-only exit, and atomic paired publication tests all pass unchanged.

## Benchmark results

Main reproducible command:

```sh
GOCACHE=/tmp/data-smith-session6-go-cache go test ./internal/datasmith/diff -run '^$' -bench '^BenchmarkDataDiffPipelines$' -benchmem -benchtime=1x -count=3
```

Environment: Darwin/arm64, Apple M4 Pro. DML batch size is 1000.

| Scenario | Pipeline | Elapsed observed | Total alloc/op | Peak buffered diff rows | SQL bytes/op |
| --- | --- | ---: | ---: | ---: | ---: |
| 10k rows / 100 differences | accumulated before | 0.281–0.382 ms | 80.8–81.3 KB | 100 | 11,980 |
| 10k rows / 100 differences | streaming after | 4.34–36.44 ms | 95.8–99.2 KB | 200 | 4,854 |
| 100k rows / 100k differences | accumulated before | 110.48–122.14 ms | 85.57 MB | 100,000 | 12,477,886 |
| 100k rows / 100k differences | streaming after | 67.38–67.86 ms | 46.13–46.17 MB | 2,000 | 5,285,286 |

Isolated peak-memory commands used a test binary in `/tmp` (never `./datasmith`):

```sh
GOCACHE=/tmp/data-smith-session6-go-cache go test -c -o /tmp/data-smith-session6-diff.test ./internal/datasmith/diff
/usr/bin/time -l /tmp/data-smith-session6-diff.test -test.run '^$' -test.bench '^BenchmarkDataDiffPipelines/high_difference/accumulated_before$' -test.benchmem -test.benchtime=1x -test.count=1
/usr/bin/time -l /tmp/data-smith-session6-diff.test -test.run '^$' -test.bench '^BenchmarkDataDiffPipelines/high_difference/streaming_after$' -test.benchmem -test.benchtime=1x -test.count=1
```

| Scenario | Pipeline | Maximum RSS | Darwin peak memory footprint |
| --- | --- | ---: | ---: |
| low difference | accumulated before | 55,017,472 B | 47,170,136 B |
| low difference | streaming after | 55,164,928 B | 47,104,600 B |
| high difference | accumulated before | 91,947,008 B | 83,968,624 B |
| high difference | streaming after | 86,327,296 B | 78,381,704 B |

The low-difference path intentionally trades fixed spool durability latency for bounded memory and smaller SQL. At high difference, buffered rows are capped, total allocations fall about 46%, generated SQL falls about 58%, elapsed time improves about 40%, and isolated peak RSS falls about 6%.

## Exact verification results

Baseline before Session 6 changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final Session 6 verification:

- `GOCACHE=/tmp/data-smith-session6-go-cache go test ./internal/datasmith/diff ./pkg/diff ./pkg/db/mysql ./pkg/db/postgres ./pkg/sql/mysql ./pkg/sql/postgres -count=1` — PASS
- `GOCACHE=/tmp/data-smith-session6-go-cache go test ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-session6-go-cache go test -race ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-session6-go-cache go vet ./...` — PASS
- `git diff --check` — PASS
- `rg -n 'OFFSET|ROW_NUMBER|row_number' pkg/db/mysql pkg/db/postgres --glob '*.go'` — no matches
- `rg -n '\bRun:' internal cmd --glob '*.go'` — no matches
- `rg -n 'os\.Exit' internal cmd --glob '*.go'` — only `internal/datasmith/root.go`
- Protected-file fingerprints and modes exactly match; staging is empty.

## Compatibility, resource, and risk decisions

- The sequential table limit is explicitly 1. No concurrency was added because deterministic output, current DB pool usage, immediate fail-fast, and full best-effort collection already satisfy the issue without introducing reorder buffers or cancellation races.
- Forward output remains inside the Issue #6 atomic staged file. Best-effort alone uses a per-table forward spool because a table may fail after emitting some rows; publishing those partial statements would be unsafe.
- Rollback order is the reverse of successful rule order, matching the existing contract. Rules should remain dependency ordered by the operator when foreign keys require a specific data order.
- Spools live beside the rollback staged output and are session-owned. Every write, sync, close, merge, removal, and final directory cleanup error is returned. The final paths are never opened directly by the streaming layer.
- Keyset discovery removes progressively deeper scans but still assumes a stable key view. Chunk hashing remains opt-in/probabilistic and concurrent writes outside a shared snapshot remain a documented risk.
- Low-difference synthetic latency regresses because durable spooling has fixed filesystem cost. High-difference memory, output size, and elapsed time improve materially. Live MySQL/PostgreSQL performance and snapshot behavior remain part of #9.
- Remaining order is **#8 → #9**.

## Copy/paste prompt for independent Session 7 / Issue #8

```text
You are the only code execution session for DataSmith remediation Session 7. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed issues #2-#7: https://github.com/jacktea/data-smith/issues/2 through https://github.com/jacktea/data-smith/issues/7
- This task #8: https://github.com/jacktea/data-smith/issues/8
- Later #9: https://github.com/jacktea/data-smith/issues/9

Committed baselines:
- #2: 697b398 fix: harden migration execution
- #3: ed79f25 fix: make exec sql handling safe
- #4: 0e846c2 fix: make data comparison exact
- #5: f0e45c4 fix: generate deterministic schema sql
- #6: ceb7938 fix: make cli output atomic
Session 6 / Issue #7 changes may still be uncommitted. Preserve the working tree exactly; inspect git status and the handoff rather than assuming commit state.

You are the only task allowed to write code in the shared checkout. Do not create a worktree, switch branches, commit, push, or create a pull request. Do not start Issue #9.

Before editing:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #8, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md completely.
2. Establish and record baselines: go test ./... -count=1, go test -race ./... -count=1, and go vet ./.... Use a /tmp GOCACHE if the macOS sandbox denies the default cache. Do not run a build command that emits ./datasmith.
3. Preserve all #2-#7 behavior: migration locking/checksums/status, exec-sql scanner/transaction/dry-run, exact numeric comparison and exact count/min/max before opt-in hashes, deterministic schema-qualified dependency-safe SQL, RunE/root-only os.Exit, default fail-fast, explicit best-effort incomplete reports, checked/paired atomic outputs, streamed forward SQL, session-owned rollback spools, bounded deterministic multi-row DML, precise UPDATE columns, keyset chunk discovery, per-run metadata caches, and the sequential deterministic table limit.
4. Protect unrelated user artifacts exactly:
   - datasmith: tracked M, executable, 7301362 bytes, mode -rwxr-xr-x, SHA-256 efadb73998d2e845b88a5d3465b12a3b21e3c4d82d59c74a1f1202cc7dd56253.
   - CODE_REVIEW_REPORT.md: untracked, 21194 bytes, mode -rw-r--r--, SHA-256 9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b.
   Do not delete, overwrite, truncate, restore, stage, or commit them. The verified emergency backup /tmp/datasmith-session5.VQf6OJ/datasmith may be used only if datasmith is accidentally changed, and any restoration must be reported.
5. Keep the staging area empty.

Issue #8 complete scope:
- Preserve ConnConfig.Proxy any compatibility, but normalize YAML-decoded proxy values through explicit validated accessors. Cover accepted concrete/map shapes and actionable rejection of invalid shapes; never silently ignore configured SSH proxy data.
- Clone connection configuration before applying tunnel endpoints, schema defaults, timeouts, or pool defaults. Prove caller-owned config values are unchanged on success and failure.
- Build MySQL DSNs with the official driver configuration/formatter and PostgreSQL DSNs with url.URL/url.Values or equivalent correct encoding. Cover special characters in usernames, passwords, database names, schemas, and query parameters. Errors/logs must not leak credentials.
- Remove insecure host-key callbacks. Require a validated known_hosts file or an explicitly pinned host fingerprint, with clear errors for missing/mismatched verification data.
- Make SSH tunnel lifecycle complete: bind a system-assigned local port, retain and close the listener, cancel/wait for forwarding goroutines, close SSH resources, and make Stop idempotent and race-free. Tests must prove no accepted connection or goroutine remains after Stop.
- Add an optional ContextDBAdapter (or similarly additive interface) so internal long-running paths use context cancellation/deadlines while legacy exported methods remain source-compatible. Configure and validate DB pool limits/lifetimes without mutating input config.
- Harden reset-db: add --dry-run, require --yes for execution, reject empty/system database or schema targets, quote identifiers with the existing dialect-safe helpers, and perform all validation before destructive SQL.
- Replace repository test/sample credentials with environment variables or unmistakable non-secret placeholders. Do not introduce or use real credentials.

Acceptance criteria to prove individually:
- YAML SSH proxy configuration is exercised by tests and cannot be silently ignored.
- Input ConnConfig is byte/value equivalent before and after connection/tunnel setup attempts.
- Special-character credentials produce valid, round-trippable DSNs without appearing in errors/logs.
- SSH verification rejects absent/mismatched trust and accepts known_hosts or a pinned fingerprint.
- Tunnel Stop is repeatable, closes the listener/connections, waits for goroutines, and passes race tests.
- Long database operations respond to context cancellation/timeouts; pool settings are validated and applied.
- reset-db cannot execute without --yes, dry-run performs no mutation, unsafe targets are rejected before DB calls, and identifiers are quoted.
- Existing #2-#7 focused and full regression tests remain green.
- go test ./... -count=1, go test -race ./... -count=1, go vet ./..., and git diff --check pass.
- datasmith and CODE_REVIEW_REPORT.md retain the exact fingerprints/status above; staged diff is empty.

Implementation principles:
- Prefer additive interfaces and compatibility wrappers; do not break public DBAdapter or ConnConfig consumers unnecessarily.
- Put validation before network/database/destructive work. Use context cancellation and bounded waits; do not leak goroutines, listeners, connections, passwords, or temporary resources.
- Use deterministic unit tests with local listeners/fakes/sqlmock. Do not require real SSH/database credentials. If acceptance genuinely requires external credentials, destructive live operations, or a product decision, stop and ask the user.

Completion requirements:
- Update docs/remediation-plan.md with Session 7 status, per-criterion evidence, compatibility/security decisions, exact commands/results, limitations, and remaining #9.
- Replace docs/remediation-handoff.md with Session 7 summary, complete changed-file list, tests/evidence, protected-file proof, risks, and a complete copy/paste Issue #9 prompt.
- Post a complete GitHub Issue #8 update with exact evidence and the Issue #9 prompt. Close #8 only if every acceptance criterion is satisfied; otherwise leave it open and list gaps.
- Final response must list completed work, changed files, per-criterion evidence, test results, residual risks, Issue #8 status/comment link, and the full fallback Issue #9 prompt.
```
