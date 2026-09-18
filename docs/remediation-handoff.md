# DataSmith Remediation Handoff

Latest completed phase: Session 3, issue [#4](https://github.com/jacktea/data-smith/issues/4), on 2026-09-18.

## Session 3 summary

Data comparison is now type-aware without converting exact numeric domains to `float64`: integer/`BIGINT` values use `math/big.Int`, decimal/numeric values use `math/big.Rat`, and only float/double/real values use an absolute `1e-9` tolerance. NaN and infinities have stable comparison semantics. NULL remains distinct from an empty value.

MySQL and PostgreSQL now return `Rows.Err()` from every database cursor loop. Batch, chunk, table, column, primary-key, and composite boundary inputs are validated before the corresponding database operation; the CLI rejects invalid batch/chunk flags before config or rules files are read. Comparison errors now propagate through the `diff-data` Cobra command.

The opt-in chunk-hash path only skips row comparison after exact source/target count, minimum primary key, and maximum primary key checks, followed by matching probabilistic fingerprints for every chunk. The first range is unbounded below and the last is unbounded above. Hash row encodings distinguish NULL, empty values, and concatenation boundaries.

Migration behavior from issue #2/commit `697b398` and `exec-sql` behavior from issue #3/commit `ed79f25` were not changed.

## Changed files

- `pkg/diff/value_comparator.go` — exact integer and arbitrary-precision decimal comparison plus float-only tolerance and NaN/Inf handling.
- `pkg/diff/data.go` — preflight validation, verified hash statistics, unbounded first range consumption, and hash/cursor error propagation.
- `pkg/chunk/chunk.go` — additive `ChunkStats` and `VerifiedChunkHasher`; existing `ChunkHasher` remains unchanged.
- `pkg/db/postgres/postgres.go`, `pkg/db/mysql/mysql.go` — input validation, `Rows.Err()` checks, exact chunk stats, unbounded ranges, and NULL/empty-safe fingerprints.
- `internal/datasmith/diff/diff_data.go` — flag/rule preflight, `RunE` error propagation, and explicit probabilistic `--chunk-hash` help text.
- `pkg/diff/data_test.go` — typed mock column metadata.
- `pkg/diff/exact_comparison_test.go` — BIGINT, unsigned, decimal, NaN/Inf, NULL/empty, low-key, verified-hash, range, and preflight regressions.
- `pkg/db/postgres/data_safety_test.go`, `pkg/db/mysql/data_safety_test.go` — interrupted cursor and no-query-on-invalid-input regressions; PostgreSQL also covers unbounded hash range SQL and NULL markers.
- `internal/datasmith/diff/diff_data_validation_test.go` — CLI size and rule-key preflight regressions.
- `docs/remediation-plan.md`, `docs/remediation-handoff.md` — Session 3 evidence, risks, and Session 4 prompt.

Unrelated and preserved: tracked `datasmith` exists, is zero bytes, and remains status `M`; `CODE_REVIEW_REPORT.md` remains untracked. Neither is staged or included in Session 3 work.

## Acceptance evidence

- Adjacent BIGINT values above 2^53: `TestValueComparatorExactNumericSemantics/bigint_beyond_2^53` compares `9007199254740992` and `9007199254740993` as distinct and ordered.
- Unsigned and decimal precision: the same table covers `uint64(math.MaxUint64)`, high-precision decimal adjacency, trailing-zero equivalence, and proof that decimal values do not receive float tolerance.
- NaN and infinities: float NaN equals float NaN; equal infinities compare equal; negative infinity orders before positive; numeric `NaN`, `Infinity`, and `+Inf` aliases are covered.
- Source `{0,1}` versus target `{1}`: `TestStreamCompareDataReportsZeroPrimaryKey` records dropped primary key `0`.
- Boundary, NULL/empty, and composite keys: verified hash tests assert a nil lower bound for the first chunk; `TestValueComparatorExactNumericSemantics` keeps NULL distinct from empty; existing multi-column primary-key coverage and new mismatched-boundary validation exercise composite keys.
- Cursor interruption: SQL-mock `RowError` tests for both adapters assert the original error is returned. All cursor loops in both adapters now check `Rows.Err()`; the migration cursor already did. `diff-data` returns comparison failures through `RunE`.
- Invalid sizes before work: streaming APIs reject non-positive batches before schema extraction, adapter batch methods reject invalid sizes/keys before `Query`, and CLI validation runs before config/rules reads. Chunk size is rejected before stats/range/hash calls.
- Hash safety: `VerifiedChunkHasher` is required for skipping. Count/min/max mismatch prevents hashing and forces row comparison; equal stats plus matching fingerprints can skip. Source `{0,1}` versus target `{1}` therefore cannot hide key `0`.

## Exact verification results

Baseline before Session 3 changes:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final Session 3 verification:

- `GOCACHE=/tmp/data-smith-go-cache go test ./pkg/diff ./pkg/db/postgres ./pkg/db/mysql ./internal/datasmith/diff -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go test ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go test -race ./... -count=1` — PASS
- `GOCACHE=/tmp/data-smith-go-cache go vet ./...` — PASS
- `git diff --check` — PASS

Protected-file evidence after final verification:

- `datasmith` — present, size `0`, tracked status `M`.
- `CODE_REVIEW_REPORT.md` — present, size `21194`, untracked status `??`.
- `git diff --cached --name-only` — empty; nothing was staged.

## Comparison, hash, and compatibility limits

- Float/double/real retains the existing absolute `1e-9` tolerance. Integer and decimal/numeric never use it. A database driver must expose exact integer/decimal data as integer or text/bytes; precision already lost upstream in a `float64` cannot be reconstructed.
- Decimal/numeric finite values are parsed exactly, including exponents. Special values have the stable order `-Infinity < finite < Infinity < NaN`; two NaNs compare equal for diff purposes.
- Chunk hashes are probabilistic fingerprints, not an equivalence proof. PostgreSQL uses nested MD5; MySQL uses XOR-combined CRC32, which is weaker and order-insensitive. Exact count/min/max checks rule out common range omissions but not collisions or concurrent changes.
- Stats, range discovery, and hashes are not read under a shared cross-database snapshot. Concurrent writes can make a run internally inconsistent; leave `--chunk-hash` off when this risk is unacceptable.
- `--chunk-hash` remains opt-in. Existing third-party `ChunkHasher` implementations still compile, but safely fall back to row comparison unless they also implement the additive `VerifiedChunkHasher` statistics method.
- Exported comparison and streaming function signatures are unchanged. `CompareValues`/`AreValuesEqual` now intentionally provide exact semantics for integer and decimal types. The built-in MySQL/PostgreSQL adapters add `GetChunkStats` without changing `DBAdapter`.
- Live PostgreSQL/MySQL comparison E2E remains deferred to issue #9. Session 3 evidence is deterministic in-memory and SQL-mock coverage plus full repository test/race/vet.

## Blockers and residual risks

- No blocker remains for issue #4.
- Schema qualification, identifier quoting, deterministic SQL ordering, dependency ordering, and TABLE↔VIEW transitions are intentionally deferred to issue #5.
- Fail-fast behavior for other CLI paths and atomic output-file replacement remain issue #6; Session 3 only made `diff-data` comparison/cursor failures observable as command errors.
- Output files may contain partial results if a later table fails; issue #6 owns atomic output semantics.

## Copy/paste prompt for independent Session 4 / Issue #5

```text
You are the only code execution session for DataSmith remediation Session 4. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed dependency #2: https://github.com/jacktea/data-smith/issues/2
- Completed dependency #3: https://github.com/jacktea/data-smith/issues/3
- Completed dependency #4: https://github.com/jacktea/data-smith/issues/4
- This task #5: https://github.com/jacktea/data-smith/issues/5
- Next task #6: https://github.com/jacktea/data-smith/issues/6

Committed baseline before Session 3:
- Issue #2: 697b398 fix: harden migration execution
- Issue #3: ed79f25 fix: make exec sql handling safe
Session 3 / Issue #4 changes may still be uncommitted when you start. Preserve them exactly; inspect the working tree and this handoff rather than assuming commit state.

Required baseline and preservation rules:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #5, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md before editing.
2. Preserve all completed #2, #3, and #4 behavior. Do not revert, rewrite, or broaden migration discovery/ledger/locking/checksum, exec-sql scanner/transaction/dry-run, or exact comparison/verified chunk-hash semantics.
3. The tracked datasmith file must remain present, zero bytes, and status M. CODE_REVIEW_REPORT.md must remain untracked. Do not delete, overwrite, stage, or commit either user artifact. If a full-suite command overwrites datasmith, restore only its exact protected state: present, zero bytes, M.
4. Do not create a worktree, switch branches, commit, push, or create a pull request. The scheduler will inspect and decide later Git operations.
5. Establish go test, race, and vet baselines before changing code and distinguish pre-existing failures from regressions.

Issue #5 complete scope:
- Centralize dialect identifier quoting and qualified table names, including embedded quote escaping.
- Use qualified names in data reads and INSERT/UPDATE/DELETE generation.
- Model TABLE↔VIEW transitions as drop + create.
- Create new tables without foreign keys, then add foreign keys after all tables exist.
- Topologically order table deletion, foreign keys, and view creation; detect dependency cycles.
- Stabilize ordering of tables, columns, indexes, foreign keys, and output SQL.
- Add golden tests for non-public schemas, reserved/mixed-case names, mutual table references, view chains, transitions, and rollback.

Acceptance criteria to prove individually:
- Generated PostgreSQL data SQL works outside public.
- Added related tables do not depend on Go map iteration order.
- Repeated generation is byte-for-byte identical.
- Object type transitions produce executable changes.
- Focused and golden tests pass.
- go test ./... passes.
- go test -race ./... passes.
- go vet ./... passes.
- git diff --check passes.
- datasmith and CODE_REVIEW_REPORT.md retain their exact protected states.

Non-goals and constraints:
- Do not start issue #6. In particular, do not expand into general CLI fail-fast or atomic output-file work beyond what is strictly required by #5.
- Preserve exported APIs where practical. Do not weaken #4 exact numeric comparison, count/min/max hash gating, unbounded-first-range behavior, cursor error propagation, or preflight validation.
- Use no real credentials. If completion requires a live database, external permission, a product decision, or a destructive action, stop and request the user's decision.

Session handoff requirements:
- Update docs/remediation-plan.md with Session 4 status, per-criterion evidence, risks, and remaining #6→#9 order.
- Replace docs/remediation-handoff.md with the latest Session 4 summary, changed files, exact commands/results, dependency/ordering decisions, compatibility notes, blockers/risks, protected-file evidence, and a complete copy/paste prompt for independent Issue #6.
- Post a concise but complete GitHub Issue #5 update with dependency decisions, golden-test evidence, exact test commands/results, remaining limitations/blockers, and the Issue #6 prompt. Close #5 only if every acceptance criterion is satisfied; otherwise leave it open and state the gaps.
- Final response must list completed work, changed files, per-criterion evidence, test results, residual risks, Issue #5 status, issue-update link, and the fallback Issue #6 prompt.
```
