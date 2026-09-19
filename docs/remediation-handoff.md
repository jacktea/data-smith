# DataSmith Remediation Handoff

Current boundary: Session 7 / Issue [#8](https://github.com/jacktea/data-smith/issues/8) is complete in the shared checkout. No commit, push, branch switch, worktree, or pull request was created. Session 8 / Issue [#9](https://github.com/jacktea/data-smith/issues/9) is the only remaining child issue.

GitHub evidence: Issue #8 was updated with the complete evidence and Issue #9 prompt at [issue comment 5738300406](https://github.com/jacktea/data-smith/issues/8#issuecomment-5738300406), then closed as completed.

## Session 7 summary

- Preserved `ConnConfig.Proxy any` while adding strict normalization for concrete and YAML map shapes. Malformed configured proxies now fail explicitly instead of being ignored.
- Cloned caller-owned connection configs before proxy endpoints, PostgreSQL defaults, timeouts, or pool defaults are applied. Success and failure tests require value equality with the input.
- Replaced manual DSNs with the official MySQL config formatter and encoded PostgreSQL URLs/query values, including safe schema/search-path handling and credential redaction.
- Replaced insecure SSH host-key acceptance with known_hosts or pinned SHA-256 verification. Tunnels now use a system-assigned port, own/close their listener and accepted connections, cancel and wait for forwarding, and tolerate concurrent/repeated `Stop`.
- Added an optional `ContextDBAdapter`, context-aware streaming comparison entry points, context-aware constructors, and context-aware `exec-sql`/reset execution while retaining every legacy public method.
- Added validated/defaulted pool and lifetime settings on cloned configs.
- Hardened `reset-db` with `--dry-run`, mandatory `--yes`, pre-connection dangerous-target rejection, context execution, and dialect-safe identifier quoting.
- Replaced repository test/sample credential strings with obvious placeholders and documented the new connection/SSH/reset workflow.
- Preserved all Issue #2–#7 behavior and the later rollback safety fix: forward and rollback scripts retain `-- DATASMITH EXECUTE-ON: source`; `exec-sql` still requires an explicit database and blocks source-only scripts on target; README commands still execute generated diff and rollback SQL on source.

## Changed files

- `README.md`
- `configs/config.yaml`
- `docs/remediation-plan.md`
- `docs/remediation-handoff.md`
- `internal/datasmith/diff/diff_data.go`
- `internal/datasmith/diff/diff_schema.go`
- `internal/datasmith/exec/exec_sql.go`
- `internal/datasmith/exec/exec_sql_safety_test.go`
- `internal/datasmith/migrate/migrate_script.go`
- `internal/datasmith/migrate/reset.go`
- `internal/datasmith/migrate/reset_test.go`
- `pkg/config/config.go`
- `pkg/config/config_test.go`
- `pkg/conn/db.go`
- `pkg/db/base/base.go`
- `pkg/db/base/base_test.go`
- `pkg/db/driver.go`
- `pkg/db/mysql/mysql.go`
- `pkg/db/mysql/connection_test.go`
- `pkg/db/postgres/postgres.go`
- `pkg/db/postgres/connection_test.go`
- `pkg/db/postgres/postgres_test.go`
- `pkg/diff/data.go`
- `pkg/diff/context_test.go`
- `pkg/migrate/migrate.go`
- `pkg/migrate/reset_test.go`
- `pkg/proxy/proxy.go`
- `pkg/proxy/ssh.go`
- `pkg/proxy/proxy_test.go`
- `pkg/sql/postgres/postgres_test.go`

The tracked modified executable `datasmith` and untracked `CODE_REVIEW_REPORT.md` are protected user files, not Session 7 edits.

## Per-criterion acceptance evidence

- **YAML proxy normalization:** `TestSSHProxyConfigNormalizesYAMLMap`, `TestSSHProxyConfigAcceptedConcreteAndMapShapes`, and `TestSSHProxyConfigRejectsConfiguredInvalidShapes` cover real YAML decoding, both concrete forms, both supported map forms, typed/field/key/type failures, and unknown fields. `BaseAdapter.Init` always calls the accessor.
- **Caller config immutability:** `TestConnConfigCloneDoesNotShareMutableValues`, `TestInitClonesConfigBeforeTunnelEndpointMutation`, `TestInitFailureLeavesCallerConfigUnchanged`, `TestNewMySQLAdapterValidationDoesNotMutateInput`, `TestNewMySQLAdapterConnectionFailureDoesNotMutateInput`, and `TestNewPostgresAdapterConnectionFailureDoesNotMutateInput` cover mutable maps/proxies, successful tunnel setup, and validation/connection failures.
- **DSN safety and redaction:** `TestBuildMySQLDSNRoundTripsSpecialCharacters` parses the official formatted DSN back through `mysql.ParseDSN`. `TestBuildPostgresDSNRoundTripsSpecialCharacters` parses the generated URL and verifies user, password, slash/question-mark database name, special schema/search path, and query parameters. `TestRedactErrorDoesNotLeakCredentials` covers raw and URL-escaped user/password forms.
- **SSH trust:** `TestCreateSSHTunnelRequiresHostVerification`, `TestPinnedHostFingerprintAcceptsOnlyPinnedKey`, and `TestKnownHostsCallbackAcceptsMatchingEntryAndRejectsMismatch` prove missing/mismatched trust is rejected and both approved trust models work. Static search finds no `InsecureIgnoreHostKey`.
- **Tunnel lifecycle:** `TestSSHTunnelStopClosesListenerAndAcceptedConnections` uses `127.0.0.1:0`, establishes a real local connection, waits for the injected forwarding goroutine, calls eight concurrent stops plus another repeat, and verifies the goroutine, connection, and listener are all closed. Full race passes.
- **Context and pools:** MySQL/PostgreSQL adapters implement the additive context capability with `QueryContext`; streaming comparison uses it from Cobra context. `exec-sql` uses `BeginTx`/`ExecContext`, reset uses `ExecContext`, constructors use `PingContext`, and tests prove cancellation/deadline behavior. Pool normalization rejects negative/inconsistent values, supplies bounded defaults, and applies settings before ping; `TestApplyConnectionSettingsConfiguresPoolLimit` verifies the applied open-connection cap.
- **reset-db:** `TestResetDBRequiresYesBeforeReadingConfig` proves the confirmation gate precedes config/DB work. `TestResetDBDryRunPrintsSQLWithoutConnecting` uses an unreachable host and succeeds, proving no connection. `TestResetDBRejectsDangerousTargetBeforeDBConnection` and `TestResetDatabaseRejectsDangerousTargetBeforeConnectionAccess` prove system targets fail pre-DB. `TestBuildResetSQLQuotesIdentifiers` covers embedded backticks and quotes. `TestResetDatabaseContextHonorsDeadline` proves cancellation.
- **Credentials and previous sessions:** hard-coded credential-like samples were replaced with `obvious-placeholder`/`obvious-test-placeholder`. Full #2–#7 regression suites pass. Static checks retain only root `os.Exit`, source execution markers, explicit DB selection, and target blocking.

## Exact verification results

Baseline before Session 7 edits:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS

Final Session 7 verification:

- `go test ./... -count=1` — PASS
- `go test -race ./... -count=1` — PASS
- `go vet ./...` — PASS
- `git diff --check` — PASS
- focused configuration/proxy/base/MySQL/PostgreSQL/diff/migrate/exec suites — PASS
- static safety audit (`InsecureIgnoreHostKey`, random fixed-range tunnel port, manual DSN patterns, source marker/target guard, root-only `os.Exit`) — PASS

Protected-file proof after the final important test rounds:

- `datasmith`: tracked `M`, executable `-rwxr-xr-x`, 7,401,858 bytes, SHA-256 `84fd988415654588c2bfc2a14b2b2490575d43d6d0e46ea61264e1a41cfc1c57`
- `CODE_REVIEW_REPORT.md`: untracked `??`, 21,194 bytes, SHA-256 `9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b`
- staged diff: empty

## Compatibility, security, and residual-risk decisions

- `DBAdapter`, its legacy methods, `NewDBAdapter`, both legacy database constructors, `ExecuteSQL`, `ResetDatabase`, and all pre-existing streaming entry points remain. New context methods/functions are additive wrappers/capabilities.
- MySQL's official DSN grammar cannot escape `:` in a username. The implementation deliberately uses `mysql.Config.FormatDSN` and covers supported reserved characters instead of inventing a non-driver-compatible escape. Passwords and database/query values retain official encoding behavior; PostgreSQL userinfo/path/query components use standard URL encoding.
- Third-party adapters implementing only legacy `DBAdapter` remain compatible. The helper checks cancellation before their legacy read, but only adapters implementing `ContextDBAdapter` can interrupt a database call already in progress.
- SSH tests are deterministic and credential-free: generated keys, temporary known_hosts, local listeners, and injected forwarding. A live SSH bastion test was not attempted because no approved external host or credentials were supplied.
- Pool duration values remain `time.Duration`; YAML numeric values are nanoseconds and README states this explicitly. Zero values mean bounded defaults.
- Live MySQL/PostgreSQL apply/empty-diff/rollback, migration, concurrent-mutation, CI, and coverage work remains intentionally assigned to Issue #9.

## Copy/paste prompt for independent Session 8 / Issue #9

```text
You are the only code execution session for DataSmith remediation Session 8. Work directly in the saved local checkout:

/Users/xiaogang/github/jacktea/data-smith

GitHub:
- Epic #1: https://github.com/jacktea/data-smith/issues/1
- Completed issues #2-#8: https://github.com/jacktea/data-smith/issues/2 through https://github.com/jacktea/data-smith/issues/8
- This task #9: https://github.com/jacktea/data-smith/issues/9

Committed historical baselines:
- #2: 697b398 fix: harden migration execution
- #3: ed79f25 fix: make exec sql handling safe
- #4: 0e846c2 fix: make data comparison exact
- #5: f0e45c4 fix: generate deterministic schema sql
- #6: ceb7938 fix: make cli output atomic
- #7: daaacdb perf: stream large data diffs
- additional rollback safety: 7a8a474 fix: guard sql execution target

Session 7 / Issue #8 is complete but intentionally uncommitted in the shared checkout. Preserve its working-tree changes exactly; inspect git status and docs/remediation-handoff.md rather than assuming commit state.

You are the only task allowed to write code in the shared checkout. Do not create a worktree, switch branches, commit, push, or create a pull request unless the user explicitly changes those instructions.

Before editing:
1. Read every applicable AGENTS.md (if any), Epic #1, Issue #9, current git status, docs/remediation-plan.md, and docs/remediation-handoff.md completely.
2. Establish and record baselines: go test ./... -count=1, go test -race ./... -count=1, go vet ./..., and git diff --check. Do not run a build command that writes ./datasmith.
3. Preserve the current protected files exactly:
   - datasmith: tracked M, executable, 7401858 bytes, SHA-256 84fd988415654588c2bfc2a14b2b2490575d43d6d0e46ea61264e1a41cfc1c57.
   - CODE_REVIEW_REPORT.md: untracked, 21194 bytes, SHA-256 9c529456726a93167b326e1a0c07f740ad013dcc42269f0a2b893539d2d3424b.
   Never delete, overwrite, truncate, restore, stage, or commit them. Recheck after every important test round. Keep the staging area empty.

Preserve all #2-#8 behavior:
- migration locking/checksums/states, SQL scanner/transaction/dry-run behavior, exact numeric comparison and opt-in hash gate, deterministic schema qualification/dependency ordering, RunE/root-only os.Exit, fail-fast/best-effort and checked atomic output pairs;
- streaming forward output, session-owned rollback spools, bounded deterministic multi-row DML, precise UPDATE, keyset chunking, per-run metadata cache, and table concurrency limit 1;
- EXECUTE-ON: source markers, explicit exec-sql database selection, blocking source-only scripts on target, and correct README commands;
- strict YAML proxy normalization with ConnConfig.Proxy any compatibility, caller config immutability, official/encoded credential-safe DSNs, verified SSH trust, complete race-free tunnel shutdown, additive context cancellation, validated pool defaults, reset-db --dry-run/--yes/preflight/identifier quoting, and placeholder-only repository credentials.

Issue #9 complete scope:
- Add Docker Compose fixtures for supported MySQL and PostgreSQL versions and integration tests behind explicit integration build tags. Keep default unit tests independent of Docker and external credentials.
- Exercise both engines end to end: seed source/target, generate schema/data forward and rollback SQL, apply forward, prove a second diff is empty, apply rollback, and prove the original state is restored.
- Cover migration success, failure, retry/repeat/idempotence, ledger/lock/checksum behavior, non-public PostgreSQL schemas, dependency ordering, BIGINT values above 2^53, NULL versus empty values, and reserved/mixed-case/embedded-delimiter identifiers.
- Add GitHub Actions gates for gofmt, go vet, staticcheck, race, govulncheck, unit tests, and dual-database integration tests. Pin action versions and database images; use health checks and bounded timeouts; never add real credentials.
- Reach at least 60% overall statement coverage and 70% on critical db/diff/sql/migrate/exec paths. Measure from reproducible commands, do not exclude failing/important packages to inflate numbers, and add meaningful tests rather than coverage-only assertions.
- Update README and CODE_REVIEW_REPORT.md/status matrix only if allowed by the protected-file rule. Because CODE_REVIEW_REPORT.md is explicitly protected in this handoff, stop and request user direction before modifying it unless the Session 8 prompt explicitly supersedes that protection. Update other operator documentation with final behavior, limitations, safety workflow, CI commands, coverage, E2E setup, and Session 6 performance results.

Acceptance criteria:
- MySQL and PostgreSQL E2E flows both pass apply → empty diff → rollback → restored state.
- Migration E2E covers success/failure/repeat and preserves the Session 1 safety ledger semantics.
- CI configuration is reproducible from a clean checkout and every configured local equivalent passes where tools are available.
- Overall statement coverage is at least 60%, with at least 70% on critical db/diff/sql/migrate/exec paths, supported by saved command output.
- Every original P0/P1 finding has an automated regression test or an explicitly documented residual limitation approved in Epic #1.
- gofmt verification, go test ./... -count=1, go test -race ./... -count=1, go vet ./..., staticcheck, govulncheck, integration suites, coverage commands, and git diff --check pass. If staticcheck/govulncheck or Docker is unavailable, do not claim success: record the exact blocker and keep #9 open.
- Protected files keep the exact fingerprints/status above and staged diff remains empty unless the user explicitly supersedes the protection.

Implementation rules:
- Use disposable Docker volumes/databases and deterministic fixture setup/teardown; no destructive operation may target an operator-provided or non-fixture database.
- Put integration tests behind build tags and environment guards. Validate fixture identity before destructive reset/drop operations.
- Use environment variables or unmistakable test-only placeholders for credentials. Never print credentials in CI logs, errors, generated artifacts, or test output.
- Preserve additive interfaces and existing public API compatibility. Do not weaken any security or atomicity guard to make E2E tests easier.
- If Docker, GitHub Actions execution, coverage policy decisions, protected-report editing, external credentials, or destructive real operations require user authority, stop at that boundary and request direction.

Completion requirements:
- Update docs/remediation-plan.md with Session 8 completion status, exact E2E/CI/coverage evidence, final limitations, and Epic #1 closure recommendation.
- Replace docs/remediation-handoff.md with final project state, complete changed-file list, reproducible verification commands, CI links if available, coverage tables, remaining P2/P3 backlog, protected-file proof, and closure recommendation.
- Post complete evidence to GitHub Issue #9 and close it only if every acceptance criterion is satisfied; otherwise keep it open and enumerate exact gaps.
- Update Epic #1 checklist/evidence and close it only if all child issues and epic completion criteria are satisfied.
- Final response must list completed work, changed files, E2E evidence for both engines, CI/static-analysis results, coverage numbers, residual risks, protected-file proof, and Issue #9/Epic status with links.
```
