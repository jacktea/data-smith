# DataSmith Remediation Handoff

Current boundary: Session 8 / Issue [#9](https://github.com/jacktea/data-smith/issues/9) is complete. The authorized review-matrix update, commit/push, and clean-checkout remote CI verification are complete. Epic [#1](https://github.com/jacktea/data-smith/issues/1) is ready for closure after the final issue updates.

## Session 8 result

- Added four disposable, fixed-version Docker Compose services: MySQL 8.4.3 source/target and PostgreSQL 17.2 source/target. They use tmpfs storage, health checks, bounded waits, placeholder-only credentials, deterministic init SQL, and a fixture identity marker verified before destructive database setup/teardown.
- Added integration-tagged E2E for both engines: initialize source/target, generate schema and data forward/rollback SQL, apply forward, prove second schema/data diff empty, apply rollback, and prove the original schema and rows restored.
- Covered non-public and mixed-case PostgreSQL schema, dependency-safe table/FK/view order, BIGINT above 2^53, NULL versus empty, reserved/mixed-case identifiers, embedded backticks/double quotes, and bounded batch traversal.
- Added live migration success, failure, same-checksum retry, repeat/idempotent skip, checksum drift, one-row ledger, and lock contention checks for both engines.
- Added live MySQL CLI generation checks for atomic schema/data forward and rollback pairs with the source execution marker.
- Added pinned GitHub Actions gates for gofmt, unit tests, race, vet, staticcheck, govulncheck, dual-engine integration, and coverage thresholds.
- Fixed defects found only by live fixtures: schema/table scoping in MySQL primary-key metadata, nonexistent MySQL foreign-key metadata relation, and PostgreSQL search-path identifier quoting.
- Upgraded to Go toolchain 1.26.8 and `golang.org/x/crypto` 0.56.0 to eliminate reachable govulncheck findings. Staticcheck-driven cleanup removed unused private helpers and fixed a possible nil dereference.

## Session 8 changed files

- `.github/workflows/ci.yml`
- `README.md`
- `CODE_REVIEW_REPORT.md` (authorized status-matrix update)
- `docs/remediation-plan.md`
- `docs/remediation-handoff.md`
- `go.mod`
- `go.sum`
- `internal/datasmith/diff/atomic_output.go`
- `internal/datasmith/diff/diff_schema.go`
- `internal/datasmith/diff/integration_test.go`
- `pkg/db/mysql/mysql.go`
- `pkg/db/postgres/postgres.go`
- `pkg/db/postgres/connection_test.go`
- `pkg/diff/data.go`
- `pkg/diff/data_test.go`
- `pkg/diff/exact_comparison_test.go`
- `pkg/diff/schema_test.go` (pre-existing formatting defect corrected)
- `pkg/migrate/migrate_test.go`
- `pkg/sql/generator_test.go`
- `pkg/sql/ident/ident_test.go`
- `scripts/integration-test.sh`
- `scripts/check-coverage.sh`
- `test/integration/docker-compose.yml`
- `test/integration/e2e_test.go`
- `test/integration/fixtures/mysql-init.sql`
- `test/integration/fixtures/postgres-init.sql`

The tracked modified executable `datasmith` and untracked `CODE_REVIEW_REPORT.md` are protected user files, not Session 8 edits.

## Reproducible verification commands

```bash
test -z "$(git ls-files -co --exclude-standard -z '*.go' | xargs -0 gofmt -l)"
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
./scripts/integration-test.sh
./scripts/check-coverage.sh
git diff --check
git diff --cached --name-only
```

No command builds to or overwrites `./datasmith`.

## E2E evidence

| Engine | Schema/data apply | Second diff | Rollback | Migration lifecycle |
| --- | --- | --- | --- | --- |
| MySQL 8.4.3 | PASS | empty | original schema/rows restored | success, failed ledger, retry, repeat, checksum drift, lock PASS |
| PostgreSQL 17.2 | PASS in non-public `DataSmith_App` schema | empty | original schema/rows restored | success, failed ledger, retry, repeat, checksum drift, advisory lock PASS |

Every destructive setup/drop is limited to names hard-coded by the tests and occurs only after the control database returns fixture id `data-smith-integration-v1`. Containers and tmpfs data are removed by an exit trap.

## Coverage evidence

`./scripts/check-coverage.sh` runs all tests with `-tags=integration -covermode=atomic -coverpkg=./...`, de-duplicates identical cover blocks across test binaries, and applies weighted statement thresholds without excluding failing or important packages.

| Scope | Covered / total | Coverage | Required |
| --- | ---: | ---: | ---: |
| Overall | 3,199 / 4,413 | 72.5% | >=60% |
| db | 585 / 752 | 77.8% | >=70% |
| diff | 965 / 1,342 | 71.9% | >=70% |
| sql | 759 / 1,083 | 70.1% | >=70% |
| migrate | 284 / 402 | 70.6% | >=70% |
| exec | 348 / 396 | 87.9% | >=70% |

## P0/P1 regression map

| Original finding | Automated evidence |
| --- | --- |
| P0 `.down.sql`/JSON executed as upgrades | `internal/datasmith/migrate/local/parser_test.go` |
| P0 failed SQL re-executed for diagnosis | `internal/datasmith/exec/exec_sql_test.go`, `exec_sql_safety_test.go` |
| P1 chunk hash missed rows below first target key | `pkg/diff/exact_comparison_test.go`, adapter safety tests |
| P1 migration atomicity/placeholders/ledger gaps | `pkg/migrate/migrate_test.go`, dual-engine migration E2E |
| P1 unreadable migration recorded as success | `TestMigrationFileReadContentReportsMissingFile`, preflight tests |
| P1 integer/decimal float64 corruption | exact comparator tests plus >2^53 live rows |
| P1 non-public PostgreSQL schema unreliable | PostgreSQL E2E in `DataSmith_App` |
| P1 table/view type transition ignored | `pkg/diff/schema_test.go` |
| P1 incomplete dependency order | SQL generator golden/unit tests plus live FK/view apply |
| P1 cursor errors ignored | MySQL/PostgreSQL `data_safety_test.go` |
| P1 table failure still successful/partial output | internal diff fail-fast and atomic output tests |
| P1 transaction cleaning changed business content | SQL scanner unit/fuzz tests |
| P1 YAML SSH proxy silently ignored | `pkg/config/config_test.go` |
| P1 SSH identity/lifecycle unsafe | `pkg/proxy/proxy_test.go` |

No P0/P1 residual limitation needs Epic approval; every original P0/P1 has an automated regression, and the authorized report matrix now records the status and evidence.

## Static analysis and CI status

- gofmt verification — PASS.
- `go test ./... -count=1` — PASS.
- `go test -race ./... -count=1` — PASS.
- `go vet ./...` — PASS.
- staticcheck v0.8.1 — PASS.
- govulncheck v1.1.4 — PASS with zero reachable vulnerabilities; one advisory in imported/required code is not called.
- Docker/Compose and both integration suites — PASS locally.
- `git diff --check` — PASS.
- GitHub Actions remote run — PASS: [run 35419579967](https://github.com/jacktea/data-smith/actions/runs/35419579967), including dual-database E2E/coverage and unit/race/static-analysis jobs.
- Final remediation commit — `6a7e1dc` pushed to `origin/main`.

## Compatibility and remaining P2/P3 backlog

- Historical successful migration rows with null checksum remain accepted for compatibility; drift cannot be proven for those old rows.
- Old ledgers with duplicate versions must be reconciled before the unique version index upgrade can succeed.
- MySQL DDL is not transactional; if DDL commits but the success-ledger update fails, inspect schema and ledger before retrying.
- SQL scanning is lexical; client protocols such as MySQL `DELIMITER` and PostgreSQL `COPY ... FROM STDIN` remain unsupported.
- Chunk hash remains an opt-in probabilistic optimization after exact count/min/max checks; concurrent writes outside a shared snapshot remain a limitation.
- View order depends on visible dependency metadata; permissions that hide metadata can prevent correct ordering.
- Cross-file atomic publication minimizes but cannot eliminate the OS crash window between two final renames.
- Third-party legacy `DBAdapter` implementations cannot cancel a database call already in progress unless they implement `ContextDBAdapter`.
- SSH tests are local and credential-free; no external bastion was used.

Session 6 performance evidence remains: at 100k rows / 100k differences, streaming reduced elapsed time from 110.48–122.14 ms to 67.38–67.86 ms, allocation from 85.57 MB to 46.13–46.17 MB, SQL bytes from 12,477,886 to 5,285,286, and peak buffered rows from 100,000 to 2,000. Low-difference runs pay deliberate spool lifecycle overhead.

## Protected-file proof

- `datasmith`: tracked `M`, executable, 7,401,858 bytes, SHA-256 `84fd988415654588c2bfc2a14b2b2490575d43d6d0e46ea61264e1a41cfc1c57`.
- `CODE_REVIEW_REPORT.md`: tracked in final remediation commit; the authorized update is included in `6a7e1dc`.
- `datasmith` remains an unrelated tracked modification and was not staged or committed.

## Closure result

Issue #9 and Epic #1 were closed after the authorized report update, the pushed commit, and the green remote CI run. The modified `datasmith` binary remains preserved in the working tree.
