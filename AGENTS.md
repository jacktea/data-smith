# AGENTS.md

DataSmith（`github.com/jacktea/data-smith`）— Go CLI 数据库管理工具：库表结构比对、表数据比对、差异 SQL 生成与执行、版本迁移脚本、目标库重置。支持 MySQL 与 PostgreSQL，接口驱动的适配器架构。用法与功能细节见 `README.md`（以中文撰写，保持同步）。

## 常用命令

```bash
# 构建（产物 bin/datasmith）
./scripts/build.sh

# 单元测试（不启动 Docker、不读外部数据库凭据）
go test ./... -count=1
go test -race ./... -count=1

# 静态检查（CI 全部为硬性 gate）
test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)"
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

# 集成测试（需 Docker）：自动拉起 tmpfs MySQL 8.4.3 + PostgreSQL 17.2 fixtures，
# 退出时销毁。测试代码受双重保护：必须同时有 `integration` build tag 和
# DATASMITH_INTEGRATION=1 环境变量，二者缺一不可。
./scripts/integration-test.sh            # test 模式
./scripts/integration-test.sh coverage /tmp/profile.out

# 覆盖率 gate：overall >= 60%，db/diff/sql/migrate/exec 每组 >= 70%
./scripts/check-coverage.sh
```

CLI 子命令：`diff-schema`、`diff-data`、`exec-sql`、`reset-db`、`migrate-script`（示例配置在 `configs/`）。

## 架构

```
cmd/main.go                 仅入口；禁止业务逻辑
internal/
  config/                   配置加载与解析（YAML/JSON）
  datasmith/root.go         cobra 根命令；子命令包经 Install(rootCmd) 注册
  datasmith/diff/           diff-schema、diff-data（streaming_data.go 流水线，
                            atomic_output.go 原子写文件）
  datasmith/exec/           exec-sql（sql_scanner.go：词法级 SQL 语句拆分器）
  datasmith/migrate/        reset-db、migrate-script
pkg/
  conn/                     连接管理 + DBAdapter 接口（接口定义在此，避免 import cycle）
  db/                       driver.go 工厂（按 cfg.Type switch）；base/ 公共实现；
                            mysql/ postgres/ 驱动适配器
  diff/                     结构与数据比对核心算法
  sql/{mysql,postgres}/     方言 SQL 生成（forward + rollback，文件头带
                            `DATASMITH EXECUTE-ON:` 指示目标库）
  migrate/                  版本迁移逻辑（唯一版本、SHA-256 checksum、状态 ledger、
                            advisory lock；local/ 为本地状态存储）
  chunk/                    数据分块与 --chunk-hash 概率比对
  proxy/                    SSH 隧道/代理（强制主机身份校验）
  config/  consts/  logger/  utils/   配置模型、常量、日志、工具函数
```

数据流：`internal/datasmith/*`（CLI 编排）→ `pkg/diff`（比对）→ `pkg/sql`（方言 SQL 生成）→ `internal/datasmith/exec`（执行/回滚）。

## 开发约定

- **新增数据库类型**：`pkg/db/<type>/` 实现 `conn.DBAdapter`，`pkg/sql/<type>/` 实现方言生成，`pkg/consts/` 注册类型常量，`pkg/db/driver.go` 工厂加分支。不改动上层包。
- **配置外置**：连接信息、比对规则一律走 YAML/JSON 配置文件；禁止硬编码凭据。
- **代码位置**：只在 `internal/`、`pkg/`、`cmd/`、`scripts/`、`configs/` 下写代码；`main.go` 不含业务逻辑。
- **测试**：核心逻辑（diff/db/sql/migrate/exec）必须有单元测试，默认不依赖 Docker；真实数据库行为放 `integration` tag 的测试里，走 `scripts/integration-test.sh`。
- **CI gate**（`.github/workflows/ci.yml`）：gofmt 无 diff、单测、race、vet、staticcheck、govulncheck、双库 E2E + 覆盖率 gate 全部必须通过。工具版本固定（staticcheck v0.8.1、govulncheck v1.1.4），升级需同步 CI 与本文档。
- **提交**：语义化提交；PR 关联 issue 并过 review。目录结构或核心接口变更需团队评审。
- **进行中整改**：`docs/remediation-plan.md`、`docs/remediation-handoff.md`、`CODE_REVIEW_REPORT.md`。

## 操作安全红线（改动相关代码时必须维持）

1. forward/rollback SQL 在隔离副本生成并人工审阅后才能执行；两个文件都带 `DATASMITH EXECUTE-ON: source`，执行必须显式选择目标库。
2. `exec-sql` 先 `--dry-run` 或 `--tx` 验证；MySQL DDL 与 MySQL migration 的 dry-run 无可靠事务回滚，会被拒绝或要求人工恢复预案。
3. `reset-db` 先 `--dry-run` 再 `--yes`；系统数据库、空目标、危险 schema 在连接/破坏前直接拒绝。
4. SQL scanner 是词法扫描器而非完整客户端协议：不支持 MySQL `DELIMITER`、PostgreSQL `COPY ... FROM STDIN`，歧义脚本保守拒绝（保持该行为）。
5. SSH 代理必须配置 `knownHostsPath` 或 SHA-256 `hostFingerprint`，缺失时在数据库访问前失败。

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues on `jacktea/data-smith`, operated via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: root `CONTEXT.md` + `docs/adr/`. See `docs/agents/domain.md`.
