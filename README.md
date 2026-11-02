# DataSmith

DataSmith（`github.com/jacktea/data-smith`）是一个接口驱动的 Go 数据库管理工具：库表结构比对、表数据比对、差异 SQL 生成与执行、版本迁移脚本、目标库重置，并提供与 CLI 共享同一套引擎的 Web 控制台。支持 MySQL 与 PostgreSQL，所有连接信息与比对规则一律走 YAML/JSON 配置，forward/rollback SQL 成对生成、人工审阅后显式选择目标库执行。

---

## 功能总览

- **结构比对（diff-schema）**：表、列、索引、主键、外键、CHECK 约束、注释的差异检测。PostgreSQL 额外覆盖函数、存储过程、序列（含 SERIAL 隐式序列及 `OWNED BY`）与视图注释；table/view/routine/sequence 进入统一对象依赖 DAG——创建按正拓扑、删除按逆拓扑，重复生成字节一致。依赖环、动态 SQL `EXECUTE`、不支持语言或重载调用歧义会保守拒绝并报告对象链。
- **数据比对（diff-data）**：行级增/删/改差异与 DML 生成。行身份定位统一策略：主键 → 非空唯一索引（非部分、非表达式、全列 NOT NULL）→ 规则显式 `comparisonKey` 业务键 → 无可靠身份时 DELETE 用排序全行字段定位、UPDATE 不生成。规则可省略（整库模式）或通配；迁移账本表始终排除。
- **完全对比（diff-full）**：一次比对结构+数据，产出四份 SQL（结构/数据 × 正向/回滚），可经 `--migrate-dir` 一步合成迁移 up/down 对。数据比对默认两阶段影子事务（详见下文）；四产物原子发布，进程中断后恢复完整旧组或新组，不暴露混合产物。
- **SQL 执行（exec-sql）**：词法级语句拆分、`--dry-run`/`--tx` 单事务原子执行、失败定位到「第 i/N 条语句」、`DATASMITH EXECUTE-ON:` 执行目标硬校验。
- **版本迁移（migrate-script / migrate-rollback / migrate-remove）**：唯一版本、SHA-256 checksum、状态账本、advisory lock 防并发；栈式回退（最新或指定版本）；failed/rolled_back 账本记录可显式删除以修复 checksum drift。
- **重置（reset-db）**：先 `--dry-run` 输出校验后的重置 SQL，再 `--yes` 显式执行；系统数据库、空目标、危险 schema 直接拒绝。
- **Web 控制台（web）**：连接管理（含 SSH 隧道与主机身份校验）、比对/执行/重置/迁移任务、脚本库托管与增量版本登记、比对方案存储；与 CLI 调用同一套引擎与安全校验。
- **连接与安全**：SSH 隧道/代理强制主机身份校验（`knownHostsPath` 或 SHA-256 `hostFingerprint`）；池化连接参数可配。

---

## 快速开始

### 构建

```bash
./scripts/build.sh    # 产物 bin/datasmith；检测到 pnpm 时先构建 web/ 前端并 embed
```

要求 Go 1.26+；前端为 React 18 + Vite 5 + TypeScript + Ant Design 5（仅中文），产物经 go:embed 打进单二进制。

### 配置数据库连接

编辑 `configs/config.yaml`（完整示例见 `configs/`）：

```yaml
sourceDb:
  type: postgres            # postgres | mysql
  host: 127.0.0.1
  port: 5432
  user: example_user
  password: obvious-placeholder
  dbname: source_db
  tableSchema: public
  # Duration 以纳秒数字书写
  connectTimeout: 10000000000
  maxOpenConns: 20
  maxIdleConns: 5
  connMaxLifetime: 1800000000000
  connMaxIdleTime: 300000000000

targetDb:
  type: postgres
  host: 127.0.0.1
  port: 5433
  user: example_user
  password: obvious-placeholder
  dbname: target_db

# 全局排除表（系统默认已含 schema_migrations 与 flyway_schema_history）
excludeTables:
  - flyway_schema_history
  - schema_migrations
```

SSH 代理必须配置主机身份验证，二选一使用 `knownHostsPath` 或固定的 `hostFingerprint`；未配置或不匹配时连接会在数据库访问前失败：

```yaml
  proxy:
    host: bastion.example
    port: 22
    user: example_user
    type: pass             # pass | rsa
    pass: obvious-placeholder
    knownHostsPath: /path/to/known_hosts
```

仅在测试环境等无法提供主机验证材料的场景，可显式设置 `allowInsecureHostKey: true`（须与上述两项均不同时配置）跳过主机身份校验；此时不防范中间人攻击，生产环境不建议开启。

### 配置比对规则

编辑 `configs/rules.json`：

```json
{
  "rules": [
    {
      "table": "users",
      "comparisonKey": ["name", "email"]
    }
  ]
}
```

- 行身份：默认「主键 → 非空唯一索引」定位行；两者皆缺的无键表可显式配置 `comparisonKey` 业务键——业务键列必须存在于表中且全部 NOT NULL，否则报错说明原因。
- 规则省略与通配：表名含 `*`/`?` 的条目按模式展开；`--rules` 整个省略（或 `rules` 为空数组）时按整库模式比对「全部具有行身份的表」。通配条目只允许 `table` 字段。
- 排除清单：配置文件 `excludeTables` + CLI `--exclude-tables`，默认始终包含迁移账本表 `schema_migrations` 与 `flyway_schema_history`；被排除表拥有的 SERIAL 隐式序列一并排除，账本结构不出现在任何比对产物中。

### CLI 命令一览

| 命令 | 必填参数 | 可选参数 |
| --- | --- | --- |
| `diff-schema` | `-c` 配置文件 | 产物固定输出到当前目录 `schema_diff.sql` / `schema_diff_rollback.sql` |
| `diff-data` | `-c` | `-r/--rules`（省略=整库）、`-o/--output`、`--rollback-output`、`--exclude-tables`、`--batch-size`、`--chunk-size`、`--chunk-hash`、`--dml-batch-size`、`--best-effort`、`--skip-missing-tables` |
| `diff-full` | `-c` | `-r`、`-o/--output`（产物目录）、`--data-diff-mode auto\|shadow\|direct`、`--migrate-dir`、`--version`、`--title`、`--exclude-tables` 及数据比对参数 |
| `exec-sql` | `-c`、`-f/--file`、`-d/--db source\|target` | `--source`/`--target`（`-d` 快捷方式）、`-n/--dry-run`、`--tx` |
| `migrate-script` | `-c`、`-d/--dir` | `-v/--version`（只推进到指定版本）、`-n/--dry-run` |
| `migrate-rollback` | `-c`、`-d` | `--target`（回退到指定版本）、`-n/--dry-run`、`--yes` |
| `migrate-remove` | `-c`、`-d`、`-v/--version` | `-n/--dry-run`、`--yes` |
| `reset-db` | `-c` | `-n/--dry-run`、`--yes` |
| `web` | 无 | `--addr`（默认 `127.0.0.1:8080`）、`--data-dir`（默认 `./datasmith-web-data`） |

### 结构比对

```bash
./datasmith diff-schema -c configs/config.yaml
# 当前目录生成 schema_diff.sql（正向）与 schema_diff_rollback.sql（回滚），
# 两个文件头部均带 DATASMITH EXECUTE-ON: source
```

配置文件中的 `includeTables` / `excludeTables` 可限定参与结构比对的表清单；数据比对还支持 CLI `--exclude-tables` 追加排除。

### 数据比对

```bash
# 数据比对
./datasmith diff-data -c configs/config.yaml -r configs/rules.json
# 整库数据比对(省略规则, 账本表自动排除)
./datasmith diff-data -c configs/config.yaml
# 跳过尚不存在的表(迁移链场景: 晚建表由后续版本轮次同步),并输出 warning 清单
./datasmith diff-data -c configs/config.yaml -r configs/rules.json --skip-missing-tables
# opt-in 概率预过滤:仅在精确 count/min/max 一致后跳过范围
./datasmith diff-data -c configs/config.yaml -r configs/rules.json --chunk-hash
```

### 完全对比（diff-full）

```bash
# 一次比对结构与数据,产出四份 SQL(结构/数据 × 正向/回滚)
./datasmith diff-full -c configs/config.yaml -r configs/rules.json -o output/diff
# 并一步生成迁移 up/down 对(仅生成脚本;执行仍需 migrate-script 显式进行)
./datasmith diff-full -c configs/config.yaml -r configs/rules.json -o output/diff \
  --migrate-dir data/dbscripts --version 1.0.0 --title full-sync
```

数据比对默认两阶段（`--data-diff-mode auto`）：source 为 PostgreSQL 且存在结构差异时，先把结构正向 DDL 应用在 source 连接内的**影子事务**（`BEGIN; 结构 forward; 数据比对; ROLLBACK`），数据比对经同一会话读取对齐后的影子结构再生成 up/down——不污染库、单连接，两侧结构漂移（加列/删列/建表）轮的 up 一次执行即可对齐结构与数据，无需人工预对齐。影子对齐失败会直接报错（说明 forward 产物在真实结构上不可执行），不会静默降级。

MySQL 无事务性 DDL，自动回退为直接比对（公共列/主键列集的漂移容错）并输出告警；直接模式中的 target-only 表把 source 视为空集并生成全量 INSERT，source-only 表只由结构 DROP 处理。`--data-diff-mode shadow` 强制影子（仅 PostgreSQL source，MySQL 强制影子会被拒绝），`direct` 强制禁用。

四份产物先完整写入同目录暂存区，全部成功后通过持久化发布日志作为一组发布；进程中断或主机重启后，下一次运行会恢复完整旧组或确认完整新组，不暴露混合产物。

### 执行 SQL 文件

可在源数据库（source）或目标数据库（target）上执行 SQL 脚本（如比对生成的差异 SQL、回滚 SQL 或自定义脚本）：

```bash
# diff 产物的正向 SQL 在源数据库执行(forward = 让 source 对齐 target)
./datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source
# 回滚 SQL 也在源数据库执行,用于恢复 source 的原始状态
./datasmith exec-sql -c configs/config.yaml -f data_diff_rollback.sql -d source
# 自定义 SQL 可显式执行到目标数据库;--source/--target 是 -d 的快捷方式
./datasmith exec-sql -c configs/config.yaml -f custom.sql --target
# 模拟执行 (在事务中执行后自动回滚,验证脚本正确性)
./datasmith exec-sql -c configs/config.yaml -f schema_diff.sql -d source -n
# 启用事务执行 (全部成功后提交,遇错自动回滚)
./datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source --tx
```

`--tx` 与 `--dry-run` 在单个事务内按扫描出的语句逐条执行，整文件原子——全部成功才提交，任一语句失败即回滚整个事务；失败时错误信息直接定位到「第 i/N 条语句」并附该语句文本预览。非事务模式整体发送。注意 MySQL DDL 会隐式提交，事务模式对 MySQL DDL 仍会拒绝（无可靠回滚手段）。

### 版本迁移与回退

脚本文件名规范：`V<版本>__<标题>[.up|.down].sql`；不符合规范的文件不参与迁移链（如单下划线的 `V1.0.1_update.up.sql`），扫描发现时 CLI 与 Web 任务日志会输出 WARNING 清单但不中断合法迁移链。

```bash
# 执行迁移脚本
./datasmith migrate-script -c configs/config.yaml -d data/dbscripts
# 只推进到指定版本
./datasmith migrate-script -c configs/config.yaml -d data/dbscripts -v 1.0.0
# 模拟执行 (PostgreSQL 事务回滚验证;MySQL migration dry-run 被拒绝)
./datasmith migrate-script -c configs/config.yaml -d data/dbscripts -n

# 回退最新一个已应用版本(破坏性操作,必须显式 --yes)
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --yes
# 回退到指定版本:从最新版本起逐个回退,直到该版本成为最新(该版本本身保留)
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --target 1.0.0 --yes
# 预览将回退的版本清单,不执行任何数据库变更
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --target 1.0.0 --dry-run
```

删除账本记录（修复 checksum drift 的逃生舱）：

```bash
# 预览:显示记录状态/checksum 与警告,不执行删除
./datasmith migrate-remove -c configs/config.yaml -d data/dbscripts -v 1.0.1 --dry-run
# 真实删除必须显式 --yes
./datasmith migrate-remove -c configs/config.yaml -d data/dbscripts -v 1.0.1 --yes
```

`migrate-remove` 只删除 `schema_migrations` 中 **failed / rolled_back** 状态的记录（success 与真实库结构绑定、running/rolling_back 可能有活跃迁移，一律拒绝），本身不执行任何变更 SQL。删除后同版本号的修正脚本可重新迁移并重新登记 checksum——这是修复「同版本内容修改后 checksum drift 拒绝迁移」的正规通道。删除前 CLI 与 Web 端都会输出提示：脚本库缺该版本 up 脚本（删除后无法重跑）、存在更新的已成功版本（重跑后该版本排到回退栈顶）、MySQL failed 记录可能有半途自动提交的 DDL 残留（修正脚本应写成幂等）。

### 重置数据库

```bash
# 仅输出经过校验和安全引用的重置 SQL,不连接数据库
./datasmith reset-db -c configs/config.yaml --dry-run
# 真实重置必须显式确认;空目标与系统数据库/Schema 会被拒绝
./datasmith reset-db -c configs/config.yaml --yes
```

---

## Web 控制台

```bash
./datasmith web                      # 默认 127.0.0.1:8080
./datasmith web --addr 127.0.0.1:9000 --data-dir ./datasmith-web-data
```

默认仅监听 `127.0.0.1:8080`；显式配置非 loopback `--addr` 会提示当前没有远程认证，生产远程访问必须另行配置可信网络边界与访问控制。`--data-dir` 存储命名连接、比对方案、脚本库与任务产物（`store.json`），含连接凭据，禁止入库（已在 `.gitignore`）。

主要能力（与 CLI 共用同一引擎与安全校验，不重写任何安全逻辑）：

- **连接管理**：命名连接、SSH 隧道（主机校验二选一强制，`allowInsecureHostKey` 显式开关可跳过）、连接测试。
- **比对任务**：结构比对、数据比对、完全比对（高级参数中可选数据比对模式：自动/强制影子事务/直接比对，默认自动；任务参数与日志区分 `requested` 与 `effective` 模式）。
- **SQL 控制台**：CodeMirror 编辑器，执行写入脚本时必须显式选择当前连接的 `source`/`target` 角色；与 CLI 共用同一 EXECUTE-ON 硬校验；切换物理连接会清空此前选择的角色，必须针对新连接重新确认。
- **迁移管理**：脚本库（`V<版本>__<标题>` up/down 对）托管、执行计划账本、栈式回退、failed/已回退账本记录删除；以 up（含省略方向的旧式文件名）作为版本的生命周期依据，删除 up 时清理版本登记，启动时执行 store 一致性自检。
- **脚本库复制**：**复制脚本库**把当前库的全部脚本（含 down/JSON）与版本登记元数据克隆为全新脚本库；勾选脚本后**复制到其他库**可跨库批量复制，目标库存在同名脚本或同版本不同标题时会整批拒绝并列出冲突清单，不产生部分复制结果。
- **增量版本登记**：比对产物人工审阅后登记为脚本库版本，up 为「结构正向→数据正向」串接，down 为「数据回滚→结构回滚」串接。

---

## 目录结构

```
/
├── cmd/                    # main.go 入口（仅初始化，禁止业务逻辑）
├── internal/
│   ├── config/             # 配置加载与解析（YAML/JSON）
│   ├── datasmith/          # cobra CLI：root.go 注册，子命令编排
│   │   ├── diff/           # diff-schema / diff-data / diff-full：streaming_data.go 流水线、
│   │   │                   #   atomic_output.go 原子写、run.go 供 Web 复用、shadow.go 影子事务、
│   │   │                   #   full.go 完全对比、migration_assembly.go 合成 up/down
│   │   ├── exec/           # exec-sql（sql_scanner.go 词法级 SQL 语句拆分器）
│   │   ├── migrate/        # reset-db / migrate-script / migrate-rollback / migrate-remove
│   │   │                   #   （local/ 迁移文件名解析；RunMigrations/Rollback* 供 Web 复用）
│   │   └── web/            # web 子命令（--addr、--data-dir）
│   └── server/             # Web 控制台后端：连接管理、任务、脚本库、比对方案存储；
│                           #   webfs/ 为 go:embed 的前端产物
├── pkg/
│   ├── conn/               # 连接管理 + DBAdapter 接口（接口定义在此，避免 import cycle）
│   ├── db/                 # driver.go 工厂（按 cfg.Type switch）；base/ 公共实现；
│   │                       #   mysql/ postgres/ 驱动适配器
│   ├── diff/               # 结构与数据比对核心算法
│   ├── rowloc/             # 数据比对行身份定位（主键/唯一索引/业务键/全行）
│   ├── chunk/              # 数据分块与 --chunk-hash 概率比对
│   ├── sql/                # 方言 SQL 生成（forward + rollback，ident/ 安全标识符引用）
│   ├── migrate/            # 版本迁移（唯一版本、SHA-256 checksum、状态 ledger、advisory lock）
│   ├── proxy/              # SSH 隧道/代理（强制主机身份校验）
│   └── config/ consts/ logger/ utils/
├── web/                    # Web 控制台前端（React 18 + Vite 5 + TS + AntD5，仅中文；
│                           #   dev 端口 5173 代理 /api 到 :8080）
├── configs/                # 配置文件示例（YAML/JSON）
├── scripts/                # build.sh / integration-test.sh / check-coverage.sh
├── test/integration/       # integration tag 双库 E2E（Docker fixtures）
└── docs/                   # 验收报告与 agent 配置说明
```

数据流：`internal/datasmith/*`（CLI 编排）→ `pkg/diff`（比对）→ `pkg/sql`（方言 SQL 生成）→ `internal/datasmith/exec`（执行/回滚）。

## 扩展与开发

- **新增数据库类型**：`pkg/db/<type>/` 实现 `conn.DBAdapter`，`pkg/sql/<type>/` 实现方言生成，`pkg/consts/` 注册类型常量，`pkg/db/driver.go` 工厂加分支；不改动上层包。
- **配置外置**：连接信息、比对规则一律走 YAML/JSON 配置文件；禁止硬编码凭据。
- **代码位置**：只在 `internal/`、`pkg/`、`cmd/`、`scripts/`、`configs/`、`web/` 下写代码。
- **测试**：核心逻辑（diff/db/sql/migrate/exec）必须有单元测试，默认不依赖 Docker；真实数据库行为放 `integration` tag 的测试里。
- 完整开发约定与操作安全红线见 [AGENTS.md](AGENTS.md)；领域词汇见 [CONTEXT.md](CONTEXT.md)。

## 测试、CI 与覆盖率

默认单元测试不启动 Docker，也不读取外部数据库凭据：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

真实数据库测试必须同时具备 `integration` build tag 和 `DATASMITH_INTEGRATION=1` 环境保护。推荐通过脚本启动固定版本、tmpfs 存储的 MySQL 8.4.3 与 PostgreSQL 17.2 fixtures；脚本会验证 fixture identity，使用独立 source/target 实例，并在退出时销毁容器和数据：

```bash
./scripts/integration-test.sh              # test 模式
./scripts/integration-test.sh coverage /tmp/profile.out
./scripts/check-coverage.sh                # 覆盖率 gate
```

端到端矩阵覆盖两种引擎的 schema/data forward SQL 生成与应用、第二次 diff 为空、rollback 恢复原状态；同时覆盖非 `public` PostgreSQL schema、依赖顺序、超过 2^53 的 BIGINT、NULL/空串和保留字/混合大小写/内嵌引号标识符。Migration E2E 覆盖成功、失败后同 checksum 重试、重复执行幂等、checksum drift、ledger 状态和并发锁。

`scripts/check-coverage.sh` 强制总体 statement coverage >= 60%，db/diff/sql/migrate/exec 每组 >= 70%。最近一次全量复审（2026-09-21）实测：overall 74.8%、db 78.9%、diff 77.8%、sql 76.3%、migrate 75.9%、exec 89.1%。GitHub Actions（`.github/workflows/ci.yml`）使用固定 action SHA、固定工具版本、固定数据库镜像、health check 和有界 timeout 执行同等 gate。

## 操作安全红线与已知限制

1. forward/rollback SQL 在隔离副本生成并人工审阅后才能执行；两个文件都带 `DATASMITH EXECUTE-ON: source`，必须显式选择 source 执行。
2. `exec-sql` 先使用 `--dry-run`/事务模式验证；MySQL DDL 和 MySQL migration dry-run 不具备可靠事务回滚能力，因此会被拒绝或要求人工恢复预案。
3. `reset-db` 先运行 `--dry-run`，确认目标后才使用 `--yes`；系统数据库、空目标和危险 schema 会在连接/破坏前被拒绝。
4. Migration 使用唯一版本、SHA-256 checksum、状态 ledger 和 advisory lock。旧 ledger 的历史成功行可能没有 checksum，无法证明这些旧行的 drift；旧表中若已有重复 version，唯一索引升级会要求先人工清理。
5. `--chunk-hash` 仍是 opt-in 概率优化：只有精确 count/min/max 一致后才允许跳过范围；并发写入不在共享快照中时仍可能让比较失效。
6. SQL scanner 是词法扫描器，不是完整客户端协议实现；MySQL `DELIMITER` 和 PostgreSQL `COPY ... FROM STDIN` 等客户端格式不受支持，歧义脚本会被保守拒绝。
7. PostgreSQL 例程依赖提取只承诺 SQL/PLpgSQL 的静态对象引用；动态 SQL、无法判定的重载或其他语言会拒绝生成，不会猜测顺序。trigger、identity/generated column、分区和用户定义类型仍未纳入结构迁移能力。
8. SSH 默认必须配置 `knownHostsPath` 或 SHA-256 `hostFingerprint`；两者均未配置的连接仅在显式设置 `allowInsecureHostKey: true` 时放行（跳过主机校验，不防中间人攻击）。

性能基准（Apple M4 Pro，`-benchtime=1x -count=3`）：100k 行且 100k 差异时，streaming pipeline 从 110.48–122.14 ms / 85.57 MB alloc/op 降至 67.38–67.86 ms / 46.13–46.17 MB，SQL 输出从 12,477,886 降至 5,285,286 bytes/op，峰值缓冲从 100,000 行降至固定 2,000 引用。10k 行仅 100 差异时会承担 spool 创建/同步/删除的固定延迟，因此不是低差异场景的纯速度优化。

## 相关文档

- [AGENTS.md](AGENTS.md) — 开发约定、CI gate 与操作安全红线（面向 AI 与开发者）
- [CONTEXT.md](CONTEXT.md) — 领域词汇表（CLI 与 Web 共享同一套语言）
- [docs/migration-chain-regression-report.md](docs/migration-chain-regression-report.md) — 迁移链能力线验收报告（88 轮全链回归、闭环结论与遗留缺口）
- `docs/agents/` — issue tracker、triage 标签与领域文档的 agent 操作说明
