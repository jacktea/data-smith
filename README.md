# DataSmith

一个高效、可扩展的 Go 语言命令行数据库管理工具，专为开发者与数据库管理员设计，支持多种数据库系统，聚焦于数据库结构与数据的智能比对与同步，自动生成可执行 SQL 脚本，助力数据库变更的自动化与安全落地。

---

## 目录结构与功能说明

```
/
├── cmd/                    # 主程序入口（main.go），初始化 CLI
├── internal/
│   ├── config/             # 配置加载与解析
│   └── datasmith/
│       ├── root.go         # CLI 根命令注册
│       ├── diff/           # 数据与结构比对命令实现
│       ├── exec/           # SQL 文件执行命令实现
│       └── migrate/        # 数据迁移与重置命令实现
├── pkg/
│   ├── config/             # 配置相关通用逻辑
│   ├── conn/               # 数据库连接管理
│   ├── consts/             # 常量定义
│   ├── db/
│   │   ├── base/           # 数据库适配基础接口
│   │   ├── mysql/          # MySQL 驱动实现
│   │   └── postgres/       # PostgreSQL 驱动实现
│   ├── diff/               # 结构与数据比对核心逻辑
│   ├── logger/             # 通用日志库及适配器
│   ├── migrate/            # 数据迁移相关逻辑
│   ├── proxy/              # 代理与 SSH 支持
│   ├── sql/
│   │   ├── mysql/          # MySQL SQL 生成
│   │   └── postgres/       # PostgreSQL SQL 生成
│   └── utils/              # 工具函数与通用工具
├── configs/                # 配置文件示例（YAML/JSON）
├── scripts/                # 构建与工具脚本
├── go.mod, go.sum          # Go 依赖管理
├── README.md               # 项目说明
```

### 主要功能模块说明

- **cmd/**  
  主程序入口，负责 CLI 初始化。

- **internal/config/**  
  负责加载和解析数据库连接、比对规则等配置。

- **internal/datasmith/**  
  CLI 命令注册与分发，包含结构和数据比对命令实现。

- **pkg/db/**  
  数据库驱动适配层，包含基础接口和 MySQL、PostgreSQL 驱动实现。

- **pkg/diff/**  
  结构与数据比对的核心算法和逻辑。

- **pkg/sql/**  
  SQL 差异脚本生成，支持多数据库方言。

- **pkg/logger/**  
  通用日志库，支持多种日志输出方式。

- **pkg/conn/**  
  数据库连接管理，支持多种连接方式。

- **pkg/proxy/**  
  代理与 SSH 隧道支持，适配复杂网络环境。

- **pkg/utils/**  
  通用工具函数和辅助逻辑。

- **configs/**  
  配置文件示例，便于快速上手。

- **scripts/**  
  构建、发布等自动化脚本。

---

## 项目功能

- **数据库结构比对**：表、字段、索引、视图等对象的差异检测，自动识别新增、删除、修改。PostgreSQL 额外覆盖函数、存储过程与序列（含 SERIAL 隐式序列），并在产物中先于表 DDL 生成（列默认值 `nextval`/函数依赖可直接解析）。
- **视图依赖拓扑**：列类型/删除等变更会影响视图时，产物自动按依赖闭包先 DROP 受影响视图，表 DDL 完成后按拓扑序重建；回滚对称。聚合函数与窗口函数暂不支持比对。
- **表数据比对**：比对两库间表数据，生成 INSERT、DELETE、UPDATE SQL，支持自定义主键和比对规则。
- **多数据库支持**：驱动架构，现支持 MySQL、PostgreSQL，易于扩展。
- **自动 SQL 脚本生成**：根据比对结果生成可执行 SQL。
- **配置化管理**：所有连接信息、比对规则均通过 YAML/JSON 配置文件管理。
- **日志与代理支持**：内置日志库和 SSH/代理支持，适配多种部署环境。

---

## 技术特色

- **接口驱动架构**，易于扩展新数据库类型
- **高内聚低耦合**，各模块职责清晰
- **自动化脚本生成**，提升变更效率与安全性
- **丰富注释与文档**，便于二次开发
- **配置即约定**，所有敏感信息与规则均外部配置

---

## 快速开始

### 1. 配置数据库连接

编辑 `configs/config.yaml`：

```yaml
sourceDb:
  type: postgres
  host: 127.0.0.1
  port: 5433
  user: example_user
  password: obvious-placeholder
  dbname: source_db
  # Duration values are nanoseconds when written as YAML numbers.
  connectTimeout: 10000000000
  maxOpenConns: 20
  maxIdleConns: 5
  connMaxLifetime: 1800000000000
  connMaxIdleTime: 300000000000

targetDb:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: example_user
  password: obvious-placeholder
  dbname: target_db
```

SSH 代理必须配置主机身份验证，二选一使用 `knownHostsPath` 或固定的
`hostFingerprint`；未配置或不匹配时连接会在数据库访问前失败：

```yaml
  proxy:
    host: bastion.example
    port: 22
    user: example_user
    type: pass
    pass: obvious-placeholder
    knownHostsPath: /path/to/known_hosts
```

### 2. 配置比对规则

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

### 3. 数据或结构比对

```bash
# 结构比对
./datasmith diff-schema -c configs/config.yaml
# 数据比对
./datasmith diff-data -c configs/config.yaml -r configs/rules.json
# 数据比对跳过尚不存在的表(迁移链场景: 晚建表由后续版本轮次同步),并输出 warning 清单
./datasmith diff-data -c configs/config.yaml -r configs/rules.json --skip-missing-tables
# 完全对比:一次同时比对结构与数据,产出四份 SQL(结构/数据 × 正向/回滚)
./datasmith diff-full -c configs/config.yaml -r configs/rules.json -o output/diff
# 完全对比并一步生成迁移 up/down 对(仅生成脚本;执行仍需 migrate-script 显式进行)
./datasmith diff-full -c configs/config.yaml -r configs/rules.json -o output/diff \
  --migrate-dir data/dbscripts --version 1.0.0 --title full-sync
```

### 4. 执行 SQL 文件 (`exec-sql`)

可在配置文件中定义的源数据库 (`source`) 或目标数据库 (`target`) 上执行 SQL 脚本（如比对生成的差异 SQL、回滚 SQL 或自定义脚本）：

```bash
# diff-data / diff-schema 的正向 SQL 在源数据库执行
./datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source

# 回滚 SQL 也在源数据库执行，用于恢复 source 的原始状态
./datasmith exec-sql -c configs/config.yaml -f data_diff_rollback.sql -d source

# 自定义 SQL 可显式执行到目标数据库；-d/--source/--target 必须选择其一
./datasmith exec-sql -c configs/config.yaml -f custom.sql -d target

# 模拟执行 (在事务中执行后自动回滚，验证脚本正确性)
./datasmith exec-sql -c configs/config.yaml -f schema_diff.sql -d source -n

# 启用事务执行 (全部成功后提交，遇错自动回滚)
./datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source --tx
```

### 5. 数据库版本迁移与重置

脚本文件目录：

```
.
├── baseline
│   └── V1.0.0__baseline.sql
└── versionupdates
    └── v1
        └── v1_0
            └── v1_0_0
                ├── V1.0.0.10__upgrade.sql
                ├── V1.0.0.100__upgradesql.sql
                ├── V1.0.0.101__upgradesqlProduct.sql
                ├── V1.0.0.102__upgradesql.sql
                ├── V1.0.0.103__upgradesql.sql
                ├── V1.0.0.104__upgradesql.sql
                ├── V1.0.0.105__upgradesqlProduct.sql
                ├── V1.0.0.106__upgradesql.sql
                ├── V1.0.0.107__upgradesqlProduct.sql
                ├── V1.0.0.108__upgradesqlProduct.sql
                ├── V1.0.0.11__upgrade.sql
```

```bash
# 仅输出经过校验和安全引用的重置 SQL，不连接数据库
./datasmith reset-db -c configs/config.yaml --dry-run
# 真实重置必须显式确认；空目标与系统数据库/Schema 会被拒绝
./datasmith reset-db -c configs/config.yaml --yes
# 执行迁移脚本
./datasmith migrate-script -c configs/config.yaml -d data/dbscripts
# 执行迁移脚本, 模拟执行
./datasmith migrate-script -c configs/config.yaml -d data/dbscripts -n
# 回退最新一个已应用版本(破坏性操作,必须显式 --yes)
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --yes
# 回退到指定版本:从最新版本起逐个回退,直到该版本成为最新(该版本本身保留)
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --target 1.0.0 --yes
# 预览将回退的版本清单,不执行任何数据库变更
./datasmith migrate-rollback -c configs/config.yaml -d data/dbscripts --target 1.0.0 --dry-run
```

---

## 扩展与开发规范

- 新增数据库类型：在 `pkg/db/` 下新建子目录，实现接口
- 业务逻辑仅可写于 `internal/`、`pkg/`、`cmd/`、`scripts/`、`configs/`
- 禁止将业务逻辑写在 `main.go`
- 接口驱动、配置管理、依赖最小化
- 核心逻辑需编写单元测试
- 语义化提交，PR 需关联 issue 并通过 review

## CI、端到端测试与覆盖率

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
./scripts/integration-test.sh
./scripts/check-coverage.sh
```

端到端矩阵覆盖两种引擎的 schema/data forward SQL 生成与应用、第二次 diff 为空、rollback 恢复原状态；同时覆盖非 `public` PostgreSQL schema、依赖顺序、超过 2^53 的 BIGINT、NULL/空串和保留字/混合大小写/内嵌引号标识符。Migration E2E 覆盖成功、失败后同 checksum 重试、重复执行幂等、checksum drift、ledger 状态和并发锁。

`scripts/check-coverage.sh` 对同一个带 integration tag 的全仓 profile 去重并强制总体 statement coverage >= 60%，以及 db/diff/sql/migrate/exec 每组 >= 70%。2026-09-19 本地验证结果为：overall 72.5%、db 77.8%、diff 71.9%、sql 70.1%、migrate 70.6%、exec 87.9%。GitHub Actions 使用固定 action SHA、固定工具版本、固定数据库镜像、health check 和有界 timeout 执行同等 gate。

## 操作安全流程与已知限制

1. 在隔离副本上生成并人工审阅 forward/rollback SQL；两个文件都带 `DATASMITH EXECUTE-ON: source`，必须显式选择 source 执行。
2. `exec-sql` 先使用 `--dry-run`/事务模式验证；MySQL DDL 和 MySQL migration dry-run 不具备可靠事务回滚能力，因此会被拒绝或要求人工恢复预案。
3. `reset-db` 先运行 `--dry-run`，确认目标后才使用 `--yes`；系统数据库、空目标和危险 schema 会在连接/破坏前被拒绝。
4. Migration 使用唯一版本、SHA-256 checksum、状态 ledger 和 advisory lock。旧 ledger 的历史成功行可能没有 checksum，无法证明这些旧行的 drift；旧表中若已有重复 version，唯一索引升级会要求先人工清理。
5. `--chunk-hash` 仍是 opt-in 概率优化：只有精确 count/min/max 一致后才允许跳过范围；并发写入不在共享快照中时仍可能让比较失效。
6. SQL scanner 是词法扫描器，不是完整客户端协议实现；MySQL `DELIMITER` 和 PostgreSQL `COPY ... FROM STDIN` 等客户端格式不受支持，歧义脚本会被保守拒绝。
7. SSH 必须配置 `knownHostsPath` 或 SHA-256 `hostFingerprint`；不提供主机身份验证材料的连接会失败。

Session 6 性能基准（Apple M4 Pro，`-benchtime=1x -count=3`）显示：100k 行且 100k 差异时，streaming pipeline 从 110.48–122.14 ms / 85.57 MB alloc/op 降至 67.38–67.86 ms / 46.13–46.17 MB，SQL 输出从 12,477,886 降至 5,285,286 bytes/op，峰值缓冲从 100,000 行降至固定 2,000 引用。10k 行仅 100 差异时会承担 spool 创建/同步/删除的固定延迟，因此不是低差异场景的纯速度优化。
