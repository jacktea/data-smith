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
│       └── diff/           # 数据与结构比对命令实现
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

- **数据库结构比对**：表、字段、索引、视图等对象的差异检测，自动识别新增、删除、修改。
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
source_db:
  type: postgres
  host: 127.0.0.1
  port: 5433
  user: user
  password: password
  dbname: source_db

target_db:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: user
  password: password
  dbname: target_db
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

### 3. 执行比对

```bash
# 结构比对
./datasmith compare schema -c configs/config.yaml

# 数据比对
./datasmith compare data -c configs/config.yaml -r configs/rules.json
```

---

## 扩展与开发规范

- 新增数据库类型：在 `pkg/db/` 下新建子目录，实现接口
- 业务逻辑仅可写于 `internal/`、`pkg/`、`cmd/`、`scripts/`、`configs/`
- 禁止将业务逻辑写在 `main.go`
- 接口驱动、配置管理、依赖最小化
- 核心逻辑需编写单元测试
- 语义化提交，PR 需关联 issue 并通过 review
