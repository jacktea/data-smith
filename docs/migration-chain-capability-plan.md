# 迁移链能力补齐计划（基于 airedge 改造实测）

> 背景：以「把 airedge 的 88 个 Flyway 迁移脚本改造为 datasmith 迁移链」作为能力探针，
> 逐版本回放原脚本到演练库 target，用 datasmith 的 diff-schema / diff-data / exec-sql /
> migrate-script 生成并应用 up/down 对。本文记录实测暴露的工具缺陷、已完成的修复、
> 待补齐的 feature 与实施顺序。**改造任务暂停，能力补齐后重跑验收。**
>
> 实测窗口：2026-09-20。环境：本机 Docker PG 14.22，一次性库
> `airedge_mig_src`（落后一版）/ `airedge_mig_tgt`（领先一版）。

## 一、实测结论总览

- **2.1.0 / 2.2.0 / 2.3.0 三轮全流程跑通**（回放→结构比对→数据比对→推进 source），
  产物 up 972KB / 315KB 级别，全部经 datasmith 自身命令执行成功。
- 流水线在 **2.4.0 停止**：结构预对齐被「视图依赖的列类型变更」阻断（见 C2）。
- 为跑通前三轮，流水线用了 5 处**外部脚本 workaround**，每一处都对应一个工具缺口
  （见下表）。本计划的目标：让这些 workaround 变成 data-smith 的内建能力，之后
  外部脚本全部废弃，全流程仅用 datasmith 命令完成。

| 外部 workaround（临时） | 对应缺口 |
|---|---|
| gen_nontable.py 函数/序列快照差补片段（仅 `prokind='f'`，存储过程亦漏） | C1 |
| gen_nontable.py 视图整组删建 + 拓扑排序 | C2 |
| gen_effective_rules.py 剔除未创建的表 | C3 |
| 两阶段：预对齐结构→数据比对→撤销 | C5 |
| source 推进改用 exec-sql（弃用 migrate-script） | C6 |

## 二、已完成的修复（工作区，未提交）

- **F0【bug】无主键表触发生成层空指针 panic**
  - `pkg/sql/postgres/postgre.go` `GenerateDeleteBatchSql` / `GenerateUpdateSql` 与
    `pkg/sql/mysql/mysql.go` 同名函数直接解引用 `tbl.PrimaryKey.Columns`，
    无主键表（如关联表）数据比对生成 DELETE 时直接 panic（SIGSEGV，实测触发）。
  - 修复：新增 `rowKeyColumns`——有主键用主键，无主键回退行内全列定位（排序保证
    确定性）；UPDATE 在无主键时安全返回空串（全列身份下变更行走 DELETE+INSERT 语义）。
  - 测试：`TestPostgreDialect_NoPrimaryKeyDataSQL`、`TestMySQLDialect_NoPrimaryKeyDataSQL`，
    `go test ./pkg/sql/...`、`go vet`、gofmt 全部通过。
  - 已提交：`d58cd31 fix(sql): 无主键表生成 DELETE/UPDATE 不再空指针,回退为全列定位`。

## 三、缺陷与 Feature 清单

### C1【P0·阻断】函数 / 存储过程 / 序列不参与结构比对

- **现象**：原脚本创建的函数（`generate_lookup_number_sort_key`、`has_revision`、
  `ws_items_index_snowflake_id/_local_id`）与序列（`air_ws_items_index_seq_*` 及 SERIAL
  隐式序列）在 diff 产物中完全缺失；生成的列默认值 `DEFAULT nextval('...')` /
  自定义函数默认值执行时报 `relation/function does not exist`。3.0.0 与 3.0.2.5（建/删序列与函数）必现。
- **范围**：函数（`prokind='f'`，含返回 `trigger` 的触发器函数）、**存储过程
  （`prokind='p'`，PG 11+ 与函数同存于 `pg_proc`，`pg_get_functiondef` 可直接还原
  `CREATE OR REPLACE PROCEDURE`，实现成本与函数一致）**、序列。注：临时增强脚本
  只快照了 `prokind='f'`，存储过程同样漏掉——进一步证明需要正式建模而非补丁。
  聚合函数（`prokind='a'`）`pg_get_functiondef` 不支持，需经 `pg_aggregate` 重组
  `CREATE AGGREGATE` 或首版明确不支持并在文档注明；触发器（`CREATE TRIGGER`）为
  同类相邻对象，本任务未涉及（airedge 零触发器），列为首版后的独立子项。
- **根因**：`pkg/conn` 无函数/存储过程/序列模型；`pkg/db/postgres` 不提取；
  `pkg/diff` 不比对；`pkg/sql` 不生成。
- **方案**：
  1. `pkg/conn` 增加 Routine（kind: function/procedure，`pg_get_functiondef` 全文 +
     身份签名 `name(identity_args)`）与 Sequence（`pg_sequences` + 所有权/隐式判定）模型；
  2. `pkg/db/postgres` 提取；`pkg/diff` 增加 RoutineDiff / SequenceDiff（增/删/定义变更）；
  3. `pkg/sql/postgres` 生成 `CREATE OR REPLACE FUNCTION/PROCEDURE`（注意补结尾分号）、
     `CREATE SEQUENCE`、`DROP ... CASCADE` 及回滚；
  4. 产物排序：序列/函数/存储过程先于表 DDL（列默认值依赖）。
- **验收**：演练库上 2.1.0→3.0.2.5 区间不再需要任何外部补丁即可执行；含存储过程与
  序列增删的构造用例单测覆盖。

### C2【P0·阻断】视图依赖的列变更无拓扑排序

- **现象**：`pq: cannot alter type of a column used by a view or rule`（2.4.0 预对齐实测阻断）。
- **根因**：schema diff 按对象类型生成语句，不排依赖；列被视图引用时 PG 拒绝 ALTER，
  需要「DROP 依赖视图 → 改列 → 重建视图」，且重建定义（回滚用旧定义）引擎未管理。
- **方案**：结构产物按依赖排序——受影响视图（依赖闭包）先 DROP，表/列/索引变更，
  之后重建视图；回滚对称。实现可为「视图依赖图 + 拓扑排序」或简化为「整组视图删建」。
- **验收**：2.4.0 生成的 schema_diff.sql 在含 15 个视图的库上直接可执行。

### C3【P0·阻断】rules 引用不存在的表 → 空模型陷阱

- **现象**：rules 中晚创建的表（如 `air_sys_mobile_app_user` 在 2.4.0 才建）在创建前的每轮
  diff-data 报 `primary key or not-null unique index required`，误导排查。
- **根因**：`pkg/db/postgres.ExtractTable` 对缺失表返回零列空模型而非错误
  （`run.go` 的 "table not found" 分支因此永远走不到）。
- **方案**：ExtractTable 对缺失表返回明确错误或 (nil, nil)；diff 层对 rules 中不存在的表
  显式报 `table not found: X`，或按配置跳过并输出 warning 清单。
- **验收**：错误信息直指表名；提供跳过开关后流水线无需外部过滤。

### C4【P2·设计决策】无物理主键表的数据比对约束

- **现象**：`primary key or not-null unique index required`（`pkg/diff/data.go:482`）；
  rules 的 `comparisonKey` 不能替代行身份（校验只认物理主键/非空唯一索引）。
  `air_user_client_role` 等无键关联表被排除。
- **方案选项**：
  a. 放宽校验：行身份允许 rules.comparisonKey（业务键比对）；
  b. 全列身份模式：无键表按全列定位（生成层已由 F0 打通），比对层同步支持；
  c. 至少在错误信息与文档中写明约束与替代方案。
- **建议**：a（含非空校验）+ c。

### C5【P1·核心】两侧结构漂移时数据比对的列集处理

- **现象**：target 新增列后 diff-data 报 `column "notes" does not exist`（2.2.0 实测）。
- **本质**：迁移链场景中数据差异必须跑在**结构对齐之后**；当前 diff-full 的数据比对
  跑在未对齐状态上，仅靠「公共列 + keep 列集」部分容错。
- **方案**（推荐 a）：
  a. diff-full 内建两阶段：结构比对 → 在 source 连接内以**影子事务**（PG 事务 DDL：
     BEGIN; 应用结构 forward; 数据比对; ROLLBACK）完成数据比对 → 生成 up/down。
     不污染库、单连接、语义最干净；MySQL 无事务 DDL，回退影子库或方案 b。
  b. data diff 原生支持漂移：行读取 source 用公共列、target 用全列；INSERT 用 target
     全行；UPDATE 覆盖新增列。
- **验收**：2.2.0（加列）与 3.0.0（删列）轮的 up 一次执行通过，无需预对齐编排。

### C6【P1】表选择 / 排除能力（含账本表污染）

- **现象**：
  1. diff-schema 把 source 上的 `schema_migrations`（migrate 引擎账本）当差异对象生成
     `DROP TABLE`，反向污染迁移链（实测）；
  2. data diff 必须逐表枚举 rules，无「全表」模式（本任务曾用脚本生成 116 条全量清单）。
- **方案**：
  - diff（结构+数据）支持 `excludeTables` 配置，默认排除 `schema_migrations`、
    `flyway_schema_history` 等账本表；
  - rules 支持省略/通配（如 `air_sys_*`）时比对「全部有行身份的表」。
- **验收**：不写 rules 可出整库链；账本表不出现在任何产物；migrate-script 推进与
  diff 共存互不干扰（主链恢复使用 migrate-script，找回账本/checksum/回退通道的全程验证）。

### C7【P2·DX】不合规迁移文件名静默跳过

- `internal/datasmith/migrate/local/parser.go` 对不匹配文件名**静默跳过**
  （如单下划线 `V1.0.1_update.up.sql`），迁移缺版本无任何提示。
- **方案**：ScanMigrations 返回 skipped 清单，CLI 与 Web 输出 warning。

### C8【P2·bug】Web 删除脚本留下孤儿版本登记

- `internal/server/libraries.go` `handleDeleteScript` 删文件不清理 `LibraryMeta.Versions`
  （现网 store.json 已有 3.7.1 孤儿条目）。
- **方案**：删除 up 文件时同步清理版本元数据；提供 store 一致性自检命令。

### C9【P3·可选】exec-sql 失败定位增强

- 驱动不提供错误位置时报「语句范围 1-44」粗粒度信息。
- **方案**：错误信息附失败语句文本前 N 字符预览；文档写明 multi-statement 单事务语义
  （--tx 为整文件原子，实测正确）。

### C10【验收项】exec-sql 对外部脚本的兼容性

- 原 Flyway 脚本含 `$$` DO 块 / `CREATE FUNCTION`；scanner 已支持 dollar-quote，
  但「保守拒绝歧义脚本」策略下能否吃下全部 88 个原脚本未验证。
- **做法**：能力补齐后，回放通道优先用 exec-sql（--tx），失败脚本清单作为 scanner
  改进输入（预期少量，可个案处理）。

## 四、实施顺序

| 批次 | 内容 | 出口标准 |
|---|---|---|
| 第 1 批 | F0 提交；C1；C2；C3 | 单测 + 2.1.0→2.4.0 区间纯命令跑通（当前阻断点解除） |
| 第 2 批 | C5；C6 | 撤销两阶段预对齐编排，diff-full 单命令出链；账本共存 |
| 第 3 批 | C4a；C7；C8；C9 | 无键表按配置可比；DX 项完成 |
| 回归 | C10 + 全链重跑（88 轮） | 闭环 diff 为空；全新回放库经 migrate-script 全链应用成功；回退冒烟通过；登记脚本库 |

## 五、回归计划（能力补齐后的重跑）

全部使用 datasmith 命令，外部脚本废弃：

1. `reset-db` 重建三个一次性库（src / tgt / verify）；
2. 每轮：`exec-sql` 回放原脚本到 target → `diff-full` 生成 `V<版本>__<标题>.up/.down.sql`
   → `migrate-script` 推进 source（账本/checksum 全程在链）；
3. 闭环：末轮 diff 为空；verify 库从零 `migrate-script` 全链应用后与 target diff 为空；
   回退冒烟（最近版本往返）；
4. 产物登记 `datasmith-web-data/libraries/lib_8989e9f44806929e/`（airedge 库），
   按命名规范 `V<版本>__<标题>.up.sql/.down.sql`，同步 store.json；
5. 输出报告：逐版本产物摘要、函数/序列/视图增强清单、Java 迁移（8 个）效果缺口、
   回退对破坏性变更的固有限制。

## 六、当前暂停状态

- 演练库：`airedge_mig_src` 停在 2.3.0 应用后状态；`airedge_mig_tgt` 停在 2.4.0 回放后；
  重跑时一律全量重建，不续用。
- 临时工作区 `/tmp/airedge-migration/`（外部脚本与产物）仅作缺陷证据留存，能力补齐后删除。
- data-smith 仓库工作区：F0 修复已提交（`d58cd31`），本计划文档随 docs 提交入库。
