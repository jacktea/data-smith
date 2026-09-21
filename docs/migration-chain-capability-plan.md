# 迁移链能力补齐计划（基于 airedge 改造实测）

> 背景：以「把 airedge 的 88 个 Flyway 迁移脚本改造为 datasmith 迁移链」作为能力探针，
> 逐版本回放原脚本到演练库 target，用 datasmith 的 diff-schema / diff-data / exec-sql /
> migrate-script 生成并应用 up/down 对。本文记录实测暴露的工具缺陷、已完成的修复、
> 当前能力与实施顺序。**能力补齐（第 1–3 批）、回归批、C11–C13 收尾和
> 代码评审阶段 4 均已完成；当前只保留明确缓置的 Java 迁移缺口。**
>
> 实测窗口：2026-09-20。环境：本机 Docker PG 14.22，一次性库
> `airedge_mig_src`（落后一版）/ `airedge_mig_tgt`（领先一版）。
> **状态：能力补齐（第 1–3 批）、回归批与 C11/C12/C13 收尾批均已完成，
> airedge 88 版本全链纯 datasmith 命令跑通，src/tgt 终态闭环「新增 0 /
> 删除 0 / 修改 0」；遗留 Java 缺口见「八」「七」。**

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

## 二、已完成的修复与能力（第 1 批，已提交）

- **F1【bug·第 1 批实测】主键背书索引被生成 DROP INDEX**（`15ff479`）
  - 目标侧已删主键时，前向产物对主键背书索引同时生成 `DROP INDEX` 与
    `DROP CONSTRAINT`，PG 以 cannot drop index ... because constraint ... requires it
    拒绝（2.4.1 实测）。修复：索引删除/变更删除跳过 `Primary` 索引，
    其生命周期唯一跟随 PrimaryKeyChange 的约束删除。
- **C1/C2/C3 已全部补齐**（`6d6eb99`、`d8ee90b`），实现与验收见下文各条目「完成情况」。
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
- **完成情况（`6d6eb99`）**：pkg/conn 建模 Routine（prokind f/p，
  `pg_get_functiondef` 全文 + `pg_get_function_identity_arguments` 身份签名）与
  Sequence（`pg_sequences` + pg_depend 判定 SERIAL 隐式序列 owned_by）；例程排除
  扩展对象（deptype='e'）。pkg/diff 新增 RoutineDiff/SequenceDiff。pkg/sql/postgres
  生成 `CREATE OR REPLACE FUNCTION/PROCEDURE`（补结尾分号）/`CREATE SEQUENCE`/
  `ALTER SEQUENCE`（参数漂移以 ALTER 对齐，不 DROP 重建）/`DROP ... IF EXISTS ...
  CASCADE` 及回滚；序列/例程先于表 DDL。**聚合函数（prokind='a'）与窗口函数
  （prokind='w'）首版明确不支持**（pg_get_functiondef 无法还原），提取阶段排除。
- **阶段 3 闭环（2026-09-21）**：`Sequence.OwnedBy` 进入 equality，并以
  `ALTER SEQUENCE ... OWNED BY table.column/NONE` 对称生成；table、view、routine、
  sequence 统一进入对象依赖 DAG（dependent → prerequisite），创建使用正拓扑、删除
  使用逆拓扑，原视图依赖闭包不再单独排序。SQL/PLpgSQL 静态表引用、复合返回类型、
  routine 调用和 view 调用参与排序；动态 SQL、不支持语言、重载歧义与依赖环保守拒绝并
  输出对象链。真实 PostgreSQL 往返验证 forward 收敛、rollback 恢复结构及 ownership。

### C2【P0·阻断】视图依赖的列变更无拓扑排序

- **现象**：`pq: cannot alter type of a column used by a view or rule`（2.4.0 预对齐实测阻断）。
- **根因**：schema diff 按对象类型生成语句，不排依赖；列被视图引用时 PG 拒绝 ALTER，
  需要「DROP 依赖视图 → 改列 → 重建视图」，且重建定义（回滚用旧定义）引擎未管理。
- **方案**：结构产物按依赖排序——受影响视图（依赖闭包）先 DROP，表/列/索引变更，
  之后重建视图；回滚对称。实现可为「视图依赖图 + 拓扑排序」或简化为「整组视图删建」。
- **验收**：2.4.0 生成的 schema_diff.sql 在含 15 个视图的库上直接可执行。
- **完成情况（`6d6eb99`）**：CompareSchemas 计算「定义未变但（传递）依赖被变更
  对象」的双侧视图闭包（SchemaDiff.ViewsAffected，取新态侧定义）；产物按
  DROP（逆拓扑，CASCADE）→ 表/列 DDL → CREATE（拓扑）编排，回滚对称。

### C3【P0·阻断】rules 引用不存在的表 → 空模型陷阱

- **现象**：rules 中晚创建的表（如 `air_sys_mobile_app_user` 在 2.4.0 才建）在创建前的每轮
  diff-data 报 `primary key or not-null unique index required`，误导排查。
- **根因**：`pkg/db/postgres.ExtractTable` 对缺失表返回零列空模型而非错误
  （`run.go` 的 "table not found" 分支因此永远走不到）。
- **方案**：ExtractTable 对缺失表返回明确错误或 (nil, nil)；diff 层对 rules 中不存在的表
  显式报 `table not found: X`，或按配置跳过并输出 warning 清单。
- **验收**：错误信息直指表名；提供跳过开关后流水线无需外部过滤。
- **完成情况（`d8ee90b`）**：PG/MySQL ExtractTable 缺表显式返回
  `conn.ErrTableNotFound`（信息含 schema.table）；RunDataDiff 新增 SkipMissingTables
  （默认关闭保持报错），开启后缺表规则跳过、结果 Status=skipped + SkippedTables 清单、
  产物保持 COMPLETE；CLI 暴露 `--skip-missing-tables`（diff-data/diff-full）。

### C4【P2·设计决策】无物理主键表的数据比对约束

- **现象**：`primary key or not-null unique index required`（`pkg/diff/data.go:482`）；
  rules 的 `comparisonKey` 不能替代行身份（校验只认物理主键/非空唯一索引）。
  `air_user_client_role` 等无键关联表被排除。
- **方案选项**：
  a. 放宽校验：行身份允许 rules.comparisonKey（业务键比对）；
  b. 全列身份模式：无键表按全列定位（生成层已由 F0 打通），比对层同步支持；
  c. 至少在错误信息与文档中写明约束与替代方案。
- **建议**：a（含非空校验）+ c。
- **完成情况（第 3 批，方案 a+c）**：`pkg/diff.AllFieldsEqualRule` 新增
  `BusinessKey`（经 `CreateCompareRuleColumns` 记录显式
  `rules.comparisonKey`，比对列与业务键并列配置时行身份仍跟随业务键）。
  行身份解析收敛为 `rowIdentityColumns`：物理身份（主键 → 非空唯一索引，
  `RowIdentityColumns` 判定基线不变）优先，两者皆缺时回退业务键——业务键
  列必须存在且全部 NOT NULL（NULL 无法稳定定位行），否则报错说明原因；
  无任何身份时错误信息写明 comparisonKey 替代方案。整库通配展开无法携带
  业务键，仍只展开有物理行身份的表。生成层无键表回退全列定位（F0）配合
  业务键模式天然成立（行记录只含业务键+比对列）。单测：业务键身份/物理
  优先级/非空与存在性校验/端到端 ADD-MODIFY-DROP
  （`pkg/diff/business_key_test.go`）；集成：
  `TestRunDataDiffComparesKeylessTableByBusinessKeyOnPostgres`。

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
- **完成情况（第 2 批）**：采用方案 a。`diff-full` 内建两阶段：结构比对后，把
  结构 forward 语句逐条应用在 source 连接内的**影子事务**（`BEGIN; DDL; 数据
  比对; ROLLBACK`），数据比对经 `base.BaseAdapter` 新增的会话路由
  （`BindSession`，`QueryContext/QueryRow` 统一走会话或连接池）读取对齐后的
  影子结构——不落库、单连接，单侧表差异也被事务内 DDL 消除（target 独有表
  纳入数据比对、source 独有表随 DROP 消失），数据模型仅预热 target 侧；direct
  模式则把 target-only 的 source 视为空集生成全量 INSERT，source-only 仅走 DROP。模式
  由 `--data-diff-mode auto|shadow|direct` 控制（auto：PostgreSQL source 且
  存在结构差异时启用）；MySQL 无事务性 DDL，auto 自动回退直接比对并告警，
  强制 shadow 对 MySQL source 显式报错（DDL 隐式提交会污染库）。影子对齐
  失败即整体失败（forward 产物不可执行的信号），绝不静默降级；影子内只应用
  forward，绝不触碰 down。单测：模式决策/语句预览/会话路由
  （`TestDecideShadowDataDiff`、`TestBindSessionRoutesReadsToSession` 等）；
  集成：`TestFullDiffShadowTwoPhaseOnPostgres`（加列/删列轮 up 一次执行、
  影子 ROLLBACK 零残留、闭环收敛）。airedge 实测见「六」。

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
- **完成情况（第 2 批）**：①数据比对全面接入排除清单（默认账本表
  `schema_migrations`/`flyway_schema_history` + 配置 `excludeTables` + CLI
  `--exclude-tables`），命中规则在展开后统一过滤并计入
  `DataDiffResult.ExcludedTables`；②规则省略（`--rules` 不传或空数组）与
  通配（表名含 `*`/`?`，仅允许 `table` 字段）展开为「target 侧存在且具有行身份
  （`pkg/diff.RowIdentityColumns`：主键 → 普通、完整、全列非空唯一索引）的基础表」；
  双侧表仍要求两侧均有行身份，target-only 在影子/direct 分别使用事务空表/空行集，
  source-only 由结构 DROP 覆盖。展开确定性排序、显式
  规则优先不重复；③发现并修复账本**从属序列**缺口：被排除表拥有的 SERIAL
  隐式序列（如 `schema_migrations_id_seq`）随表一起排除
  （`filterSequencesByOwnedTable`），否则结构产物会对账本反向生成
  `DROP SEQUENCE`/`CREATE SEQUENCE`（88 轮链实测抓到）；④Web 完全比对保持
  显式选择语义（空表选择仍报错），整库通配仅 CLI/引擎层开放。单测：
  `TestExpandRules*`、`TestFilterRulesByExcludes*`、
  `TestFilterSequencesByOwnedTable`、`TestResolveDataRules*` 等。

### C7【P2·DX】不合规迁移文件名静默跳过

- `internal/datasmith/migrate/local/parser.go` 对不匹配文件名**静默跳过**
  （如单下划线 `V1.0.1_update.up.sql`），迁移缺版本无任何提示。
- **方案**：ScanMigrations 返回 skipped 清单，CLI 与 Web 输出 warning。
- **完成情况（第 3 批）**：`ScanMigrations` 签名改为返回
  `(files, skipped, error)`——文件名不符合规范的文件按相对路径收集进
  skipped（排序保证确定性），JSON/重复版本仍硬报错。CLI 侧
  `RunMigrations/RollbackLatest/RollbackTo/PlanRollback` 经
  `warnSkippedMigrations` 输出 `logger.Warn` + 进度回调；Web 侧迁移计划
  API 新增 `warnings` 字段、迁移任务日志透出 WARNING。不中断合法迁移链。
  单测：`TestScanMigrationsReportsNonCompliantFileNames`、
  `TestRunMigrationsWarnsOnSkippedFileNames`。

### C8【P2·bug】Web 删除脚本留下孤儿版本登记

- `internal/server/libraries.go` `handleDeleteScript` 删文件不清理
  `LibraryMeta.Versions`（现网 store.json 已有 3.7.1 孤儿条目）。
- **方案**：删除 up 文件时同步清理版本元数据；提供 store 一致性自检命令。
- **完成情况（阶段 4 最终口径）**：up（含省略方向、解析为 up 的旧式文件）
  是版本可执行入口，也是 `LibraryMeta.Versions` 生命周期依据。删除 up 时即使
  down 仍保留也清理登记；只删 down 且 up 仍在则保留；无 up 时重复清理幂等。
  `handleDeleteScript` 的目录扫描与 `DeleteVersionMeta` 错误向 HTTP 传播，并以
  非 2xx 明确表示“脚本已删除、元数据清理失败”；`SweepOrphanVersionMeta`
  返回 `(removed, error)`，启动扫描失败由 `server.New` 返回。测试覆盖删 up 留
  down、删 down 留 up、删除最后脚本、隐式 up、扫描失败、持久化失败与重复清理。

### C9【P3·可选】exec-sql 失败定位增强

- 驱动不提供错误位置时报「语句范围 1-44」粗粒度信息。
- **方案**：错误信息附失败语句文本前 N 字符预览；文档写明 multi-statement
  单事务语义（--tx 为整文件原子，实测正确）。
- **完成情况（第 3 批）**：①语句预览实现收敛到公共包
  `pkg/utils.StatementPreview`（压缩空白单行化、120 字符截断），
  影子事务（shadow.go 委托）与 exec-sql 共用同一格式；②`--tx` 与
  `--dry-run` 改为在事务内**逐条执行**扫描出的语句：任一失败即报
  「第 i/N 条语句，起始于脚本第 r 行第 c 列: <语句预览>」，不再依赖驱动
  位置信息；整文件原子性不变（单事务，全部成功才提交，失败整体回滚；
  扫描失败的脚本仍走整文件执行路径）。③非事务模式保持整文件发送，
  PostgreSQL 位置分支与单语句分支的错误信息同样附带语句预览。
  README 写明 multi-statement 单事务语义。单测：
  `TestExecuteSQLTransactionAttributesFailingStatement`（sqlmock 中途
  失败脚本定位）、`TestExecutionErrorIncludesStatementPreviewOnPostgresPosition`、
  `TestStatementPreviewCollapsesWhitespaceAndTruncates`。

### C10【验收项】exec-sql 对外部脚本的兼容性

- 原 Flyway 脚本含 `$$` DO 块 / `CREATE FUNCTION`；scanner 已支持 dollar-quote，
  但「保守拒绝歧义脚本」策略下能否吃下全部 88 个原脚本未验证。
- **做法**：能力补齐后，回放通道优先用 exec-sql（--tx），失败脚本清单作为 scanner
  改进输入（预期少量，可个案处理）。
- **第 1 批实测结论（2026-09-20）**：**88/88 全部经 exec-sql --tx 回放成功，scanner
  零拒绝**，C10 风险对 PG 链基本解除。唯一注意事项：重复回放已应用脚本会在
  `DROP TABLE` 处被正确拒绝（事务回滚，无副作用）——符合预期语义。
- **回归批复核结论（2026-09-20，逐条执行新路径）**：第 3 批将 `--tx`/`--dry-run`
  改为事务内逐条执行后，回归批 88 个原脚本（含 `$$` DO 块、CREATE FUNCTION、
  DDL/DML 混排）再次全数通过——**零拒绝、零错拆、零语句级定位触发**，C10 正式关闭。

### C11【P2】CHECK 约束不参与结构比对（回归批发现）

- **现象（2026-09-20 回归批闭环 catalog 复核）**：原脚本内联创建的 CHECK 约束
  （`air_inst_bug`、`air_inst_testconfig` 的 `delete_flag` 检查）只存在于回放侧
  target，链推进的 source/verify 侧缺失；diff 产物与回退产物均不覆盖。
- **根因**：`pkg/conn` Table 模型无 CHECK 约束；提取/比对/生成全链未建模。
- **方案**：PG 侧经 `pg_constraint contype='c'` 提取（含表达式规范化），diff 增加
  ChecksAdded/Dropped，生成 `ADD CONSTRAINT ... CHECK` / `DROP CONSTRAINT`，
  回滚对称；MySQL 8 同源支持。不阻断链收敛，列为独立子项。
- **验收**：含内联 CHECK 的表在 diff 产物中出现约束语句，删建闭环为空。
- **完成情况（收尾批）**：pkg/conn 建模 `CheckConstraint`（Name + 方言原生
  Definition：PG 为 `pg_get_constraintdef` 全文，可带 `NOT VALID`；MySQL 为
  `information_schema.check_constraints.check_clause` 裸表达式，生成层补 CHECK
  包裹），Table 新增 `Checks` 映射。PG/MySQL 提取均按表级查询（contype='c' /
  constraint_type='CHECK'）；排除口径与既有索引/约束提取一致——约束从属于表，
  扩展对象与账本经表级 include/exclude 过滤生效，无独立从属对象需要额外建模。
  pkg/diff compareTable 新增 ChecksAdded/Dropped/Modified（同名不同义计修改），
  定义比较只做空白规范化（对齐 equalViewDefinition 的 clean 口径），不做表达式
  语义改写。pkg/sql 新增 `ICheckConstraintDialect` 附加能力接口（沿
  IDataBatchDialect 源兼容模式，两方言均实现）：删除（含 modified 旧约束）进
  dropDependencies 阶段先于列删除执行，新增在 buildKeys 阶段列 DDL 之后添加；
  新建表的 CREATE TABLE 内联携带 CHECK。** airedge 实测：除报告所列 2 条外，
  `air_inst_testset` 还有第 3 条 `delete_flag` CHECK（原报告目录抽查未列），
  一并被捕获对齐。**
- **验收结果**：✅。单测 `TestCompareSchemasCheckConstraint*`、
  `TestGenerateSchemaSQLCheckConstraint*`、
  `TestGenerateSchemaSQLAddedTableCarriesInlineChecks`（不依赖 Docker）；
  集成 `TestCheckConstraintsAndViewCommentsRoundTrip`（PG）与
  `TestMySQLCheckConstraintsRoundTrip`（MySQL 8.4），均验证「检出→正向执行→
  闭环为空→回滚复原」；airedge 双库实测见「九」。

### C12【P3】视图注释（COMMENT ON VIEW）不参与比对（回归批发现）

- **现象（同上复核）**：4 个视图在 target 有 `COMMENT ON VIEW`，source/verify
  缺失；`compareTable` 对视图按定义相等短路返回，不比注释；生成层无 COMMENT 语句。
- **方案**：视图比对在定义相等后追加 Comment 比较；`generateView` 补
  `COMMENT ON VIEW`；表注释已有（`equalTableComment`），对齐即可。
- **验收**：仅注释差异的视图在产物中生成 COMMENT 语句而非整组删建。
- **完成情况（收尾批）**：视图注释以 `Table.Comment` 为统一载体（PG
  ExtractView 经 obj_description 读取 COMMENT ON VIEW 注释；**MySQL 不支持视图
  注释，information_schema.tables 的 TABLE_COMMENT 恒为常量 'VIEW'，提取层不再
  读取**，避免伪差异）。compareTable 视图分支在定义相等后按既有
  `equalTableComment` 口径追加比较，仅注释差异返回只含 `CommentChange` 的
  TableDiff（不设 ViewDefinitionChange）。生成层新增 `IViewCommentDialect` 附加
  能力接口，仅 PG 实现 `COMMENT ON VIEW ... IS '...'`（空注释恢复为 `IS ''`，
  提取层把 NULL 与空串同等归一）；MySQL 跳过并顺带移除不可达的
  `ALTER VIEW ... COMMENT` 输出（MySQL 无此语法）。`GenerateViewDDL` 在
  ViewDefinition.Comment 为空时回退读 Table.Comment——依赖闭包弹跳重建的视图
  不丢注释。
- **验收结果**：✅。单测 `TestCompareSchemasViewCommentOnlyChange`、
  `TestGenerateSchemaSQLViewCommentChangeEmitsCommentOnly`（断言产物恰为 1 条
  COMMENT、无 DROP/CREATE VIEW）、`TestGenerateViewDDLKeepsTableCommentOnRebuild`；
  集成含于 `TestCheckConstraintsAndViewCommentsRoundTrip`；airedge 实测见「九」。

### C13【P3·DX】TablesModified 计数含「零语句差异」表（回归批发现）

- **现象（同上复核）**：末轮闭环产物为空（0 语句）但汇总行报「修改 1 张表」——
  `air_inst_checklist_item` 主键背书索引名不同（原脚本整表复制建出），
  `equalIndex` 按名称判异计入修改，而生成层按 F1 语义跳过主键背书索引 →
  计数与产物不自洽。
- **方案**：生成后回收「该表实际产语句数」再汇总；或 `equalIndex` 对 `Primary`
  索引忽略名称（与 `equalPrimaryKey`「仅比对列及顺序」口径对齐）。
- **验收**：闭环场景 diff-full 汇总行为「新增 0 / 删除 0 / 修改 0」。
- **完成情况（收尾批，采用方案二并按实测形态扩展落地）**：主键背书索引在
  `compareTable` 的索引三路（Added/Dropped/Modified）全部不建模为索引差异。
  **选择理由**： airedge 实测形态是两侧背书索引**名不同**——该差异落入
  Added/Dropped 而非 Modified，仅在 `equalIndex` 忽略名称覆盖不到；三路排除才
  与生成层 F1 语义（DROP/CREATE/重建三条路径全部跳过 Primary 索引）形成完全
  对齐的最小闭包。主键列集差异仍由 `PrimaryKeyChange` 检出（`equalPrimaryKey`
  口径不变），非背书索引的名称判异不受影响（`equalIndex` 本体未改动）。
  方案一（生成后按产语句数回收汇总）不采用：它只修正报表数字，不修正差异
  建模本身，且需让汇总依赖生成结果、耦合两层。
- **验收结果**：✅。airedge 终态双库 diff-full 闭环实测「新增 0 / 删除 0 /
  修改 0」且产物为空（见「九」）；单测
  `TestCompareSchemasIgnoresPrimaryKeyBackingIndexName`（含非背书索引名称判异
  不放松的反向断言）、`TestGenerateSchemaSQLPrimaryKeyBackingIndexRenameYieldsNoStatements`
  （手工构造该形态 TableDiff 时生成层仍产出 0 条语句，F1 兜底不回归）；
  `TestGenerateSchemaSQLSkipsPrimaryKeyBackingIndexDrop`（F1 既有单测）不回归。

## 四、实施顺序

| 批次 | 内容 | 出口标准 |
|---|---|---|
| 第 1 批 | F0 提交；C1；C2；C3 | ✅ 完成（2026-09-20）。实测超出出口标准：**88 个版本全链 2.1.0→3.7.0.35 纯 datasmith 命令跑通**，末轮结构比对 0 语句、数据比对 0 DML，双闭环为空 |
| 第 2 批 | C5；C6 | ✅ 完成（2026-09-20）。两阶段预对齐编排与外部 rules 生成脚本全部废弃：**88 轮以 `exec-sql 回放 → diff-full 单命令（整库通配 + auto 影子两阶段 + --migrate-dir）→ migrate-script 推进 source` 重跑全链成功**，39 轮触发影子对齐，账本 88/88 success，末轮结构 0 语句 / 数据 0 DML |
| 第 3 批 | C4a；C7；C8；C9 | ✅ 完成（2026-09-20）。无键表按 comparisonKey 业务键可比（物理身份基线不变）；迁移文件名跳过清单 CLI/Web 告警；Web 删除脚本同步清理版本登记 + 启动 store 自检；exec-sql 事务模式逐条执行并定位失败语句（整文件原子语义不变） |
| 回归 | C10 + 全链重跑（88 轮） | ✅ 完成（2026-09-20）。闭环 diff 产物为空；verify 从零 migrate-script 全链应用成功；回退冒烟通过；88 对产物登记脚本库。详见 `docs/migration-chain-regression-report.md` |
| 收尾 | C11；C12；C13 | ✅ 完成（2026-09-20）。CHECK 约束/视图注释进入比对与产物（双库 fixture 集成用例验证闭环与回滚对称）；主键背书索引不再计入表修改，airedge 终态闭环「新增 0 / 删除 0 / 修改 0」。见「九」 |
| 评审阶段 4 | S6；SP5；SP6；SP7；S7 | ✅ 完成（2026-09-21）。C8 生命周期与错误传播统一；任务记录 requested/effective mode；文档状态消歧；共享行定位策略落入 `pkg/rowloc` |

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

## 六、当前状态（第 2 批完成后，2026-09-20）

- **C5+C6 已补齐并经 airedge 全链实测**：`/tmp/ds-acceptance-batch2/` 工作区，
  一次性库全量重建后 88 轮（2.1.0→3.7.0.35）以最终形态跑通——
  `exec-sql --tx 回放 target → diff-full 单命令（省略 rules 整库通配、auto
  影子两阶段、--migrate-dir 生成 up/down）→ migrate-script 推进 source`。
  全程 **0 次预对齐编排、0 次外部脚本加工、0 次 --best-effort /
  --skip-missing-tables、0 次 psql 回退**；39 轮触发影子对齐，每轮产物
  grep 校验账本零出现；migrate-script 推进 88/88 success（账本/checksum
  与 diff 全程共存）；末轮 diff-schema 0 语句、整库 diff-data 0 DML，
  两侧各 248 张表。C5 热点轮 2.2.0（加列）与 3.0.0（删列）的 up 均一次
  执行通过。
- **本批新增修复**：账本从属序列缺口（被排除表的 SERIAL 隐式序列曾以
  `DROP/CREATE SEQUENCE` 出现在结构产物，`schema_migrations_id_seq` 实测）。
- 第 1 批演练工作区 `/tmp/ds-acceptance/` 保留作对照；第 2 批迁移脚本库在
  `/tmp/ds-acceptance-batch2/migrations/`（88 对 up/down，40MB，尚未登记
  脚本库，留回归批）。
- **当前遗留**：airedge 的 8 个 Java 迁移效果缺口仍缓置；Web 已有
  auto/shadow/direct 开关，任务现分别记录 requested/effective mode。
- **下一批（第 3 批）**：C4a（无键表按配置可比）+ C7（迁移文件名静默跳过
  告警）+ C8（Web 删除脚本孤儿版本清理）+ C9（exec-sql 失败定位增强）；
  随后回归批：全链重跑 + verify 库 migrate-script 全链应用 + 回退冒烟 +
  脚本库登记 + Java 迁移缺口报告。

## 七、当前状态（第 3 批完成后，2026-09-20）

- **C4a/C7/C8/C9 已全部补齐**（各条目「完成情况」含实现与单测清单）。
  实测：本机 PG 14.22 冒烟——中途失败脚本经 `exec-sql --tx` 报
  「第 4/5 条语句，起始于脚本第 4 行第 1 列: INSERT INTO no_such_table …」
  且事务零残留；`migrate-script` 对混入的单下划线文件名在推进前输出
  `[WARN] 跳过不合规迁移文件名` 且不中断合法链；C4a 经双库 fixture
  集成用例（`integration_keyless_test.go`）验证无键关联表按业务键
  检出 ADD/MODIFY/DROP 并生成以业务键定位的 DML。
  注意一项执行语义变化：`exec-sql` 的 `--tx`/`--dry-run` 现按扫描出的
  语句**逐条执行**（单事务内，整文件原子不变），失败直接定位到语句与
  文本预览——回归批的 88 轮 `exec-sql --tx` 回放将首次走该路径，等于
  对 scanner 语句边界做一次全链验证（39 轮影子对齐已按同粒度逐条应用过
  生成的 forward）。
- 当前仅余 8 个 Java 迁移效果缺口；88 对 up/down 已登记脚本库，Web 已有
  data diff mode 开关并分别记录 requested/effective mode。
- **下一批（回归批）**：全链重跑（88 轮 exec-sql 回放 → diff-full 单命令
  → migrate-script 推进）+ verify 库从零全链应用 + 最近版本回退冒烟 +
  产物登记 `datasmith-web-data/libraries/lib_8989e9f44806929e/` 并同步
  store.json + Java 迁移效果缺口报告。

## 八、当前状态（回归批完成后，2026-09-20）

**回归批验收通过**，全部使用 datasmith 命令（0 外部脚本加工、0 psql 回退、
0 `--best-effort` / `--skip-missing-tables`）。完整数据与报告见
`docs/migration-chain-regression-report.md`。要点：

- **全链重跑**：reset-db 全量重建 src/tgt（`--dry-run` 审阅后 `--yes`）、新建
  verify 空库；88 轮（2.1.0→3.7.0.35）零失败，12 分钟跑完；39 轮影子对齐
  （最大单轮 133 条 DDL）；账本 88/88 success、checksum 全非空（V2.6.1/V2.6.2
  与 V3.7.0.8/V3.7.0.10 两对原脚本内容相同，属原仓库重复）。
- **C10 正式关闭**：`exec-sql --tx` 逐条执行新路径吃下 88/88 原脚本，零拒绝、
  零错拆（含 `$$` DO 块与 CREATE FUNCTION），语句级定位未触发。
- **三闭环为空**：末轮、verify（从零 migrate-script 88/88 后与 target，
  194 表参与）、回退往返复原（V3.7.0.35 down→账本 `rolled_back`→再推进
  `success`→闭环仍空），产物均为结构 0 语句、数据 0 DML。
- **登记**：88 对 176 文件（40MB）入 `lib_8989e9f44806929e`，store.json 88 条
  版本元数据（expectedConnectionId 沿用），清理旧样例 `V3.7.0__update.*` 与
  孤儿条目 `3.7.1`，一致性复核通过。
- **C11/C12/C13 当前结论**：CHECK 约束与视图注释已进入比对/产物，主键背书
  索引名称差异不再计入 TablesModified；airedge 收尾复核为 0/0/0 且产物为空。
- **Java 迁移缺口**：8 个跳过；V3_0_1（链断级）与 V3_2_0_99（登录不可用）
  为最高优先，全部原则上可 PL/pgSQL 等效重写，建议顺序与影响分级见报告「七」。
- **未竟事项（如实）**：仅 8 个 Java 迁移缺口仍缓置；如需闭合，按报告
  「七」顺序以 PL/pgSQL 等效重写并重跑闭环。

## 九、当前状态（C11/C12/C13 收尾批完成后，2026-09-20）

**收尾批验收通过。** C11/C12/C13 完成情况与选择理由已回填各条目；全部 CI gate
（gofmt、单测、race、vet、staticcheck v0.8.1、govulncheck v1.1.4、双库 E2E +
覆盖率 gate：overall 72.8%，各组 ≥ 70%）通过。

- **双库 fixture 集成**（`test/integration/check_view_test.go`，MySQL 8.4.3 +
  PostgreSQL 17.2）：PG 用例覆盖 C11+C12——CHECK 检出、`ADD CONSTRAINT ... CHECK`
  与 `COMMENT ON VIEW` 产物、注释差异不触发视图删建、正向执行闭环为空、回滚
  复原；MySQL 用例覆盖 C11 同源闭环与回滚对称。
- **airedge 终态双库实测**（`airedge_mig_src` → `airedge_mig_tgt`，全程
  datasmith 命令，工作区 `/tmp/ds-acceptance-batch3/`）：
  1. 第 1 轮 diff-full：**修改 7 张表**——3 条 `delete_flag` CHECK
     （`air_inst_bug`/`air_inst_testconfig`/`air_inst_testset`，第 3 条为原报告
     目录抽查未列的新捕获）+ 4 个视图注释；产物 7 条语句全部经
     `exec-sql --tx` 一次执行通过；数据比对 0 DML；
     `air_inst_checklist_item`（背书索引名差异）不再出现（C13）。
  2. 第 2 轮 diff-full：**新增 0 / 删除 0 / 修改 0**，结构产物仅剩
     EXECUTE-ON 头（32 字节）——C11/C12 删建闭环为空 + C13 口径自洽同时达成。
  3. 回退往返：down 应用后残差完整重现，且重新生成的 up/down 与第 1 轮产物
     **逐字节一致**（回滚对称 + 生成确定性）；再次推进后复归 0/0/0。
- **环境状态**：`airedge_mig_src` 已收敛为与 `airedge_mig_tgt` 一致（3 CHECK +
  4 视图注释补齐），原残差不再存在；`airedge_mig_verify` 未动。
- **未竟事项（如实）**：8 个 Java 迁移缺口仍缓置（其中 V3_2_0_99 涉及配置数据
  与 DDL、V3_0_1 含 2 条列默认值 ALTER，链产物与真实 Flyway 升级库在这几处
  不一致，见「八」「七」）。Web 已有模式开关并记录 requested/effective。
- **下一批建议**：如闭合 Java 缺口，按报告「七」顺序（V3_0_1 → V3_2_0_99 →
  0_1/0_2/0_4 → 0_5 → V3_1_0_1）以 PL/pgSQL 等效重写后经 `exec-sql` 补链并
  重跑闭环；否则迁移链能力线已收口。

## 十、当前状态（Web 数据比对模式开关完成后，2026-09-20）

Web 控制台完全比对具备独立 data diff mode 开关，引擎语义零重写：

- **Server**（`internal/server/diffjobs.go`）：diff-full 任务请求新增
  `dataDiffMode` 字段，经引擎 `IsValidDataDiffMode` 提交期校验（非法值 400）、
  `NormalizeDataDiffMode` 归一（空串 → auto），透传 `FullDiffParams`。任务参数
  保存 `requestedDataDiffMode`；引擎用 `DataDiffModeSelection` 决策后，任务 API
  摘要、`summary.json` 和日志统一记录 requested 与 effective（shadow/direct）。
  diff-data 任务不涉及影子机制，不设开关（与 CLI 一致）。
- **前端**（`web/src/pages/DiffFull.tsx`）：高级参数区新增「数据比对模式」
  单选（自动(推荐) / 强制影子事务 / 直接比对），每个选项带行为说明（含
  MySQL 强制影子将被拒绝的提示），默认自动；`api.ts` 增补
  `DataDiffMode` 类型。
- **引擎**（`internal/datasmith/diff/shadow.go`）：新增附加 helper
  `IsValidDataDiffMode` / `NormalizeDataDiffMode`，校验与决策逻辑不变。
- **验收**：①单测 `TestDiffFullSubmitValidatesDataDiffMode`（非法值 400；
  shadow/direct/缺省三形态的参数归一与日志可见）及
  `TestDiffFullTaskModeIsConsistentAcrossAPIArtifactAndLog`（auto→shadow/direct、
  显式 shadow/direct 的 API/摘要/日志一致）；②API 驱动的 Web E2E
  `TestWebFullDiffDataDiffMode`（真实双库 fixture + 内嵌 server）：PG
  auto/shadow 走影子两阶段、PG direct 走直接比对并告警、MySQL auto 回退
  直接比对、MySQL 强制 shadow 执行期被引擎拒绝——五形态全部通过。另实测
  确认 direct 模式对「target 侧新增列」会硬失败（源行读取缺列），属文档化
  的公共列容错边界，选择影子/auto 即可规避。
- 全部 CI gate 通过；覆盖率 overall 73.7%（各组 ≥ 70%）。
- **未竟事项（如实）**：仅剩 8 个 Java 迁移缺口（用户缓置，闭合方案见
  报告「七」）。

## 十一、当前状态（代码评审阶段 4 完成后，2026-09-21）

- C8 版本元数据统一以 up 可执行入口为生命周期依据，错误传播与幂等场景均有
  HTTP/sweep 回归测试；同一脚本库的文件与元数据变更按库串行化，并有受控交错并发测试。
- diff-full 用 `DataDiffModeSelection` 记录 requested/effective，任务 API 摘要、
  `summary.json` 与日志在四种决策场景中一致。
- `pkg/rowloc` 统一主键、非空唯一身份与无可靠身份的定位策略；MySQL/PostgreSQL
  引用和值渲染保持各自方言，最终 SQL 等价测试通过。
- 完整 gate 通过：overall 74.8%（6206/8296），db 78.9%、diff 77.8%、sql
  76.3%、migrate 75.9%、exec 89.1%；MySQL 8.4.3/PostgreSQL 17.2 双库集成
  测试、race、vet、staticcheck v0.8.1、govulncheck v1.1.4 和前端构建均通过。
- 当前仅余 8 个已缓置 Java 迁移效果缺口；远程 Web 认证和其他数据库对象扩展
  不属于本阶段。
