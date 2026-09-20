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
  纳入数据比对、source 独有表随 DROP 消失），数据模型仅预热 target 侧。模式
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
  通配（表名含 `*`/`?`，仅允许 `table` 字段）展开为「两侧均存在且具有行身份
  （`pkg/diff.RowIdentityColumns`：主键 → 非空唯一索引）的基础表」，影子模式
  下候选为 target 全集（单侧差异已被事务内 DDL 消除），展开确定性排序、显式
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
- **完成情况（第 3 批）**：①`handleDeleteScript` 删除脚本后经
  `removeVersionMetaIfOrphan` 检查：该版本已无任何脚本文件（up/down 均无）
  时同步删除版本登记（`Store.DeleteVersionMeta`，幂等清理语义，不重写
  任何安全逻辑）；②`SweepOrphanVersionMeta` 一致性自检：扫描各脚本库
  目录，清理「已无对应脚本文件」的孤儿版本登记并返回清单，
  `server.New` 启动时自动执行一次并以 `logger.Warn` 告警——现网
  store.json 的 3.7.1 孤儿条目在下次启动 Web 控制台时即被修复。
  单测：`TestDeleteScriptCleansOrphanVersionMeta`、
  `TestSweepOrphanVersionMetaRemovesEntriesWithoutScripts`、
  `TestServerNewRunsOrphanVersionSweep`。

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

## 四、实施顺序

| 批次 | 内容 | 出口标准 |
|---|---|---|
| 第 1 批 | F0 提交；C1；C2；C3 | ✅ 完成（2026-09-20）。实测超出出口标准：**88 个版本全链 2.1.0→3.7.0.35 纯 datasmith 命令跑通**，末轮结构比对 0 语句、数据比对 0 DML，双闭环为空 |
| 第 2 批 | C5；C6 | ✅ 完成（2026-09-20）。两阶段预对齐编排与外部 rules 生成脚本全部废弃：**88 轮以 `exec-sql 回放 → diff-full 单命令（整库通配 + auto 影子两阶段 + --migrate-dir）→ migrate-script 推进 source` 重跑全链成功**，39 轮触发影子对齐，账本 88/88 success，末轮结构 0 语句 / 数据 0 DML |
| 第 3 批 | C4a；C7；C8；C9 | ✅ 完成（2026-09-20）。无键表按 comparisonKey 业务键可比（物理身份基线不变）；迁移文件名跳过清单 CLI/Web 告警；Web 删除脚本同步清理版本登记 + 启动 store 自检；exec-sql 事务模式逐条执行并定位失败语句（整文件原子语义不变） |
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
- **未竟事项（如实）**：airedge 有 8 个 Java 迁移无法 SQL 回放，效果缺口
  报告留回归批；`--data-diff-mode` 与影子机制当前仅 CLI，Web 完全比对走
  引擎默认 auto（行为自动受益），无独立开关。
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
- 未竟事项不变：8 个 Java 迁移效果缺口报告留回归批；88 对 up/down 迁移
  脚本库登记留回归批；`--data-diff-mode` 与影子机制仍仅 CLI 暴露。
- **下一批（回归批）**：全链重跑（88 轮 exec-sql 回放 → diff-full 单命令
  → migrate-script 推进）+ verify 库从零全链应用 + 最近版本回退冒烟 +
  产物登记 `datasmith-web-data/libraries/lib_8989e9f44806929e/` 并同步
  store.json + Java 迁移效果缺口报告。
