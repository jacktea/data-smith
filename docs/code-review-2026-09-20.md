# DataSmith 增量代码评审报告（2026-09-20）

## 1. 评审范围

- 固定基线：`debb972443ad8068c36393b4ca3836250ba832cc`
- 当前版本：`0742d0e`
- 增量范围：`debb972...0742d0e`，共 18 个提交、75 个文件，约 7,129 行新增、360 行删除
- 主要规格：`docs/migration-chain-capability-plan.md`
- 验收记录：`docs/migration-chain-regression-report.md`
- 评审轴：Standards（仓库约定、操作安全、正确性、原子性）与 Spec（C1-C13、Web 数据比对模式）

初始评审为只读评审；截至 2026-09-21，已从起始提交 `0742d0e` 依次完成阶段 1（O1、O2、O3、S2）、阶段 2（SP1、SP2、S3、S4）和阶段 3（S1、S5、SP3、SP4）整改，状态见各发现及第 8 节。

## 2. 结论

本轮增强补充了 PostgreSQL 普通函数、存储过程、序列、CHECK、视图注释、主键背书索引、整库规则展开与 PostgreSQL 影子事务数据比对，方向正确，现有自动化门禁全部通过。

阶段 1/2/3 已关闭既有安全边界、diff-full 正确性/原子性以及 PostgreSQL 非表对象闭环问题。序列 ownership 已参与 diff/DDL；table、view、routine、sequence 使用统一依赖 DAG，依赖不可靠或成环时失败关闭。

函数和序列已进入当前功能范围，但属于 PostgreSQL 首版部分支持，而不是完整数据库对象迁移能力。

## 3. Standards 发现

### S1【P1｜已修复 2026-09-21】例程、序列和表使用固定阶段排序，缺少对象依赖图

位置：`pkg/sql/generator.go:257-275`

生成器固定按“删视图/依赖/表 → 删序列/例程 → 建序列/例程 → 改列/建表”输出。返回某张表复合类型的函数会阻止先行 `DROP TABLE`；引用本轮新增表的函数又会在表创建前执行失败；例程之间的调用关系也没有参与排序。

建议建立 schema object dependency DAG；至少覆盖 table、view、routine、sequence 的创建与删除方向，并在不可解环时明确拒绝生成。

整改：新增统一对象依赖 DAG Module，节点携带对象类型、schema、identity 与操作步骤，边统一为 dependent → prerequisite；创建按正拓扑、删除按逆拓扑。原视图依赖闭包接入同一排序，函数复合返回类型、SQL/PLpgSQL 静态表引用、routine 调用、view 调用、列默认值和 sequence ownership 均形成依赖边。排序以稳定键打破并列，依赖环返回实际对象链；动态 SQL、不支持语言和重载调用歧义保守拒绝。

### S2【P1｜已修复 2026-09-20】影子事务成功路径忽略 rollback 错误

位置：`internal/datasmith/diff/full.go:160-164`

成功路径在 defer 中调用 `_ = txs.rollback()`，随后仍返回成功并报告 source 已恢复。网络或驱动回滚失败时，调用者无法证明影子结构“零落库”。应把显式 rollback 纳入主返回路径并传播错误，defer 只兜底清理。

整改：数据阶段结束后先解除会话绑定并显式 rollback；rollback 失败会使任务失败，只有成功后才记录“source 结构恢复原状”，defer 仅用于异常路径兜底。失败注入回归测试覆盖错误传播与日志语义。

### S3【P1｜已修复 2026-09-20】部分/表达式唯一索引可被误当作行身份

位置：`pkg/diff/data.go:422-453`

`RowIdentityColumns` 没有排除 `Index.Where`、`Index.Expression`，也没有把索引列在表模型中缺失视为失败。例如 `UNIQUE (tenant, lower(email))` 可能退化为只用 `tenant` 定位，导致合并行或漏差。

行身份候选必须是非部分、非表达式、全部列存在且 NOT NULL 的普通唯一索引。

整改：`RowIdentityColumns` 现在同时检查唯一性、谓词、表达式、列完整性和 NOT NULL；PostgreSQL 的完整 `pg_get_indexdef` 单独保存在 `Index.Definition`，不再污染表达式判定。回归覆盖部分、纯表达式、表达式混合、缺列与普通唯一索引。

### S4【P1｜已修复 2026-09-20】`diff-full` 四个产物不是整体原子发布

位置：`internal/datasmith/diff/run.go:222-241`、`internal/datasmith/diff/full.go:167-184`

schema 正反向文件在数据阶段开始前已经发布。数据比对失败时，目录中可能留下本轮 schema 文件和上一轮 data 文件，形成可被登记或人工误用的混合版本。

建议四个文件统一写入暂存目录；全部生成并校验成功后再一次性发布。

整改：`diff-full` 在最终目录同文件系统的 staging 目录中生成结构/数据四产物，全部存在后统一备份并发布；生成、写盘、目录同步或任一 rename 失败会恢复上一整组。失败注入覆盖数据阶段、写盘和第三个文件发布失败。

### S5【P2｜已修复 2026-09-21】序列 ownership 建模后没有进入相等判断和 SQL

位置：`pkg/conn/db.go:208-236`、`pkg/sql/postgres/postgre.go:425-500`

`Sequence.OwnedBy` 已提取，但 `Equal` 忽略该字段，CREATE/ALTER 也不生成 `ALTER SEQUENCE ... OWNED BY ...`。SERIAL 序列重建后会失去所有权，删除表时可能遗留孤儿序列，二次 diff 却错误报告收敛。

整改：`OwnedBy` 纳入 `Sequence.Equal`；新增/修改序列分别在拥有表与列存在后生成 `OWNED BY table.column`，解除时生成 `OWNED BY NONE`。真实 PostgreSQL 用例同时断言新增 SERIAL ownership、ownership 修改、forward 二次收敛和 rollback 恢复 `NONE`。

### S6【P2】删除脚本后的元数据清理错误被吞掉

位置：`internal/server/libraries.go:292-315`

脚本删除成功后，目录扫描失败或 `DeleteVersionMeta` 持久化失败均不影响 HTTP 200。清理函数应返回 error，并由接口返回明确失败或可观测的部分成功状态。

### S7【代码气味】MySQL/PostgreSQL 行定位逻辑重复

位置：`pkg/sql/mysql/mysql.go:66-105`、`pkg/sql/postgres/postgre.go:73-112`

两种方言的行身份选择和 DELETE 定位逻辑高度重复。建议提取共享行定位策略，避免修复只落到单一数据库。

## 4. Spec 发现

### SP1【P1｜C12｜已修复 2026-09-20】视图定义与注释同时变化时漏掉定义更新

位置：`pkg/diff/schema.go:251-280`

规格要求“定义相等后再比较注释”；实现却先比较注释并立即返回。两者同时变化时只得到 `CommentChange`，不会产生 `ViewDefinitionChange`。

需要让同一 `TableDiff` 同时携带两种变化；只有定义相等且仅注释变化时才走 comment-only 路径。

整改：视图定义与注释独立比较，同一差异可同时携带 `ViewDefinitionChange` 和 `CommentChange`；生成器按 DROP/CREATE 后 COMMENT 的顺序恢复目标态。

### SP2【P1｜C5｜已修复 2026-09-20】单侧表的数据迁移语义不完整

位置：`internal/datasmith/diff/full.go:131-142`、`internal/datasmith/diff/rules.go:77-163`、`internal/datasmith/diff/run.go:409-448`

存在两个相反方向的问题：

1. direct/MySQL 模式过滤 target 独有表；schema up 只创建空表，target 中已有的数据不会生成 INSERT，但任务仍可显示 COMPLETE。
2. shadow 模式显式配置 source 独有表时，规则会原样保留；forward 已在事务内删除该表，而 target 本来也不存在，数据阶段会以 table not found 失败。

统一语义应是：target-only 将 source 视为空集并生成全量 INSERT；source-only 由 schema DROP 覆盖，不进入数据阶段；显式与通配规则保持一致。

整改：direct 模式为 target-only 注入空 source 行集并读取目标完整列，shadow 模式复用事务内新建空表；显式和通配 source-only 均不进入数据阶段。PostgreSQL/MySQL 双库测试覆盖两类规则、执行 forward 和二次 diff 收敛。

### SP3【P1｜C1｜已修复 2026-09-21】序列所有权验收不成立

位置：`pkg/conn/db.go:225-236`、`pkg/sql/postgres/postgre.go:425-500`、`docs/migration-chain-regression-report.md:184-188`

报告宣称 SERIAL 所有权链路已实测，但实现和断言只覆盖序列存在及参数，不覆盖 ownership，属于假收敛。

整改：回归报告已改为真实验收口径；集成测试直接读取并断言 `with_serial_id_seq → with_serial.id`、`shared_owner_seq → ownership_table.id`，回滚后断言后者恢复为空，并在两个方向执行结构 diff 为空。

### SP4【P1｜C1｜已修复 2026-09-21】例程/序列依赖安全排序未完成

位置：`pkg/sql/generator.go:257-275`

C1 的“序列/函数先于表”只解决列默认值依赖，不能覆盖函数依赖表类型、SQL 函数查询新表、删表前需先删函数等场景，尚未达到“迁移链可直接执行”。

整改：统一 DAG 回归覆盖函数返回新增表复合类型、SQL/PLpgSQL 静态查询新增表、删表前删除依赖例程、routine→routine、view→routine/table、跨对象环和确定性；真实 PostgreSQL forward/rollback 整组执行通过。

### SP5【P2｜C8】删除 up 文件的实现口径与原始规格不一致

位置：`docs/migration-chain-capability-plan.md:202-210`、`internal/server/libraries.go:302-315`、`internal/server/libraries_versions_test.go:30-40`

原始方案写明“删除 up 文件时同步清理版本元数据”；实现和测试要求 up/down 都不存在才清理。需要先明确业务语义，再同步规格、实现和测试。

### SP6【P2】Web 任务记录的不是最终生效模式

位置：`internal/server/diffjobs.go:349-365`

任务参数记录请求值 `auto`，而不是决策后的 `shadow` 或 `direct`，与 README 所称“记录生效模式”不符。

### SP7【P3】回归报告内部状态冲突

位置：`docs/migration-chain-regression-report.md`

报告前段仍称 C11-C13 待完成/不建模，后段又宣布完成，应以当前实现和可重复命令重新生成统一结论。

## 5. 当前 HEAD 仍存在的既有安全问题

这些问题不一定由本轮 18 个提交引入，但在当前版本仍可确认。

### O1【P0｜已止血 2026-09-20】Web 默认对所有网卡开放且无认证授权

位置：`internal/datasmith/web/web.go:35-43,62`、`internal/server/server.go:79-125`

默认 `:8080` 暴露连接管理、SQL 执行、reset、迁移、回退和脚本接口。最低限度应默认绑定 `127.0.0.1:8080`；远程模式必须显式开启并具备认证、授权及 Origin/CSRF 防护。

整改：默认地址已改为 `127.0.0.1:8080`；显式非 loopback 监听会在启动前输出当前不提供远程认证的安全警告。完整远程认证、授权及 Origin/CSRF 方案仍需独立产品设计，未使用临时 token 掩盖该需求。

### O2【P1｜已修复 2026-09-20】Web EXECUTE-ON 只告警，没有硬校验

位置：`internal/server/sqlexec.go:66-74`、`internal/datasmith/exec/exec_sql.go:145-156,203-248`

CLI 在打开连接前校验 source/target 标记；Web 只记录警告，然后调用不接收数据库角色的执行引擎。这与前端描述和 AGENTS.md 安全红线不一致。

整改：新增 CLI/Web 共用的目标身份校验入口；Web 写入脚本必须显式选择 `source` 或 `target`，请求入队前和数据库执行前均硬校验。匹配、冲突、非法/空标记、无标记及缺少角色均有回归测试。

### O3【P1｜已修复 2026-09-20】RollbackTo 在取得迁移锁前生成计划

位置：`pkg/migrate/rollback_to.go:85-117`

`PlanRollbackTo` 在 `withMigrationLock` 前读取 ledger。并发迁移可能在两者之间改变已应用版本，使执行使用过期计划。锁外 plan 只能作为预览；锁内必须重新构建权威计划。

整改：迁移文件在锁外只做静态校验；`RollbackToMigration` 获取 migration lock 后，使用同一锁定连接读取 ledger、构建权威计划并执行。`PlanRollbackTo` 保持只读预览语义，回归测试验证锁先于账本读取且能观察抢锁前提交的新版本。

## 6. 数据库对象能力边界

### 已支持

- MySQL/PostgreSQL：表、列、主键、索引、外键、CHECK、普通视图、表/列注释和数据 diff。
- PostgreSQL：普通函数（`prokind=f`）、存储过程（`prokind=p`）、独立序列与 SERIAL 从属序列（含 ownership）的提取、比对、依赖排序和正反向 SQL 生成；SQL/PLpgSQL 静态依赖环失败关闭。

### 部分支持

- 函数/过程：能按身份签名 diff 定义和排序静态依赖；动态 SQL、非 SQL/PLpgSQL 语言及无法消歧的重载调用拒绝生成，返回类型等不兼容变更仍交由 PostgreSQL 显式拒绝。
- 物化视图：解析为普通 VIEW 类型，没有独立刷新、存储和依赖语义。
- 唯一约束：主要按唯一索引处理，未完整保留 constraint 语义。

### 尚未建模

- MySQL 存储函数/过程。
- PostgreSQL 聚合函数、窗口函数、触发器对象。
- identity/generated columns、分区表及分区边界。
- enum/domain/composite type 等用户定义类型。
- owner、grant、privilege、row-level security、policy。
- extension、collation 等数据库级对象。

现阶段最高优先级不是继续扩大对象清单，而是保证已声明支持的对象能够正确生成、执行、回滚和二次收敛。

## 7. 建议迭代顺序

### 阶段 A：安全止血（已完成 2026-09-20）

1. Web 默认改为 loopback；明确远程模式认证方案。
2. Web EXECUTE-ON 使用连接角色或 expected connection identity 做硬校验。
3. RollbackTo 在锁内构建权威计划。
4. 影子事务显式检查 rollback 结果。

完成标准：每项都有失败路径测试；所有破坏性入口在目标身份不明确时拒绝执行。

### 阶段 B：diff-full 正确性（已完成 2026-09-20）

1. 修复视图定义+注释组合变化。
2. 完成 target-only/source-only 表的数据迁移语义。
3. 排除部分/表达式唯一索引作为行身份。
4. 四产物统一暂存和原子发布。

完成标准：MySQL direct 与 PostgreSQL shadow 均覆盖单侧表、失败清理和二次 diff 收敛。

### 阶段 C：PostgreSQL 非表对象闭环（已完成 2026-09-21）

1. `OwnedBy` 参与序列 equality 与 ALTER/CREATE SQL。
2. 建立对象依赖 DAG。
3. 增加跨对象依赖与 SERIAL ownership 集成测试。

完成标准：forward 后结构 diff 为空，rollback 后恢复原始结构及 ownership。

### 阶段 D：一致性与产品化

1. 明确 C8 元数据生命周期并修正错误传播。
2. 记录最终生效的 shadow/direct 模式。
3. 清理回归报告冲突。
4. 再依据业务迁移语料扩展新对象。

## 8. 验证结果

初始评审的门禁在 `0742d0e` 上通过。阶段 1、阶段 2、阶段 3 整改工作树均重新执行以下完整门禁并全部通过：

```bash
test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)"
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
pnpm --dir web build
./scripts/integration-test.sh
./scripts/check-coverage.sh
```

阶段 1 覆盖率结果：overall 74.1%（5720/7719）、db 78.8%、diff 78.1%、sql 73.6%、migrate 75.9%、exec 88.6%。前端另通过 `./scripts/build.sh` 构建并同步 go:embed 产物。

阶段 2 覆盖率结果：overall 74.4%（5768/7754）、db 78.9%、diff 78.6%、sql 74.4%、migrate 75.9%、exec 89.1%。前端 `pnpm --dir web build` 通过且未产生 embed 差异；双库测试新增显式/通配单侧表、forward 执行和二次收敛，单元测试新增同时变化、索引资格和四产物失败注入。

阶段 3 覆盖率结果：overall 74.9%（6074/8108）、db 78.9%（674/854）、diff 78.6%（1773/2257）、sql 76.9%（1268/1648）、migrate 75.9%（480/632）、exec 89.1%（376/422）。gofmt、相关最小测试、单测、race、vet、staticcheck v0.8.1、govulncheck v1.1.4、前端构建、MySQL/PostgreSQL 双库集成测试和覆盖率 gate 全部通过；真实 PostgreSQL 用例验证跨对象正/逆拓扑、forward 收敛、rollback 恢复结构与 ownership。

阶段 3 新增统一对象依赖 DAG、sequence ownership 正反向 SQL和真实 PostgreSQL 往返断言；相关最小测试、完整门禁及覆盖率结果见本节后续记录。尚未完成的阶段 4 仍需处理元数据错误传播、C8 语义、任务 effective mode 与共享行定位策略；本阶段未扩展 trigger、分区、generated column、identity 或用户定义类型。
