# 2026-09-20 代码评审修复交接

## 目标

从评审起点 `0742d0e` 开始，按阶段修复 `docs/code-review-2026-09-20.md` 中的发现，使 DataSmith 已声明支持的迁移能力满足：产物可执行、失败不留下可误用状态、forward 后收敛、rollback 后恢复。

本交接不要求一次扩展所有数据库对象。trigger、identity/generated column、分区和用户定义类型等新能力在现有正确性闭环完成后再排期。

## 当前进度

- 阶段 1（O1、O2、O3、S2）已于 2026-09-20 完成，起始提交为 `0742d0efdd671251047feb0d50dcf9c1ce755705`。
- 阶段 2（SP1、SP2、S3、S4）已于 2026-09-20 完成，实际起始提交为 `6e09e2a33556f0b2419de1926c7275b7a9b73165`，起始工作树干净。
- 阶段 3（S1、S5、SP3、SP4）已于 2026-09-21 从 `accf91b` 开始完成，起始工作树干净。
- 完整门禁已通过：gofmt、单测、race、vet、staticcheck v0.8.1、govulncheck v1.1.4、前端/Go 构建、MySQL/PostgreSQL 双库集成测试和覆盖率 gate。
- 阶段 2 覆盖率：overall 74.4%（5768/7754）、db 78.9%、diff 78.6%、sql 74.4%、migrate 75.9%、exec 89.1%。
- 阶段 3 覆盖率：overall 74.9%（6074/8108）、db 78.9%（674/854）、diff 78.6%（1773/2257）、sql 76.9%（1268/1648）、migrate 75.9%（480/632）、exec 89.1%（376/422）。
- Web 远程认证/授权/Origin/CSRF 仍是需要产品决策的独立设计项；当前默认仅监听 loopback，显式非 loopback 会清晰告警。
- 下一阶段：阶段 4（S6、SP5、SP6、SP7、S7）；阶段 3 未提前处理这些事项。

## 开始前

1. 阅读根目录 `AGENTS.md`、`CONTEXT.md`、`docs/code-review-2026-09-20.md`。
2. 确认工作树状态并记录实际起始提交，不假设仍是 `0742d0e`。
3. 保留用户已有和与当前阶段无关的改动。
4. 使用 TDD：每个发现先添加能稳定复现问题的测试，再修复，再跑相关包和完整 gate。
5. 每次只完成一个阶段；阶段结束时形成可独立评审的语义化 commit。

开始完成标准：已记录起始 commit、工作树状态、选定阶段和该阶段的测试清单。

## 阶段 1：安全边界（已完成 2026-09-20）

范围：报告中的 O1、O2、O3、S2。

执行顺序：

1. 将 Web 默认地址改为 `127.0.0.1:8080`，为显式非 loopback 监听增加清晰风险提示。完整远程认证若缺少产品决策，保留为独立设计项，不用临时 token 掩盖需求。
2. 将 EXECUTE-ON 校验下沉为 Web 与 CLI 可共同调用的引擎入口；Web 必须提供可验证的目标角色或连接身份。
3. 把 `RollbackToMigration` 的 ledger 读取、计划构建和执行放进同一个 migration lock；锁外 plan API 明确标为预览。
4. 调整影子事务生命周期：主路径显式 rollback 并传播错误，defer 只兜底。

测试要求：

- 默认监听地址和显式非 loopback 行为。
- Web source/target 标记匹配、冲突、非法标记和无标记。
- 并发改变 ledger 时不会执行过期 rollback 计划。
- shadow 数据阶段成功但 rollback 失败时任务失败，不报告已恢复。

阶段完成标准：破坏性入口在目标身份不明确或恢复状态不确定时失败关闭；完整 gate 通过。

完成记录：

- Web 默认绑定 `127.0.0.1:8080`，非 loopback 启动前输出无远程认证警告。
- SQL 写入执行使用 CLI/Web 共用的 EXECUTE-ON 校验；Web 请求新增必填 `targetRole`，前端要求显式选择角色。
- `RollbackToMigration` 在 migration lock 内读取 ledger、构建并执行权威计划；锁外计划仅用于预览。
- 影子事务在主路径显式 rollback 并传播错误，恢复日志只在 rollback 成功后输出。
- 每项均有先失败后修复的回归测试；未扩展 trigger、分区、generated column 等对象能力。

## 阶段 2：diff-full 正确性与原子性（已完成 2026-09-20）

范围：报告中的 SP1、SP2、S3、S4。

执行顺序：

1. 让视图 `TableDiff` 同时携带 `ViewDefinitionChange` 与 `CommentChange`；生成器先重建定义，再恢复注释。
2. 实现单侧表语义：target-only 将 source 视为空集并生成全量 INSERT；source-only 由 schema DROP 覆盖，不进入数据 diff；显式与通配规则行为一致。
3. `RowIdentityColumns` 只接受普通、完整、全列 NOT NULL 的唯一索引；部分/表达式索引要求显式业务键或跳过/报错。
4. 为 `diff-full` 增加四文件 staging transaction：全部成功后发布；任一失败时旧产物保持完整。

测试要求：

- 视图定义与注释同时变化。
- PostgreSQL shadow 和 MySQL direct 的 target-only/source-only，分别覆盖显式与通配规则。
- 部分索引、纯表达式索引、表达式混合索引和缺失列索引。
- 数据阶段失败、磁盘写失败和发布失败时不存在混合产物。

阶段完成标准：两种数据库的单侧表迁移 forward 后二次 diff 为空；失败注入后磁盘上只有上一组完整产物或没有产物。

完成记录：

- 视图定义与注释独立比较，同一个 `TableDiff` 可同时携带两类变化；生成顺序由回归测试固定为先重建定义、后恢复注释。
- target-only 在 MySQL direct 中通过空 source 适配器生成完整行 INSERT，在 PostgreSQL shadow 中使用事务内空表；显式和通配 source-only 均只由 schema DROP 处理。
- PostgreSQL 索引完整定义与表达式元数据分离；隐式行身份排除部分索引、纯/混合表达式索引、缺列索引和可空唯一索引。
- `diff-full` 先在同目录 staging 中生成四产物，再作为一组发布；数据阶段、写盘和发布失败注入均验证上一组产物不变且无暂存泄漏。
- 双库集成测试执行 forward 后二次完全对比为空；同时修复 MySQL 不接受的 `--- diff` 分节标记，统一为合法 `-- diff` SQL 注释。

## 阶段 3：PostgreSQL 非表对象闭环（已完成 2026-09-21）

范围：报告中的 S1、S5、SP3、SP4。

先使用 `codebase-design` 明确依赖图接口，再进入实现。依赖发现和排序属于核心接口变化，应保持上层编排不感知数据库特例。

执行顺序：

1. 将序列 ownership 纳入 equality；生成 `OWNED BY table.column` 和 `OWNED BY NONE` 的正反向 SQL。
2. 建模统一对象节点与依赖边；创建使用正拓扑，删除使用逆拓扑。
3. 保留 view 依赖闭包能力，但让它接入统一排序，而不是继续增加固定阶段。
4. 对无法可靠提取或存在依赖环的对象保守拒绝并给出对象链。

测试要求：

- SERIAL 风格序列 ownership 的新增、修改、解除和回滚。
- 函数返回新增表类型。
- SQL/PLpgSQL 函数引用新增表。
- 删除表前删除依赖例程。
- routine→routine、view→routine/table 的拓扑和环检测。

阶段完成标准：forward 后结构 diff 为空；rollback 后结构与 ownership 恢复；排序结果确定且重复运行字节一致。

完成记录：

- `Sequence.OwnedBy` 进入 equality；新增/修改/解除分别生成依赖安全的 `OWNED BY table.column` / `OWNED BY NONE`，新增从属序列按“建序列 → 建表 → 附着 ownership”执行。
- 统一对象依赖 DAG 的节点为类型 + schema + identity + 操作步骤，边统一为 dependent → prerequisite；创建正拓扑、删除逆拓扑，稳定键保证重复生成字节一致。原 view 依赖闭包已接入该 Module，不再使用 view 专用排序阶段。
- SQL/PLpgSQL 静态依赖发现覆盖复合返回表类型、表引用和 routine 调用；table 默认值、view→routine/table、sequence ownership 与外键附着/解除均进入图。动态 SQL `EXECUTE`、非 SQL/PLpgSQL 语言、重载歧义和依赖环失败关闭并输出对象链。
- 单元测试覆盖 ownership 正反向、函数返回新增表、routine→routine、view→routine/table、删除逆序、外键解除、环与不可可靠提取；真实 PostgreSQL 测试执行 forward 后结构 diff 为空，rollback 后恢复结构、SERIAL ownership 与 `OWNED BY NONE`。
- 未扩展 trigger、分区、generated column、identity 或用户定义类型；阶段 4 保持未开始。
- 完整门禁通过：gofmt、相关最小测试、`go test ./...`、race、vet、staticcheck v0.8.1、govulncheck v1.1.4、`pnpm --dir web build`、MySQL/PostgreSQL 双库集成测试和覆盖率 gate。覆盖率为 overall 74.9%（6074/8108）、db 78.9%（674/854）、diff 78.6%（1773/2257）、sql 76.9%（1268/1648）、migrate 75.9%（480/632）、exec 89.1%（376/422）。

## 阶段 4：元数据与文档一致性

范围：报告中的 S6、SP5、SP6、SP7、S7。

1. 明确删除 up 后保留 down 的业务语义，再同步修改计划、实现和测试。
2. 元数据清理返回并传播错误。
3. 任务摘要同时记录 requested mode 和 effective mode。
4. 更新 README、能力计划和回归报告，删除互相冲突的状态。
5. 在行为稳定后提取共享行定位策略，保持方言引用和 SQL 文本差异在 dialect 层。

阶段完成标准：文档、API 响应、任务日志和实际行为对同一场景给出一致结论。

## 每阶段验证

先运行最小相关测试，再运行完整门禁：

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

修改 Web 前端或 embed 产物时，使用仓库既有构建脚本保持 `internal/server/webfs/dist` 与源码同步。

## 新 Session 启动提示词

复制下面内容到新的 Codex session：

```text
请在当前 data-smith 仓库继续修复 2026-09-20 代码评审问题。

先完整阅读 AGENTS.md、CONTEXT.md、docs/code-review-2026-09-20.md 和 docs/code-review-remediation-handoff-2026-09-20.md。确认当前 commit 和工作树状态，不要覆盖用户已有改动。

使用 $codebase-design 先确定统一对象依赖 DAG 接口，再使用 $tdd 执行交接文档的“阶段 3：PostgreSQL 非表对象闭环”。每个缺陷先写能失败的回归测试，再做最小修复；维持 AGENTS.md 的所有数据库操作安全红线。不要顺带扩展 trigger、分区、generated column 等新对象能力。

阶段 3 全部完成后运行相关测试、go test、race、vet、staticcheck、govulncheck、双库集成测试、覆盖率 gate 和前端构建。更新评审/交接文档中的状态，并汇报修改文件、关键设计决定、验证结果和尚未完成的后续阶段。持续推进，不要只停在分析。
```
