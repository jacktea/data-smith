# DataSmith 全项目代码审查报告

审查日期：2026-09-18
审查范围：仓库内全部 Go 源码、配置、构建脚本、测试、生成器与数据库适配层（约 7,915 行 Go）
审查方式：静态代码审查 + `go test ./...` + `go test -race ./...` + `go vet ./...` + 覆盖率分析

## 结论摘要

DataSmith 的总体分层思路是清楚的：CLI 编排、数据库适配、差异计算、SQL 方言生成、迁移执行、SSH 代理各自成包；结构差异生成最近又增加了分阶段执行，方向正确。

但当前版本还不适合直接用于无人值守的生产数据库同步或迁移。主要原因不是编译或竞态问题，而是若干数据正确性与执行安全问题：可选的 chunk hash 会漏掉部分真实差异；迁移器会把 `.down.sql` 当成升级脚本执行；MySQL 迁移记录使用 PostgreSQL 占位符；SQL 执行失败后会在真实数据库上重新执行语句以“定位错误”；数值比较会因 `float64` 精度丢失把不同的大整数或高精度 decimal 判为相同。

上述判断反映 2026-09-18 初始审查基线。随后按 Issue #2 至 #9 完成修复、回归测试、双数据库端到端测试和 CI 门禁；当前 P0/P1 状态见下方矩阵。仍需结合具体生产权限、数据规模和运维流程做环境级验证。

## P0/P1 修复状态矩阵（2026-09-19）

| 原始发现 | 状态 | 修复与验证证据 |
| --- | --- | --- |
| P0：升级流程执行 `.down.sql`/`.json` | 已修复 | Issue #2；`internal/datasmith/migrate/local/parser_test.go` |
| P0：失败 SQL 在真实数据库上重放 | 已修复 | Issue #3；`internal/datasmith/exec/exec_sql_test.go`、`exec_sql_safety_test.go` |
| P1：chunk hash 漏掉目标最小主键之前的记录 | 已修复 | Issue #4；`pkg/diff/exact_comparison_test.go`、数据库 adapter safety tests |
| P1：迁移执行与 ledger 记录不原子、MySQL 占位符错误 | 已修复 | Issue #2；`pkg/migrate/migrate_test.go`、`test/integration/e2e_test.go` |
| P1：迁移文件读取失败仍记录成功 | 已修复 | Issue #2；`TestMigrationFileReadContentReportsMissingFile`、迁移 preflight tests |
| P1：整数/decimal 使用 `float64` 导致精度错误 | 已修复 | Issue #4；精确比较测试及 `>2^53` 双数据库 E2E |
| P1：PostgreSQL 非 `public` schema 不可靠 | 已修复 | Issue #5/#9；`DataSmith_App` PostgreSQL E2E |
| P1：TABLE/VIEW 类型变化被忽略 | 已修复 | Issue #5；`pkg/diff/schema_test.go` |
| P1：对象依赖排序不完整 | 已修复 | Issue #5/#9；SQL generator golden tests、FK/view apply E2E |
| P1：数据库游标错误未检查 | 已修复 | Issue #4/#8；MySQL/PostgreSQL `data_safety_test.go` |
| P1：数据 diff 失败仍成功退出并留下部分输出 | 已修复 | Issue #6；fail-fast 与 atomic output tests |
| P1：事务清洗修改字符串/注释业务内容 | 已修复 | Issue #3；SQL scanner unit/fuzz tests |
| P1：YAML SSH proxy 配置被静默忽略 | 已修复 | Issue #8；`pkg/config/config_test.go` |
| P1：SSH 主机身份与隧道生命周期不安全 | 已修复 | Issue #8；`pkg/proxy/proxy_test.go`、race tests |

Issue #9 的双数据库 E2E、覆盖率门禁和 GitHub Actions 远程验证均已通过：
[run 35419579967](https://github.com/jacktea/data-smith/actions/runs/35419579967)。

## 功能与架构地图

```text
cmd/main.go
  └─ internal/datasmith (Cobra CLI / 用例编排)
       ├─ diff-schema / diff-data
       ├─ exec-sql
       └─ migrate-script / reset-db
            │
            ├─ pkg/db + pkg/conn       数据库元数据、分页读取、chunk hash
            ├─ pkg/diff                schema/data 差异计算
            ├─ pkg/sql                 MySQL/PostgreSQL SQL 生成
            ├─ pkg/migrate             版本表与迁移执行
            └─ pkg/proxy               SSH 隧道
```

优点：

- 核心差异算法与 CLI 基本分离，MySQL/PostgreSQL 通过适配器和方言接口扩展。
- 数据全量比较使用 keyset pagination，正常模式下内存可以控制在批次级别。
- schema SQL 已按解除依赖、修改列、重建键、最终关联四阶段组织。
- 已有回滚 SQL、chunk 预过滤、`exec-sql` 事务模式和一定数量的 SQL 生成单测。

主要架构短板：

- `DBAdapter` 同时承担元数据读取、数据扫描、连接生命周期，并暴露 `*sql.DB` 与可变配置；上层很难替换、模拟或统一施加超时。
- CLI 中直接创建连接、打开文件、调用 `os.Exit`，用例层与进程层耦合，错误不能统一返回，难做端到端测试。
- 表名、schema 名、列名的限定与引用规则散落在各方言和适配器中，没有统一的 `QualifiedName/QuoteIdentifier` 抽象。
- schema 模型没有依赖图；虽然 `ViewDefinition.Dependencies` 已存在，但生成器没有用它排序表、外键和视图。
- “流式比较”与“结果聚合/文件输出”没有形成流式管线，CLI 最终仍把全部差异留在内存。

## 关键缺陷

### P0：迁移扫描会执行 `.down.sql`，甚至把 `.json` 当 SQL

位置：`internal/datasmith/migrate/local/parser.go:45-78`、`internal/datasmith/migrate/migrate_script.go:81-95`

`ScanMigrations` 接受 `up`、`down`、无方向以及 `json` 文件；`SortMigrations` 在相同版本下还会把 `down` 排到 `up` 前面。`runMigrations` 只按版本筛选，不检查 `Direction` 或 `Ext`，随后把所有文件交给 `ApplyMigrations`。

一个常见目录同时包含 `1__create.up.sql` 与 `1__create.down.sql` 时，程序会先执行 down，再执行 up。JSON 文件也会原样交给数据库执行。应立即限定升级流程只接受 `.up.sql`（是否允许无方向文件需明确约定），回滚建立独立命令和逆序选择逻辑。

### P0：SQL 执行失败后会在真实数据库上再次执行语句

位置：`internal/datasmith/exec/exec_sql.go:179-194`

非事务模式先执行完整 SQL；失败后，为定位语句，代码逐条在同一真实数据库中重新执行。MySQL DDL 会隐式提交，某些语句或存储引擎也不可回滚，因此“诊断”本身可能再次修改数据库、重复副作用，并把第一个因“对象已存在”而失败的语句误报为原始故障点。

失败路径必须只报告驱动返回的错误与已知语句边界，不能通过重放生产 SQL 做诊断。若要精确定位，应在执行前使用可靠解析器分句并按显式事务逐条执行，或仅在隔离的临时数据库中重放。

### P1：chunk hash 预过滤会漏掉目标最小主键之前的源端记录

位置：`pkg/diff/data.go:125-150`

范围只由目标库生成，然后源库和目标库对相同范围求 hash。第一个范围从目标最小 PK 开始；如果源库额外存在更小的 PK，这些记录不属于任何范围。当其余数据一致时，所有 hash 相同，函数直接返回“无差异”。例如源 `{0,1}`、目标 `{1}` 会漏掉应删除的 PK=0。

此外，MySQL 的 `BIT_XOR(CRC32(...))`（`pkg/db/mysql/mysql.go:488-512`）存在碰撞、偶数抵消以及 NULL/空串、分隔符歧义；PostgreSQL 拼接表达式也有 NULL/空串和分隔符歧义。短期应关闭该特性或先比较精确的 count/min/max，并覆盖无界首尾范围；长期使用包含长度与 NULL 标记的规范编码和更强的可组合 hash，同时保留抽样或二次校验。

### P1：迁移执行与版本记录不原子，且 MySQL 占位符错误

位置：`pkg/migrate/migrate.go:25-66`、`pkg/migrate/migrate.go:142-153`

- 所有版本记录 SQL 都使用 `$1`，MySQL 应使用 `?`。
- 正式迁移先 `conn.Exec(content)`，再单独写版本表，没有事务。脚本成功、版本记录失败时，数据库已经变化但版本仍旧，下一次会重复执行。
- 失败记录的插入错误被忽略。
- `schema_migrations.version` 没有唯一约束，也没有 checksum、dirty 状态或并发锁。

在 MySQL 上，这一组合很容易出现“迁移已生效，但命令报错且未记录”的状态。应按方言生成占位符；每个可事务化迁移在同一事务中执行脚本和版本记录；对不能事务化的 DDL 明确 dirty 状态和恢复流程；增加唯一版本、checksum 与 advisory lock。

### P1：迁移文件读取失败会被记录为成功

位置：`pkg/migrate/model.go:14-22`

`GetContent` 吞掉 `os.ReadFile` 错误并返回空字符串。文件在扫描后被删除、权限变化或读取失败时，空 SQL 的 `Exec` 可能成功，随后版本被写成成功，实际迁移却没有执行。应将签名改为 `GetContent() (string, error)`，空脚本也应按明确策略拒绝或告警。

### P1：整数和 decimal 经 `float64` 比较会产生错误相等与错误排序

位置：`pkg/diff/value_comparator.go:49-61`、`pkg/diff/value_comparator.go:130-137`

所有数值最终转换成 `float64`。大于 `2^53` 的相邻 BIGINT（如 9007199254740992 与 9007199254740993）可能变成同一个浮点值；高精度 decimal 也会丢失精度，绝对误差小于 `1e-9` 还会被直接判为相等。主键合并排序和字段相等判断都复用这套逻辑，因此会漏报新增、删除或修改。

整数应使用 `math/big.Int` 或保持驱动原始整数类型；decimal 使用 `math/big.Rat` 或 decimal 库按精度比较；浮点容差只能用于真实 float/double，并采用可配置的绝对+相对误差。

### P1：PostgreSQL 非 public schema 的数据比较和生成 SQL 不可靠

位置：`pkg/db/postgres/postgres.go:68-116`、`pkg/sql/postgres/postgre.go:24-87`

配置中的 `TableSchema` 仅用于元数据查询，数据分页查询仍是 `FROM "table"`，没有 schema 限定。生成的 INSERT/DELETE/UPDATE 更直接使用未引用、未限定的 `tbl.Name`。结果是：

- 非 public schema 可能查询失败，或误读 `search_path` 中同名表。
- 大小写敏感名、保留字、含特殊字符的表名会生成无效 SQL。
- 标识符中的 `"` 没有统一转义，来自不可信元数据时还有 SQL 注入风险。

应集中实现方言级 `QuoteIdentifier` 与 `QualifiedTableName`，所有元数据查询、数据扫描和 SQL 生成统一使用 schema+table。

### P1：表与视图类型变化被完全忽略

位置：`pkg/diff/schema.go:60-67`

同名对象从 TABLE 变成 VIEW（或反向）时，`compareTable` 直接返回 `nil`，最终不会生成任何差异；视图定义任一侧为 nil 时也被当成无变化。此类变化应建模为 drop old object + create new object，并按依赖排序。

### P1：新增/删除对象没有完整依赖排序

位置：`pkg/diff/schema.go:34-56`、`pkg/sql/generator.go:107-143`、`pkg/sql/postgres/postgre.go:233-247`、`pkg/sql/mysql/mysql.go:215-239`

新增表直接通过 `GenerateTableDDL` 创建，而 DDL 内联所有外键。`TablesAdded` 来自 Go map 遍历，顺序不稳定；引用另一个新增表时，可能先创建子表并失败。删除表时也只移除 `TablesModified` 中显式变化的外键，未必解除来自未修改表的引用。新增/修改视图同样没有使用依赖信息排序。

应构建对象依赖 DAG：先创建所有表和主键，再按拓扑序加外键；删除时反向拓扑；视图单独按依赖拓扑处理并检测环。

### P1：所有数据库游标循环都没有检查 `rows.Err()`

位置：`pkg/db/mysql/mysql.go:94-109`、`pkg/db/mysql/mysql.go:185-205`、`pkg/db/postgres/postgres.go:101-116`、`pkg/db/postgres/postgres.go:192-211`、`pkg/db/postgres/postgres.go:531-553`

网络中断、服务端游标错误或解码错误可能在 `Next()` 后通过 `rows.Err()` 才暴露。当前实现会把已读取的部分行当作完整结果返回，进而产生漏数据的 diff、错误 schema 或不完整 chunk 范围。每个循环结束后必须返回 `rows.Err()`。

### P1：数据 diff 遇到表错误仍以成功退出，并可能留下部分输出

位置：`internal/datasmith/diff/diff_data.go:70-82`、`internal/datasmith/diff/diff_data.go:90-126`、`internal/datasmith/diff/diff_data.go:130-189`

目标表提取或数据比较失败时只记录日志并 `continue`，最终命令退出码仍为 0。输出文件在连接和逐表验证前就被截断创建；全部 `WriteString` 返回值又被忽略，磁盘满或 I/O 错误也会报告成功。用户可能拿到看似有效、实则缺表或截断的 SQL。

应先完成校验，再写临时文件；任何表失败默认使整个命令失败；检查所有写入和 `Close/Sync` 错误；成功后原子 rename。若要支持 best-effort，应要求显式开关并在结果中列出失败表。

### P1：事务清洗会修改字符串和注释里的业务内容

位置：`pkg/utils/utils.go:13-22`、`internal/datasmith/exec/exec_sql.go:139-175`

`CleanTransaction` 用未锚定的正则全局删除 `BEGIN;`/`COMMIT;`，不理解 SQL 字符串、注释、过程体或 PostgreSQL dollar quote。`INSERT ... VALUES ('BEGIN;')` 会被改写，`--tx` 和 dry-run 执行的内容因此可能与原 SQL 不同。应使用方言解析器或状态机只移除顶层事务控制语句；无法可靠解析时应拒绝嵌套事务，而不是改写文本。

另外，MySQL DDL 可能隐式提交，所以“dry-run 后全部回滚”的承诺并不成立，需限制可模拟语句或在临时数据库验证。

### P1：YAML 中的 SSH proxy 配置会被静默忽略

位置：`pkg/config/config.go:13-24`、`pkg/db/base/base.go:29-48`

`Proxy` 类型是 `any`；`yaml.v2` 解码后通常是 map，而初始化代码只接受 `*config.SSHProxy` 类型断言。断言失败时没有错误，直接继续连接原始数据库地址。应把字段改成 `*SSHProxy`，补齐 YAML tag 和配置校验。

### P1：SSH 主机身份不校验，隧道停止也不能可靠释放监听器

位置：`pkg/proxy/proxy.go:58-68`、`pkg/proxy/ssh.go:33-87`

- `ssh.InsecureIgnoreHostKey()` 允许中间人攻击，应支持 known_hosts 或固定公钥指纹。
- `Stop` 只关闭 `done` channel，没有关闭被 `Accept()` 阻塞的 listener；没有新连接时 goroutine 和端口会一直存活。
- 第二次 `Stop` 会再次关闭同一个 channel 并 panic。
- 随机挑选 51000-51999 端口存在检查/使用竞态；应监听 `127.0.0.1:0` 并读取系统分配端口。

## 性能问题

### 1. CLI 抵消了核心算法的流式优势

`StreamCompareDataDetailed` 本身按批次合并，但 `StreamCompareDataToDiffWithChunkFilter` 在 `pkg/diff/data.go:159-178` 把所有 Added/Dropped/Modified 行重新积累进 `DataDiff`，CLI 再统一写文件。内存复杂度实际是 `O(差异行数 × 行宽)`，最坏接近整表大小。

建议让 handler 直接写到受缓冲的临时文件；回滚可以按表写独立临时片段，最后倒序拼接，避免保留所有记录。

### 2. MySQL chunk 切分是典型的深 OFFSET 退化

`pkg/db/mysql/mysql.go:462-469` 对每个 chunk 执行 `ORDER BY ... LIMIT 1 OFFSET n`，大表会反复扫描/跳过前缀，累计接近 O(n²)。PostgreSQL 版本在 `pkg/db/postgres/postgres.go:517-522` 用 `ROW_NUMBER()` 对整表排序，再做每个范围 hash；hash 中的 `string_agg` 也会产生较大内存压力。

建议基于 keyset 持续取下一个边界，或按可索引的 PK 范围/数据库统计分片；先基准测试“hash 预扫 + fallback 全扫”是否真的比直接 keyset 合并更快。

### 3. schema 读取是逐表多查询

每张普通表依次查询 columns、PK、indexes、FK、comment，约为 4N+1/5N+1 次往返；数据 diff 又对规则表重复 `ExtractTable`。可以按 schema 一次性批量读取各类元数据并在内存组装，同时缓存本次运行的表模型。

### 4. 输出和执行都是逐行 SQL

每个差异行生成单独 INSERT/UPDATE/DELETE，文件体积、解析成本和网络往返都高。可为 INSERT/DELETE 做受参数限制的批量语句；直接执行模式优先使用 prepared statements、COPY（PostgreSQL）或批量写入，而不是先拼接字面量。

### 5. 缺少上下文、超时和连接池策略

适配器普遍调用 `Query/Exec/Ping` 而非 Context 版本，命令无法响应取消，也没有 statement timeout。连接池未设置 MaxOpen/Idle/Lifetime。建议从 CLI 根部传递 `context.Context`，为元数据、批次扫描、hash 和执行设置独立超时，并在配置中提供安全的池参数。

### 6. 适合增加“按表有界并行”，不适合无界并发

不同表的 schema 抽取和数据比较可有限并行，但每表内部仍应保持确定性顺序；需要全局连接/内存/写文件信号量，默认并发数保守，并保证输出按表名稳定排序。

## 其他质量与安全问题

- `batch-size` 没有校验；传 0 会让两端分页都返回空结果，从而错误报告“无差异”。位置：`internal/datasmith/diff/diff_data.go:194-201`、`pkg/db/mysql/mysql.go:86-109`、`pkg/db/postgres/postgres.go:93-116`。
- 配置与 DSN 直接字符串拼接，用户名、密码、数据库名和 extra 参数没有 URL/DSN 编码；特殊字符会导致连接失败或参数串改写。位置：`pkg/config/config.go:26-34`、`pkg/db/mysql/mysql.go:22-30`、`pkg/db/postgres/postgres.go:22-33`。
- `BaseAdapter.Init` 会原地改写传入配置的 Host/Port；PostgreSQL/MySQL 构造器还会改写 Extra/TableSchema。重用配置时会携带已关闭隧道的本地地址。位置：`pkg/db/base/base.go:29-46`。
- 表、列、索引来自 map 遍历，差异和 SQL 输出顺序不稳定，导致同一输入生成不同文件，不利于审核、缓存和回归。位置：`pkg/diff/schema.go:21-57`、`pkg/conn/db.go:58-64`。
- 标识符普遍只包一层引号/反引号，没有转义内嵌 quote；数据库元数据若含特殊标识符会生成坏 SQL，非可信源还可能形成注入链。
- 仓库提交了编译产物 `datasmith`，当前工作树中它已被修改；这会放大仓库体积并造成平台相关 diff。建议从版本控制移除，通过 release/CI 产出二进制。
- `configs/config.yaml` 和 PostgreSQL 测试包含明文密码/历史地址（如 `pkg/db/postgres/postgres_test.go:15`）。即使只是开发凭据，也应确认失效并改用环境变量；若仍有效应立即轮换。
- `reset-db` 是破坏性命令，但没有交互确认、环境保护或数据库名白名单；schema/database 标识符也未引用。建议要求 `--yes`，打印完整目标，禁止空名/系统库，并支持 dry-run。

## 测试与工程化现状

实际验证结果：

- `go test ./...`：通过。
- `go test -race ./...`：通过，当前测试覆盖路径未发现数据竞态。
- `go vet ./...`：通过。
- 总语句覆盖率：28.5%。
- 覆盖相对较好：`pkg/diff` 59.3%、`pkg/sql` 74.7%、`pkg/utils` 77.0%。
- 关键空白：CLI、配置加载、连接工厂、MySQL adapter、migrate、proxy、logger 基本为 0%；PostgreSQL adapter 仅 5.8%。
- PostgreSQL 的所谓集成测试写死本地凭据，连接失败即 `Skip`，因此日常测试“全绿”并不代表真实数据库行为正确，而且会额外等待重试。
- `gofmt -l .` 显示 `pkg/diff/schema_test.go`、`pkg/sql/postgres/postgres_test.go` 未格式化。
- 未发现 CI 工作流、覆盖率门槛、staticcheck 或真实双数据库矩阵。

最需要补的测试：

1. 迁移目录同时存在 up/down、文件读取失败、MySQL/PostgreSQL 占位符、脚本成功但版本记录失败、并发迁移锁。
2. chunk 范围首尾、源端低于目标 min 的额外行、NULL/空串/分隔符、hash 碰撞 fallback。
3. BIGINT 边界、unsigned bigint、decimal 高精度、NaN/Inf、时区与 timestamp without time zone。
4. 非 public schema、大小写敏感/保留字/含 quote 的标识符。
5. 新增互相依赖表、删除被引用表、多层视图依赖、table↔view 转换。
6. 网络中断后的 `rows.Err()`、写文件失败、部分表失败时的退出码与原子输出。
7. 使用 Testcontainers 或 docker compose 跑 MySQL + PostgreSQL 真实端到端矩阵；生成 SQL 再应用到克隆库，最后二次 diff 必须为空。

## 建议的演进路线

### 阶段 0：生产安全止血（1-3 天）

1. 默认关闭 chunk hash；修复首尾范围前不允许用于生产。
2. 升级迁移只筛选 `.up.sql`；`GetContent` 返回错误；MySQL 使用 `?`。
3. 删除失败后的 SQL 重放诊断；移除正则式 `CleanTransaction` 改写。
4. 数据 diff 任一表失败即非零退出；检查所有文件写入错误并原子落盘。
5. 禁用 SSH `InsecureIgnoreHostKey`；确认并轮换仓库中的历史密码。

### 阶段 1：正确性基线（1-2 周）

1. 引入统一的标识符引用和 schema 限定层，覆盖所有查询与生成器。
2. 用精确整数/decimal 比较器替换 `float64`；为 float 单独定义容差策略。
3. 所有 rows 循环检查 `rows.Err()`；所有数据库操作改用 Context。
4. schema 差异构建依赖 DAG，外键和视图拓扑排序，输出全量稳定排序。
5. 迁移 ledger 增加唯一版本、checksum、dirty、耗时、错误摘要和 advisory lock；明确事务能力矩阵。
6. 建立双数据库端到端测试和 CI：format、vet、staticcheck、race、govulncheck、coverage。

### 阶段 2：大数据性能（2-4 周）

1. 比较 handler 直连流式 SQL writer，取消 `DataDiff` 全量积累。
2. 批量生成 INSERT/DELETE；直接执行场景使用 prepared/COPY/bulk API。
3. 批量读取并缓存 schema 元数据，消除逐表 N+1 查询。
4. 重做 chunk 策略并用真实规模基准验证；支持断点、进度、每表统计和资源限额。
5. 引入按表有界并行与背压，保证输出顺序确定。

### 阶段 3：产品化能力

1. 将 CLI 编排重构为可注入的 application service，CLI 只负责参数和退出码。
2. 增加 plan/apply 两阶段：plan 文件包含源/目标指纹、schema checksum、风险等级和审批信息；apply 前校验漂移。
3. 扩展 schema 模型：check constraint、unique constraint、sequence/identity、generated column、partition、trigger、function/procedure、materialized view、权限与 owner。
4. 提供机器可读 JSON 报告、结构化日志、指标和可恢复 checkpoint。

## 推荐验收门槛

- P0/P1 缺陷全部有回归测试。
- MySQL 与 PostgreSQL 端到端场景均满足：生成 diff → 应用 → 再次 diff 为空；rollback → 再次 diff 恢复原状态。
- 关键包（db/diff/sql/migrate/exec）语句覆盖率至少 70%，整体至少 60%，并逐步提升。
- CI 强制 gofmt、go vet、staticcheck、race、依赖漏洞检查和双数据库集成测试。
- 对 1M/10M 行、低差异率/高差异率分别建立 CPU、内存、查询次数、生成文件大小基准。
- 所有命令在部分失败、取消、磁盘满、网络中断下返回明确非零退出码，且不会留下可误用的“成功”产物。

## 审查边界

本次没有连接真实业务数据库，也没有执行破坏性命令；因此数据库版本差异、真实数据分布、权限模型和超大表基准仍需在隔离环境验证。仓库在审查前已有 `datasmith` 二进制修改，本次未修改该文件。
