# DataSmith

数据库管理工具的统一语言:库表结构比对、表数据比对、差异 SQL 生成与执行、版本迁移、目标库重置。CLI 与 Web 控制台共享同一套领域词汇。

## Language

### 连接与角色

**连接 (Connection)**:
一条命名的数据库连接档案(MySQL 或 PostgreSQL),由服务端统一管理。
_Avoid_: 数据源、实例、DSN

**Source(源库)**:
比对任务中"被变更方":正向 SQL 以 Source 方言生成、在 Source 上执行,使其对齐 Target。
_Avoid_: 主库、本地库

**Target(目标库)**:
比对任务中"参照方":Source 向 Target 对齐。
_Avoid_: 从库、线上库

### 比对

**结构比对 (Schema Diff)**:
比较两库的表、列、索引、主键、外键、注释差异。
_Avoid_: DDL diff

**数据比对 (Data Diff)**:
按行比较两库同名表的数据差异,产出增/删/改三类行级差异。
_Avoid_: 数据同步

**匹配键 (Match Key)**:
数据比对中定位同一行的键:优先主键,回退到非空唯一索引。
_Avoid_: comparisonKey(旧概念,见"比对键")

**比对列 (Compare Columns)**:
参与相等性判断的列集合;未指定时为全部列。
_Avoid_: 对比字段、比较列

**忽略列 (Ignore Columns)**:
从比对列集合中显式剔除的列:不参与相等性判断;但两侧均存在的忽略列仍会随行读取取值,并进入生成的 INSERT(回滚侧同理),UPDATE SET 仅含比对出差异的列。
_Avoid_: 排除字段

**比对键 (comparisonKey)**:
旧规则字段。历史上身兼两职(匹配键+限定比对列),正在被"匹配键/比对列"二分取代。
_Avoid_: 把它当作匹配键使用

**对象依赖图 (Schema Object Dependency DAG)**:
结构产物中 table、view、routine、sequence 的统一有向无环图;边写作 dependent → prerequisite。创建按正拓扑执行,删除按逆拓扑执行;序列 ownership 作为独立附着操作在序列与拥有列均存在后执行。无法可靠提取依赖或发现环时拒绝生成并报告对象链。
_Avoid_: 固定对象阶段、视图专用排序

### 差异产物

**正向 SQL (Forward SQL)**:
把 Source 升级为 Target 的差异脚本;文件头带 `DATASMITH EXECUTE-ON: source` 执行目标标记。
_Avoid_: upgrade 脚本

**回滚 SQL (Rollback SQL)**:
把已升级的 Source 还原回原状态的脚本;与正向 SQL 成对原子生成,头部同为 `EXECUTE-ON: source`。可人工审阅后登记为脚本库版本的 down 脚本;其互逆保证以"执行前 Source 未被再改动"为前提。
_Avoid_: 撤销脚本

**执行目标标记 (EXECUTE-ON)**:
SQL 文件头部声明预期执行库的标记;执行时与用户选择的目标做一致性校验,不一致即拒绝。
_Avoid_: 魔法注释

**任务 (Job)**:
服务端一次长耗时操作(比对/执行/重置/迁移)的运行实例,带状态与产物。
_Avoid_: 请求、作业

**产物 (Artifacts)**:
任务落盘的输出文件:正向/回滚 SQL、汇总报告、日志,按任务目录存放、限期保留。
_Avoid_: 临时文件

### 执行与迁移

**试跑 (Dry Run)**:
在事务中执行后回滚的验证性运行;用于执行前确认。MySQL DDL 场景无可靠回滚,被拒绝而非降级。
_Avoid_: 预览(预览仅指不连接的展示)

**事务执行 (Tx Run)**:
显式事务中执行,成功提交、失败回滚。

**脚本库 (Migration Library)**:
服务端托管的版本迁移脚本集合,文件名形如 `V1__描述.up.sql`/`1.2.0__描述.down.sql`。
_Avoid_: 迁移目录、脚本目录

**版本 (Version)**:
脚本的点分数字序号(如 `1.2.0`),库内唯一;迁移只向前应用到指定版本。
_Avoid_: 时间戳版本

**up/down 脚本**:
脚本库中同一版本的"前进/回退"脚本对:up 使库前进一个版本,down 撤销该版本;down 仅经回退通道执行(回退最新或回退到指定版本,栈式)。与比对的"正向/回滚 SQL"角色同构;比对产物经人工审阅后可登记为此脚本对。
_Avoid_: rollback 脚本

**增量版本 (Increment Version)**:
由比对产物登记而成的版本:up 为结构正向→数据正向串接,down 为数据回滚→结构回滚串接;一次迭代对应一个版本,修复迭代必须发新版本号。
_Avoid_: 补丁、热修

**预期执行库 (Expected Target)**:
登记版本时记录的来源比对 source 连接;跨库执行时警告放行,方言不匹配硬拒绝。
_Avoid_: 归属库

**账本 (Ledger)**:
目标库中记录已成功应用版本(checksum、状态)的表;迁移前先校验全部脚本再写账本,配合 advisory lock 防并发。
_Avoid_: 历史表

**重置 (Reset)**:
清空并重建目标库对象的破坏性操作;必须先试跑预览生成 SQL,再显式确认执行;系统库、空目标、危险 schema 直接拒绝。
_Avoid_: 清库、格式化

**完全对比 (Full Diff)**:
一次同时比对结构与数据,产出四份产物(结构/数据 × 正向/回滚);可一步登记为脚本库增量版本(自动合成 up/down 对)。四产物在同目录暂存,全部成功后作为一组发布。数据比对默认两阶段:存在结构差异时先经影子事务在事务内对齐 source 结构再比对;直接模式把 target-only 的 source 视为空集并生成全量 INSERT,source-only 仅由结构 DROP 处理。CLI 与 Web 完全比对均可选数据比对模式:自动(auto,引擎默认判定)/强制影子事务(shadow,仅 PostgreSQL source)/直接比对(direct,禁用影子)。
_Avoid_: 全量比对(指数据全量,非结构+数据)

**影子事务 (Shadow Transaction)**:
完全对比的两阶段机制:在 source 连接内 `BEGIN; 应用结构正向 DDL; 数据比对; ROLLBACK`,数据比对经同一会话读取对齐后的影子结构——不落库、单连接,两侧结构漂移轮的 up 一次执行即可对齐结构+数据。仅 PostgreSQL 支持事务性 DDL;MySQL 自动回退直接比对并告警;影子内绝不执行 down 脚本。
_Avoid_: 影子库、预对齐

**整库比对 (Whole-Database Rules)**:
规则省略(--rules 不传或空数组)或表名含 `*`/`?` 通配时,展开为全部具有行身份(主键或非空唯一索引)的基础表;账本表及被排除表(含其 SERIAL 隐式序列)始终排除。
_Avoid_: 全表扫描、通配一切

**回退 (Rollback)**:
撤销已应用版本:从最新版本起逐个执行 down 脚本并把账本标记为 rolled_back,直到目标版本成为最新(目标版本仅回退最新一个时为单步);需确认。
_Avoid_: 回滚 SQL(那是比对产物)、重置
