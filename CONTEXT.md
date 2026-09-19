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
脚本库中同一版本的"前进/回退"脚本对:up 使库前进一个版本,down 撤销该版本;down 仅用于回退最新已应用版本(栈式)。与比对的"正向/回滚 SQL"角色同构;比对产物经人工审阅后可登记为此脚本对。
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

**回退 (Rollback)**:
撤销最新已应用版本:执行其 down 脚本并将账本标记为 rolled_back;单步、需确认。
_Avoid_: 回滚 SQL(那是比对产物)、重置
