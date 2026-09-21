# 迁移链回归批验收报告（airedge 88 版本全链，2026-09-20）

> 对应计划：`docs/migration-chain-capability-plan.md`「五、回归计划」。
> 执行环境：本机 Docker `postgres14_22`（PG 14.22），一次性库
> `airedge_mig_src` / `airedge_mig_tgt` / `airedge_mig_verify`（全量重建）。
> 工作区：`/tmp/ds-acceptance-regress/`（configs、chain.log、out/、migrations/、closure/）。
> 二进制：`e467e75`（第 3 批）经 `./scripts/build.sh` 重建。

## 一、结论总览

**回归批验收通过。** 88 轮全链（2.1.0 → 3.7.0.35）在「第 3 批代码 + exec-sql 逐条
执行新路径」上零失败重跑；三项闭环验收（末轮、verify 从零、回退往返）的产物全部为
**结构 0 语句、数据 0 DML**；账本 88/88 success；88 对产物已登记 airedge 脚本库。
全程 0 外部脚本加工、0 psql 回退、0 `--best-effort` / `--skip-missing-tables`。

原回归批发现的 CHECK 约束、视图注释和「修改 1 张表」口径（C11/C12/C13）
已在收尾批全部修复并以双库 fixture、airedge 终态闭环和回退往返复核：当前结果为
**新增 0 / 删除 0 / 修改 0**，产物为空。本文以下只保留修复后的当前结论。

## 二、验收标准逐项结果

| 计划「五」验收项 | 结果 |
|---|---|
| 1. reset-db 重建三个一次性库 | ✅ src/tgt `reset-db --dry-run` 审阅后 `--yes`（`DROP SCHEMA public CASCADE`），verify 新建空库 |
| 2. 每轮 exec-sql → diff-full → migrate-script | ✅ 88/88 轮零失败，12 分钟跑完 |
| 3a. 末轮 diff 为空 | ✅ 收尾复核为新增 0 / 删除 0 / 修改 0，结构 0 语句、数据 0 DML |
| 3b. verify 从零 migrate-script 全链应用后与 target diff 为空 | ✅ 88/88 success，结构 0 语句、数据 0 DML（194 表参与比对） |
| 3c. 回退冒烟（最近版本往返） | ✅ V3.7.0.35 down 回退（账本 `rolled_back`）→ 再推进（`success`）→ 复跑闭环仍为空 |
| 4. 产物登记脚本库 + store.json | ✅ 88 对 176 文件（40MB）入 `lib_8989e9f44806929e`，store.json 88 条版本元数据一致（见「六」） |
| 5. 报告（逐版本摘要/增强清单/Java 缺口/回退限制） | ✅ 见本文件「五/七/八/九」 |

## 三、全链重跑详情

- **88 轮链形态**：`exec-sql --tx` 回放原脚本到 target → `diff-full` 单命令
  （省略 rules 整库通配 + auto 影子两阶段 + `--migrate-dir` 生成
  `V<版本>__<标题>.up/.down.sql`）→ `migrate-script` 推进 source。
- **账本**：source 侧 `schema_migrations` 88/88 `success`，checksum 全非空；
  86 个去重值——V2.6.1 与 V2.6.2、V3.7.0.8 与 V3.7.0.10 两对原脚本内容完全相同，
  属原仓库重复脚本（版本唯一约束不受影响）。
- **影子两阶段**：39 轮触发影子结构对齐（auto：PG source 且存在结构差异），
  最大单轮影子应用 133 条 DDL；每轮产物 grep 校验账本表零出现。
- **产出规模**：88 对 up/down 合计 up 30.4MB / down 10.3MB；up 内结构语句 1849 条、
  DML 引用 22795 处；两侧终态各 198 张业务表、49 个视图。

### scanner 语句边界全链验证（C10 后续）

第 3 批起 `exec-sql --tx` 改为事务内**逐条执行**扫描语句。本轮 88 个原脚本
（含 `$$` dollar-quote DO 块、`CREATE FUNCTION`、多语句 DDL/DML 混排）全部
经该路径回放成功：**零拒绝、零错拆**，任一失败会带「第 i/N 条语句 + 行列 +
预览」定位，实际未触发。C10 风险正式关闭。

## 四、闭环残差收尾结论

C11/C12/C13 收尾批已让 CHECK 约束和视图注释进入比对/生成，并忽略生命周期
跟随主键约束的背书索引名称差异。airedge 第 1 轮检出 3 条 CHECK 与 4 个视图
注释，执行 7 条语句后第 2 轮为**新增 0 / 删除 0 / 修改 0**；down 后残差重现，
再次推进后复归 0/0/0，重新生成的 up/down 逐字节一致。仍不处理 PostgreSQL
无法 ALTER 调序的列序差异；它不进入结构语义比较。

## 五、逐版本产物摘要（88 对）

| 版本 | 标题 | up KB | down KB | up 结构语句 | up DML 引用 | 函数/过程 | 序列 | 视图 |
|---|---|---|---|---|---|---|---|---|
| 版本 | 标题 | up KB | down KB | up 结构语句 | up DML 引用 | 函数/过程 | 序列 | 视图 |
|---|---|---|---|---|---|---|---|---|
| 2.1.0 | baseline | 13065 | 2682 | 184 | 116 | 2 | 0 | 14 |
| 2.2.0 | upgradesql | 970 | 374 | 27 | 1582 | 2 | 0 | 4 |
| 2.3.0 | upgradesql | 316 | 270 | 3 | 1796 | 0 | 0 | 0 |
| 2.4.0 | upgradesql | 291 | 185 | 42 | 1061 | 0 | 0 | 11 |
| 2.4.1 | upgradesql | 3 | 4 | 0 | 0 | 0 | 0 | 0 |
| 2.4.2 | upgradesql | 6 | 6 | 0 | 30 | 0 | 0 | 0 |
| 2.5.0 | upgradesql | 789 | 286 | 81 | 1096 | 0 | 0 | 8 |
| 2.5.1 | upgradesql | 4 | 5 | 0 | 6 | 0 | 0 | 0 |
| 2.6.0 | upgradesql | 357 | 152 | 70 | 758 | 0 | 0 | 6 |
| 2.6.1 | upgradesql | 4 | 5 | 0 | 0 | 0 | 0 | 0 |
| 2.6.2 | upgradesql | 4 | 5 | 0 | 0 | 0 | 0 | 0 |
| 2.7.0 | upgradesql | 779 | 542 | 44 | 2318 | 1 | 0 | 5 |
| 2.7.1 | upgradesql | 4 | 5 | 0 | 3 | 0 | 0 | 0 |
| 2.7.2 | upgradesql | 5 | 6 | 0 | 10 | 0 | 0 | 0 |
| 2.7.3 | upgradesql | 4 | 5 | 0 | 2 | 0 | 0 | 0 |
| 3.0.0 | upgradesql | 3222 | 960 | 695 | 2227 | 3 | 3 | 33 |
| 3.0.2 | upgradesql | 198 | 169 | 25 | 416 | 0 | 0 | 6 |
| 3.0.2.1 | upgradesql | 41 | 44 | 0 | 230 | 0 | 0 | 0 |
| 3.0.2.2 | upgradesql | 19 | 13 | 0 | 36 | 0 | 0 | 0 |
| 3.0.2.3 | upgradesql | 30 | 31 | 0 | 206 | 0 | 0 | 0 |
| 3.0.2.4 | upgradesql | 18 | 9 | 0 | 14 | 0 | 0 | 0 |
| 3.0.2.5 | upgradesql | 6 | 8 | 4 | 0 | 0 | 0 | 0 |
| 3.0.2.6 | upgradesql | 12 | 9 | 0 | 14 | 0 | 0 | 0 |
| 3.0.2.7 | upgradesql | 10 | 12 | 6 | 0 | 0 | 0 | 3 |
| 3.0.2.8 | upgradesql | 13 | 10 | 0 | 19 | 0 | 0 | 0 |
| 3.0.2.9 | upgradesql | 6 | 6 | 0 | 1 | 0 | 0 | 0 |
| 3.0.2.10 | upgradesql | 52 | 54 | 0 | 376 | 0 | 0 | 0 |
| 3.0.2.11 | upgradesql | 6 | 7 | 0 | 7 | 0 | 0 | 0 |
| 3.0.2.12 | upgradesql | 6 | 6 | 0 | 1 | 0 | 0 | 0 |
| 3.0.2.13 | migration | 6 | 6 | 0 | 3 | 0 | 0 | 0 |
| 3.1.0 | upgradesql | 255 | 142 | 30 | 624 | 0 | 0 | 9 |
| 3.1.0.2 | upgradesql | 83 | 71 | 8 | 77 | 0 | 0 | 3 |
| 3.2.0 | upgradesql | 2042 | 732 | 59 | 1912 | 0 | 0 | 20 |
| 3.2.0.1 | upgradesql | 310 | 185 | 10 | 1086 | 0 | 0 | 4 |
| 3.3.0 | upgradesql | 834 | 409 | 135 | 1301 | 0 | 0 | 24 |
| 3.3.0.1 | upgradesql | 15 | 31 | 0 | 39 | 0 | 0 | 0 |
| 3.3.0.2 | upgradesql | 28 | 27 | 0 | 211 | 0 | 0 | 0 |
| 3.4.0 | upgradesql | 250 | 135 | 21 | 358 | 0 | 0 | 3 |
| 3.5.0 | upgradesql | 789 | 408 | 96 | 668 | 0 | 0 | 11 |
| 3.5.1 | upgradesql | 1081 | 388 | 44 | 740 | 0 | 0 | 11 |
| 3.5.2 | upgradesql | 70 | 42 | 8 | 80 | 0 | 0 | 3 |
| 3.5.3 | upgradesql | 41 | 20 | 0 | 76 | 0 | 0 | 0 |
| 3.5.4 | upgradesql | 6 | 6 | 0 | 0 | 0 | 0 | 0 |
| 3.5.5 | upgradesql | 34 | 16 | 0 | 37 | 0 | 0 | 0 |
| 3.5.6 | upgradesql | 6 | 6 | 0 | 1 | 0 | 0 | 0 |
| 3.5.7 | upgradesql_hgs | 12 | 12 | 7 | 0 | 0 | 0 | 3 |
| 3.5.8 | upgradesql | 39 | 16 | 0 | 43 | 0 | 0 | 0 |
| 3.5.9 | upgradesql | 10 | 10 | 0 | 27 | 0 | 0 | 0 |
| 3.5.10 | upgradesql | 6 | 6 | 1 | 0 | 0 | 0 | 0 |
| 3.5.11 | upgradesql | 552 | 282 | 13 | 253 | 0 | 0 | 3 |
| 3.5.12 | upgradesql | 64 | 22 | 0 | 23 | 0 | 0 | 0 |
| 3.5.13 | upgradesql | 17 | 9 | 0 | 9 | 0 | 0 | 0 |
| 3.6.0 | upgradesql | 864 | 463 | 48 | 1993 | 0 | 0 | 17 |
| 3.7.0.1 | add_view_schema_columns | 6 | 7 | 2 | 0 | 0 | 0 | 0 |
| 3.7.0.2 | upgradesql | 617 | 152 | 0 | 99 | 0 | 0 | 0 |
| 3.7.0.3 | upgradesql_zxm | 14 | 7 | 5 | 0 | 0 | 0 | 0 |
| 3.7.0.4 | upgradesql_cjc | 22 | 7 | 18 | 0 | 0 | 0 | 0 |
| 3.7.0.5 | create_links3_test_management_base_tables | 24 | 7 | 16 | 0 | 0 | 0 | 0 |
| 3.7.0.6 | upgradesql_qs | 6 | 7 | 1 | 0 | 0 | 0 | 0 |
| 3.7.0.7 | upgradesql | 427 | 119 | 0 | 156 | 0 | 0 | 0 |
| 3.7.0.8 | change_model_class_move | 6 | 7 | 0 | 0 | 0 | 0 | 0 |
| 3.7.0.9 | upgradesql | 57 | 40 | 0 | 274 | 0 | 0 | 0 |
| 3.7.0.10 | upgradesql_jiangkun | 6 | 7 | 0 | 0 | 0 | 0 | 0 |
| 3.7.0.11 | upgradesql | 269 | 71 | 0 | 31 | 0 | 0 | 0 |
| 3.7.0.12 | upgradesql_zxm | 7 | 7 | 8 | 0 | 0 | 0 | 0 |
| 3.7.0.13 | upgradesql | 250 | 80 | 0 | 24 | 0 | 0 | 0 |
| 3.7.0.14 | upgradesql | 19 | 21 | 0 | 14 | 0 | 0 | 0 |
| 3.7.0.15 | upgradesql | 7 | 7 | 0 | 1 | 0 | 0 | 0 |
| 3.7.0.16 | upgradesql | 102 | 33 | 0 | 18 | 0 | 0 | 0 |
| 3.7.0.17 | upgradesql_zxm | 7 | 7 | 1 | 0 | 0 | 0 | 1 |
| 3.7.0.18 | upgradesql_zxm | 6 | 7 | 2 | 0 | 0 | 0 | 0 |
| 3.7.0.19 | upgradesql | 56 | 23 | 0 | 46 | 0 | 0 | 0 |
| 3.7.0.20 | upgradesql | 383 | 89 | 0 | 25 | 0 | 0 | 0 |
| 3.7.0.21 | upgradesql_sch | 14 | 7 | 9 | 0 | 0 | 0 | 1 |
| 3.7.0.22 | upgradesql_zsj | 9 | 10 | 8 | 0 | 0 | 0 | 0 |
| 3.7.0.23 | upgradesql | 57 | 22 | 0 | 46 | 0 | 0 | 0 |
| 3.7.0.24 | upgradesql | 33 | 15 | 0 | 42 | 0 | 0 | 0 |
| 3.7.0.25 | rebuild_test_core_tables | 31 | 14 | 98 | 0 | 0 | 0 | 3 |
| 3.7.0.26 | upgrade_cjc_testcase | 15 | 7 | 2 | 0 | 0 | 0 | 2 |
| 3.7.0.27 | upgradesql | 36 | 18 | 0 | 40 | 0 | 0 | 0 |
| 3.7.0.28 | upgradesql | 110 | 54 | 0 | 58 | 0 | 0 | 0 |
| 3.7.0.29 | upgradesql | 8 | 8 | 0 | 4 | 0 | 0 | 0 |
| 3.7.0.30 | upgradesql_zxm | 10 | 7 | 0 | 1 | 0 | 0 | 0 |
| 3.7.0.31 | upgradesql_zsj | 63 | 63 | 11 | 0 | 0 | 0 | 4 |
| 3.7.0.32 | test_run_assignee_and_assignment_mode | 11 | 15 | 6 | 0 | 0 | 0 | 1 |
| 3.7.0.33 | upgradesql | 32 | 12 | 0 | 15 | 0 | 0 | 0 |
| 3.7.0.34 | upgradesql_zxm | 6 | 7 | 1 | 0 | 0 | 0 | 0 |
| 3.7.0.35 | upgradesql | 23 | 12 | 0 | 15 | 0 | 0 | 0 |

> 指标口径：结构语句 = up 中行首 CREATE/ALTER/DROP 数；DML 引用 = INSERT INTO /
> UPDATE / DELETE FROM 出现次数；函数/过程 = CREATE OR REPLACE FUNCTION/PROCEDURE；
> 序列 = CREATE SEQUENCE；视图 = CREATE [OR REPLACE] VIEW。同一对象跨轮
> 删建会在多轮重复出现。

## 六、函数 / 序列 / 视图增强清单（C1/C2 实效）

- **例程**（prokind f/p，`pg_get_functiondef` 全文 + 身份签名比对，产物带结尾
  分号）：链上共出现 6 函数 + 2 存储过程——
  `generate_lookup_number_sort_key`、`has_revision`、
  `ws_items_index_snowflake_id`、`ws_items_index_snowflake_local_id`、
  `search_id_across_database`、`string_to_sorted_text_array`；
  存储过程 `update_doc_enabled_icon`、`update_part_enabled_icon`。
  终态存活 4 函数 + 2 存储过程（snowflake 两函数被 3.0.x 轮删除，删建闭环正确）。
- **序列**（`pg_sequences` + owned_by 判定）：链上共 3 条独立序列——
  `air_ws_items_index_seq_snowflake_id`、`air_ws_items_index_seq_snowflake_local_id`、
  `air_ws_items_index_seq_merge_seq`；终态存活 1 条独立序列
  （另含账本从属序列 `schema_migrations_id_seq`，已按 C6 随表排除）。阶段 3 已补齐
  ownership equality 与 `OWNED BY table.column/NONE` 正反向 SQL；真实 PostgreSQL
  用例覆盖新增 SERIAL 从属序列、独立序列改为从属、回滚恢复 `OWNED BY NONE`，并以
  forward/rollback 后结构 diff 为空验证所有权收敛，不再仅以序列存在和参数代替验收。
- **统一对象依赖 DAG**（阶段 3）：table/view/routine/sequence 共享同一排序模块，覆盖
  函数返回新增表复合类型、SQL/PLpgSQL 静态引用新增表、删除表前删除依赖例程、
  routine→routine 与 view→routine/table；创建正拓扑、删除逆拓扑。依赖环与不可可靠
  提取的动态 SQL/语言/重载调用输出对象链并拒绝生成，重复运行保持字节一致。
- **视图依赖闭包**（C2）：27 轮含视图重编排，链上 CREATE VIEW 共 213 处
  （受影响视图先逆拓扑 DROP CASCADE、表 DDL 后拓扑重建）；热点轮
  3.0.0（33 建/18 删）、3.3.0（24）、3.2.0（20）。2.4.0 列变更轮
  「cannot alter type of a column used by a view」不再出现。
- **汇总**：含增强对象的轮次 27/88；增强产物合计 函数/过程 8、序列 3、视图 213。

## 七、Java 迁移效果缺口报告（8 个，如实）

 airedge 原链含 8 个 Flyway Java 迁移，无法 SQL 回放，本轮全部跳过。
按业务影响排序：

| 版本 | 性质 | 主要表 | SQL 可等效重写度 | 业务影响 |
|---|---|---|---|---|
| V3_0_1（2.7→3.0 数据迁移） | 混合：海量 DML + 2 条 ALTER | air_inst_common、air_ws_tracker、air_inst_tracker(_rev)、air_ws_items(_revision)_index、air_ws_workingset_branch_relation、38 张 tab 表等 50+ | 中（≈35–40% 现成；核心可 SQL 化但工作量大） | **高（链断级）**：无 Tracker/无索引体系，3.0+ 数据浏览、工作集、页签、版本功能整体不可用 |
| V3_2_0_99（goauth 替换） | 混合：DML + DROP TABLE | air_sys_node、air_sys_property；删 air_sys_client_ldap_server/air_sys_client_details | 高（≈85%，JSON 可用 jsonb_build_object） | **高**：IdP/OAuth 配置节点缺失，SSO/LDAP 登录与本地客户端认证不可用；两张旧表残留 |
| V3_6_0_5（PBS 同步到自定义工作集） | 纯 DML 搬移 | air_inst_workingset、air_ws_tracker、branch_relation、air_inst_common、air_inst_tracker_rev、air_ws_items(_revision)_index | 中（≈60%，需 WITH RECURSIVE） | 中高：自定义工作集看不到 PBS 整机/结构树 |
| V3_6_0_1（根空间补 PBS 整机） | 纯 DML 回填 | 同上 + air_inst_pbs | 高（≈80–85%） | 中高：历史根空间无 PBS 整机入口 |
| V3_6_0_2（根空间补 CI Tracker） | 纯 DML 回填 | tracker 四表 | 高（≈85%） | 中：PBS 树无法挂 CI |
| V3_6_0_4（子空间补 PBS 整机） | 纯 DML 回填 | 同 0_1 | 高（≈80–85%，与 0_1 同构） | 中：子空间 PBS 缺失 |
| V3_1_0_1（知识库文件夹重分类） | 纯 DML | air_inst_folder、folder_tab_permission 等 9 表 | 高（≈90%） | 中：文件夹类型/权限错乱 |
| V3_6_0_3（JS 不安全 ID 修复） | 纯 DML 修复 | 27 个 (表，列) 位 | 高（≈95%） | 无/低（前置 0_1 同被跳过时无对象可修） |

**技术要点**：无法 SQL 化的根因集中在——Snowflake ID 生成器与 `TK-<millis>` 编号
（V3_0_1）、三个 ConcurrentHashMap 内存映射驱动的逐行 UPDATE（V3_0_1）、
`Math.random()` 16 位 ID + 查重循环与 `PBS%04d`/`T%04d` 序号自增（0_1/0_2/0_4/0_5）、
Jackson 拼接嵌套 JSON（V3_2_0_99）、工作集父子链拓扑排序 + parent_local_id 新旧
映射（V3_6_0_5）。除上述点外均为「固定模板 INSERT/UPDATE + 循环判存 + i18n 名称
回退」，原则上可用 PL/pgSQL（`WITH RECURSIVE`、`string_to_array`、
`jsonb_build_object`、随机 ID + ON CONFLICT）等效重写。**如需闭合缺口，建议顺序：
V3_0_1 → V3_2_0_99 → 0_1/0_2/0_4 → 0_5 → V3_1_0_1（0_3 随 0_1 重写自动闭合）。**

## 八、回退对破坏性变更的固有限制

回退通道执行的是 diff 引擎在**当轮结构状态下**对称生成的 down 语句，对以下情形
天然无法还原，使用前必须人工评估：

1. **数据不可逆**：DROP COLUMN/TABLE 的 down 只重建结构，被删数据不复活；
   down 内的 UPDATE/DELETE 反向 DML 无法找回覆盖前的旧值（本轮 V3.7.0.35 为
   结构变更轮回退完整，但含数据清理的轮次不保证）。
2. **多轮链只能逐版本回退**：down 把库还原到「上一版本 applied 后的状态」，
   依赖后续版本补偿性变更的情形（A 轮改名、B 轮再改名）需按链序整段回退。
3. **未建模对象不回退**：trigger、分区、generated/identity column、用户定义
   类型等未声明支持的对象不进入产物，回退自然不覆盖；CHECK 与视图注释现已
   建模并有正反向测试。
4. **列序与重命名类差异**：ADD COLUMN 追加在表尾，回退再推进后列序可能与原库
   不同；`equalIndex` 忽略主键背书索引名，回退不会恢复原索引命名。
5. **语义时钟类对象**：序列当前值、`pg_get_functiondef` 重建的函数属性以产物
   生成时快照为准，回退窗口内的并发写入不追踪。

## 九、脚本库登记

- `datasmith-web-data/libraries/lib_8989e9f44806929e/`（airedge 库）：88 对
  176 文件（40MB），命名 `V<版本>__<标题>.up.sql/.down.sql`，与迁移目录产物
  逐字节一致（未做任何加工）。
- `store.json`：`versions` 重写为 88 条 `{expectedConnectionId: conn_4c67f13084395838}`
  （沿用原登记的预期连接，同方言守卫仅告警、异方言硬拒）。
- 清理：原手工样例 `V3.7.0__update.*`（与链版本 3.7.0 冲突，已由链产物
  `V3.7.0__upgradesql.*` 取代）；已知孤儿条目 `3.7.1`。C8 当前以 up 可执行
  入口为版本元数据生命周期：无 up 即清理，down 不单独维持登记。
- 一致性复核：88 版本 up+down 齐全、无孤儿元数据、无未登记文件、无不合规文件名；
  Web 启动 `SweepOrphanVersionMeta` 幂等。

## 十、遗留与下一批

- **C11** CHECK 约束建模、**C12** 视图注释比对、**C13** TablesModified 口径
  均已完成（收尾批，2026-09-20），实现与验收见计划文档各条目
  「完成情况」及「九」。 airedge 终态双库闭环已复核为「新增 0 / 删除 0 /
  修改 0」且产物为空；本报告「四」所列残差（3 条 CHECK——含抽查未列的
  `air_inst_testset`、4 个视图注释）已全部对齐，「八·3」回退限制中
  「不建模对象不回退」一项相应消解。
- Web 完全比对页已提供 auto/shadow/direct 开关。阶段 4 后任务参数保留
  requested mode，任务 API 摘要、`summary.json` 和日志同时记录 requested 与
  effective（shadow/direct），不再把 auto 误称为生效模式。
- 8 个 Java 迁移缺口如需闭合，按「七」建议顺序以 PL/pgSQL 等效重写后，
  以 `exec-sql` 补入链轮并重跑闭环。

## 十一、阶段 4 与最终复审一致性回归（2026-09-21）

- C8：删 up 留 down、删 down 留 up、删除最后脚本、隐式 up、目录扫描失败、
  `DeleteVersionMeta` 失败与重复清理均由 server 回归测试覆盖；最终复审增加
  删除/重新登记受控交错测试，验证 up 存在时版本元数据不会被并发删除。
- SQL 控制台切换物理连接会清空此前的 source/target 角色；diff-full 的持久化
  发布日志恢复测试覆盖未提交的半发布旧组恢复与已提交新组保留。
- 模式记录：auto→shadow、auto→direct、显式 shadow/direct 的任务参数、API 摘要、
  `summary.json` 与日志一致。
- 行定位：主键、普通非空唯一索引和无可靠身份三类策略在 MySQL/PostgreSQL
  等价测试中通过；反引号/双引号和值渲染仍保持方言差异。
- 完整 gate 通过，覆盖率 overall 74.8%（6206/8296），db/diff/sql/migrate/exec
  分别为 78.9%/77.8%/76.3%/75.9%/89.1%；双库集成测试使用 MySQL 8.4.3 与
  PostgreSQL 17.2。
