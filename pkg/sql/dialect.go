package sql

import (
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/sql/mysql"
	"github.com/jacktea/data-smith/pkg/sql/postgres"
)

type IDialect interface {
	// GenerateInsertSql 生成插入语句
	// 参数：
	// tbl: 表
	// row: 行数据
	// 返回：
	// 插入语句
	GenerateInsertSql(tbl *conn.Table, row conn.Record) string

	// GenerateDeleteSql 生成删除语句
	// 参数：
	// tbl: 表
	// row: 行数据
	// 返回：
	// 删除语句
	GenerateDeleteSql(tbl *conn.Table, row conn.Record) string

	// GenerateUpdateSql 生成更新语句
	// 参数：
	// tbl: 表
	// row: 行数据
	// updateCols: 更新列
	// 返回：
	// 更新语句
	GenerateUpdateSql(tbl *conn.Table, row conn.Record, updateCols []string) string

	// GenerateCreateIndexSql 生成创建索引语句
	// 参数：
	// t: 表
	// idx: 索引
	// 返回：
	// 创建索引语句
	GenerateCreateIndexSql(t *conn.Table, idx *conn.Index) string

	// GenerateDropIndexSql 生成删除索引语句
	// 参数：
	// t: 表
	// idx: 索引
	// 返回：
	// 删除索引语句
	GenerateDropIndexSql(t *conn.Table, idx *conn.Index) string

	// GenerateAddPrimaryKeySql 生成创建主键语句
	// 参数：
	// t: 表
	// pk: 主键
	// 返回：
	// 创建主键语句
	GenerateAddPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string

	// GenerateDropPrimaryKeySql 生成删除主键语句
	// 参数：
	// t: 表
	// pk: 主键
	// 返回：
	// 删除主键语句
	GenerateDropPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string

	// GenerateDropTableSql 生成删除表语句
	// 参数：
	// t: 表
	// 返回：
	// 删除表语句
	GenerateDropTableSql(t *conn.Table) string

	// GenerateTableDDL 生成表DDL
	// 参数：
	// t: 表
	// 返回：
	// 表DDL
	GenerateTableDDL(t *conn.Table) string

	// GenerateViewDDL 生成视图DDL
	// 参数：
	// t: 视图
	// 返回：
	// 视图DDL
	GenerateViewDDL(t *conn.Table) string

	// GenerateDropViewSql 生成删除视图语句
	// 参数：
	// t: 视图
	// 返回：
	// 删除视图语句
	GenerateDropViewSql(t *conn.Table) string

	// GenerateAddColumnSql 生成添加列语句
	// 参数：
	// t: 表
	// col: 列
	// 返回：
	// 添加列语句
	GenerateAddColumnSql(t *conn.Table, col *conn.Column) string

	// GenerateDropColumnSql 生成删除列语句
	// 参数：
	// t: 表
	// col: 列
	// 返回：
	// 删除列语句
	GenerateDropColumnSql(t *conn.Table, col *conn.Column) string

	// GenerateAlterColumnSql 生成修改列语句
	// 参数：
	// t: 表
	// oldCol: 旧列
	// newCol: 新列
	// 返回：
	// 修改列语句
	GenerateAlterColumnSql(t *conn.Table, oldCol, newCol *conn.Column) string

	// GenerateAddForeignKeySql 生成创建外键语句
	GenerateAddForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string

	// GenerateDropForeignKeySql 生成删除外键语句
	GenerateDropForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string

	// GenerateAlterTableCommentSql 生成修改表注释语句
	GenerateAlterTableCommentSql(t *conn.Table, comment string) string
}

// IDataBatchDialect is an additive capability for bounded multi-row DML.
// Keeping it separate preserves source compatibility for external IDialect
// implementations; callers can fall back to the single-row methods.
type IDataBatchDialect interface {
	GenerateInsertBatchSql(tbl *conn.Table, rows []conn.Record) string
	GenerateDeleteBatchSql(tbl *conn.Table, rows []conn.Record) string
}

// INonTableObjectDialect is an additive capability for dialects that can emit
// DDL for schema-level non-table objects: routines (functions/procedures) and
// sequences. Only PostgreSQL implements it for now; other dialects skip the
// phases in the schema generator. This mirrors the IDataBatchDialect pattern.
type INonTableObjectDialect interface {
	// GenerateCreateRoutineSql 输出例程的 CREATE 语句（pg_get_functiondef 全文，
	// 补结尾分号）；定义变更同样经 CREATE OR REPLACE 重新应用。
	GenerateCreateRoutineSql(r *conn.Routine) string
	// GenerateDropRoutineSql 按身份签名删除例程。
	GenerateDropRoutineSql(r *conn.Routine) string
	// GenerateCreateSequenceSql 生成 CREATE SEQUENCE。
	GenerateCreateSequenceSql(s *conn.Sequence) string
	// GenerateAlterSequenceSql 生成把 old 对齐到 new 的 ALTER SEQUENCE 语句集。
	GenerateAlterSequenceSql(old, new *conn.Sequence) []string
	// GenerateDropSequenceSql 生成删除序列语句。
	GenerateDropSequenceSql(s *conn.Sequence) string
}

// ICheckConstraintDialect is an additive capability for dialects that model
// table-level CHECK constraints (C11). Both PostgreSQL and MySQL implement it;
// other IDialect implementations keep working via type assertion in the
// schema generator.
type ICheckConstraintDialect interface {
	// GenerateAddCheckConstraintSql 生成 ADD CONSTRAINT ... CHECK 语句，
	// 约束体沿用提取侧的原生定义文本。
	GenerateAddCheckConstraintSql(t *conn.Table, c *conn.CheckConstraint) string
	// GenerateDropCheckConstraintSql 生成删除 CHECK 约束语句。
	GenerateDropCheckConstraintSql(t *conn.Table, c *conn.CheckConstraint) string
}

// IViewCommentDialect is an additive capability for dialects whose views carry
// comments (C12). Only PostgreSQL implements it — MySQL has no view comments.
type IViewCommentDialect interface {
	// GenerateAlterViewCommentSql 生成视图注释语句（COMMENT ON VIEW）。
	GenerateAlterViewCommentSql(t *conn.Table, comment string) string
}

func NewDialect(dbType consts.DBType) IDialect {
	switch dbType {
	case consts.DBTypePostgres:
		return postgres.NewPostgreDialect()
	case consts.DBTypeMySQL:
		return mysql.NewMySQLDialect()
	default:
		return nil
	}
}
