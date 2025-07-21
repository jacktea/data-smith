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
