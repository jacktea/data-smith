package sql

import (
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

// GenerateSchemaSQL 根据差异和目标数据库类型生成 SQL 脚本
func GenerateSchemaSQL(diff *diff.SchemaDiff, dialect consts.DBType) []string {
	dbDialect := NewDialect(dialect)
	var sqls []string
	var views []string
	for _, tbl := range diff.TablesAdded {
		if tbl.Type == conn.TableTypeView {
			views = append(views, dbDialect.GenerateViewDDL(tbl))
		} else {
			sqls = append(sqls, dbDialect.GenerateTableDDL(tbl))
		}
	}
	for _, tbl := range diff.TablesDropped {
		if tbl.Type == conn.TableTypeView {
			views = append(views, dbDialect.GenerateDropViewSql(tbl))
		} else {
			sqls = append(sqls, dbDialect.GenerateDropTableSql(tbl))
		}
	}
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView {
			views = append(views, genAlterView(tdiff, dbDialect)...) // 多条
		} else {
			sqls = append(sqls, genAlterTable(tdiff, dbDialect)...) // 多条
		}
	}
	return append(sqls, views...)
}

func genAlterTable(diff *diff.TableDiff, dialect IDialect) []string {
	var sqls []string
	tbl := diff.Table
	for _, col := range diff.ColumnsAdded {
		sqls = append(sqls, dialect.GenerateAddColumnSql(tbl, col))
	}
	for _, col := range diff.ColumnsDropped {
		sqls = append(sqls, dialect.GenerateDropColumnSql(tbl, col))
	}
	for _, cmod := range diff.ColumnsModified {
		sqls = append(sqls, dialect.GenerateAlterColumnSql(tbl, cmod.Old, cmod.New))
	}
	for _, idx := range diff.IndexesAdded {
		sqls = append(sqls, dialect.GenerateCreateIndexSql(tbl, idx))
	}
	for _, idx := range diff.IndexesDropped {
		sqls = append(sqls, dialect.GenerateDropIndexSql(tbl, idx))
	}
	for _, imod := range diff.IndexesModified {
		if imod.Old != nil {
			sqls = append(sqls, dialect.GenerateDropIndexSql(tbl, imod.Old))
		}
		if imod.New != nil {
			sqls = append(sqls, dialect.GenerateCreateIndexSql(tbl, imod.New))
		}
	}
	if diff.PrimaryKeyChange != nil {
		if diff.PrimaryKeyChange.Old != nil {
			sqls = append(sqls, dialect.GenerateDropPrimaryKeySql(tbl, diff.PrimaryKeyChange.Old))
		}
		if diff.PrimaryKeyChange.New != nil {
			sqls = append(sqls, dialect.GenerateAddPrimaryKeySql(tbl, diff.PrimaryKeyChange.New))
		}
	}
	// 外键略，可扩展
	return sqls
}

func genAlterView(diff *diff.TableDiff, dialect IDialect) []string {
	var sqls []string
	tbl := diff.Table
	if diff.ViewDefinitionChange != nil {
		sqls = append(sqls, dialect.GenerateDropViewSql(tbl))
		sqls = append(sqls, dialect.GenerateViewDDL(&conn.Table{
			Name:           tbl.Name,
			Schema:         tbl.Schema,
			Type:           conn.TableTypeView,
			ViewDefinition: diff.ViewDefinitionChange.New,
		}))
	}
	return sqls
}
