package sql

import (
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

// GenerateSchemaSQL 根据差异和目标方言类型生成分阶段、安全的 SQL 脚本序列
func GenerateSchemaSQL(diff *diff.SchemaDiff, dialect consts.DBType) []string {
	dbDialect := NewDialect(dialect)
	if dbDialect == nil {
		return nil
	}

	var dropDepSqls []string  // 阶段 1: 解除旧依赖 (Drop Views, Drop FKs, Drop Indexes, Drop PKs, Drop Columns)
	var alterColSqls []string // 阶段 2: 调整表列结构 (Alter Columns, Add Columns)
	var buildKeySqls []string // 阶段 3: 建立主键、索引与创建新增表 (Add PKs, Add Indexes, Create Tables)
	var finalizeSqls []string // 阶段 4: 关联外键、删除旧表、创建视图与更新注释 (Add FKs, Drop Tables, Create Views, Alter Comments)

	addStmt := func(slice *[]string, s string) {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			*slice = append(*slice, trimmed)
		}
	}

	// 阶段 1.1：删除废弃或待修改的视图
	for _, tbl := range diff.TablesDropped {
		if tbl.Type == conn.TableTypeView {
			addStmt(&dropDepSqls, dbDialect.GenerateDropViewSql(tbl))
		}
	}
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView && tdiff.ViewDefinitionChange != nil {
			addStmt(&dropDepSqls, dbDialect.GenerateDropViewSql(tdiff.Table))
		}
	}

	// 阶段 1.2: 对修改表先解除外键、索引、主键与被删列
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView {
			continue
		}
		tbl := tdiff.Table
		// 删外键
		for _, fk := range tdiff.ForeignKeysDropped {
			addStmt(&dropDepSqls, dbDialect.GenerateDropForeignKeySql(tbl, fk))
		}
		for _, fkMod := range tdiff.ForeignKeysModified {
			if fkMod.Old != nil {
				addStmt(&dropDepSqls, dbDialect.GenerateDropForeignKeySql(tbl, fkMod.Old))
			}
		}
		// 删索引
		for _, idx := range tdiff.IndexesDropped {
			addStmt(&dropDepSqls, dbDialect.GenerateDropIndexSql(tbl, idx))
		}
		for _, imod := range tdiff.IndexesModified {
			if imod.Old != nil {
				addStmt(&dropDepSqls, dbDialect.GenerateDropIndexSql(tbl, imod.Old))
			}
		}
		// 删主键
		if tdiff.PrimaryKeyChange != nil && tdiff.PrimaryKeyChange.Old != nil {
			addStmt(&dropDepSqls, dbDialect.GenerateDropPrimaryKeySql(tbl, tdiff.PrimaryKeyChange.Old))
		}
		// 删列
		for _, col := range tdiff.ColumnsDropped {
			addStmt(&dropDepSqls, dbDialect.GenerateDropColumnSql(tbl, col))
		}
	}

	// 阶段 2: 调整表列结构 (修改列类型/默认值/非空，添加新列)
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView {
			continue
		}
		tbl := tdiff.Table
		for _, cmod := range tdiff.ColumnsModified {
			addStmt(&alterColSqls, dbDialect.GenerateAlterColumnSql(tbl, cmod.Old, cmod.New))
		}
		for _, col := range tdiff.ColumnsAdded {
			addStmt(&alterColSqls, dbDialect.GenerateAddColumnSql(tbl, col))
		}
	}

	// 阶段 3: 建立主键、索引与创建新增表
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView {
			continue
		}
		tbl := tdiff.Table
		if tdiff.PrimaryKeyChange != nil && tdiff.PrimaryKeyChange.New != nil {
			addStmt(&buildKeySqls, dbDialect.GenerateAddPrimaryKeySql(tbl, tdiff.PrimaryKeyChange.New))
		}
		for _, imod := range tdiff.IndexesModified {
			if imod.New != nil {
				addStmt(&buildKeySqls, dbDialect.GenerateCreateIndexSql(tbl, imod.New))
			}
		}
		for _, idx := range tdiff.IndexesAdded {
			addStmt(&buildKeySqls, dbDialect.GenerateCreateIndexSql(tbl, idx))
		}
	}
	// 创建新增的普通表
	for _, tbl := range diff.TablesAdded {
		if tbl.Type != conn.TableTypeView {
			addStmt(&buildKeySqls, dbDialect.GenerateTableDDL(tbl))
		}
	}

	// 阶段 4: 关联外键、删除旧表、创建视图与更新注释
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView {
			continue
		}
		tbl := tdiff.Table
		for _, fkMod := range tdiff.ForeignKeysModified {
			if fkMod.New != nil {
				addStmt(&finalizeSqls, dbDialect.GenerateAddForeignKeySql(tbl, fkMod.New))
			}
		}
		for _, fk := range tdiff.ForeignKeysAdded {
			addStmt(&finalizeSqls, dbDialect.GenerateAddForeignKeySql(tbl, fk))
		}
		if tdiff.CommentChange != nil {
			addStmt(&finalizeSqls, dbDialect.GenerateAlterTableCommentSql(tbl, tdiff.CommentChange.New))
		}
	}
	// 删除废弃的普通表
	for _, tbl := range diff.TablesDropped {
		if tbl.Type != conn.TableTypeView {
			addStmt(&finalizeSqls, dbDialect.GenerateDropTableSql(tbl))
		}
	}
	// 创建新增视图及修改后的视图
	for _, tbl := range diff.TablesAdded {
		if tbl.Type == conn.TableTypeView {
			addStmt(&finalizeSqls, dbDialect.GenerateViewDDL(tbl))
		}
	}
	for _, tdiff := range diff.TablesModified {
		if tdiff.Table.Type == conn.TableTypeView && tdiff.ViewDefinitionChange != nil {
			tbl := tdiff.Table
			addStmt(&finalizeSqls, dbDialect.GenerateViewDDL(&conn.Table{
				Name:           tbl.Name,
				Schema:         tbl.Schema,
				Type:           conn.TableTypeView,
				ViewDefinition: tdiff.ViewDefinitionChange.New,
			}))
		}
	}

	var allSqls []string
	allSqls = append(allSqls, dropDepSqls...)
	allSqls = append(allSqls, alterColSqls...)
	allSqls = append(allSqls, buildKeySqls...)
	allSqls = append(allSqls, finalizeSqls...)

	return allSqls
}
