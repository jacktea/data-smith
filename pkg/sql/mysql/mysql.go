package mysql

import (
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/sql/ident"
)

type mysqlDialect struct {
	converter *MySQLTypeConverter
}

func NewMySQLDialect() *mysqlDialect {
	return &mysqlDialect{
		converter: NewMySQLTypeConverter(),
	}
}

func mysqlTableName(t *conn.Table) string {
	return ident.Qualified(ident.Backtick, t.Schema, t.Name)
}

func sortedIndexes(indexes map[string]*conn.Index) []*conn.Index {
	result := make([]*conn.Index, 0, len(indexes))
	for _, index := range indexes {
		result = append(result, index)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (d *mysqlDialect) GenerateInsertSql(tbl *conn.Table, row conn.Record) string {
	return d.GenerateInsertBatchSql(tbl, []conn.Record{row})
}

func (d *mysqlDialect) GenerateInsertBatchSql(tbl *conn.Table, rows []conn.Record) string {
	if len(rows) == 0 {
		return ""
	}
	var colNames []string
	cols := tbl.GetColumnsByPosition()
	for _, col := range cols {
		colNames = append(colNames, ident.Quote(ident.Backtick, col.Name))
	}
	valueRows := make([]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, 0, len(colNames))
		for _, col := range cols {
			values = append(values, d.escapedValue(col.DataType, row[col.Name]))
		}
		valueRows = append(valueRows, "("+strings.Join(values, ", ")+")")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", mysqlTableName(tbl), strings.Join(colNames, ", "), strings.Join(valueRows, ", "))
}

func (d *mysqlDialect) GenerateDeleteSql(tbl *conn.Table, row conn.Record) string {
	return d.GenerateDeleteBatchSql(tbl, []conn.Record{row})
}

// rowKeyColumns 返回行定位列: 有主键用主键; 无主键表回退为行内全部列
// (与全列比对的行身份语义一致), 排序保证生成 SQL 确定性。
func rowKeyColumns(tbl *conn.Table, row conn.Record) []string {
	if tbl.PrimaryKey != nil && len(tbl.PrimaryKey.Columns) > 0 {
		return tbl.PrimaryKey.Columns
	}
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (d *mysqlDialect) GenerateDeleteBatchSql(tbl *conn.Table, rows []conn.Record) string {
	if len(rows) == 0 {
		return ""
	}
	predicates := make([]string, 0, len(rows))
	for _, row := range rows {
		var where []string
		for _, k := range rowKeyColumns(tbl, row) {
			col := tbl.Columns[k]
			val := row[k]
			var colType string
			if col != nil {
				colType = col.DataType
			}
			if val == nil {
				where = append(where, fmt.Sprintf("%s IS NULL", ident.Quote(ident.Backtick, k)))
			} else {
				where = append(where, fmt.Sprintf("%s = %s", ident.Quote(ident.Backtick, k), d.escapedValue(colType, val)))
			}
		}
		predicates = append(predicates, "("+strings.Join(where, " AND ")+")")
	}
	if len(predicates) == 1 {
		return fmt.Sprintf("DELETE FROM %s WHERE %s;", mysqlTableName(tbl), strings.Trim(predicates[0], "()"))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", mysqlTableName(tbl), strings.Join(predicates, " OR "))
}

func (d *mysqlDialect) GenerateUpdateSql(tbl *conn.Table, row conn.Record, updateCols []string) string {
	var set, where []string
	pks := rowKeyColumns(tbl, row)
	if len(updateCols) == 0 {
		updateCols = tbl.GetColumns()
	}
	for _, c := range updateCols {
		if slices.Contains(pks, c) {
			continue
		}
		col := tbl.Columns[c]
		if col == nil {
			continue
		}
		val := row[c]
		set = append(set, fmt.Sprintf("%s = %s", ident.Quote(ident.Backtick, c), d.escapedValue(col.DataType, val)))
	}
	if len(set) == 0 {
		return ""
	}
	for _, k := range pks {
		col := tbl.Columns[k]
		val := row[k]
		var colType string
		if col != nil {
			colType = col.DataType
		}
		if val == nil {
			where = append(where, fmt.Sprintf("%s IS NULL", ident.Quote(ident.Backtick, k)))
		} else {
			where = append(where, fmt.Sprintf("%s = %s", ident.Quote(ident.Backtick, k), d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", mysqlTableName(tbl), strings.Join(set, ", "), strings.Join(where, " AND "))
}

func (d *mysqlDialect) GenerateCreateIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder

	if idx.Primary {
		ddl.WriteString("ALTER TABLE ")
		ddl.WriteString(mysqlTableName(t))
		ddl.WriteString(" ADD PRIMARY KEY (")
		ddl.WriteString(ident.List(ident.Backtick, idx.Columns, ", "))
		ddl.WriteString(");")
		return ddl.String()
	}

	ddl.WriteString("CREATE ")
	if idx.Unique {
		ddl.WriteString("UNIQUE ")
	}

	ddl.WriteString("INDEX ")
	ddl.WriteString(ident.Quote(ident.Backtick, idx.Name))
	ddl.WriteString(" ON ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" (")
	ddl.WriteString(ident.List(ident.Backtick, idx.Columns, ", "))
	ddl.WriteString(")")

	if idx.Where != nil {
		ddl.WriteString(fmt.Sprintf(" WHERE %s", *idx.Where))
	}

	ddl.WriteString(";")

	return ddl.String()
}

func (d *mysqlDialect) GenerateDropIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder
	ddl.WriteString("DROP INDEX ")
	ddl.WriteString(ident.Quote(ident.Backtick, idx.Name))
	ddl.WriteString(" ON ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAddPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" ADD CONSTRAINT ")
	ddl.WriteString(ident.Quote(ident.Backtick, pk.Name))
	ddl.WriteString(" PRIMARY KEY (")
	ddl.WriteString(ident.List(ident.Backtick, pk.Columns, ", "))
	ddl.WriteString(");")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" DROP PRIMARY KEY;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropTableSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateTableDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeTable {
		return ""
	}

	var ddl strings.Builder

	// CREATE TABLE语句
	ddl.WriteString("CREATE TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" (\n")

	// 添加列定义
	var columnDefs []string
	for _, col := range t.GetColumnsByPosition() {
		columnDefs = append(columnDefs, d.converter.GenerateColumnDDL(col))
	}

	// 添加主键
	if t.PrimaryKey != nil && len(t.PrimaryKey.Columns) > 0 {
		pkCols := make([]string, len(t.PrimaryKey.Columns))
		for i, col := range t.PrimaryKey.Columns {
			pkCols[i] = ident.Quote(ident.Backtick, col)
		}
		constraintDef := fmt.Sprintf("  PRIMARY KEY (%s)",
			strings.Join(pkCols, ", "))
		columnDefs = append(columnDefs, constraintDef)
	}

	// 添加外键。Schema generator 会为新表传入无外键副本，并在所有表
	// 都存在之后单独添加；直接调用此 API 仍保留完整 DDL 行为。
	fkNames := make([]string, 0, len(t.ForeignKeys))
	for name := range t.ForeignKeys {
		fkNames = append(fkNames, name)
	}
	sort.Strings(fkNames)
	for _, name := range fkNames {
		fk := t.ForeignKeys[name]
		fkCols := make([]string, len(fk.Columns))
		for i, col := range fk.Columns {
			fkCols[i] = ident.Quote(ident.Backtick, col)
		}
		refCols := make([]string, len(fk.ReferencedColumns))
		for i, col := range fk.ReferencedColumns {
			refCols[i] = ident.Quote(ident.Backtick, col)
		}

		refSchema := fk.ReferencedSchema
		if refSchema == "" {
			refSchema = t.Schema
		}
		constraintDef := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			ident.Quote(ident.Backtick, fk.Name), strings.Join(fkCols, ", "),
			ident.Qualified(ident.Backtick, refSchema, fk.ReferencedTable), strings.Join(refCols, ", "))

		if fk.OnDelete != "" {
			constraintDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != "" {
			constraintDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}

		columnDefs = append(columnDefs, constraintDef)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))
	ddl.WriteString("\n)")

	// 添加表选项
	if t.Comment != "" {
		ddl.WriteString(fmt.Sprintf(" COMMENT='%s'", strings.ReplaceAll(t.Comment, "'", "''")))
	}

	ddl.WriteString(";")

	// 添加索引
	for _, idx := range sortedIndexes(t.Indexes) {
		if idx.Primary {
			continue // 主键索引已经在表定义中
		}
		ddl.WriteString("\n\n")
		ddl.WriteString(d.GenerateCreateIndexSql(t, idx))
	}

	return ddl.String()
}

func (d *mysqlDialect) GenerateViewDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeView || t.ViewDefinition == nil {
		return ""
	}

	var ddl strings.Builder

	// 基本CREATE VIEW语句
	ddl.WriteString("CREATE VIEW ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" AS\n")

	// 添加SELECT语句
	ddl.WriteString(t.ViewDefinition.SelectStatement)

	// 添加检查选项
	if t.ViewDefinition.CheckOption != "" && t.ViewDefinition.CheckOption != "NONE" {
		ddl.WriteString(fmt.Sprintf("\nWITH %s CHECK OPTION", t.ViewDefinition.CheckOption))
	}

	ddl.WriteString(";")

	// 添加注释
	if t.ViewDefinition.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nALTER VIEW %s COMMENT = '%s';",
			mysqlTableName(t), strings.ReplaceAll(t.ViewDefinition.Comment, "'", "''")))
	}

	return ddl.String()
}

func (d *mysqlDialect) GenerateDropViewSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP VIEW ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAddColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" ADD COLUMN ")
	ddl.WriteString(d.converter.GenerateColumnDDL(col))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" DROP COLUMN ")
	ddl.WriteString(ident.Quote(ident.Backtick, col.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAlterColumnSql(t *conn.Table, oldCol, newCol *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(mysqlTableName(t))
	ddl.WriteString(" MODIFY COLUMN ")
	ddl.WriteString(d.converter.GenerateColumnDDL(newCol))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAddForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (", mysqlTableName(t), ident.Quote(ident.Backtick, fk.Name)))
	ddl.WriteString(ident.List(ident.Backtick, fk.Columns, ", "))
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = t.Schema
	}
	ddl.WriteString(fmt.Sprintf(") REFERENCES %s (", ident.Qualified(ident.Backtick, refSchema, fk.ReferencedTable)))
	ddl.WriteString(ident.List(ident.Backtick, fk.ReferencedColumns, ", "))
	ddl.WriteString(")")
	if fk.OnDelete != "" && strings.ToUpper(fk.OnDelete) != "NO ACTION" {
		ddl.WriteString(fmt.Sprintf(" ON DELETE %s", fk.OnDelete))
	}
	if fk.OnUpdate != "" && strings.ToUpper(fk.OnUpdate) != "NO ACTION" {
		ddl.WriteString(fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate))
	}
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string {
	return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", mysqlTableName(t), ident.Quote(ident.Backtick, fk.Name))
}

func (d *mysqlDialect) GenerateAlterTableCommentSql(t *conn.Table, comment string) string {
	return fmt.Sprintf("ALTER TABLE %s COMMENT = '%s';", mysqlTableName(t), strings.ReplaceAll(comment, "'", "''"))
}

func (d *mysqlDialect) escapedValue(dataType string, val any) string {
	dt := strings.ToLower(dataType)
	if val == nil {
		return "NULL"
	}

	// 1. 二进制类型（BLOB, BINARY, VARBINARY）
	if strings.Contains(dt, "blob") || strings.Contains(dt, "binary") {
		switch v := val.(type) {
		case []byte:
			return fmt.Sprintf("0x%s", hex.EncodeToString(v))
		case string:
			return fmt.Sprintf("0x%s", hex.EncodeToString([]byte(v)))
		}
	}

	// 2. 布尔类型
	if strings.Contains(dt, "bool") || strings.HasPrefix(dt, "tinyint(1)") {
		switch v := val.(type) {
		case bool:
			if v {
				return "1"
			}
			return "0"
		case int, int8, int16, int32, int64:
			if fmt.Sprintf("%v", v) != "0" {
				return "1"
			}
			return "0"
		case string:
			s := strings.ToLower(strings.TrimSpace(v))
			if s == "true" || s == "t" || s == "1" {
				return "1"
			}
			return "0"
		}
	}

	// 3. 时间与日期类型
	if strings.Contains(dt, "date") || strings.Contains(dt, "time") || strings.Contains(dt, "year") {
		if t, ok := val.(time.Time); ok {
			return fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05"))
		}
		strVal := fmt.Sprintf("%v", val)
		return fmt.Sprintf("'%s'", strings.ReplaceAll(strVal, "'", "''"))
	}

	// 4. 字符串、文本与 JSON 类型
	if strings.Contains(dt, "char") || strings.Contains(dt, "text") || strings.Contains(dt, "json") || strings.Contains(dt, "enum") || strings.Contains(dt, "set") {
		var strVal string
		if b, ok := val.([]byte); ok {
			strVal = string(b)
		} else {
			strVal = fmt.Sprintf("%v", val)
		}
		escaped := strings.ReplaceAll(strVal, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "''")
		escaped = strings.ReplaceAll(escaped, "\n", "\\n")
		escaped = strings.ReplaceAll(escaped, "\r", "\\r")
		escaped = strings.ReplaceAll(escaped, "\t", "\\t")
		escaped = strings.ReplaceAll(escaped, "\b", "\\b")
		escaped = strings.ReplaceAll(escaped, "\f", "\\f")
		return fmt.Sprintf("'%s'", escaped)
	}

	// 5. 其他类型
	if b, ok := val.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", val)
}
