package mysql

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/utils"
)

type mysqlDialect struct {
	converter *MySQLTypeConverter
}

func NewMySQLDialect() *mysqlDialect {
	return &mysqlDialect{
		converter: NewMySQLTypeConverter(),
	}
}

func (d *mysqlDialect) GenerateInsertSql(tbl *conn.Table, row conn.Record) string {
	var colNames, values []string
	for _, col := range tbl.Columns {
		colNames = append(colNames, fmt.Sprintf("`%s`", col.Name))
		val := row[col.Name]
		values = append(values, d.escapedValue(col.DataType, val))
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);", tbl.Name, strings.Join(colNames, ", "), strings.Join(values, ", "))
}

func (d *mysqlDialect) GenerateDeleteSql(tbl *conn.Table, row conn.Record) string {
	var where []string
	for _, k := range tbl.PrimaryKey.Columns {
		col := tbl.Columns[k]
		val := row[k]
		var colType string
		if col != nil {
			colType = col.DataType
		}
		if val == nil {
			where = append(where, fmt.Sprintf("`%s` IS NULL", k))
		} else {
			where = append(where, fmt.Sprintf("`%s` = %s", k, d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s;", tbl.Name, strings.Join(where, " AND "))
}

func (d *mysqlDialect) GenerateUpdateSql(tbl *conn.Table, row conn.Record, updateCols []string) string {
	var set, where []string
	pks := tbl.PrimaryKey.Columns
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
		set = append(set, fmt.Sprintf("`%s` = %s", c, d.escapedValue(col.DataType, val)))
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
			where = append(where, fmt.Sprintf("`%s` IS NULL", k))
		} else {
			where = append(where, fmt.Sprintf("`%s` = %s", k, d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s;", tbl.Name, strings.Join(set, ", "), strings.Join(where, " AND "))
}

func (d *mysqlDialect) GenerateCreateIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder

	if idx.Primary {
		ddl.WriteString("ALTER TABLE `")
		ddl.WriteString(t.Name)
		ddl.WriteString("` ADD PRIMARY KEY (")
		ddl.WriteString(utils.JoinWrap(idx.Columns, "`", ", "))
		ddl.WriteString(");")
		return ddl.String()
	}

	ddl.WriteString("CREATE ")
	if idx.Unique {
		ddl.WriteString("UNIQUE ")
	}

	ddl.WriteString("INDEX `")
	ddl.WriteString(idx.Name)
	ddl.WriteString("` ON `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` (")

	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = fmt.Sprintf("`%s`", col)
	}
	ddl.WriteString(strings.Join(quotedCols, ", "))
	ddl.WriteString(")")

	if idx.Where != nil {
		ddl.WriteString(fmt.Sprintf(" WHERE %s", *idx.Where))
	}

	ddl.WriteString(";")

	return ddl.String()
}

func (d *mysqlDialect) GenerateDropIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder
	ddl.WriteString("DROP INDEX `")
	ddl.WriteString(idx.Name)
	ddl.WriteString("` ON `")
	ddl.WriteString(t.Name)
	ddl.WriteString("`;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAddPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` ADD CONSTRAINT `")
	ddl.WriteString(pk.Name)
	ddl.WriteString("` PRIMARY KEY (")
	ddl.WriteString(utils.JoinWrap(pk.Columns, "`", ", "))
	ddl.WriteString(");")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` DROP PRIMARY KEY;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropTableSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("`;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateTableDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeTable {
		return ""
	}

	var ddl strings.Builder

	// CREATE TABLE语句
	ddl.WriteString("CREATE TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` (\n")

	// 按位置排序列
	type colWithPos struct {
		col *conn.Column
		pos int
	}
	var sortedCols []colWithPos
	for _, col := range t.Columns {
		sortedCols = append(sortedCols, colWithPos{col, col.Position})
	}

	// 简单排序
	for i := 0; i < len(sortedCols); i++ {
		for j := i + 1; j < len(sortedCols); j++ {
			if sortedCols[i].pos > sortedCols[j].pos {
				sortedCols[i], sortedCols[j] = sortedCols[j], sortedCols[i]
			}
		}
	}

	// 添加列定义
	var columnDefs []string
	for _, colPos := range sortedCols {
		col := colPos.col
		columnDefs = append(columnDefs, d.converter.GenerateColumnDDL(col))
	}

	// 添加主键
	if t.PrimaryKey != nil && len(t.PrimaryKey.Columns) > 0 {
		pkCols := make([]string, len(t.PrimaryKey.Columns))
		for i, col := range t.PrimaryKey.Columns {
			pkCols[i] = fmt.Sprintf("`%s`", col)
		}
		constraintDef := fmt.Sprintf("  PRIMARY KEY (%s)",
			strings.Join(pkCols, ", "))
		columnDefs = append(columnDefs, constraintDef)
	}

	// 添加外键
	for _, fk := range t.ForeignKeys {
		fkCols := make([]string, len(fk.Columns))
		for i, col := range fk.Columns {
			fkCols[i] = fmt.Sprintf("`%s`", col)
		}
		refCols := make([]string, len(fk.ReferencedColumns))
		for i, col := range fk.ReferencedColumns {
			refCols[i] = fmt.Sprintf("`%s`", col)
		}

		constraintDef := fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)",
			fk.Name, strings.Join(fkCols, ", "),
			fk.ReferencedTable, strings.Join(refCols, ", "))

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
	for _, idx := range t.Indexes {
		if idx.Primary {
			continue // 主键索引已经在表定义中
		}
		ddl.WriteString("\n\n")
		ddl.WriteString(d.GenerateCreateIndexSql(t, idx))
	}

	// 添加列注释
	for _, col := range t.Columns {
		if col.Comment != nil && *col.Comment != "" {
			ddl.WriteString(fmt.Sprintf("\n\nALTER TABLE `%s` MODIFY COLUMN `%s` %s COMMENT '%s';",
				t.Name, col.Name, d.converter.GenerateColumnType(col), strings.ReplaceAll(*col.Comment, "'", "''")))
		}
	}

	return ddl.String()
}

func (d *mysqlDialect) GenerateViewDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeView || t.ViewDefinition == nil {
		return ""
	}

	var ddl strings.Builder

	// 基本CREATE VIEW语句
	ddl.WriteString("CREATE VIEW `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` AS\n")

	// 添加SELECT语句
	ddl.WriteString(t.ViewDefinition.SelectStatement)

	// 添加检查选项
	if t.ViewDefinition.CheckOption != "" && t.ViewDefinition.CheckOption != "NONE" {
		ddl.WriteString(fmt.Sprintf("\nWITH %s CHECK OPTION", t.ViewDefinition.CheckOption))
	}

	ddl.WriteString(";")

	// 添加注释
	if t.ViewDefinition.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nALTER VIEW `%s` COMMENT = '%s';",
			t.Name, strings.ReplaceAll(t.ViewDefinition.Comment, "'", "''")))
	}

	return ddl.String()
}

func (d *mysqlDialect) GenerateDropViewSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP VIEW `")
	ddl.WriteString(t.Name)
	ddl.WriteString("`;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAddColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` ADD COLUMN ")
	ddl.WriteString(d.converter.GenerateColumnDDL(col))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *mysqlDialect) GenerateDropColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` DROP COLUMN `")
	ddl.WriteString(col.Name)
	ddl.WriteString("`;")
	return ddl.String()
}

func (d *mysqlDialect) GenerateAlterColumnSql(t *conn.Table, oldCol, newCol *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE `")
	ddl.WriteString(t.Name)
	ddl.WriteString("` MODIFY COLUMN ")
	ddl.WriteString(d.converter.GenerateColumnDDL(newCol))
	ddl.WriteString(";")
	return ddl.String()
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
