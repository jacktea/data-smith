package postgres

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/utils"
)

type postgreDialect struct {
	converter *PostgreSQLTypeConverter
}

func NewPostgreDialect() *postgreDialect {
	return &postgreDialect{
		converter: NewPostgreSQLTypeConverter(),
	}
}

func (d *postgreDialect) GenerateInsertSql(tbl *conn.Table, row conn.Record) string {
	var colNames, values []string
	cols := tbl.GetColumnsByPosition()
	for _, col := range cols {
		colNames = append(colNames, fmt.Sprintf("\"%s\"", col.Name))
		val := row[col.Name]
		values = append(values, d.escapedValue(col.DataType, val))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tbl.Name, strings.Join(colNames, ", "), strings.Join(values, ", "))
}

func (d *postgreDialect) GenerateDeleteSql(tbl *conn.Table, row conn.Record) string {
	var where []string
	for _, k := range tbl.PrimaryKey.Columns {
		col := tbl.Columns[k]
		val := row[k]
		var colType string
		if col != nil {
			colType = col.DataType
		}
		if val == nil {
			where = append(where, fmt.Sprintf("\"%s\" IS NULL", k))
		} else {
			where = append(where, fmt.Sprintf("\"%s\" = %s", k, d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", tbl.Name, strings.Join(where, " AND "))
}

func (d *postgreDialect) GenerateUpdateSql(tbl *conn.Table, row conn.Record, updateCols []string) string {
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
		set = append(set, fmt.Sprintf("\"%s\" = %s", c, d.escapedValue(col.DataType, val)))
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
			where = append(where, fmt.Sprintf("\"%s\" IS NULL", k))
		} else {
			where = append(where, fmt.Sprintf("\"%s\" = %s", k, d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tbl.Name, strings.Join(set, ", "), strings.Join(where, " AND "))
}

func (d *postgreDialect) GenerateCreateIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder

	if idx.Primary {
		return ""
		// ddl.WriteString("ALTER TABLE ")
		// if t.Schema != "" && t.Schema != "public" {
		// 	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
		// }
		// ddl.WriteString(
		// 	fmt.Sprintf("\"%s\" ADD PRIMARY KEY (%s);",
		// 		t.Name,
		// 		utils.JoinWrap(idx.Columns, "\"", ", ")))
		// return ddl.String()
	}

	ddl.WriteString("CREATE ")
	if idx.Unique {
		ddl.WriteString("UNIQUE ")
	}

	ddl.WriteString(fmt.Sprintf("INDEX \"%s\" ON ", idx.Name))
	if t.Schema != "" && t.Schema != "public" {
		ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	}
	ddl.WriteString(fmt.Sprintf("\"%s\"", t.Name))

	if idx.Method != "" && idx.Method != "btree" {
		ddl.WriteString(fmt.Sprintf(" USING %s", idx.Method))
	}

	ddl.WriteString(" (")
	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = fmt.Sprintf("\"%s\"", col)
	}
	ddl.WriteString(strings.Join(quotedCols, ", "))
	ddl.WriteString(")")

	if idx.Where != nil {
		ddl.WriteString(fmt.Sprintf(" WHERE %s", *idx.Where))
	}

	ddl.WriteString(";")

	return ddl.String()
}

func (d *postgreDialect) GenerateDropIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder
	ddl.WriteString("DROP INDEX ")
	if t.Schema != "" && t.Schema != "public" {
		ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	}
	ddl.WriteString(fmt.Sprintf("\"%s\"", idx.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateAddPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	ddl.WriteString(fmt.Sprintf("\"%s\" ADD CONSTRAINT \"%s\" PRIMARY KEY (%s);", t.Name, pk.Name, utils.JoinWrap(pk.Columns, "\"", ", ")))
	return ddl.String()
}

func (d *postgreDialect) GenerateDropPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	ddl.WriteString(fmt.Sprintf("\"%s\" DROP CONSTRAINT \"%s\";", t.Name, pk.Name))
	return ddl.String()
}

func (d *postgreDialect) GenerateDropTableSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP TABLE ")
	if t.Schema != "" && t.Schema != "public" {
		ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	}
	ddl.WriteString(fmt.Sprintf("\"%s\"", t.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateTableDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeTable {
		return ""
	}

	var ddl strings.Builder

	// CREATE TABLE语句
	ddl.WriteString("CREATE TABLE ")
	if t.Schema != "" && t.Schema != "public" {
		ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	}
	ddl.WriteString(fmt.Sprintf("\"%s\" (\n", t.Name))

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
			pkCols[i] = fmt.Sprintf("\"%s\"", col)
		}
		constraintDef := fmt.Sprintf("  CONSTRAINT \"%s\" PRIMARY KEY (%s)",
			t.PrimaryKey.Name, strings.Join(pkCols, ", "))
		columnDefs = append(columnDefs, constraintDef)
	}

	// 添加外键
	for _, fk := range t.ForeignKeys {
		fkCols := make([]string, len(fk.Columns))
		for i, col := range fk.Columns {
			fkCols[i] = fmt.Sprintf("\"%s\"", col)
		}
		refCols := make([]string, len(fk.ReferencedColumns))
		for i, col := range fk.ReferencedColumns {
			refCols[i] = fmt.Sprintf("\"%s\"", col)
		}

		constraintDef := fmt.Sprintf("  CONSTRAINT \"%s\" FOREIGN KEY (%s) REFERENCES \"%s\".\"%s\" (%s)",
			fk.Name, strings.Join(fkCols, ", "),
			fk.ReferencedSchema, fk.ReferencedTable,
			strings.Join(refCols, ", "))

		if fk.OnDelete != "" {
			constraintDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != "" {
			constraintDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}

		columnDefs = append(columnDefs, constraintDef)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))
	ddl.WriteString("\n);")

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
		if col.Comment != nil {
			ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';",
				t.Schema, t.Name, col.Name, *col.Comment))
		}
	}

	// 添加表注释
	if t.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON TABLE \"%s\".\"%s\" IS '%s';",
			t.Schema, t.Name, t.Comment))
	}

	return ddl.String()
}

func (d *postgreDialect) GenerateViewDDL(t *conn.Table) string {
	if t.Type != conn.TableTypeView || t.ViewDefinition == nil {
		return ""
	}

	var ddl strings.Builder

	// 基本CREATE VIEW语句
	ddl.WriteString("CREATE VIEW ")
	if t.Schema != "" && t.Schema != "public" {
		ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	}
	ddl.WriteString(fmt.Sprintf("\"%s\" AS\n", t.Name))

	// 添加SELECT语句
	ddl.WriteString(t.ViewDefinition.SelectStatement)

	// 添加检查选项
	if t.ViewDefinition.CheckOption != "" && t.ViewDefinition.CheckOption != "NONE" {
		ddl.WriteString(fmt.Sprintf("\nWITH %s CHECK OPTION", t.ViewDefinition.CheckOption))
	}

	ddl.WriteString(";")

	// 添加注释
	if t.ViewDefinition.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON VIEW \"%s\".\"%s\" IS '%s';",
			t.Schema, t.Name, t.ViewDefinition.Comment))
	}

	return ddl.String()
}

func (d *postgreDialect) GenerateDropViewSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP VIEW ")
	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	ddl.WriteString(fmt.Sprintf("\"%s\"", t.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateAddColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	ddl.WriteString(fmt.Sprintf("\"%s\" ADD COLUMN ", t.Name))
	ddl.WriteString(d.converter.GenerateColumnDDL(col))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateDropColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(fmt.Sprintf("\"%s\".", t.Schema))
	ddl.WriteString(fmt.Sprintf("\"%s\" DROP COLUMN ", t.Name))
	ddl.WriteString(fmt.Sprintf("\"%s\"", col.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateAlterColumnSql(t *conn.Table, oldCol, newCol *conn.Column) string {
	var ddl strings.Builder
	prefix := fmt.Sprintf("ALTER TABLE \"%s\".\"%s\"", t.Schema, t.Name)
	// 修改字段名
	if oldCol.Name != newCol.Name {
		ddl.WriteString(fmt.Sprintf("%s %s", prefix, fmt.Sprintf("RENAME COLUMN \"%s\" TO \"%s\";", oldCol.Name, newCol.Name)))
	}
	// 修改字段类型
	oldDataType := d.converter.ConvertType(oldCol)
	newDataType := d.converter.ConvertType(newCol)
	if oldDataType != newDataType {
		suffix := ""
		if slices.Contains([]string{"int2", "int4", "int8", "jsonb", "json"}, newDataType) {
			suffix = fmt.Sprintf("USING \"%s\"::%s", newCol.Name, newDataType)
		}
		ddl.WriteString(fmt.Sprintf("%s %s", prefix, fmt.Sprintf("ALTER COLUMN \"%s\" TYPE %s %s;", newCol.Name, newDataType, suffix)))
	}
	// 修改默认值
	if (newCol.Default != nil && oldCol.Default == nil) ||
		(newCol.Default != nil && oldCol.Default != nil && *newCol.Default != *oldCol.Default) {
		ddl.WriteString(fmt.Sprintf("%s %s", prefix, fmt.Sprintf("ALTER COLUMN \"%s\" SET DEFAULT %s;", newCol.Name, *newCol.Default)))
	}
	// 修改为空状态
	if oldCol.Nullable != newCol.Nullable {
		if newCol.Nullable {
			ddl.WriteString(fmt.Sprintf("%s %s", prefix, fmt.Sprintf("ALTER COLUMN \"%s\" DROP NOT NULL;", newCol.Name)))
		} else {
			ddl.WriteString(fmt.Sprintf("%s %s", prefix, fmt.Sprintf("ALTER COLUMN \"%s\" SET NOT NULL;", newCol.Name)))
		}
	}

	// 修改注释
	if (newCol.Comment != nil && oldCol.Comment == nil) ||
		(newCol.Comment != nil && oldCol.Comment != nil && *newCol.Comment != *oldCol.Comment) {
		ddl.WriteString(fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';", t.Schema, t.Name, newCol.Name, *newCol.Comment))
	}

	return ddl.String()
}

func (d *postgreDialect) escapedValue(dataType string, val any) string {
	dt := strings.ToLower(dataType)
	if val == nil {
		return "NULL"
	}

	// 1. BYTEA 二进制类型
	if strings.Contains(dt, "bytea") {
		switch v := val.(type) {
		case []byte:
			return fmt.Sprintf("E'\\\\x%s'::bytea", hex.EncodeToString(v))
		case string:
			if strings.HasPrefix(v, "\\x") {
				return fmt.Sprintf("'%s'::bytea", v)
			}
			return fmt.Sprintf("E'\\\\x%s'::bytea", hex.EncodeToString([]byte(v)))
		}
	}

	// 2. 布尔类型
	if strings.Contains(dt, "bool") {
		switch v := val.(type) {
		case bool:
			if v {
				return "TRUE"
			}
			return "FALSE"
		case int, int8, int16, int32, int64:
			if fmt.Sprintf("%v", v) != "0" {
				return "TRUE"
			}
			return "FALSE"
		case string:
			s := strings.ToLower(strings.TrimSpace(v))
			if s == "true" || s == "t" || s == "1" {
				return "TRUE"
			}
			return "FALSE"
		}
	}

	// 3. 时间与日期类型
	if strings.Contains(dt, "date") || strings.Contains(dt, "time") {
		if t, ok := val.(time.Time); ok {
			return fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.000000-07"))
		}
		strVal := fmt.Sprintf("%v", val)
		return fmt.Sprintf("'%s'", strings.ReplaceAll(strVal, "'", "''"))
	}

	// 4. 字符串、文本与 JSON 类型
	if strings.Contains(dt, "char") || strings.Contains(dt, "text") || strings.Contains(dt, "json") || strings.Contains(dt, "uuid") {
		var strVal string
		if b, ok := val.([]byte); ok {
			strVal = string(b)
		} else {
			strVal = fmt.Sprintf("%v", val)
		}
		// PostgreSQL 普通字符串字面量遵循标准 SQL，反斜杠是普通字符，仅需转义单引号
		escaped := strings.ReplaceAll(strVal, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}

	// 5. 其他类型（整型、浮点、数值）
	if b, ok := val.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", val)
}
