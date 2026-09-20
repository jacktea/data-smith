package postgres

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

type postgreDialect struct {
	converter *PostgreSQLTypeConverter
}

func NewPostgreDialect() *postgreDialect {
	return &postgreDialect{
		converter: NewPostgreSQLTypeConverter(),
	}
}

func postgresSchema(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}

func postgresTableName(t *conn.Table) string {
	return ident.Qualified(ident.DoubleQuote, postgresSchema(t.Schema), t.Name)
}

func sortedIndexes(indexes map[string]*conn.Index) []*conn.Index {
	result := make([]*conn.Index, 0, len(indexes))
	for _, index := range indexes {
		result = append(result, index)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (d *postgreDialect) GenerateInsertSql(tbl *conn.Table, row conn.Record) string {
	return d.GenerateInsertBatchSql(tbl, []conn.Record{row})
}

func (d *postgreDialect) GenerateInsertBatchSql(tbl *conn.Table, rows []conn.Record) string {
	if len(rows) == 0 {
		return ""
	}
	var colNames []string
	cols := tbl.GetColumnsByPosition()
	for _, col := range cols {
		colNames = append(colNames, ident.Quote(ident.DoubleQuote, col.Name))
	}
	valueRows := make([]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, 0, len(cols))
		for _, col := range cols {
			values = append(values, d.escapedValue(col.DataType, row[col.Name]))
		}
		valueRows = append(valueRows, "("+strings.Join(values, ", ")+")")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", postgresTableName(tbl), strings.Join(colNames, ", "), strings.Join(valueRows, ", "))
}

func (d *postgreDialect) GenerateDeleteSql(tbl *conn.Table, row conn.Record) string {
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

func (d *postgreDialect) GenerateDeleteBatchSql(tbl *conn.Table, rows []conn.Record) string {
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
				where = append(where, fmt.Sprintf("%s IS NULL", ident.Quote(ident.DoubleQuote, k)))
			} else {
				where = append(where, fmt.Sprintf("%s = %s", ident.Quote(ident.DoubleQuote, k), d.escapedValue(colType, val)))
			}
		}
		predicates = append(predicates, "("+strings.Join(where, " AND ")+")")
	}
	if len(predicates) == 1 {
		return fmt.Sprintf("DELETE FROM %s WHERE %s;", postgresTableName(tbl), strings.Trim(predicates[0], "()"))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", postgresTableName(tbl), strings.Join(predicates, " OR "))
}

func (d *postgreDialect) GenerateUpdateSql(tbl *conn.Table, row conn.Record, updateCols []string) string {
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
		set = append(set, fmt.Sprintf("%s = %s", ident.Quote(ident.DoubleQuote, c), d.escapedValue(col.DataType, val)))
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
			where = append(where, fmt.Sprintf("%s IS NULL", ident.Quote(ident.DoubleQuote, k)))
		} else {
			where = append(where, fmt.Sprintf("%s = %s", ident.Quote(ident.DoubleQuote, k), d.escapedValue(colType, val)))
		}
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", postgresTableName(tbl), strings.Join(set, ", "), strings.Join(where, " AND "))
}

func (d *postgreDialect) GenerateCreateIndexSql(t *conn.Table, idx *conn.Index) string {
	if idx.Primary {
		return ""
	}

	// 如果 idx.Expression 已经是完整的 CREATE INDEX 定义（如来自 pg_get_indexdef）
	if idx.Expression != nil && strings.HasPrefix(strings.TrimSpace(strings.ToUpper(*idx.Expression)), "CREATE ") {
		def := strings.TrimSpace(*idx.Expression)
		if !strings.HasSuffix(def, ";") {
			def += ";"
		}
		return def
	}

	// 如果既没有列定义，也没有表达式定义，绝对不能生成空括号索引，返回空以防语法错误
	if len(idx.Columns) == 0 && (idx.Expression == nil || *idx.Expression == "") {
		return ""
	}

	var ddl strings.Builder
	ddl.WriteString("CREATE ")
	if idx.Unique {
		ddl.WriteString("UNIQUE ")
	}

	ddl.WriteString("INDEX ")
	ddl.WriteString(ident.Quote(ident.DoubleQuote, idx.Name))
	ddl.WriteString(" ON ")
	ddl.WriteString(postgresTableName(t))

	if idx.Method != "" && idx.Method != "btree" {
		ddl.WriteString(fmt.Sprintf(" USING %s", idx.Method))
	}

	if idx.Expression != nil && *idx.Expression != "" {
		ddl.WriteString(fmt.Sprintf(" (%s)", *idx.Expression))
	} else if len(idx.Columns) > 0 {
		ddl.WriteString(" (")
		ddl.WriteString(ident.List(ident.DoubleQuote, idx.Columns, ", "))
		ddl.WriteString(")")
	}

	if idx.Where != nil && *idx.Where != "" {
		ddl.WriteString(fmt.Sprintf(" WHERE %s", *idx.Where))
	}

	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateDropIndexSql(t *conn.Table, idx *conn.Index) string {
	var ddl strings.Builder
	ddl.WriteString("DROP INDEX ")
	ddl.WriteString(ident.Qualified(ident.DoubleQuote, postgresSchema(t.Schema), idx.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateAddPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(postgresTableName(t))
	ddl.WriteString(fmt.Sprintf(" ADD CONSTRAINT %s PRIMARY KEY (%s);", ident.Quote(ident.DoubleQuote, pk.Name), ident.List(ident.DoubleQuote, pk.Columns, ", ")))
	return ddl.String()
}

func (d *postgreDialect) GenerateDropPrimaryKeySql(t *conn.Table, pk *conn.PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(postgresTableName(t))
	ddl.WriteString(fmt.Sprintf(" DROP CONSTRAINT %s;", ident.Quote(ident.DoubleQuote, pk.Name)))
	return ddl.String()
}

func (d *postgreDialect) GenerateDropTableSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP TABLE ")
	ddl.WriteString(postgresTableName(t))
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
	ddl.WriteString(postgresTableName(t))
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
			pkCols[i] = ident.Quote(ident.DoubleQuote, col)
		}
		constraintDef := fmt.Sprintf("  CONSTRAINT %s PRIMARY KEY (%s)",
			ident.Quote(ident.DoubleQuote, t.PrimaryKey.Name), strings.Join(pkCols, ", "))
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
			fkCols[i] = ident.Quote(ident.DoubleQuote, col)
		}
		refCols := make([]string, len(fk.ReferencedColumns))
		for i, col := range fk.ReferencedColumns {
			refCols[i] = ident.Quote(ident.DoubleQuote, col)
		}

		refSchema := fk.ReferencedSchema
		if refSchema == "" {
			refSchema = postgresSchema(t.Schema)
		}
		constraintDef := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			ident.Quote(ident.DoubleQuote, fk.Name), strings.Join(fkCols, ", "),
			ident.Qualified(ident.DoubleQuote, refSchema, fk.ReferencedTable),
			strings.Join(refCols, ", "))

		if fk.OnDelete != "" {
			constraintDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != "" {
			constraintDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}

		columnDefs = append(columnDefs, constraintDef)
	}

	// 添加 CHECK 约束。Schema generator 会为新表传入无外键副本（CHECK 只引用
	// 本表列，随建表内联创建）。
	checkNames := make([]string, 0, len(t.Checks))
	for name := range t.Checks {
		checkNames = append(checkNames, name)
	}
	sort.Strings(checkNames)
	for _, name := range checkNames {
		chk := t.Checks[name]
		definition := strings.TrimSpace(chk.Definition)
		if definition == "" {
			continue
		}
		columnDefs = append(columnDefs, fmt.Sprintf("  CONSTRAINT %s %s",
			ident.Quote(ident.DoubleQuote, chk.Name), definition))
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))
	ddl.WriteString("\n);")

	// 添加索引
	for _, idx := range sortedIndexes(t.Indexes) {
		if idx.Primary {
			continue // 主键索引已经在表定义中
		}
		if idxSql := d.GenerateCreateIndexSql(t, idx); idxSql != "" {
			ddl.WriteString("\n\n")
			ddl.WriteString(idxSql)
		}
	}

	// 添加列注释
	for _, col := range t.GetColumnsByPosition() {
		if col.Comment != nil {
			ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON COLUMN %s.%s IS '%s';",
				postgresTableName(t), ident.Quote(ident.DoubleQuote, col.Name), strings.ReplaceAll(*col.Comment, "'", "''")))
		}
	}

	// 添加表注释
	if t.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON TABLE %s IS '%s';",
			postgresTableName(t), strings.ReplaceAll(t.Comment, "'", "''")))
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
	ddl.WriteString(postgresTableName(t))
	ddl.WriteString(" AS\n")

	// 添加SELECT语句
	ddl.WriteString(t.ViewDefinition.SelectStatement)

	// 添加检查选项
	if t.ViewDefinition.CheckOption != "" && t.ViewDefinition.CheckOption != "NONE" {
		ddl.WriteString(fmt.Sprintf("\nWITH %s CHECK OPTION", t.ViewDefinition.CheckOption))
	}

	ddl.WriteString(";")

	// 添加注释：ViewDefinition.Comment 未被提取器填充时回退到表级 Comment
	// （ExtractView 经 obj_description 读取，视图与表共用该字段）——依赖闭包
	// 弹跳重建的视图不丢注释（C12）。
	comment := t.ViewDefinition.Comment
	if comment == "" {
		comment = t.Comment
	}
	if comment != "" {
		ddl.WriteString(fmt.Sprintf("\n\nCOMMENT ON VIEW %s IS '%s';",
			postgresTableName(t), strings.ReplaceAll(comment, "'", "''")))
	}

	return ddl.String()
}

func (d *postgreDialect) GenerateDropViewSql(t *conn.Table) string {
	var ddl strings.Builder
	ddl.WriteString("DROP VIEW ")
	ddl.WriteString(postgresTableName(t))
	// 依赖闭包外的遗漏引用（如跨 schema 视图）交由 CASCADE 兜底，
	// 闭包内的依赖已在生成层显式排序删除。
	ddl.WriteString(" CASCADE;")
	return ddl.String()
}

// GenerateCreateRoutineSql 输出 pg_get_functiondef 全文并补结尾分号。
// 定义变更经 CREATE OR REPLACE 重放；若签名或返回类型发生不兼容变化，
// PostgreSQL 会拒绝执行并显式报错，而不是静默破坏依赖。
func (d *postgreDialect) GenerateCreateRoutineSql(r *conn.Routine) string {
	if r == nil {
		return ""
	}
	def := strings.TrimSpace(r.Definition)
	if def == "" {
		return ""
	}
	if !strings.HasSuffix(def, ";") {
		def += ";"
	}
	return def
}

func (d *postgreDialect) GenerateDropRoutineSql(r *conn.Routine) string {
	if r == nil || r.Name == "" {
		return ""
	}
	kind := "FUNCTION"
	if r.Kind == conn.RoutineKindProcedure {
		kind = "PROCEDURE"
	}
	args := strings.TrimSpace(r.IdentityArgs)
	signature := ident.Quote(ident.DoubleQuote, r.Name) + "(" + args + ")"
	return fmt.Sprintf("DROP %s IF EXISTS %s.%s CASCADE;", kind,
		ident.Quote(ident.DoubleQuote, postgresSchema(r.Schema)), signature)
}

func (d *postgreDialect) GenerateCreateSequenceSql(s *conn.Sequence) string {
	if s == nil || s.Name == "" {
		return ""
	}
	var ddl strings.Builder
	ddl.WriteString("CREATE SEQUENCE ")
	ddl.WriteString(ident.Qualified(ident.DoubleQuote, postgresSchema(s.Schema), s.Name))
	if dataType := strings.TrimSpace(s.DataType); dataType != "" {
		ddl.WriteString(" AS ")
		ddl.WriteString(dataType)
	}
	if v := strings.TrimSpace(s.StartValue); v != "" {
		ddl.WriteString(" START WITH ")
		ddl.WriteString(v)
	}
	if v := strings.TrimSpace(s.IncrementBy); v != "" {
		ddl.WriteString(" INCREMENT BY ")
		ddl.WriteString(v)
	}
	if v := strings.TrimSpace(s.MinValue); v != "" {
		ddl.WriteString(" MINVALUE ")
		ddl.WriteString(v)
	}
	if v := strings.TrimSpace(s.MaxValue); v != "" {
		ddl.WriteString(" MAXVALUE ")
		ddl.WriteString(v)
	}
	ddl.WriteString(" CACHE ")
	ddl.WriteString(strings.TrimSpace(orDefault(s.CacheSize, "1")))
	if s.Cycle {
		ddl.WriteString(" CYCLE")
	} else {
		ddl.WriteString(" NO CYCLE")
	}
	ddl.WriteString(";")
	return ddl.String()
}

// GenerateAlterSequenceSql 把 old 的建序参数对齐到 new，仅生成发生变化的子句。
// 用 ALTER 而非 DROP+CREATE，避免破坏列默认值对序列的依赖。
func (d *postgreDialect) GenerateAlterSequenceSql(old, new *conn.Sequence) []string {
	if old == nil || new == nil || old.Name == "" || new.Name == "" {
		return nil
	}
	var clauses []string
	if strings.TrimSpace(old.DataType) != strings.TrimSpace(new.DataType) && new.DataType != "" {
		clauses = append(clauses, "AS "+new.DataType)
	}
	if old.StartValue != new.StartValue && new.StartValue != "" {
		clauses = append(clauses, "START WITH "+new.StartValue)
	}
	if old.IncrementBy != new.IncrementBy && new.IncrementBy != "" {
		clauses = append(clauses, "INCREMENT BY "+new.IncrementBy)
	}
	if old.MinValue != new.MinValue && new.MinValue != "" {
		clauses = append(clauses, "MINVALUE "+new.MinValue)
	}
	if old.MaxValue != new.MaxValue && new.MaxValue != "" {
		clauses = append(clauses, "MAXVALUE "+new.MaxValue)
	}
	if old.CacheSize != new.CacheSize && new.CacheSize != "" {
		clauses = append(clauses, "CACHE "+new.CacheSize)
	}
	if old.Cycle != new.Cycle {
		if new.Cycle {
			clauses = append(clauses, "CYCLE")
		} else {
			clauses = append(clauses, "NO CYCLE")
		}
	}
	if len(clauses) == 0 {
		return nil
	}
	qualified := ident.Qualified(ident.DoubleQuote, postgresSchema(new.Schema), new.Name)
	return []string{fmt.Sprintf("ALTER SEQUENCE %s %s;", qualified, strings.Join(clauses, " "))}
}

func (d *postgreDialect) GenerateDropSequenceSql(s *conn.Sequence) string {
	if s == nil || s.Name == "" {
		return ""
	}
	qualified := ident.Qualified(ident.DoubleQuote, postgresSchema(s.Schema), s.Name)
	// IF EXISTS：被删表携带的 SERIAL 隐式序列已随 DROP TABLE 消失，幂等兜底；
	// CASCADE：独立序列若残留依赖，显式级联而非中断整个迁移脚本。
	return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE;", qualified)
}

func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func (d *postgreDialect) GenerateAddColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", postgresTableName(t), d.converter.GenerateColumnDDL(col)))
	if col.Comment != nil && *col.Comment != "" {
		ddl.WriteString(fmt.Sprintf("\nCOMMENT ON COLUMN %s.%s IS '%s';",
			postgresTableName(t), ident.Quote(ident.DoubleQuote, col.Name), strings.ReplaceAll(*col.Comment, "'", "''")))
	}
	return ddl.String()
}

func (d *postgreDialect) GenerateDropColumnSql(t *conn.Table, col *conn.Column) string {
	var ddl strings.Builder
	ddl.WriteString("ALTER TABLE ")
	ddl.WriteString(postgresTableName(t))
	ddl.WriteString(" DROP COLUMN ")
	ddl.WriteString(ident.Quote(ident.DoubleQuote, col.Name))
	ddl.WriteString(";")
	return ddl.String()
}

func (d *postgreDialect) GenerateAlterColumnSql(t *conn.Table, oldCol, newCol *conn.Column) string {
	prefix := fmt.Sprintf("ALTER TABLE %s", postgresTableName(t))
	var stmts []string

	// 修改字段名
	if oldCol.Name != newCol.Name {
		stmts = append(stmts, fmt.Sprintf("%s RENAME COLUMN %s TO %s;", prefix, ident.Quote(ident.DoubleQuote, oldCol.Name), ident.Quote(ident.DoubleQuote, newCol.Name)))
	}
	// 修改字段类型
	oldDataType := d.converter.ConvertType(oldCol)
	newDataType := d.converter.ConvertType(newCol)
	if oldDataType != newDataType {
		suffix := fmt.Sprintf("USING %s::%s", ident.Quote(ident.DoubleQuote, newCol.Name), newDataType)
		stmts = append(stmts, fmt.Sprintf("%s ALTER COLUMN %s TYPE %s %s;", prefix, ident.Quote(ident.DoubleQuote, newCol.Name), newDataType, suffix))
	}
	// 修改默认值
	if newCol.Default != nil && (oldCol.Default == nil || *newCol.Default != *oldCol.Default) {
		stmts = append(stmts, fmt.Sprintf("%s ALTER COLUMN %s SET DEFAULT %s;", prefix, ident.Quote(ident.DoubleQuote, newCol.Name), *newCol.Default))
	} else if newCol.Default == nil && oldCol.Default != nil {
		stmts = append(stmts, fmt.Sprintf("%s ALTER COLUMN %s DROP DEFAULT;", prefix, ident.Quote(ident.DoubleQuote, newCol.Name)))
	}
	// 修改为空状态
	if oldCol.Nullable != newCol.Nullable {
		if newCol.Nullable {
			stmts = append(stmts, fmt.Sprintf("%s ALTER COLUMN %s DROP NOT NULL;", prefix, ident.Quote(ident.DoubleQuote, newCol.Name)))
		} else {
			stmts = append(stmts, fmt.Sprintf("%s ALTER COLUMN %s SET NOT NULL;", prefix, ident.Quote(ident.DoubleQuote, newCol.Name)))
		}
	}
	// 修改注释
	if newCol.Comment != nil && (oldCol.Comment == nil || *newCol.Comment != *oldCol.Comment) {
		stmts = append(stmts, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", postgresTableName(t), ident.Quote(ident.DoubleQuote, newCol.Name), strings.ReplaceAll(*newCol.Comment, "'", "''")))
	} else if newCol.Comment == nil && oldCol.Comment != nil {
		stmts = append(stmts, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS NULL;", postgresTableName(t), ident.Quote(ident.DoubleQuote, newCol.Name)))
	}

	return strings.Join(stmts, "\n")
}

func (d *postgreDialect) GenerateAddForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string {
	var ddl strings.Builder
	schema := postgresSchema(t.Schema)
	ddl.WriteString(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (", postgresTableName(t), ident.Quote(ident.DoubleQuote, fk.Name)))
	ddl.WriteString(ident.List(ident.DoubleQuote, fk.Columns, ", "))
	ddl.WriteString(") REFERENCES ")
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schema
	}
	ddl.WriteString(fmt.Sprintf("%s (", ident.Qualified(ident.DoubleQuote, refSchema, fk.ReferencedTable)))
	ddl.WriteString(ident.List(ident.DoubleQuote, fk.ReferencedColumns, ", "))
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

func (d *postgreDialect) GenerateDropForeignKeySql(t *conn.Table, fk *conn.ForeignKey) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", postgresTableName(t), ident.Quote(ident.DoubleQuote, fk.Name))
}

// GenerateAddCheckConstraintSql 直接拼接 pg_get_constraintdef 输出的
// "CHECK (expr)" 全文（未验证约束含 NOT VALID）；空定义放弃生成，避免语法损坏。
func (d *postgreDialect) GenerateAddCheckConstraintSql(t *conn.Table, c *conn.CheckConstraint) string {
	if c == nil || c.Name == "" {
		return ""
	}
	definition := strings.TrimSpace(c.Definition)
	if definition == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;",
		postgresTableName(t), ident.Quote(ident.DoubleQuote, c.Name), definition)
}

func (d *postgreDialect) GenerateDropCheckConstraintSql(t *conn.Table, c *conn.CheckConstraint) string {
	if c == nil || c.Name == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;",
		postgresTableName(t), ident.Quote(ident.DoubleQuote, c.Name))
}

// GenerateAlterViewCommentSql 生成视图注释语句（C12）。空注释同样输出
// IS ”，与表注释路径口径一致：提取层把 NULL 与空串同等归一为 ""。
func (d *postgreDialect) GenerateAlterViewCommentSql(t *conn.Table, comment string) string {
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf("COMMENT ON VIEW %s IS '%s';", postgresTableName(t), escaped)
}

func (d *postgreDialect) GenerateAlterTableCommentSql(t *conn.Table, comment string) string {
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf("COMMENT ON TABLE %s IS '%s';", postgresTableName(t), escaped)
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
