package diff

import (
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
)

func CompareSchemasWithAdapter(src, tgt conn.DBAdapter) (*SchemaDiff, error) {
	srcSchema, err := src.ReadSchema()
	if err != nil {
		return nil, err
	}
	tgtSchema, err := tgt.ReadSchema()
	if err != nil {
		return nil, err
	}
	return CompareSchemas(srcSchema, tgtSchema), nil
}

func CompareSchemas(src, tgt *conn.DatabaseSchema) *SchemaDiff {
	diff := &SchemaDiff{}
	// 表级
	srcTables := src.Tables
	tgtTables := tgt.Tables
	srcTableSet := map[string]struct{}{}
	tgtTableSet := map[string]struct{}{}
	for name := range srcTables {
		srcTableSet[name] = struct{}{}
	}
	for name := range tgtTables {
		tgtTableSet[name] = struct{}{}
	}
	// 新增表（Target 存在，Source 不存在）
	for name, tbl := range tgtTables {
		if _, ok := srcTables[name]; !ok {
			diff.TablesAdded = append(diff.TablesAdded, tbl)
		}
	}
	// 删除表（Source 存在，Target 不存在）
	for name, tbl := range srcTables {
		if _, ok := tgtTables[name]; !ok {
			diff.TablesDropped = append(diff.TablesDropped, tbl)
		}
	}
	// 修改表
	for name, srcTbl := range srcTables {
		tgtTbl, ok := tgtTables[name]
		if !ok {
			continue
		}
		tblDiff := compareTable(srcTbl, tgtTbl)
		if tblDiff != nil {
			diff.TablesModified = append(diff.TablesModified, tblDiff)
		}
	}
	return diff
}

func compareTable(src, tgt *conn.Table) *TableDiff {
	if src.Type != tgt.Type {
		return nil
	}
	if src.Type == conn.TableTypeView {
		if src.ViewDefinition == nil || tgt.ViewDefinition == nil {
			return nil
		}
		if !equalViewDefinition(src.ViewDefinition, tgt.ViewDefinition) {
			return &TableDiff{
				SourceTable: src,
				TargetTable: tgt,
				Table:       tgt,
				ViewDefinitionChange: &ViewDefinitionDiff{
					Old: src.ViewDefinition,
					New: tgt.ViewDefinition,
				},
			}
		}
		return nil
	}
	d := &TableDiff{
		SourceTable: src,
		TargetTable: tgt,
		Table:       tgt,
	}
	// 列
	srcCols := src.Columns
	tgtCols := tgt.Columns
	// 新增列（Target 存在，Source 不存在）
	for name, col := range tgtCols {
		if _, ok := srcCols[name]; !ok {
			d.ColumnsAdded = append(d.ColumnsAdded, col)
		}
	}
	// 删除列（Source 存在，Target 不存在）
	for name, col := range srcCols {
		if _, ok := tgtCols[name]; !ok {
			d.ColumnsDropped = append(d.ColumnsDropped, col)
		}
	}
	// 修改列
	for name, srcCol := range srcCols {
		tgtCol, ok := tgtCols[name]
		if ok && !equalColumn(srcCol, tgtCol) {
			d.ColumnsModified = append(d.ColumnsModified, &ColumnDiff{Old: srcCol, New: tgtCol})
		}
	}
	// 索引
	srcIdx := src.Indexes
	tgtIdx := tgt.Indexes
	// 新增索引（Target 存在，Source 不存在）
	for name, idx := range tgtIdx {
		if _, ok := srcIdx[name]; !ok {
			d.IndexesAdded = append(d.IndexesAdded, idx)
		}
	}
	// 删除索引（Source 存在，Target 不存在）
	for name, idx := range srcIdx {
		if _, ok := tgtIdx[name]; !ok {
			d.IndexesDropped = append(d.IndexesDropped, idx)
		}
	}
	// 修改索引
	for name, srcI := range srcIdx {
		tgtI, ok := tgtIdx[name]
		if ok && !equalIndex(srcI, tgtI) {
			d.IndexesModified = append(d.IndexesModified, &IndexDiff{Old: srcI, New: tgtI})
		}
	}
	// 主键
	if !equalPrimaryKey(src.PrimaryKey, tgt.PrimaryKey) {
		d.PrimaryKeyChange = &PrimaryKeyDiff{Old: src.PrimaryKey, New: tgt.PrimaryKey}
	}
	// 外键
	srcFK := src.ForeignKeys
	tgtFK := tgt.ForeignKeys
	// 新增外键（Target 存在，Source 不存在）
	for name, fk := range tgtFK {
		if _, ok := srcFK[name]; !ok {
			d.ForeignKeysAdded = append(d.ForeignKeysAdded, fk)
		}
	}
	// 删除外键（Source 存在，Target 不存在）
	for name, fk := range srcFK {
		if _, ok := tgtFK[name]; !ok {
			d.ForeignKeysDropped = append(d.ForeignKeysDropped, fk)
		}
	}
	// 修改外键
	for name, srcF := range srcFK {
		tgtF, ok := tgtFK[name]
		if ok && !equalForeignKey(srcF, tgtF) {
			d.ForeignKeysModified = append(d.ForeignKeysModified, &ForeignKeyDiff{Old: srcF, New: tgtF})
		}
	}
	// 表注释
	if !equalTableComment(src.Comment, tgt.Comment) {
		d.CommentChange = &CommentDiff{Old: src.Comment, New: tgt.Comment}
	}

	if len(d.ColumnsAdded)+len(d.ColumnsDropped)+len(d.ColumnsModified)+
		len(d.IndexesAdded)+len(d.IndexesDropped)+len(d.IndexesModified)+
		len(d.ForeignKeysAdded)+len(d.ForeignKeysDropped)+len(d.ForeignKeysModified) > 0 ||
		d.PrimaryKeyChange != nil || d.CommentChange != nil {
		return d
	}
	return nil
}

var typeAliases = map[string]string{
	"int4":                        "integer",
	"int":                         "integer",
	"integer":                     "integer",
	"int8":                        "bigint",
	"bigint":                      "bigint",
	"int2":                        "smallint",
	"smallint":                    "smallint",
	"bool":                        "boolean",
	"boolean":                     "boolean",
	"float8":                      "double precision",
	"double precision":            "double precision",
	"float4":                      "real",
	"real":                        "real",
	"varchar":                     "varchar",
	"character varying":           "varchar",
	"char":                        "char",
	"character":                   "char",
	"timestamp without time zone": "timestamp",
	"timestamp":                   "timestamp",
	"timestamp with time zone":    "timestamptz",
	"timestamptz":                 "timestamptz",
	"time without time zone":      "time",
	"time":                        "time",
	"time with time zone":         "timetz",
	"timetz":                      "timetz",
	"decimal":                     "numeric",
	"numeric":                     "numeric",
	"varbit":                      "bit varying",
	"bit varying":                 "bit varying",
}

func normalizeDataType(dt string) string {
	lower := strings.ToLower(strings.TrimSpace(dt))
	if standard, ok := typeAliases[lower]; ok {
		return standard
	}
	return lower
}

func normalizeDefault(d *string) string {
	if d == nil {
		return ""
	}
	val := strings.TrimSpace(*d)
	if val == "" {
		return ""
	}
	// 去除外层括号，如 ('value'::text) 或 (0)
	for strings.HasPrefix(val, "(") && strings.HasSuffix(val, ")") {
		val = strings.TrimSpace(val[1 : len(val)-1])
	}
	// 去除 PG 的 ::type 类型强转后缀
	if idx := strings.Index(val, "::"); idx != -1 {
		val = strings.TrimSpace(val[:idx])
	}
	// 去除外层括号
	for strings.HasPrefix(val, "(") && strings.HasSuffix(val, ")") {
		val = strings.TrimSpace(val[1 : len(val)-1])
	}
	// 去除首尾单引号
	if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") && len(val) >= 2 {
		val = val[1 : len(val)-1]
	}
	lower := strings.ToLower(val)
	if lower == "now()" || lower == "current_timestamp" || lower == "current_timestamp()" {
		return "current_timestamp"
	}
	if lower == "null" {
		return ""
	}
	return val
}

func equalColumn(a, b *conn.Column) bool {
	if a == nil || b == nil {
		return a == b
	}
	// 比较基本字段
	if a.Name != b.Name {
		return false
	}
	if normalizeDataType(a.DataType) != normalizeDataType(b.DataType) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if a.Extra != b.Extra {
		return false
	}
	// 比较 Default 归一化值
	if normalizeDefault(a.Default) != normalizeDefault(b.Default) {
		return false
	}
	// 比较 Comment
	if !equalComment(a.Comment, b.Comment) {
		return false
	}
	// text 等无限长类型忽略 CharMaxLen 差异
	normType := normalizeDataType(a.DataType)
	if normType != "text" && !strings.Contains(normType, "text") {
		if a.CharMaxLen == nil && b.CharMaxLen == nil {
			// 相等
		} else if a.CharMaxLen == nil || b.CharMaxLen == nil {
			return false
		} else if *a.CharMaxLen != *b.CharMaxLen {
			return false
		}
	}
	// 比较 NumericPrec
	if a.NumericPrec == nil && b.NumericPrec == nil {
		// 相等
	} else if a.NumericPrec == nil || b.NumericPrec == nil {
		return false
	} else if *a.NumericPrec != *b.NumericPrec {
		return false
	}
	// 比较 NumericScale
	if a.NumericScale == nil && b.NumericScale == nil {
		// 相等
	} else if a.NumericScale == nil || b.NumericScale == nil {
		return false
	} else if *a.NumericScale != *b.NumericScale {
		return false
	}
	return true
}

func equalIndex(a, b *conn.Index) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Name != b.Name || a.Unique != b.Unique || a.Primary != b.Primary || a.Method != b.Method {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	if a.Where == nil && b.Where == nil {
		// 相等
	} else if a.Where == nil || b.Where == nil {
		return false
	} else if *a.Where != *b.Where {
		return false
	}
	if a.Expression == nil && b.Expression == nil {
		// 相等
	} else if a.Expression == nil || b.Expression == nil {
		return false
	} else if *a.Expression != *b.Expression {
		return false
	}
	return true
}

func equalPrimaryKey(a, b *conn.PrimaryKey) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// 主键不以约束名称是否一致判断变更，仅比对列及其顺序
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	return true
}

func equalForeignKey(a, b *conn.ForeignKey) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Name != b.Name || a.ReferencedSchema != b.ReferencedSchema || a.ReferencedTable != b.ReferencedTable ||
		a.OnDelete != b.OnDelete || a.OnUpdate != b.OnUpdate {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	if len(a.ReferencedColumns) != len(b.ReferencedColumns) {
		return false
	}
	for i := range a.ReferencedColumns {
		if a.ReferencedColumns[i] != b.ReferencedColumns[i] {
			return false
		}
	}
	return true
}

func equalComment(a, b *string) bool {
	if (a == nil || *a == "") && (b == nil || *b == "") {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	sa := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(*a, "\n", ""), "\r", ""))
	sb := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(*b, "\n", ""), "\r", ""))
	return sa == sb
}

func equalTableComment(a, b string) bool {
	sa := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(a, "\n", ""), "\r", ""))
	sb := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(b, "\n", ""), "\r", ""))
	return sa == sb
}

func equalViewDefinition(a, b *conn.ViewDefinition) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	clean := func(s string) string {
		s = strings.TrimSpace(s)
		s = strings.TrimSuffix(s, ";")
		s = strings.ReplaceAll(s, "\n", " ")
		s = strings.ReplaceAll(s, "\r", " ")
		for strings.Contains(s, "  ") {
			s = strings.ReplaceAll(s, "  ", " ")
		}
		return strings.TrimSpace(s)
	}
	return clean(a.SelectStatement) == clean(b.SelectStatement)
}
