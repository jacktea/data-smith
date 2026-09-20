package diff

import (
	"sort"
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
	srcTables := src.Tables
	tgtTables := tgt.Tables
	names := make([]string, 0, len(srcTables)+len(tgtTables))
	seen := make(map[string]struct{}, len(srcTables)+len(tgtTables))
	for name := range srcTables {
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for name := range tgtTables {
		if _, ok := seen[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		srcTbl, srcOK := srcTables[name]
		tgtTbl, tgtOK := tgtTables[name]
		switch {
		case !srcOK:
			diff.TablesAdded = append(diff.TablesAdded, tgtTbl)
		case !tgtOK:
			diff.TablesDropped = append(diff.TablesDropped, srcTbl)
		case srcTbl.Type != tgtTbl.Type:
			// Object type transitions cannot be expressed as ALTER. Treat them as
			// an explicit drop followed by a create of the target object.
			diff.TablesDropped = append(diff.TablesDropped, srcTbl)
			diff.TablesAdded = append(diff.TablesAdded, tgtTbl)
		default:
			tblDiff := compareTable(srcTbl, tgtTbl)
			if tblDiff != nil {
				diff.TablesModified = append(diff.TablesModified, tblDiff)
			}
		}
	}
	diff.RoutinesAdded, diff.RoutinesDropped, diff.RoutinesModified = compareRoutines(src.Routines, tgt.Routines)
	diff.SequencesAdded, diff.SequencesDropped, diff.SequencesModified = compareSequences(src.Sequences, tgt.Sequences)
	diff.ViewsAffected = collectAffectedViews(src, tgt, diff)
	return diff
}

func compareRoutines(src, tgt map[string]*conn.Routine) (added, dropped []*conn.Routine, modified []*RoutineDiff) {
	for _, name := range sortedKeys(src, tgt) {
		srcRoutine, srcOK := src[name]
		tgtRoutine, tgtOK := tgt[name]
		switch {
		case !srcOK:
			added = append(added, tgtRoutine)
		case !tgtOK:
			dropped = append(dropped, srcRoutine)
		case !equalRoutineDefinition(srcRoutine, tgtRoutine):
			modified = append(modified, &RoutineDiff{Old: srcRoutine, New: tgtRoutine})
		}
	}
	return added, dropped, modified
}

func compareSequences(src, tgt map[string]*conn.Sequence) (added, dropped []*conn.Sequence, modified []*SequenceDiff) {
	for _, name := range sortedKeys(src, tgt) {
		srcSeq, srcOK := src[name]
		tgtSeq, tgtOK := tgt[name]
		switch {
		case !srcOK:
			added = append(added, tgtSeq)
		case !tgtOK:
			dropped = append(dropped, srcSeq)
		case !srcSeq.Equal(tgtSeq):
			modified = append(modified, &SequenceDiff{Old: srcSeq, New: tgtSeq})
		}
	}
	return added, dropped, modified
}

func sortedKeys[T any](maps ...map[string]T) []string {
	seen := make(map[string]struct{})
	names := make([]string, 0)
	for _, values := range maps {
		for name := range values {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func equalRoutineDefinition(a, b *conn.Routine) bool {
	if a == nil || b == nil {
		return a == b
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
	return clean(a.Definition) == clean(b.Definition)
}

// collectAffectedViews 计算需要「先删后建」弹跳的视图依赖闭包：所有（传递）
// 依赖了被变更对象、且自身定义未变化的双侧视图。被变更对象包括被删除表/视图
// 与全部被修改表/视图（列类型变更会令 PostgreSQL 以 cannot alter type of a
// column used by a view 拒绝执行）。已在增/删/改集合中的视图不重复纳入。
// 返回值取新态侧（tgt）视图模型，供重建使用。
func collectAffectedViews(src, tgt *conn.DatabaseSchema, d *SchemaDiff) []*conn.Table {
	breakers := make(map[string]struct{})
	register := func(schema, name string) {
		breakers[qualifiedName(schema, name)] = struct{}{}
		breakers[name] = struct{}{}
	}
	for _, tbl := range d.TablesDropped {
		register(tbl.Schema, tbl.Name)
	}
	for _, tblDiff := range d.TablesModified {
		tbl := tblDiff.SourceTable
		if tbl == nil {
			tbl = tblDiff.Table
		}
		if tbl != nil {
			register(tbl.Schema, tbl.Name)
		}
	}
	if len(breakers) == 0 {
		return nil
	}

	// dependents 把被引用对象（含简名别名）映射到引用它的双侧视图。
	dependents := make(map[string]map[string]struct{})
	for name, view := range tgt.Tables {
		srcView, ok := src.Tables[name]
		if !ok || view.Type != conn.TableTypeView || srcView.Type != conn.TableTypeView {
			continue
		}
		deps := view.ViewDefinition
		if deps == nil {
			deps = srcView.ViewDefinition
		}
		if deps == nil {
			continue
		}
		for _, dependency := range deps.Dependencies {
			resolved := resolveQualifiedName(dependency, view.Schema)
			for _, key := range resolved {
				if dependents[key] == nil {
					dependents[key] = make(map[string]struct{})
				}
				dependents[key][name] = struct{}{}
			}
		}
	}

	affected := make(map[string]struct{})
	queue := make([]string, 0, len(breakers))
	for breaker := range breakers {
		queue = append(queue, breaker)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for dependent := range dependents[current] {
			if _, seen := affected[dependent]; seen {
				continue
			}
			affected[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	handled := make(map[string]struct{})
	for _, tbl := range d.TablesAdded {
		handled[tbl.Name] = struct{}{}
	}
	for _, tbl := range d.TablesDropped {
		handled[tbl.Name] = struct{}{}
	}
	for _, tblDiff := range d.TablesModified {
		tbl := tblDiff.SourceTable
		if tbl == nil {
			tbl = tblDiff.Table
		}
		if tbl != nil {
			handled[tbl.Name] = struct{}{}
		}
	}

	names := make([]string, 0, len(affected))
	for name := range affected {
		if _, skip := handled[name]; skip {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]*conn.Table, 0, len(names))
	for _, name := range names {
		if view := tgt.Tables[name]; view != nil {
			result = append(result, view)
		}
	}
	return result
}

func qualifiedName(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// resolveQualifiedName 把依赖串（来自 view_table_usage 的 schema.name）解析为
// 全限定与简名两种键，供闭包图匹配。
func resolveQualifiedName(dependency, schema string) []string {
	dependency = strings.TrimSpace(dependency)
	if dependency == "" {
		return nil
	}
	if idx := strings.LastIndex(dependency, "."); idx >= 0 {
		return []string{dependency, dependency[idx+1:]}
	}
	return []string{dependency, qualifiedName(schema, dependency)}
}

func compareTable(src, tgt *conn.Table) *TableDiff {
	if src.Type == conn.TableTypeView {
		if src.ViewDefinition == nil || tgt.ViewDefinition == nil {
			return nil
		}
		// 视图注释（C12）：定义相等后追加比较，仅注释差异生成 COMMENT 语句
		// 而非整组删建。MySQL 视图无注释（提取为空），比较自然不触发。
		if !equalTableComment(src.Comment, tgt.Comment) {
			return &TableDiff{
				SourceTable: src,
				TargetTable: tgt,
				Table:       tgt,
				CommentChange: &CommentDiff{
					Old: src.Comment,
					New: tgt.Comment,
				},
			}
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
	for _, name := range sortedKeys(tgtCols) {
		col := tgtCols[name]
		if _, ok := srcCols[name]; !ok {
			d.ColumnsAdded = append(d.ColumnsAdded, col)
		}
	}
	// 删除列（Source 存在，Target 不存在）
	for _, name := range sortedKeys(srcCols) {
		col := srcCols[name]
		if _, ok := tgtCols[name]; !ok {
			d.ColumnsDropped = append(d.ColumnsDropped, col)
		}
	}
	// 修改列
	for _, name := range sortedKeys(srcCols) {
		srcCol := srcCols[name]
		tgtCol, ok := tgtCols[name]
		if ok && !equalColumn(srcCol, tgtCol) {
			d.ColumnsModified = append(d.ColumnsModified, &ColumnDiff{Old: srcCol, New: tgtCol})
		}
	}
	// 索引（C13）：主键背书索引不建模为索引差异——其生命周期唯一跟随主键
	// 约束，生成层对增/删/改三条路径均跳过 Primary 索引（F1 语义），diff 层
	// 同步排除，消除「汇总计入修改、产物零语句」的口径不自洽；主键列集差异
	// 仍由下方 PrimaryKeyChange 检出，非背书索引的名称判异不受影响。
	srcIdx := src.Indexes
	tgtIdx := tgt.Indexes
	// 新增索引（Target 存在，Source 不存在）
	for _, name := range sortedKeys(tgtIdx) {
		idx := tgtIdx[name]
		if idx.Primary {
			continue
		}
		if _, ok := srcIdx[name]; !ok {
			d.IndexesAdded = append(d.IndexesAdded, idx)
		}
	}
	// 删除索引（Source 存在，Target 不存在）
	for _, name := range sortedKeys(srcIdx) {
		idx := srcIdx[name]
		if idx.Primary {
			continue
		}
		if _, ok := tgtIdx[name]; !ok {
			d.IndexesDropped = append(d.IndexesDropped, idx)
		}
	}
	// 修改索引
	for _, name := range sortedKeys(srcIdx) {
		srcI := srcIdx[name]
		if srcI.Primary {
			continue
		}
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
	for _, name := range sortedKeys(tgtFK) {
		fk := tgtFK[name]
		if _, ok := srcFK[name]; !ok {
			d.ForeignKeysAdded = append(d.ForeignKeysAdded, fk)
		}
	}
	// 删除外键（Source 存在，Target 不存在）
	for _, name := range sortedKeys(srcFK) {
		fk := srcFK[name]
		if _, ok := tgtFK[name]; !ok {
			d.ForeignKeysDropped = append(d.ForeignKeysDropped, fk)
		}
	}
	// 修改外键
	for _, name := range sortedKeys(srcFK) {
		srcF := srcFK[name]
		tgtF, ok := tgtFK[name]
		if ok && !equalForeignKey(srcF, tgtF) {
			d.ForeignKeysModified = append(d.ForeignKeysModified, &ForeignKeyDiff{Old: srcF, New: tgtF})
		}
	}
	// CHECK 约束（C11）
	srcChecks := src.Checks
	tgtChecks := tgt.Checks
	// 新增约束（Target 存在，Source 不存在）
	for _, name := range sortedKeys(tgtChecks) {
		chk := tgtChecks[name]
		if _, ok := srcChecks[name]; !ok {
			d.ChecksAdded = append(d.ChecksAdded, chk)
		}
	}
	// 删除约束（Source 存在，Target 不存在）
	for _, name := range sortedKeys(srcChecks) {
		chk := srcChecks[name]
		if _, ok := tgtChecks[name]; !ok {
			d.ChecksDropped = append(d.ChecksDropped, chk)
		}
	}
	// 修改约束（同名不同义）
	for _, name := range sortedKeys(srcChecks) {
		srcChk := srcChecks[name]
		tgtChk, ok := tgtChecks[name]
		if ok && !equalCheckConstraint(srcChk, tgtChk) {
			d.ChecksModified = append(d.ChecksModified, &CheckConstraintDiff{Old: srcChk, New: tgtChk})
		}
	}
	// 表注释
	if !equalTableComment(src.Comment, tgt.Comment) {
		d.CommentChange = &CommentDiff{Old: src.Comment, New: tgt.Comment}
	}

	if len(d.ColumnsAdded)+len(d.ColumnsDropped)+len(d.ColumnsModified)+
		len(d.IndexesAdded)+len(d.IndexesDropped)+len(d.IndexesModified)+
		len(d.ForeignKeysAdded)+len(d.ForeignKeysDropped)+len(d.ForeignKeysModified)+
		len(d.ChecksAdded)+len(d.ChecksDropped)+len(d.ChecksModified) > 0 ||
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

// equalCheckConstraint 比较同名 CHECK 约束的定义。规范化仅做空白压缩与
// 行尾清理（对齐 equalViewDefinition 的 clean 口径），不做表达式语义改写：
// 同一方言内 pg_get_constraintdef / check_clause 对同一约束的输出是确定的。
func equalCheckConstraint(a, b *conn.CheckConstraint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return normalizeWhitespace(a.Definition) == normalizeWhitespace(b.Definition)
}

// normalizeWhitespace 压缩空白为单空格并去掉首尾空白与结尾分号。
func normalizeWhitespace(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ";")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return strings.TrimSpace(s)
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
