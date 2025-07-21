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
	// 新增表
	for name, tbl := range srcTables {
		if _, ok := tgtTables[name]; !ok {
			diff.TablesAdded = append(diff.TablesAdded, tbl)
		}
	}
	// 删除表
	for name, tbl := range tgtTables {
		if _, ok := srcTables[name]; !ok {
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
		if src.ViewDefinition.SelectStatement != tgt.ViewDefinition.SelectStatement {
			return &TableDiff{
				Table: tgt,
				ViewDefinitionChange: &ViewDefinitionDiff{
					Old: tgt.ViewDefinition,
					New: src.ViewDefinition,
				},
			}
		}
		return nil
	}
	d := &TableDiff{Table: tgt}
	// 列
	srcCols := src.Columns
	tgtCols := tgt.Columns
	for name, col := range srcCols {
		if _, ok := tgtCols[name]; !ok {
			d.ColumnsAdded = append(d.ColumnsAdded, col)
		}
	}
	for name, col := range tgtCols {
		if _, ok := srcCols[name]; !ok {
			d.ColumnsDropped = append(d.ColumnsDropped, col)
		}
	}
	for name, srcCol := range srcCols {
		tgtCol, ok := tgtCols[name]
		if ok && !equalColumn(srcCol, tgtCol) {
			d.ColumnsModified = append(d.ColumnsModified, &ColumnDiff{Old: tgtCol, New: srcCol})
		}
	}
	// 索引
	srcIdx := src.Indexes
	tgtIdx := tgt.Indexes
	for name, idx := range srcIdx {
		if _, ok := tgtIdx[name]; !ok {
			d.IndexesAdded = append(d.IndexesAdded, idx)
		}
	}
	for name, idx := range tgtIdx {
		if _, ok := srcIdx[name]; !ok {
			d.IndexesDropped = append(d.IndexesDropped, idx)
		}
	}
	for name, srcI := range srcIdx {
		tgtI, ok := tgtIdx[name]
		if ok && !equalIndex(srcI, tgtI) {
			d.IndexesModified = append(d.IndexesModified, &IndexDiff{Old: tgtI, New: srcI})
		}
	}
	// 主键
	if !equalPrimaryKey(src.PrimaryKey, tgt.PrimaryKey) {
		d.PrimaryKeyChange = &PrimaryKeyDiff{Old: tgt.PrimaryKey, New: src.PrimaryKey}
	}
	// 外键
	srcFK := src.ForeignKeys
	tgtFK := tgt.ForeignKeys
	for name, fk := range srcFK {
		if _, ok := tgtFK[name]; !ok {
			d.ForeignKeysAdded = append(d.ForeignKeysAdded, fk)
		}
	}
	for name, fk := range tgtFK {
		if _, ok := srcFK[name]; !ok {
			d.ForeignKeysDropped = append(d.ForeignKeysDropped, fk)
		}
	}
	for name, srcF := range srcFK {
		tgtF, ok := tgtFK[name]
		if ok && !equalForeignKey(srcF, tgtF) {
			d.ForeignKeysModified = append(d.ForeignKeysModified, &ForeignKeyDiff{Old: tgtF, New: srcF})
		}
	}
	if len(d.ColumnsAdded)+len(d.ColumnsDropped)+len(d.ColumnsModified)+len(d.IndexesAdded)+len(d.IndexesDropped)+len(d.IndexesModified)+len(d.ForeignKeysAdded)+len(d.ForeignKeysDropped)+len(d.ForeignKeysModified) > 0 || d.PrimaryKeyChange != nil {
		return d
	}
	return nil
}

func equalColumn(a, b *conn.Column) bool {
	if a == nil || b == nil {
		return a == b
	}
	// 比较基本字段
	if a.Name != b.Name || a.DataType != b.DataType || a.Nullable != b.Nullable || a.Extra != b.Extra {
		return false
	}
	// 比较Default值
	if a.Default == nil && b.Default == nil {
		// 都为nil，相等
	} else if a.Default == nil || b.Default == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.Default != *b.Default {
		// 都不为nil，但值不相等
		return false
	}
	// 比较Comment
	if !equalComment(a.Comment, b.Comment) {
		return false
	}
	// 比较CharMaxLen
	if a.CharMaxLen == nil && b.CharMaxLen == nil {
		// 都为nil，相等
	} else if a.CharMaxLen == nil || b.CharMaxLen == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.CharMaxLen != *b.CharMaxLen {
		// 都不为nil，但值不相等
		return false
	}
	// 比较NumericPrec
	if a.NumericPrec == nil && b.NumericPrec == nil {
		// 都为nil，相等
	} else if a.NumericPrec == nil || b.NumericPrec == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.NumericPrec != *b.NumericPrec {
		// 都不为nil，但值不相等
		return false
	}
	// 比较NumericScale
	if a.NumericScale == nil && b.NumericScale == nil {
		// 都为nil，相等
	} else if a.NumericScale == nil || b.NumericScale == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.NumericScale != *b.NumericScale {
		// 都不为nil，但值不相等
		return false
	}
	return true
}

func equalIndex(a, b *conn.Index) bool {
	if a == nil || b == nil {
		return a == b
	}
	// 比较基本字段
	if a.Name != b.Name || a.Unique != b.Unique || a.Primary != b.Primary || a.Method != b.Method {
		return false
	}
	// 比较Columns数组
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	// 比较Where条件
	if a.Where == nil && b.Where == nil {
		// 都为nil，相等
	} else if a.Where == nil || b.Where == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.Where != *b.Where {
		// 都不为nil，但值不相等
		return false
	}
	// 比较Expression
	if a.Expression == nil && b.Expression == nil {
		// 都为nil，相等
	} else if a.Expression == nil || b.Expression == nil {
		// 一个为nil，一个不为nil，不相等
		return false
	} else if *a.Expression != *b.Expression {
		// 都不为nil，但值不相等
		return false
	}
	return true
}

func equalPrimaryKey(a, b *conn.PrimaryKey) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Name != b.Name {
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
	return true
}

func equalForeignKey(a, b *conn.ForeignKey) bool {
	if a == nil || b == nil {
		return a == b
	}
	// 比较基本字段
	if a.Name != b.Name || a.ReferencedSchema != b.ReferencedSchema || a.ReferencedTable != b.ReferencedTable || a.OnDelete != b.OnDelete || a.OnUpdate != b.OnUpdate {
		return false
	}
	// 比较Columns数组
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	// 比较ReferencedColumns数组
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
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	sa := strings.ReplaceAll(*a, "\n", "")
	sa = strings.ReplaceAll(sa, "\r", "")
	sb := strings.ReplaceAll(*b, "\n", "")
	sb = strings.ReplaceAll(sb, "\r", "")
	return sa == sb
}
