package diff

import (
	"github.com/jacktea/data-smith/pkg/conn"
)

type ICompareRule interface {
	IsEqual(a, b conn.Record) bool
	GetTable() string
}

type IDetailedCompareRule interface {
	ICompareRule
	DiffColumns(a, b conn.Record) (equal bool, diffCols []string)
	GetColumnsDef() map[string]*conn.Column
}

type AllFieldsEqualRule struct {
	Table      string
	Columns    []string
	ColumnsDef map[string]*conn.Column
}

func (r *AllFieldsEqualRule) DiffColumns(a, b conn.Record) (bool, []string) {
	var diffCols []string
	for _, c := range r.Columns {
		var colType string
		if r.ColumnsDef != nil && r.ColumnsDef[c] != nil {
			colType = r.ColumnsDef[c].DataType
		}
		if !AreValuesEqual(a[c], b[c], colType) {
			diffCols = append(diffCols, c)
		}
	}
	return len(diffCols) == 0, diffCols
}

func (r *AllFieldsEqualRule) IsEqual(a, b conn.Record) bool {
	equal, _ := r.DiffColumns(a, b)
	return equal
}

func (r *AllFieldsEqualRule) GetTable() string {
	return r.Table
}

func (r *AllFieldsEqualRule) GetColumnsDef() map[string]*conn.Column {
	return r.ColumnsDef
}

// GetCompareColumns exposes the effective compare set so row readers can fetch
// only the compared columns (plus primary keys), honouring ignore columns.
func (r *AllFieldsEqualRule) GetCompareColumns() []string {
	return r.Columns
}

// CreateCompareRuleColumns builds the compare rule with explicit precedence:
// compare columns > comparisonKey (legacy) > all columns. Ignore columns are
// removed from the resulting compare set in every branch.
func CreateCompareRuleColumns(table *conn.Table, columns, comparisonKey, ignoreColumns []string) ICompareRule {
	if len(columns) > 0 {
		return CreateCompareRule(table, columns, ignoreColumns)
	}
	return CreateCompareRule(table, comparisonKey, ignoreColumns)
}

func CreateCompareRule(table *conn.Table, comparisonKey []string, ignoreColumns ...[]string) ICompareRule {
	cols := comparisonKey
	if len(cols) == 0 && table != nil {
		cols = table.GetColumns()
	}

	// 剔除忽略字段
	if len(ignoreColumns) > 0 && len(ignoreColumns[0]) > 0 {
		ignoreSet := make(map[string]struct{}, len(ignoreColumns[0]))
		for _, c := range ignoreColumns[0] {
			ignoreSet[c] = struct{}{}
		}
		filtered := make([]string, 0, len(cols))
		for _, c := range cols {
			if _, ignored := ignoreSet[c]; !ignored {
				filtered = append(filtered, c)
			}
		}
		cols = filtered
	}

	var colsDef map[string]*conn.Column
	var tableName string
	if table != nil {
		colsDef = table.Columns
		tableName = table.Name
	}
	return &AllFieldsEqualRule{
		Table:      tableName,
		Columns:    cols,
		ColumnsDef: colsDef,
	}
}
