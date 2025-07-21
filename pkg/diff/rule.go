package diff

import (
	"fmt"

	"github.com/jacktea/data-smith/pkg/conn"
)

type ICompareRule interface {
	IsEqual(a, b conn.Record) bool
	GetTable() string
}

type AllFieldsEqualRule struct {
	Table   string
	Columns []string
}

func (r *AllFieldsEqualRule) IsEqual(a, b conn.Record) bool {
	for _, c := range r.Columns {
		if fmt.Sprintf("%v", a[c]) != fmt.Sprintf("%v", b[c]) {
			return false
		}
	}
	return true
}

func (r *AllFieldsEqualRule) GetTable() string {
	return r.Table
}

func CreateCompareRule(table *conn.Table, comparisonKey []string) ICompareRule {
	cols := comparisonKey
	if len(cols) == 0 {
		cols = table.GetColumns()
	}
	return &AllFieldsEqualRule{
		Table:   table.Name,
		Columns: cols,
	}
}
