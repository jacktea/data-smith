package diff

import "github.com/jacktea/data-smith/pkg/conn"

type DiffType string

const (
	DiffTypeAdd    DiffType = "ADD"
	DiffTypeDrop   DiffType = "DROP"
	DiffTypeModify DiffType = "MODIFY"
)

type DataDiff struct {
	Added    []conn.Record
	Dropped  []conn.Record
	Modified []ModifiedRow
}

type ModifiedRow struct {
	Old conn.Record
	New conn.Record
}
