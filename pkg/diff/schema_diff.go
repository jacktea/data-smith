package diff

import "github.com/jacktea/data-smith/pkg/conn"

type SchemaDiff struct {
	TablesAdded    []*conn.Table
	TablesDropped  []*conn.Table
	TablesModified []*TableDiff
}

type TableDiff struct {
	Table                *conn.Table
	ColumnsAdded         []*conn.Column
	ColumnsDropped       []*conn.Column
	ColumnsModified      []*ColumnDiff
	IndexesAdded         []*conn.Index
	IndexesDropped       []*conn.Index
	IndexesModified      []*IndexDiff
	PrimaryKeyChange     *PrimaryKeyDiff
	ForeignKeysAdded     []*conn.ForeignKey
	ForeignKeysDropped   []*conn.ForeignKey
	ForeignKeysModified  []*ForeignKeyDiff
	ViewDefinitionChange *ViewDefinitionDiff
}

type ColumnDiff struct {
	Old *conn.Column
	New *conn.Column
}

type IndexDiff struct {
	Old *conn.Index
	New *conn.Index
}

type PrimaryKeyDiff struct {
	Old *conn.PrimaryKey
	New *conn.PrimaryKey
}

type ForeignKeyDiff struct {
	Old *conn.ForeignKey
	New *conn.ForeignKey
}

type ViewDefinitionDiff struct {
	Old *conn.ViewDefinition
	New *conn.ViewDefinition
}
