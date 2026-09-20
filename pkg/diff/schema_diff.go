package diff

import "github.com/jacktea/data-smith/pkg/conn"

type SchemaDiff struct {
	TablesAdded    []*conn.Table
	TablesDropped  []*conn.Table
	TablesModified []*TableDiff

	// RoutinesAdded/Dropped 以身份签名区分；Modified 表示定义文本变化，
	// 以 CREATE OR REPLACE 重新应用。
	RoutinesAdded    []*conn.Routine
	RoutinesDropped  []*conn.Routine
	RoutinesModified []*RoutineDiff

	// SequencesAdded/Dropped 按名称区分；Modified 仅比较建序参数
	// （start/increment/min/max/cycle/cache/data_type），以 ALTER SEQUENCE 对齐。
	SequencesAdded    []*conn.Sequence
	SequencesDropped  []*conn.Sequence
	SequencesModified []*SequenceDiff

	// ViewsAffected 是「定义未变、但依赖了被变更对象」的视图依赖闭包。
	// 它们必须先于表 DDL 被 DROP，之后按本字段中的定义（新态侧）重建，
	// 否则 PostgreSQL 会以 cannot alter type of a column used by a view
	// 拒绝列类型变更。每项为新态侧（forward 为 target、回滚为 source）的视图模型。
	ViewsAffected []*conn.Table
}

type TableDiff struct {
	SourceTable          *conn.Table
	TargetTable          *conn.Table
	Table                *conn.Table // 兼容旧逻辑，代表目标态/操作表
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
	CommentChange        *CommentDiff
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

type RoutineDiff struct {
	Old *conn.Routine
	New *conn.Routine
}

type SequenceDiff struct {
	Old *conn.Sequence
	New *conn.Sequence
}

type CommentDiff struct {
	Old string
	New string
}
