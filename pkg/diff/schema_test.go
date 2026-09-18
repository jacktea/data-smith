package diff

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func TestCompareSchemas_ReverseDirection(t *testing.T) {
	src := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"old_table": {
				Name: "old_table",
				Type: conn.TableTypeTable,
			},
			"shared_table": {
				Name: "shared_table",
				Type: conn.TableTypeTable,
				Columns: map[string]*conn.Column{
					"id":      {Name: "id", DataType: "int"},
					"old_col": {Name: "old_col", DataType: "varchar"},
				},
			},
		},
	}

	tgt := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"new_table": {
				Name: "new_table",
				Type: conn.TableTypeTable,
			},
			"shared_table": {
				Name: "shared_table",
				Type: conn.TableTypeTable,
				Columns: map[string]*conn.Column{
					"id":      {Name: "id", DataType: "int"},
					"new_col": {Name: "new_col", DataType: "varchar"},
				},
			},
		},
	}

	diff := CompareSchemas(src, tgt)

	// 1. TablesAdded 应包含 new_table（Target 独有）
	if len(diff.TablesAdded) != 1 || diff.TablesAdded[0].Name != "new_table" {
		t.Fatalf("expected TablesAdded to have new_table, got %v", diff.TablesAdded)
	}

	// 2. TablesDropped 应包含 old_table（Source 独有）
	if len(diff.TablesDropped) != 1 || diff.TablesDropped[0].Name != "old_table" {
		t.Fatalf("expected TablesDropped to have old_table, got %v", diff.TablesDropped)
	}

	// 3. shared_table 的列差异
	if len(diff.TablesModified) != 1 {
		t.Fatalf("expected 1 modified table, got %d", len(diff.TablesModified))
	}
	mod := diff.TablesModified[0]
	if len(mod.ColumnsAdded) != 1 || mod.ColumnsAdded[0].Name != "new_col" {
		t.Errorf("expected ColumnsAdded to have new_col, got %v", mod.ColumnsAdded)
	}
	if len(mod.ColumnsDropped) != 1 || mod.ColumnsDropped[0].Name != "old_col" {
		t.Errorf("expected ColumnsDropped to have old_col, got %v", mod.ColumnsDropped)
	}
}

func TestCompareSchemas_NormalizationAndSemantics(t *testing.T) {
	def1 := "'active'::character varying"
	def2 := "active"
	def3 := "now()"
	def4 := "CURRENT_TIMESTAMP"

	src := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"users": {
				Name:    "users",
				Type:    conn.TableTypeTable,
				Comment: "用户表旧注释",
				Columns: map[string]*conn.Column{
					"id": {Name: "id", DataType: "int4"},
					"status": {
						Name:     "status",
						DataType: "character varying",
						Default:  &def1,
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp without time zone",
						Default:  &def3,
					},
				},
				PrimaryKey: &conn.PrimaryKey{
					Name:    "users_pkey",
					Columns: []string{"id"},
				},
			},
		},
	}

	tgt := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"users": {
				Name:    "users",
				Type:    conn.TableTypeTable,
				Comment: "用户表新注释",
				Columns: map[string]*conn.Column{
					"id": {Name: "id", DataType: "integer"}, // 别名等价
					"status": {
						Name:     "status",
						DataType: "varchar", // 别名等价
						Default:  &def2,     // 默认值等价
					},
					"created_at": {
						Name:     "created_at",
						DataType: "timestamp", // 别名等价
						Default:  &def4,       // now() 与 CURRENT_TIMESTAMP 等价
					},
				},
				PrimaryKey: &conn.PrimaryKey{
					Name:    "pk_users_new_name", // 名字不同但列相同，不应触发主键重建
					Columns: []string{"id"},
				},
			},
		},
	}

	diff := CompareSchemas(src, tgt)

	// 表结构没有新增或删除
	if len(diff.TablesAdded) != 0 || len(diff.TablesDropped) != 0 {
		t.Fatalf("expected no added/dropped tables, got added=%d dropped=%d", len(diff.TablesAdded), len(diff.TablesDropped))
	}
	// 只有一个 modified table
	if len(diff.TablesModified) != 1 {
		t.Fatalf("expected 1 modified table for comment change, got %d", len(diff.TablesModified))
	}
	tblDiff := diff.TablesModified[0]
	// 列不应该被报告修改
	if len(tblDiff.ColumnsModified) != 0 {
		t.Errorf("expected 0 modified columns due to normalization, got %d", len(tblDiff.ColumnsModified))
	}
	// 主键不应该被报告修改
	if tblDiff.PrimaryKeyChange != nil {
		t.Errorf("expected no primary key change when columns match, got %v", tblDiff.PrimaryKeyChange)
	}
	// 表注释应该被识别
	if tblDiff.CommentChange == nil || tblDiff.CommentChange.New != "用户表新注释" {
		t.Errorf("expected comment change to be detected, got %v", tblDiff.CommentChange)
	}
}

func TestCompareSchemas_DualRollback(t *testing.T) {
	src := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"t1": {
				Name: "t1",
				Type: conn.TableTypeTable,
				Columns: map[string]*conn.Column{
					"id": {Name: "id", DataType: "int"},
				},
			},
		},
	}
	tgt := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"t1": {
				Name: "t1",
				Type: conn.TableTypeTable,
				Columns: map[string]*conn.Column{
					"id":   {Name: "id", DataType: "int"},
					"name": {Name: "name", DataType: "varchar"},
				},
			},
			"t2": {
				Name: "t2",
				Type: conn.TableTypeTable,
			},
		},
	}

	forwardDiff := CompareSchemas(src, tgt)
	rollbackDiff := CompareSchemas(tgt, src)

	// 正向：新增 t2，修改 t1（新增 name 列）
	if len(forwardDiff.TablesAdded) != 1 || forwardDiff.TablesAdded[0].Name != "t2" {
		t.Errorf("expected forward TablesAdded to be t2")
	}
	if len(forwardDiff.TablesModified) != 1 || len(forwardDiff.TablesModified[0].ColumnsAdded) != 1 {
		t.Errorf("expected forward ColumnsAdded to be name")
	}

	// 逆向：删除 t2，修改 t1（删除 name 列）
	if len(rollbackDiff.TablesDropped) != 1 || rollbackDiff.TablesDropped[0].Name != "t2" {
		t.Errorf("expected rollback TablesDropped to be t2")
	}
	if len(rollbackDiff.TablesModified) != 1 || len(rollbackDiff.TablesModified[0].ColumnsDropped) != 1 {
		t.Errorf("expected rollback ColumnsDropped to be name")
	}
}

