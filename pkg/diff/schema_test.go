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
