package diff

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
)

// Web 任务详情直接把 SchemaDiffSummary 序列化给前端,切片为 nil 会被
// 序列化成 null,前端取 length/join 会崩溃,因此所有切片必须输出 []。
func TestProjectSchemaDiffSlicesNeverNull(t *testing.T) {
	t.Run("empty diff", func(t *testing.T) {
		raw, err := json.Marshal(projectSchemaDiff(&pkgdiff.SchemaDiff{}))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(raw), "null") {
			t.Fatalf("empty diff summary contains null: %s", raw)
		}
	})

	t.Run("added tables only", func(t *testing.T) {
		d := &pkgdiff.SchemaDiff{TablesAdded: []*conn.Table{{Name: "users"}}}
		raw, err := json.Marshal(projectSchemaDiff(d))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(raw), "null") {
			t.Fatalf("added-only diff summary contains null: %s", raw)
		}
		var summary SchemaDiffSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(summary.TablesAdded) != 1 || summary.TablesAdded[0] != "users" {
			t.Fatalf("unexpected tablesAdded: %v", summary.TablesAdded)
		}
		if summary.TablesModified == nil || len(summary.TablesModified) != 0 {
			t.Fatalf("tablesModified must be empty slice, got: %v", summary.TablesModified)
		}
	})

	t.Run("modified table with index change only", func(t *testing.T) {
		d := &pkgdiff.SchemaDiff{
			TablesModified: []*pkgdiff.TableDiff{
				{
					Table:        &conn.Table{Name: "orders"},
					IndexesAdded: []*conn.Index{{Name: "idx_orders_status"}},
				},
			},
		}
		raw, err := json.Marshal(projectSchemaDiff(d))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(raw), "null") {
			t.Fatalf("index-only diff summary contains null: %s", raw)
		}
		var summary SchemaDiffSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(summary.TablesModified) != 1 {
			t.Fatalf("expected 1 modified table, got %d", len(summary.TablesModified))
		}
		mod := summary.TablesModified[0]
		if mod.Table != "orders" || len(mod.IndexesAdded) != 1 || mod.IndexesAdded[0] != "idx_orders_status" {
			t.Fatalf("unexpected modified table: %+v", mod)
		}
		if mod.ColumnsAdded == nil || mod.ColumnsDropped == nil || mod.ColumnsModified == nil {
			t.Fatalf("column slices must be empty slices, got: %+v", mod)
		}
		if summary.Destructive {
			t.Fatalf("index-only change must not be destructive")
		}
	})
}
