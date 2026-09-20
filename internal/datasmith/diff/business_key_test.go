package diff

import (
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

// C4a：无键表按显式业务键（rules.comparisonKey）比对时，业务键注入裁剪
// 副本作为行定位键；有物理身份的表与非法业务键不受影响。
func TestApplyBusinessKeyIdentity(t *testing.T) {
	rule := pkgconfig.Rule{Table: "t", ComparisonKey: []string{"client_id", "role_id"}}

	t.Run("keyless table gains business key locator", func(t *testing.T) {
		tbl := slimTable(keylessDiffTable(), map[string]bool{"client_id": true, "role_id": true, "note": true})
		applyBusinessKeyIdentity(tbl, rule)
		if tbl.PrimaryKey == nil || strings.Join(tbl.PrimaryKey.Columns, ",") != "client_id,role_id" {
			t.Fatalf("primary key = %#v, want business key columns", tbl.PrimaryKey)
		}
	})

	t.Run("table with primary key is untouched", func(t *testing.T) {
		tbl := keylessDiffTable()
		tbl.PrimaryKey = &conn.PrimaryKey{Name: "pk", Columns: []string{"client_id"}}
		slim := slimTable(tbl, map[string]bool{"client_id": true, "role_id": true, "note": true})
		applyBusinessKeyIdentity(slim, rule)
		if strings.Join(slim.PrimaryKey.Columns, ",") != "client_id" {
			t.Fatalf("primary key = %#v, want physical key preserved", slim.PrimaryKey)
		}
	})

	t.Run("nullable business key column is rejected", func(t *testing.T) {
		tbl := keylessDiffTable()
		tbl.Columns["role_id"].Nullable = true
		slim := slimTable(tbl, map[string]bool{"client_id": true, "role_id": true, "note": true})
		applyBusinessKeyIdentity(slim, rule)
		if slim.PrimaryKey != nil {
			t.Fatalf("primary key = %#v, want none for nullable business key", slim.PrimaryKey)
		}
	})

	t.Run("original model is not mutated", func(t *testing.T) {
		tbl := keylessDiffTable()
		slim := slimTable(tbl, map[string]bool{"client_id": true, "role_id": true, "note": true})
		applyBusinessKeyIdentity(slim, rule)
		if tbl.PrimaryKey != nil {
			t.Fatal("shared table model was mutated by business key injection")
		}
	})
}

func keylessDiffTable() *conn.Table {
	tbl := &conn.Table{
		Name:    "air_user_client_role",
		Columns: map[string]*conn.Column{},
	}
	tbl.Columns["client_id"] = &conn.Column{Name: "client_id", DataType: "int8"}
	tbl.Columns["role_id"] = &conn.Column{Name: "role_id", DataType: "int8"}
	tbl.Columns["note"] = &conn.Column{Name: "note", DataType: "text"}
	return tbl
}
