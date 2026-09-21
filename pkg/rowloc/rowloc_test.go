package rowloc

import (
	"fmt"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func TestBuildSelectsSharedRowLocationStrategy(t *testing.T) {
	notNullColumns := map[string]*conn.Column{
		"id":     {Name: "id", DataType: "bigint", Nullable: false},
		"tenant": {Name: "tenant", DataType: "varchar", Nullable: false},
		"code":   {Name: "code", DataType: "varchar", Nullable: false},
		"note":   {Name: "note", DataType: "text", Nullable: true},
	}
	tests := []struct {
		name         string
		table        *conn.Table
		row          conn.Record
		wantColumns  string
		wantReliable bool
	}{
		{
			name: "primary key",
			table: &conn.Table{Columns: notNullColumns, PrimaryKey: &conn.PrimaryKey{Columns: []string{"tenant", "id"}}, Indexes: map[string]*conn.Index{
				"uq_code": {Name: "uq_code", Unique: true, Columns: []string{"code"}},
			}},
			row: conn.Record{"id": 7, "tenant": "acme", "code": "A"}, wantColumns: "[tenant id]", wantReliable: true,
		},
		{
			name: "not-null unique identity",
			table: &conn.Table{Columns: notNullColumns, Indexes: map[string]*conn.Index{
				"uq_tenant_code": {Name: "uq_tenant_code", Unique: true, Columns: []string{"tenant", "code"}},
			}},
			row: conn.Record{"tenant": "acme", "code": "A", "note": "n"}, wantColumns: "[tenant code]", wantReliable: true,
		},
		{
			name: "no reliable identity falls back to sorted row columns",
			table: &conn.Table{Columns: notNullColumns, Indexes: map[string]*conn.Index{
				"uq_note": {Name: "uq_note", Unique: true, Columns: []string{"note"}},
			}},
			row: conn.Record{"note": nil, "code": "A"}, wantColumns: "[code note]", wantReliable: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Build(tt.table, tt.row)
			if fmt.Sprint(got.Columns()) != tt.wantColumns || got.Reliable() != tt.wantReliable {
				t.Fatalf("location columns=%v reliable=%v, want %s reliable=%v", got.Columns(), got.Reliable(), tt.wantColumns, tt.wantReliable)
			}
		})
	}
}

func TestBuildCarriesDialectIndependentLocationFields(t *testing.T) {
	table := &conn.Table{
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint"},
			"note": {Name: "note", DataType: "text", Nullable: true},
		},
	}
	got := Build(table, conn.Record{"note": nil, "id": int64(7)})
	want := []Field{{Column: "id", DataType: "bigint", Value: int64(7)}, {Column: "note", DataType: "text", Value: nil}}
	if fmt.Sprint(got.Fields) != fmt.Sprint(want) {
		t.Fatalf("fields=%v, want %v", got.Fields, want)
	}
}
