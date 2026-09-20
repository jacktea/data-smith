package diff

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func TestRowIdentityColumnsPrefersPrimaryKey(t *testing.T) {
	tbl := &conn.Table{
		PrimaryKey: &conn.PrimaryKey{Name: "t_pkey", Columns: []string{"org_id", "id"}},
		Indexes: map[string]*conn.Index{
			"t_uq": {Name: "t_uq", Unique: true, Columns: []string{"code"}},
		},
	}
	got := RowIdentityColumns(tbl)
	if len(got) != 2 || got[0] != "org_id" || got[1] != "id" {
		t.Fatalf("RowIdentityColumns = %v, want primary key columns", got)
	}
}

func TestRowIdentityColumnsFallsBackToNotNullUniqueIndex(t *testing.T) {
	tbl := &conn.Table{
		Columns: map[string]*conn.Column{
			"code":   {Name: "code", Nullable: false},
			"tenant": {Name: "tenant", Nullable: false},
			"note":   {Name: "note", Nullable: true},
		},
		Indexes: map[string]*conn.Index{
			"t_nullable_uq": {Name: "t_nullable_uq", Unique: true, Columns: []string{"note"}},
			"t_uq":          {Name: "t_uq", Unique: true, Columns: []string{"tenant", "code"}},
			"t_idx":         {Name: "t_idx", Unique: false, Columns: []string{"code"}},
		},
	}
	got := RowIdentityColumns(tbl)
	if len(got) != 2 || got[0] != "tenant" || got[1] != "code" {
		t.Fatalf("RowIdentityColumns = %v, want the not-null unique index columns", got)
	}
}

func TestRowIdentityColumnsNone(t *testing.T) {
	if got := RowIdentityColumns(nil); got != nil {
		t.Fatalf("nil table must have no identity, got %v", got)
	}
	tbl := &conn.Table{
		Columns: map[string]*conn.Column{"note": {Name: "note", Nullable: true}},
		Indexes: map[string]*conn.Index{
			"t_uq": {Name: "t_uq", Unique: true, Columns: []string{"note"}},
		},
	}
	if got := RowIdentityColumns(tbl); got != nil {
		t.Fatalf("nullable unique index must not count as identity, got %v", got)
	}
}
