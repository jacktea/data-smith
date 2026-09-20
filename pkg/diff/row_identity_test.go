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

func TestRowIdentityColumnsRejectsNonOrdinaryOrIncompleteUniqueIndexes(t *testing.T) {
	where := "deleted_at IS NULL"
	expression := "lower(email)"
	columns := map[string]*conn.Column{
		"tenant": {Name: "tenant", Nullable: false},
		"email":  {Name: "email", Nullable: false},
	}
	tests := []struct {
		name  string
		index *conn.Index
	}{
		{
			name:  "partial index",
			index: &conn.Index{Name: "uq_active_email", Unique: true, Columns: []string{"email"}, Where: &where},
		},
		{
			name:  "pure expression index",
			index: &conn.Index{Name: "uq_lower_email", Unique: true, Expression: &expression},
		},
		{
			name:  "mixed expression index",
			index: &conn.Index{Name: "uq_tenant_lower_email", Unique: true, Columns: []string{"tenant"}, Expression: &expression},
		},
		{
			name:  "missing metadata column",
			index: &conn.Index{Name: "uq_missing", Unique: true, Columns: []string{"tenant", "missing"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := &conn.Table{Columns: columns, Indexes: map[string]*conn.Index{tt.index.Name: tt.index}}
			if got := RowIdentityColumns(tbl); got != nil {
				t.Fatalf("RowIdentityColumns = %v, want nil for %+v", got, tt.index)
			}
		})
	}
}

func TestRowIdentityColumnsAcceptsOrdinaryPostgresUniqueIndexDefinition(t *testing.T) {
	tbl := &conn.Table{
		Columns: map[string]*conn.Column{
			"email": {Name: "email", Nullable: false},
		},
		Indexes: map[string]*conn.Index{
			"uq_email": {
				Name:       "uq_email",
				Unique:     true,
				Columns:    []string{"email"},
				Definition: `CREATE UNIQUE INDEX uq_email ON public.users USING btree (email)`,
			},
		},
	}
	got := RowIdentityColumns(tbl)
	if len(got) != 1 || got[0] != "email" {
		t.Fatalf("RowIdentityColumns = %v, want ordinary PostgreSQL unique column", got)
	}
}
