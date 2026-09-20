package sql

import (
	"os"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

func TestIssue5GoldenForwardRollbackDeterministic(t *testing.T) {
	source, target := issue5Schemas()
	cases := []struct {
		name   string
		from   *conn.DatabaseSchema
		to     *conn.DatabaseSchema
		golden string
	}{
		{name: "forward", from: source, to: target, golden: "testdata/issue5_forward.golden"},
		{name: "rollback", from: target, to: source, golden: "testdata/issue5_rollback.golden"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			wantBytes, err := os.ReadFile(test.golden)
			if err != nil {
				t.Fatal(err)
			}
			var first string
			for iteration := 0; iteration < 30; iteration++ {
				statements, err := GenerateSchemaSQLSafe(diff.CompareSchemas(test.from, test.to), consts.DBTypePostgres)
				if err != nil {
					t.Fatal(err)
				}
				got := strings.Join(statements, "\n") + "\n"
				if iteration == 0 {
					first = got
				} else if got != first {
					t.Fatalf("generation %d differs byte-for-byte from generation 0", iteration)
				}
				if got != string(wantBytes) {
					t.Fatalf("golden mismatch:\n--- got ---\n%s--- want ---\n%s", got, string(wantBytes))
				}
			}
		})
	}
}

func TestGenerateSchemaSQLSafeRejectsViewDependencyCycle(t *testing.T) {
	left := issue5View("Left", "SELECT 1", "sales\"Ops.Right")
	right := issue5View("Right", "SELECT 1", "sales\"Ops.Left")
	_, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{TablesAdded: []*conn.Table{right, left}}, consts.DBTypePostgres)
	if err == nil || !strings.Contains(err.Error(), "schema object dependency cycle") ||
		!strings.Contains(err.Error(), `view:sales"Ops.Left`) || !strings.Contains(err.Error(), `view:sales"Ops.Right`) {
		t.Fatalf("expected view cycle error, got %v", err)
	}
}

func TestPostgresDataSQLUsesQualifiedEscapedNames(t *testing.T) {
	dialect := NewDialect(consts.DBTypePostgres)
	table := &conn.Table{
		Schema: "sales\"Ops",
		Name:   "Order",
		Columns: map[string]*conn.Column{
			"ID":       {Name: "ID", DataType: "integer", Position: 1},
			"select\"": {Name: "select\"", DataType: "text", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"ID"}},
	}
	got := dialect.GenerateInsertSql(table, conn.Record{"ID": 7, "select\"": "ok"})
	want := `INSERT INTO "sales""Ops"."Order" ("ID", "select""") VALUES (7, 'ok');`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDialectDDLMapFieldsAreStable(t *testing.T) {
	table := issue5Table("public", "Stable")
	table.Columns = map[string]*conn.Column{
		"z": {Name: "z", DataType: "integer", Position: 2},
		"b": {Name: "b", DataType: "integer", Position: 1},
		"a": {Name: "a", DataType: "integer", Position: 1},
	}
	table.PrimaryKey = nil
	table.Indexes = map[string]*conn.Index{
		"z_idx": {Name: "z_idx", Columns: []string{"z"}},
		"a_idx": {Name: "a_idx", Columns: []string{"a"}},
	}
	table.ForeignKeys = map[string]*conn.ForeignKey{
		"z_fk": {Name: "z_fk", Columns: []string{"z"}, ReferencedSchema: "public", ReferencedTable: "Zed", ReferencedColumns: []string{"id"}},
		"a_fk": {Name: "a_fk", Columns: []string{"a"}, ReferencedSchema: "public", ReferencedTable: "Able", ReferencedColumns: []string{"id"}},
	}
	dialect := NewDialect(consts.DBTypePostgres)
	first := dialect.GenerateTableDDL(table)
	for iteration := 0; iteration < 30; iteration++ {
		if got := dialect.GenerateTableDDL(table); got != first {
			t.Fatalf("DDL generation %d was not byte-for-byte stable", iteration)
		}
	}
	for _, pair := range [][2]string{{`"a" int4`, `"b" int4`}, {`"b" int4`, `"z" int4`}, {`"a_fk"`, `"z_fk"`}, {`"a_idx"`, `"z_idx"`}} {
		if left, right := strings.Index(first, pair[0]), strings.Index(first, pair[1]); left < 0 || right < 0 || left >= right {
			t.Fatalf("expected %s before %s in:\n%s", pair[0], pair[1], first)
		}
	}
}

func issue5Schemas() (*conn.DatabaseSchema, *conn.DatabaseSchema) {
	schema := "sales\"Ops"
	source := &conn.DatabaseSchema{Tables: map[string]*conn.Table{
		"Convert": issue5Table(schema, "Convert"),
		"Rebuild": issue5View("Rebuild", `SELECT 1 AS "ID"`),
	}}
	alpha := issue5Table(schema, "Alpha")
	beta := issue5Table(schema, "Beta")
	alpha.ForeignKeys["Alpha_to_Beta"] = &conn.ForeignKey{
		Name: "Alpha_to_Beta", Columns: []string{"ID"}, ReferencedSchema: schema,
		ReferencedTable: "Beta", ReferencedColumns: []string{"ID"},
	}
	beta.ForeignKeys["Beta_to_Alpha"] = &conn.ForeignKey{
		Name: "Beta_to_Alpha", Columns: []string{"ID"}, ReferencedSchema: schema,
		ReferencedTable: "Alpha", ReferencedColumns: []string{"ID"},
	}
	target := &conn.DatabaseSchema{Tables: map[string]*conn.Table{
		"Summary":  issue5View("Summary", `SELECT * FROM "sales""Ops"."Convert"`, schema+".Convert"),
		"Convert":  issue5View("Convert", `SELECT * FROM "sales""Ops"."BaseView"`, schema+".BaseView"),
		"BaseView": issue5View("BaseView", `SELECT * FROM "sales""Ops"."Alpha"`, schema+".Alpha"),
		"Rebuild":  issue5Table(schema, "Rebuild"),
		"Beta":     beta,
		"Alpha":    alpha,
	}}
	return source, target
}

func issue5Table(schema, name string) *conn.Table {
	return &conn.Table{
		Schema: schema,
		Name:   name,
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"ID": {Name: "ID", DataType: "integer", Position: 1},
		},
		PrimaryKey:  &conn.PrimaryKey{Name: name + "_pk", Columns: []string{"ID"}},
		Indexes:     map[string]*conn.Index{},
		ForeignKeys: map[string]*conn.ForeignKey{},
	}
}

func issue5View(name, statement string, dependencies ...string) *conn.Table {
	return &conn.Table{
		Schema: "sales\"Ops",
		Name:   name,
		Type:   conn.TableTypeView,
		ViewDefinition: &conn.ViewDefinition{
			SelectStatement: statement,
			Dependencies:    dependencies,
		},
	}
}
