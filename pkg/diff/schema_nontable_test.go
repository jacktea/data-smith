package diff

import (
	"strconv"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func routine(schema, name, args, definition string) *conn.Routine {
	return &conn.Routine{
		Name:         name,
		Schema:       schema,
		Kind:         conn.RoutineKindFunction,
		IdentityArgs: args,
		Definition:   definition,
	}
}

func sequence(schema, name string, increment int) *conn.Sequence {
	return &conn.Sequence{
		Name:        name,
		Schema:      schema,
		DataType:    "bigint",
		StartValue:  "1",
		IncrementBy: strconv.Itoa(increment),
		MinValue:    "1",
		MaxValue:    "9223372036854775807",
		CacheSize:   "1",
	}
}

func baseTableWithColumn(schema, table, col, dataType string) *conn.Table {
	return &conn.Table{
		Name:   table,
		Schema: schema,
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			col: {Name: col, DataType: dataType, Position: 1},
		},
	}
}

func tableView(schema, name, definition string, deps ...string) *conn.Table {
	return &conn.Table{
		Name:   name,
		Schema: schema,
		Type:   conn.TableTypeView,
		Columns: map[string]*conn.Column{
			"id": {Name: "id", DataType: "integer", Position: 1},
		},
		ViewDefinition: &conn.ViewDefinition{
			SelectStatement: definition,
			Dependencies:    deps,
		},
	}
}

func schemaWith(objects ...*conn.Table) *conn.DatabaseSchema {
	s := &conn.DatabaseSchema{
		Tables:    map[string]*conn.Table{},
		Routines:  map[string]*conn.Routine{},
		Sequences: map[string]*conn.Sequence{},
	}
	for _, o := range objects {
		s.Tables[o.Name] = o
	}
	return s
}

func TestCompareSchemasRoutinesAddedDroppedModified(t *testing.T) {
	src := schemaWith()
	src.Routines["f_a()"] = routine("public", "f_a", "", "CREATE OR REPLACE FUNCTION public.f_a() RETURNS integer LANGUAGE sql AS $f$SELECT 1$f$")
	src.Routines["f_b(integer)"] = routine("public", "f_b", "integer", "CREATE OR REPLACE FUNCTION public.f_b(integer) RETURNS integer LANGUAGE sql AS $f$SELECT $1$f$")
	tgt := schemaWith()
	tgt.Routines["f_b(integer)"] = routine("public", "f_b", "integer", "CREATE OR REPLACE FUNCTION public.f_b(integer) RETURNS integer LANGUAGE sql AS $f$SELECT $1 + 0$f$")
	tgt.Routines["f_c(text)"] = routine("public", "f_c", "text", "CREATE OR REPLACE FUNCTION public.f_c(text) RETURNS text LANGUAGE sql AS $f$SELECT $1$f$")

	diff := CompareSchemas(src, tgt)
	if len(diff.RoutinesAdded) != 1 || diff.RoutinesAdded[0].Name != "f_c" {
		t.Fatalf("expected f_c added, got %+v", diff.RoutinesAdded)
	}
	if len(diff.RoutinesDropped) != 1 || diff.RoutinesDropped[0].Name != "f_a" {
		t.Fatalf("expected f_a dropped, got %+v", diff.RoutinesDropped)
	}
	if len(diff.RoutinesModified) != 1 || diff.RoutinesModified[0].Old.Name != "f_b" {
		t.Fatalf("expected f_b modified, got %+v", diff.RoutinesModified)
	}

	// 同一签名定义仅空白/结尾分号不同视为相等。
	srcSame := schemaWith()
	srcSame.Routines["f_b(integer)"] = routine("public", "f_b", "integer", "CREATE OR REPLACE FUNCTION public.f_b(integer)\n  RETURNS integer\n  LANGUAGE sql AS $f$SELECT $1$f$")
	tgtSame := schemaWith()
	tgtSame.Routines["f_b(integer)"] = routine("public", "f_b", "integer", "CREATE OR REPLACE FUNCTION public.f_b(integer) RETURNS integer LANGUAGE sql AS $f$SELECT $1$f$;")
	if diff := CompareSchemas(srcSame, tgtSame); len(diff.RoutinesModified) != 0 {
		t.Fatalf("expected whitespace-only difference to be equal, got %+v", diff.RoutinesModified)
	}
}

func TestCompareSchemasProceduresKindKeptInDiff(t *testing.T) {
	src := schemaWith()
	tgt := schemaWith()
	proc := routine("public", "p_audit", "text", "CREATE OR REPLACE PROCEDURE public.p_audit(text) LANGUAGE plpgsql AS $p$BEGIN END$p$")
	proc.Kind = conn.RoutineKindProcedure
	tgt.Routines["p_audit(text)"] = proc

	diff := CompareSchemas(src, tgt)
	if len(diff.RoutinesAdded) != 1 || diff.RoutinesAdded[0].Kind != conn.RoutineKindProcedure {
		t.Fatalf("expected procedure added, got %+v", diff.RoutinesAdded)
	}
}

func TestCompareSchemasSequencesAddedDroppedModified(t *testing.T) {
	src := schemaWith()
	src.Sequences["seq_old"] = sequence("public", "seq_old", 1)
	src.Sequences["seq_chg"] = sequence("public", "seq_chg", 1)
	src.Sequences["seq_owner"] = sequence("public", "seq_owner", 1)
	tgt := schemaWith()
	tgt.Sequences["seq_chg"] = sequence("public", "seq_chg", 3)
	tgt.Sequences["seq_owner"] = sequence("public", "seq_owner", 1)
	tgt.Sequences["seq_owner"].OwnedBy = "orders.id"
	tgt.Sequences["seq_new"] = sequence("public", "seq_new", 1)

	diff := CompareSchemas(src, tgt)
	if len(diff.SequencesAdded) != 1 || diff.SequencesAdded[0].Name != "seq_new" {
		t.Fatalf("expected seq_new added, got %+v", diff.SequencesAdded)
	}
	if len(diff.SequencesDropped) != 1 || diff.SequencesDropped[0].Name != "seq_old" {
		t.Fatalf("expected seq_old dropped, got %+v", diff.SequencesDropped)
	}
	if len(diff.SequencesModified) != 2 {
		t.Fatalf("expected parameter and ownership changes, got %+v", diff.SequencesModified)
	}
	if diff.SequencesModified[0].New.Name != "seq_chg" || diff.SequencesModified[1].New.OwnedBy != "orders.id" {
		t.Fatalf("expected deterministic seq_chg then seq_owner changes, got %+v", diff.SequencesModified)
	}
}

func TestSequenceEqualIncludesOwnership(t *testing.T) {
	a := sequence("public", "seq_a", 1)
	a.OwnedBy = "t.col"
	b := sequence("public", "seq_a", 1)
	if a.Equal(b) {
		t.Fatal("expected different ownership to make sequences unequal")
	}
	b.OwnedBy = "t.col"
	if !a.Equal(b) {
		t.Fatal("expected identical ownership and creation parameters to be equal")
	}
	b.IncrementBy = "2"
	if a.Equal(b) {
		t.Fatal("expected different increment to be unequal")
	}
}

func TestCollectAffectedViewsClosure(t *testing.T) {
	src := schemaWith(
		baseTableWithColumn("public", "base", "amount", "integer"),
		tableView("public", "v_direct", "SELECT * FROM public.base", "public.base"),
		tableView("public", "v_nested", "SELECT * FROM public.v_direct", "public.v_direct"),
		tableView("public", "v_isolated", "SELECT 1"),
	)
	tgt := schemaWith(
		baseTableWithColumn("public", "base", "amount", "bigint"),
		tableView("public", "v_direct", "SELECT * FROM public.base", "public.base"),
		tableView("public", "v_nested", "SELECT * FROM public.v_direct", "public.v_direct"),
		tableView("public", "v_isolated", "SELECT 1"),
	)

	diff := CompareSchemas(src, tgt)
	names := make(map[string]bool)
	for _, view := range diff.ViewsAffected {
		names[view.Name] = true
	}
	if !names["v_direct"] || !names["v_nested"] {
		t.Fatalf("expected v_direct and v_nested in closure, got %v", names)
	}
	if names["v_isolated"] {
		t.Fatalf("v_isolated must not be affected, got %v", names)
	}
	// 返回的是新态（target）侧模型。
	for _, view := range diff.ViewsAffected {
		if view != tgt.Tables[view.Name] {
			t.Fatalf("expected target-side model for %s", view.Name)
		}
	}
}

func TestCollectAffectedViewsExcludesChangedViews(t *testing.T) {
	src := schemaWith(
		baseTableWithColumn("public", "base", "amount", "integer"),
		tableView("public", "v_changed", "SELECT amount + 1 AS v FROM public.base", "public.base"),
		tableView("public", "v_dependent", "SELECT * FROM public.v_changed", "public.v_changed"),
	)
	tgt := schemaWith(
		baseTableWithColumn("public", "base", "amount", "bigint"),
		tableView("public", "v_changed", "SELECT amount + 2 AS v FROM public.base", "public.base"),
		tableView("public", "v_dependent", "SELECT * FROM public.v_changed", "public.v_changed"),
	)

	diff := CompareSchemas(src, tgt)
	var viewChange bool
	for _, tblDiff := range diff.TablesModified {
		if tblDiff.Table != nil && tblDiff.Table.Name == "v_changed" && tblDiff.ViewDefinitionChange != nil {
			viewChange = true
		}
	}
	if !viewChange {
		t.Fatalf("expected v_changed to appear as view definition change, got %+v", diff.TablesModified)
	}
	for _, view := range diff.ViewsAffected {
		if view.Name == "v_changed" {
			t.Fatal("definition-changed view must be handled by ViewDefinitionChange, not ViewsAffected")
		}
	}
	var found bool
	for _, view := range diff.ViewsAffected {
		if view.Name == "v_dependent" {
			found = true
		}
	}
	if !found {
		t.Fatal("v_dependent depends on a changed view and must bounce")
	}
}

func TestCollectAffectedViewsUnqualifiedDependency(t *testing.T) {
	src := schemaWith(
		baseTableWithColumn("public", "base", "amount", "integer"),
		tableView("public", "v_short", "SELECT * FROM base", "base"),
	)
	tgt := schemaWith(
		baseTableWithColumn("public", "base", "amount", "bigint"),
		tableView("public", "v_short", "SELECT * FROM base", "base"),
	)

	diff := CompareSchemas(src, tgt)
	if len(diff.ViewsAffected) != 1 || diff.ViewsAffected[0].Name != "v_short" {
		t.Fatalf("expected unqualified dependency to resolve, got %+v", diff.ViewsAffected)
	}
}

func TestCollectAffectedViewsNoBreakers(t *testing.T) {
	src := schemaWith(tableView("public", "v1", "SELECT 1"))
	tgt := schemaWith(tableView("public", "v1", "SELECT 1"))
	diff := CompareSchemas(src, tgt)
	if len(diff.ViewsAffected) != 0 {
		t.Fatalf("expected no affected views, got %+v", diff.ViewsAffected)
	}
}
