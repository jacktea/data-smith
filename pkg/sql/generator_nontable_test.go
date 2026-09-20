package sql

import (
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

func nontableTable(schema, name string) *conn.Table {
	return &conn.Table{
		Name:   name,
		Schema: schema,
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"id": {Name: "id", DataType: "bigint", Position: 1, Nullable: false},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
}

func TestGenerateSchemaSQLRoutineLifecycle(t *testing.T) {
	oldDef := "CREATE OR REPLACE FUNCTION public.lookup_key(text) RETURNS integer LANGUAGE sql AS $f$SELECT 1$f$"
	newDef := "CREATE OR REPLACE FUNCTION public.lookup_key(text) RETURNS integer LANGUAGE sql AS $f$SELECT 2$f$"
	procedure := "CREATE OR REPLACE PROCEDURE public.p_audit(text) LANGUAGE plpgsql AS $p$BEGIN END$p$"

	src := &diff.SchemaDiff{
		RoutinesDropped: []*conn.Routine{
			{Name: "obsolete", Schema: "public", Kind: conn.RoutineKindFunction, IdentityArgs: "integer, text"},
		},
		RoutinesModified: []*diff.RoutineDiff{
			{
				Old: &conn.Routine{Name: "lookup_key", Schema: "public", IdentityArgs: "text", Definition: oldDef},
				New: &conn.Routine{Name: "lookup_key", Schema: "public", IdentityArgs: "text", Definition: newDef},
			},
		},
		RoutinesAdded: []*conn.Routine{
			{Name: "p_audit", Schema: "public", Kind: conn.RoutineKindProcedure, IdentityArgs: "text", Definition: procedure},
		},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 3 {
		t.Fatalf("expected 3 statements, got %d: %v", len(statements), statements)
	}
	if statements[0] != "DROP FUNCTION IF EXISTS \"public\".\"obsolete\"(integer, text) CASCADE;" {
		t.Fatalf("unexpected drop: %q", statements[0])
	}
	if !containsExactStatement(statements, procedure+";") {
		t.Fatalf("added procedure must be applied with trailing semicolon, got %v", statements)
	}
	if !containsExactStatement(statements, newDef+";") {
		t.Fatalf("modified routine must be re-applied with trailing semicolon, got %v", statements)
	}
}

func TestGenerateSchemaSQLSequenceLifecycle(t *testing.T) {
	oldSeq := &conn.Sequence{Name: "air_seq", Schema: "public", DataType: "bigint", StartValue: "1", IncrementBy: "1", MinValue: "1", MaxValue: "1000", Cycle: false, CacheSize: "1"}
	newSeq := &conn.Sequence{Name: "air_seq", Schema: "public", DataType: "bigint", StartValue: "1", IncrementBy: "5", MinValue: "1", MaxValue: "9223372036854775807", Cycle: true, CacheSize: "4"}
	src := &diff.SchemaDiff{
		SequencesDropped: []*conn.Sequence{
			{Name: "dead_seq", Schema: "public", DataType: "integer", StartValue: "1", IncrementBy: "1", MinValue: "1", MaxValue: "10"},
		},
		SequencesAdded: []*conn.Sequence{
			{Name: "new_seq", Schema: "public", DataType: "integer", StartValue: "100", IncrementBy: "1", MinValue: "1", MaxValue: "10", Cycle: true, CacheSize: "2"},
		},
		SequencesModified: []*diff.SequenceDiff{{Old: oldSeq, New: newSeq}},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`DROP SEQUENCE IF EXISTS "public"."dead_seq" CASCADE;`,
		`CREATE SEQUENCE "public"."new_seq" AS integer START WITH 100 INCREMENT BY 1 MINVALUE 1 MAXVALUE 10 CACHE 2 CYCLE;`,
		`ALTER SEQUENCE "public"."air_seq" INCREMENT BY 5 MAXVALUE 9223372036854775807 CACHE 4 CYCLE;`,
	}
	if len(statements) != len(want) {
		t.Fatalf("expected %d statements, got %v", len(want), statements)
	}
	for _, expected := range want {
		if !containsExactStatement(statements, expected) {
			t.Fatalf("missing %q in %v", expected, statements)
		}
	}
}

func TestGenerateSchemaSQLSequenceOwnershipForwardAndRollback(t *testing.T) {
	owned := &conn.Sequence{Name: "orders_id_seq", Schema: "public", OwnedBy: "orders.id"}
	standalone := &conn.Sequence{Name: "orders_id_seq", Schema: "public"}

	forward, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		SequencesModified: []*diff.SequenceDiff{{Old: owned, New: standalone}},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	wantForward := `ALTER SEQUENCE "public"."orders_id_seq" OWNED BY NONE;`
	if len(forward) != 1 || forward[0] != wantForward {
		t.Fatalf("ownership removal:\n got %v\nwant [%s]", forward, wantForward)
	}

	rollback, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		SequencesModified: []*diff.SequenceDiff{{Old: standalone, New: owned}},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	wantRollback := `ALTER SEQUENCE "public"."orders_id_seq" OWNED BY "public"."orders"."id";`
	if len(rollback) != 1 || rollback[0] != wantRollback {
		t.Fatalf("ownership restoration:\n got %v\nwant [%s]", rollback, wantRollback)
	}
}

func TestGenerateSchemaSQLCreatesOwnedSequenceAroundOwningTable(t *testing.T) {
	table := nontableTable("public", "orders")
	table.Columns["id"].Default = strPtr("nextval('public.orders_id_seq'::regclass)")
	sequence := &conn.Sequence{
		Name: "orders_id_seq", Schema: "public", DataType: "bigint",
		StartValue: "1", IncrementBy: "1", MinValue: "1",
		MaxValue: "9223372036854775807", CacheSize: "1", OwnedBy: "orders.id",
	}

	statements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesAdded:    []*conn.Table{table},
		SequencesAdded: []*conn.Sequence{sequence},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	createSequence := indexOfContaining(statements, `CREATE SEQUENCE "public"."orders_id_seq"`)
	createTable := indexOfContaining(statements, `CREATE TABLE "public"."orders"`)
	attachOwnership := indexOfContaining(statements, `ALTER SEQUENCE "public"."orders_id_seq" OWNED BY "public"."orders"."id";`)
	if createSequence < 0 || createTable < 0 || attachOwnership < 0 {
		t.Fatalf("missing owned-sequence lifecycle statements: %v", statements)
	}
	if !(createSequence < createTable && createTable < attachOwnership) {
		t.Fatalf("owned sequence must be created before its table and attached afterwards: %v", statements)
	}
}

func TestGenerateSchemaSQLCreatesTableBeforeRoutineReturningItsCompositeType(t *testing.T) {
	table := nontableTable("public", "customer")
	routine := &conn.Routine{
		Name: "find_customer", Schema: "public", Kind: conn.RoutineKindFunction,
		IdentityArgs: "bigint",
		Definition:   "CREATE OR REPLACE FUNCTION public.find_customer(bigint) RETURNS public.customer LANGUAGE sql AS $f$SELECT * FROM public.customer WHERE id = $1$f$",
	}

	statements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesAdded:   []*conn.Table{table},
		RoutinesAdded: []*conn.Routine{routine},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	tableIndex := indexOfContaining(statements, `CREATE TABLE "public"."customer"`)
	routineIndex := indexOfContaining(statements, "CREATE OR REPLACE FUNCTION public.find_customer")
	if tableIndex < 0 || routineIndex < 0 || tableIndex > routineIndex {
		t.Fatalf("table composite type must exist before the routine: %v", statements)
	}
}

func TestGenerateSchemaSQLOrdersRoutineDependencies(t *testing.T) {
	callee := &conn.Routine{
		Name: "z_callee", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.z_callee() RETURNS integer LANGUAGE sql AS $f$SELECT 1$f$",
	}
	caller := &conn.Routine{
		Name: "a_caller", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.a_caller() RETURNS integer LANGUAGE plpgsql AS $f$BEGIN RETURN public.z_callee(); END$f$",
	}

	statements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		RoutinesAdded: []*conn.Routine{caller, callee},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	calleeIndex := indexOfContaining(statements, "CREATE OR REPLACE FUNCTION public.z_callee")
	callerIndex := indexOfContaining(statements, "CREATE OR REPLACE FUNCTION public.a_caller")
	if calleeIndex < 0 || callerIndex < 0 || calleeIndex > callerIndex {
		t.Fatalf("callee must be created before caller: %v", statements)
	}
}

func TestGenerateSchemaSQLDoesNotTreatOverloadedDeclarationsAsCalls(t *testing.T) {
	routines := []*conn.Routine{
		{
			Name: "normalize", Schema: "public", Kind: conn.RoutineKindFunction, IdentityArgs: "integer",
			Definition: "CREATE OR REPLACE FUNCTION public.normalize(integer) RETURNS integer LANGUAGE sql AS $f$SELECT $1$f$",
		},
		{
			Name: "normalize", Schema: "public", Kind: conn.RoutineKindFunction, IdentityArgs: "text",
			Definition: "CREATE OR REPLACE FUNCTION public.normalize(text) RETURNS text LANGUAGE sql AS $f$SELECT $1$f$",
		},
	}
	statements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{RoutinesAdded: routines}, consts.DBTypePostgres)
	if err != nil {
		t.Fatalf("overloaded declarations without cross-calls must be orderable: %v", err)
	}
	if len(statements) != 2 {
		t.Fatalf("expected both overloads, got %v", statements)
	}
}

func TestGenerateSchemaSQLRejectsRoutineDependencyCycleWithObjectChain(t *testing.T) {
	alpha := &conn.Routine{
		Name: "alpha", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.alpha() RETURNS integer LANGUAGE sql AS $f$SELECT public.beta()$f$",
	}
	beta := &conn.Routine{
		Name: "beta", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.beta() RETURNS integer LANGUAGE sql AS $f$SELECT public.alpha()$f$",
	}

	_, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{RoutinesAdded: []*conn.Routine{alpha, beta}}, consts.DBTypePostgres)
	if err == nil {
		t.Fatal("expected routine dependency cycle to be rejected")
	}
	message := err.Error()
	for _, want := range []string{"schema object dependency cycle", "routine:public.alpha()", "routine:public.beta()", " -> "} {
		if !strings.Contains(message, want) {
			t.Fatalf("cycle error must contain an actionable object chain, got %q", message)
		}
	}
}

func TestGenerateSchemaSQLRejectsUnresolvedDynamicRoutineDependency(t *testing.T) {
	table := nontableTable("public", "audit_log")
	routine := &conn.Routine{
		Name: "write_audit", Schema: "public", Kind: conn.RoutineKindProcedure,
		Definition: "CREATE OR REPLACE PROCEDURE public.write_audit() LANGUAGE plpgsql AS $f$BEGIN EXECUTE 'INSERT INTO public.audit_log DEFAULT VALUES'; END$f$",
	}

	_, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesAdded:   []*conn.Table{table},
		RoutinesAdded: []*conn.Routine{routine},
	}, consts.DBTypePostgres)
	if err == nil {
		t.Fatal("expected dynamic SQL dependency extraction to be rejected")
	}
	if message := err.Error(); !strings.Contains(message, "routine:public.write_audit() -> unresolved dynamic SQL EXECUTE") {
		t.Fatalf("unexpected error: %q", message)
	}
}

func TestGenerateSchemaSQLRejectsRoutineLanguageWithoutReliableDependencyExtraction(t *testing.T) {
	routine := &conn.Routine{
		Name: "native_lookup", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.native_lookup() RETURNS integer LANGUAGE c AS 'native_lookup'",
	}
	_, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesAdded:   []*conn.Table{nontableTable("public", "lookup_data")},
		RoutinesAdded: []*conn.Routine{routine},
	}, consts.DBTypePostgres)
	if err == nil {
		t.Fatal("expected unsupported routine language to be rejected")
	}
	if message := err.Error(); !strings.Contains(message, "routine:public.native_lookup() -> unsupported dependency extraction language c") {
		t.Fatalf("unexpected error: %q", message)
	}
}

func TestGenerateSchemaSQLOrdersViewRoutineAndTableInBothDirections(t *testing.T) {
	table := nontableTable("public", "ledger")
	routine := &conn.Routine{
		Name: "ledger_count", Schema: "public", Kind: conn.RoutineKindFunction,
		Definition: "CREATE OR REPLACE FUNCTION public.ledger_count() RETURNS bigint LANGUAGE plpgsql AS $f$DECLARE result bigint; BEGIN SELECT count(*) INTO result FROM public.ledger; RETURN result; END$f$",
	}
	view := tableViewDef("public", "ledger_summary",
		"SELECT public.ledger_count() AS total FROM public.ledger", "public.ledger")

	create, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesAdded:   []*conn.Table{view, table},
		RoutinesAdded: []*conn.Routine{routine},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	createTable := indexOfContaining(create, `CREATE TABLE "public"."ledger"`)
	createRoutine := indexOfContaining(create, "CREATE OR REPLACE FUNCTION public.ledger_count")
	createView := indexOfContaining(create, `CREATE VIEW "public"."ledger_summary"`)
	if !(createTable >= 0 && createTable < createRoutine && createRoutine < createView) {
		t.Fatalf("create order must be table -> routine -> view: %v", create)
	}

	drop, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{
		TablesDropped:   []*conn.Table{table, view},
		RoutinesDropped: []*conn.Routine{routine},
	}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	dropView := indexOfContaining(drop, `DROP VIEW "public"."ledger_summary"`)
	dropRoutine := indexOfContaining(drop, `DROP FUNCTION IF EXISTS "public"."ledger_count"`)
	dropTable := indexOfContaining(drop, `DROP TABLE "public"."ledger"`)
	if !(dropView >= 0 && dropView < dropRoutine && dropRoutine < dropTable) {
		t.Fatalf("drop order must be view -> routine -> table: %v", drop)
	}
}

// C1 验收：序列/例程先于表 DDL，列默认值依赖（nextval、函数）必须可解析。
func TestGenerateSchemaSQLSequencesAndRoutinesBeforeTableDDL(t *testing.T) {
	tbl := nontableTable("public", "air_ws_items")
	tbl.Columns["code"] = &conn.Column{
		Name: "code", DataType: "bigint", Position: 2, Nullable: false,
		Default: strPtr("nextval('air_ws_items_seq'::regclass)"),
	}
	tbl.Columns["sort_key"] = &conn.Column{
		Name: "sort_key", DataType: "integer", Position: 3, Nullable: false,
		Default: strPtr("generate_lookup_number_sort_key(1)"),
	}
	seq := &conn.Sequence{Name: "air_ws_items_seq", Schema: "public", DataType: "bigint", StartValue: "1", IncrementBy: "1", MinValue: "1", MaxValue: "9223372036854775807", CacheSize: "1"}
	fn := &conn.Routine{Name: "generate_lookup_number_sort_key", Schema: "public", IdentityArgs: "integer", Definition: "CREATE OR REPLACE FUNCTION public.generate_lookup_number_sort_key(integer) RETURNS integer LANGUAGE sql AS $f$SELECT $1$f$"}
	src := &diff.SchemaDiff{
		SequencesAdded: []*conn.Sequence{seq},
		RoutinesAdded:  []*conn.Routine{fn},
		TablesAdded:    []*conn.Table{tbl},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	seqIdx := indexOfContaining(statements, "CREATE SEQUENCE ")
	fnIdx := indexOfContaining(statements, "CREATE OR REPLACE FUNCTION public.generate_lookup_number_sort_key")
	tblIdx := indexOfContaining(statements, "CREATE TABLE ")
	if seqIdx < 0 || fnIdx < 0 || tblIdx < 0 {
		t.Fatalf("missing statements: %v", statements)
	}
	if seqIdx > tblIdx || fnIdx > tblIdx {
		t.Fatalf("sequences/routines must precede table DDL: %v", statements)
	}
}

// C2 验收：定义未变但依赖被变更表的视图先 DROP（逆拓扑）后 CREATE（拓扑），
// 且全部发生在表 DDL 之前/之后。
func TestGenerateSchemaSQLAffectedViewsBounceAroundTableDDL(t *testing.T) {
	baseSrc := nontableTable("public", "base")
	baseSrc.Columns["amount"] = &conn.Column{Name: "amount", DataType: "integer", Position: 2}
	baseTgt := nontableTable("public", "base")
	baseTgt.Columns["amount"] = &conn.Column{Name: "amount", DataType: "bigint", Position: 2}

	outer := tableViewDef("public", "v_outer", "SELECT * FROM \"public\".\"v_inner\"", "public.v_inner")
	inner := tableViewDef("public", "v_inner", "SELECT * FROM \"public\".\"base\"", "public.base")

	srcSchema := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{"base": baseSrc, "v_inner": inner, "v_outer": outer},
	}
	tgtSchema := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{"base": baseTgt, "v_inner": inner, "v_outer": outer},
	}
	srcDiff := diff.CompareSchemas(srcSchema, tgtSchema)
	statements, err := GenerateSchemaSQLSafe(srcDiff, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	dropOuter := indexOfContaining(statements, `DROP VIEW "public"."v_outer" CASCADE;`)
	dropInner := indexOfContaining(statements, `DROP VIEW "public"."v_inner" CASCADE;`)
	alterIdx := indexOfContaining(statements, "ALTER TABLE")
	createInner := indexOfContaining(statements, `CREATE VIEW "public"."v_inner"`)
	createOuter := indexOfContaining(statements, `CREATE VIEW "public"."v_outer"`)
	for _, idx := range []int{dropOuter, dropInner, alterIdx, createInner, createOuter} {
		if idx < 0 {
			t.Fatalf("missing statement: %v", statements)
		}
	}
	if !(dropOuter < dropInner && dropInner < alterIdx && alterIdx < createInner && createInner < createOuter) {
		t.Fatalf("view bounce ordering wrong: %v", statements)
	}

	// 回滚对称：从 tgt 回到 src，同样可直接执行。
	rollbackDiff := diff.CompareSchemas(tgtSchema, srcSchema)
	rollbackStatements, err := GenerateSchemaSQLSafe(rollbackDiff, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if len(rollbackStatements) == 0 || !containsStatement(rollbackStatements, "ALTER TABLE") {
		t.Fatalf("rollback should alter the column back: %v", rollbackStatements)
	}
}

// 主键背书索引的生命周期跟随约束: 删除主键时不得再生成 DROP INDEX。
func TestGenerateSchemaSQLSkipsPrimaryKeyBackingIndexDrop(t *testing.T) {
	tbl := nontableTable("public", "events")
	tbl.PrimaryKey = &conn.PrimaryKey{Name: "events_pkey", Columns: []string{"id"}}
	tbl.Indexes = map[string]*conn.Index{
		"events_pkey": {Name: "events_pkey", Primary: true, Unique: true, Columns: []string{"id"}},
		"events_kind": {Name: "events_kind", Columns: []string{"kind"}},
	}
	src := &diff.SchemaDiff{
		TablesModified: []*diff.TableDiff{{
			SourceTable:      tbl,
			TargetTable:      tbl,
			Table:            tbl,
			PrimaryKeyChange: &diff.PrimaryKeyDiff{Old: tbl.PrimaryKey, New: nil},
			IndexesDropped:   []*conn.Index{tbl.Indexes["events_pkey"], tbl.Indexes["events_kind"]},
		}},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if got := indexOfContaining(statements, `DROP INDEX "public"."events_pkey"`); got >= 0 {
		t.Fatalf("primary-key backing index must not be dropped directly: %v", statements)
	}
	if got := indexOfContaining(statements, `DROP INDEX "public"."events_kind"`); got < 0 {
		t.Fatalf("ordinary index drop is still required: %v", statements)
	}
	if got := indexOfContaining(statements, `DROP CONSTRAINT "events_pkey"`); got < 0 {
		t.Fatalf("primary key constraint drop is required: %v", statements)
	}
}

func tableViewDef(schema, name, definition string, deps ...string) *conn.Table {
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

func strPtr(s string) *string { return &s }

func indexOfContaining(statements []string, needle string) int {
	for i, statement := range statements {
		if strings.Contains(statement, needle) {
			return i
		}
	}
	return -1
}

func containsStatement(statements []string, needle string) bool {
	return indexOfContaining(statements, needle) >= 0
}

func containsExactStatement(statements []string, expected string) bool {
	for _, statement := range statements {
		if statement == expected {
			return true
		}
	}
	return false
}
