package sql

import (
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

func checkedTable(schema, name string) *conn.Table {
	return &conn.Table{
		Name:   name,
		Schema: schema,
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint", Position: 1, Nullable: false},
			"flag": {Name: "flag", DataType: "integer", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Name: name + "_pkey", Columns: []string{"id"}},
	}
}

// C11 验收（PG）：CHECK 增/删/改生成对应语句，删除（含 modified 旧约束）
// 先于新增执行；非 PG 方言不支持时不产生语句。
func TestGenerateSchemaSQLCheckConstraintLifecycle(t *testing.T) {
	tbl := checkedTable("public", "air_inst")
	src := &diff.SchemaDiff{
		TablesModified: []*diff.TableDiff{{
			SourceTable: tbl,
			TargetTable: tbl,
			Table:       tbl,
			ChecksDropped: []*conn.CheckConstraint{
				{Name: "air_inst_old_check", Definition: "CHECK ((flag >= 0))"},
			},
			ChecksModified: []*diff.CheckConstraintDiff{{
				Old: &conn.CheckConstraint{Name: "air_inst_range_check", Definition: "CHECK ((flag >= 0))"},
				New: &conn.CheckConstraint{Name: "air_inst_range_check", Definition: "CHECK ((flag >= -1))"},
			}},
			ChecksAdded: []*conn.CheckConstraint{
				{Name: "air_inst_delete_flag_check", Definition: "CHECK ((delete_flag IN (0, 1)))"},
			},
		}},
	}

	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		`ALTER TABLE "public"."air_inst" DROP CONSTRAINT IF EXISTS "air_inst_old_check";`:                              false,
		`ALTER TABLE "public"."air_inst" DROP CONSTRAINT IF EXISTS "air_inst_range_check";`:                            false,
		`ALTER TABLE "public"."air_inst" ADD CONSTRAINT "air_inst_range_check" CHECK ((flag >= -1));`:                  false,
		`ALTER TABLE "public"."air_inst" ADD CONSTRAINT "air_inst_delete_flag_check" CHECK ((delete_flag IN (0, 1)));`: false,
	}
	for _, statement := range statements {
		if _, ok := want[statement]; ok {
			want[statement] = true
		}
	}
	for statement, found := range want {
		if !found {
			t.Fatalf("missing statement %q in %v", statement, statements)
		}
	}
	dropOld := indexOfContaining(statements, `DROP CONSTRAINT IF EXISTS "air_inst_range_check"`)
	addNew := indexOfContaining(statements, `ADD CONSTRAINT "air_inst_range_check"`)
	if dropOld > addNew {
		t.Fatalf("modified check must drop the old definition before adding the new one: %v", statements)
	}

	// 回滚对称：反向 diff 生成镜像的 ADD / DROP。
	rollbackDiff := &diff.SchemaDiff{
		TablesModified: []*diff.TableDiff{{
			SourceTable: tbl,
			TargetTable: tbl,
			Table:       tbl,
			ChecksAdded: []*conn.CheckConstraint{
				{Name: "air_inst_old_check", Definition: "CHECK ((flag >= 0))"},
			},
		}},
	}
	rollbackStatements, err := GenerateSchemaSQLSafe(rollbackDiff, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStatement(rollbackStatements, `ADD CONSTRAINT "air_inst_old_check"`) {
		t.Fatalf("rollback must re-add the old check: %v", rollbackStatements)
	}
	if containsStatement(rollbackStatements, `DROP CONSTRAINT IF EXISTS "air_inst_old_check"`) {
		t.Fatalf("rollback must not drop a constraint the target never had: %v", rollbackStatements)
	}
}

// C11 验收（MySQL 8 同源）：check_clause 裸表达式由生成层补 CHECK 包裹。
func TestGenerateSchemaSQLCheckConstraintMySQL(t *testing.T) {
	tbl := checkedTable("datasmith", "air_inst")
	src := &diff.SchemaDiff{
		TablesModified: []*diff.TableDiff{{
			SourceTable: tbl,
			TargetTable: tbl,
			Table:       tbl,
			ChecksDropped: []*conn.CheckConstraint{
				{Name: "air_inst_old_check", Definition: "`flag` >= 0"},
			},
			ChecksAdded: []*conn.CheckConstraint{
				{Name: "air_inst_flag_check", Definition: "`flag` <> 3"},
			},
		}},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypeMySQL)
	if err != nil {
		t.Fatal(err)
	}
	dropIdx := indexOfContaining(statements, "DROP CONSTRAINT `air_inst_old_check`")
	addIdx := indexOfContaining(statements, "ADD CONSTRAINT `air_inst_flag_check` CHECK (`flag` <> 3)")
	if dropIdx < 0 || addIdx < 0 {
		t.Fatalf("missing MySQL check statements: %v", statements)
	}
	if dropIdx > addIdx {
		t.Fatalf("drop must precede add: %v", statements)
	}
}

// C11：新增表携带内联 CHECK（建表即有约束，无需二次 ALTER）。
// 注意方言口径：PG 定义带 CHECK 包裹，MySQL check_clause 是裸表达式。
func TestGenerateSchemaSQLAddedTableCarriesInlineChecks(t *testing.T) {
	pgTable := checkedTable("public", "new_table")
	pgTable.Checks = map[string]*conn.CheckConstraint{
		"new_table_flag_check": {Name: "new_table_flag_check", Definition: "CHECK ((flag >= 0))"},
	}
	statements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{TablesAdded: []*conn.Table{pgTable}}, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStatement(statements, `CONSTRAINT "new_table_flag_check" CHECK ((flag >= 0))`) {
		t.Fatalf("CREATE TABLE must carry the inline CHECK: %v", statements)
	}

	mysqlTable := checkedTable("datasmith", "new_table")
	mysqlTable.Checks = map[string]*conn.CheckConstraint{
		"new_table_flag_check": {Name: "new_table_flag_check", Definition: "`flag` >= 0"},
	}
	mysqlStatements, err := GenerateSchemaSQLSafe(&diff.SchemaDiff{TablesAdded: []*conn.Table{mysqlTable}}, consts.DBTypeMySQL)
	if err != nil {
		t.Fatal(err)
	}
	if !containsStatement(mysqlStatements, "CONSTRAINT `new_table_flag_check` CHECK (`flag` >= 0)") {
		t.Fatalf("MySQL CREATE TABLE must carry the inline CHECK: %v", mysqlStatements)
	}
}

// C13：与 airedge 实测同形态的差异——主键背书索引名不同——diff 层不再计入
// 修改后，即使手工构造该 TableDiff，生成层也必须产出 0 条语句（F1 兜底）。
func TestGenerateSchemaSQLPrimaryKeyBackingIndexRenameYieldsNoStatements(t *testing.T) {
	tbl := checkedTable("public", "air_inst_checklist_item")
	src := &diff.SchemaDiff{
		TablesModified: []*diff.TableDiff{{
			SourceTable: tbl,
			TargetTable: tbl,
			Table:       tbl,
			IndexesDropped: []*conn.Index{
				{Name: "air_inst_checklist_item_pkey", Primary: true, Unique: true, Columns: []string{"id"}},
			},
			IndexesAdded: []*conn.Index{
				{Name: "air_inst_checklist_item_rev_copy1_pkey", Primary: true, Unique: true, Columns: []string{"id"}},
			},
		}},
	}
	statements, err := GenerateSchemaSQLSafe(src, consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 0 {
		t.Fatalf("backing-index rename must generate no statements, got %v", statements)
	}
}

// C12 验收（PG）：仅注释差异的视图生成 COMMENT ON VIEW，而非整组删建；
// 回滚对称（恢复为空注释）。
func TestGenerateSchemaSQLViewCommentChangeEmitsCommentOnly(t *testing.T) {
	view := func(comment string) *conn.Table {
		v := tableViewDef("public", "v_report", `SELECT "id" FROM "public"."base"`, "public.base")
		v.Comment = comment
		return v
	}
	srcSchema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"v_report": view("")}}
	tgtSchema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"v_report": view("报表视图")}}

	forward, err := GenerateSchemaSQLSafe(diff.CompareSchemas(srcSchema, tgtSchema), consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if len(forward) != 1 || forward[0] != `COMMENT ON VIEW "public"."v_report" IS '报表视图';` {
		t.Fatalf("comment-only diff must emit exactly one COMMENT statement, got %v", forward)
	}
	if containsStatement(forward, "DROP VIEW") || containsStatement(forward, "CREATE VIEW") {
		t.Fatalf("comment-only diff must not rebuild the view: %v", forward)
	}

	rollback, err := GenerateSchemaSQLSafe(diff.CompareSchemas(tgtSchema, srcSchema), consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	if len(rollback) != 1 || rollback[0] != `COMMENT ON VIEW "public"."v_report" IS '';` {
		t.Fatalf("rollback must restore the empty comment, got %v", rollback)
	}
}

func TestGenerateSchemaSQLViewDefinitionAndCommentChangeRebuildsBeforeComment(t *testing.T) {
	view := func(definition, comment string) *conn.Table {
		v := tableViewDef("public", "v_report", definition, "public.base")
		v.Comment = comment
		return v
	}
	src := &conn.DatabaseSchema{Tables: map[string]*conn.Table{
		"v_report": view(`SELECT "id" FROM "public"."base"`, "旧注释"),
	}}
	tgt := &conn.DatabaseSchema{Tables: map[string]*conn.Table{
		"v_report": view(`SELECT "id", "name" FROM "public"."base"`, "新注释"),
	}}

	statements, err := GenerateSchemaSQLSafe(diff.CompareSchemas(src, tgt), consts.DBTypePostgres)
	if err != nil {
		t.Fatal(err)
	}
	drop := indexOfContaining(statements, `DROP VIEW "public"."v_report"`)
	create := indexOfContaining(statements, `CREATE VIEW "public"."v_report" AS`)
	comment := -1
	for i := len(statements) - 1; i >= 0; i-- {
		if strings.Contains(statements[i], `COMMENT ON VIEW "public"."v_report" IS '新注释'`) {
			comment = i
			break
		}
	}
	if drop < 0 || create < 0 || comment < 0 {
		t.Fatalf("missing view rebuild/comment statements: %v", statements)
	}
	if !(drop < create && create < comment) {
		t.Fatalf("view definition must rebuild before its new comment is restored: %v", statements)
	}
}

// C12：依赖闭包弹跳重建的视图在 CREATE VIEW 时携带注释，重建不丢注释。
func TestGenerateViewDDLKeepsTableCommentOnRebuild(t *testing.T) {
	dialect := NewDialect(consts.DBTypePostgres)
	view := tableViewDef("public", "v_report", `SELECT 1`)
	view.Comment = "报表视图"
	ddl := dialect.GenerateViewDDL(view)
	if !strings.Contains(ddl, `COMMENT ON VIEW "public"."v_report" IS '报表视图';`) {
		t.Fatalf("rebuilt view must carry its comment: %s", ddl)
	}
}
