//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

// TestCheckConstraintsAndViewCommentsRoundTrip C11+C12 验收（PostgreSQL）：
// 内联 CHECK 约束与视图注释参与结构比对——产物含 ADD CONSTRAINT ... CHECK 与
// COMMENT ON VIEW（注释差异不触发视图删建）；正向执行后闭环为空；回滚对称。
func TestCheckConstraintsAndViewCommentsRoundTrip(t *testing.T) {
	requireIntegration(t)

	fixture := postgresFixture(t)
	fixture.setupSource = []string{
		`CREATE TABLE "air_inst" ("id" BIGINT NOT NULL PRIMARY KEY, "flag" INTEGER NOT NULL)`,
		`INSERT INTO "air_inst" VALUES (1, 1)`,
		`CREATE VIEW "v_report" AS SELECT "id", "flag" FROM "air_inst"`,
	}
	fixture.setupTarget = []string{
		`CREATE TABLE "air_inst" ("id" BIGINT NOT NULL PRIMARY KEY, "flag" INTEGER NOT NULL, CONSTRAINT "air_inst_flag_check" CHECK ("flag" >= 0))`,
		`INSERT INTO "air_inst" VALUES (1, 1)`,
		`CREATE VIEW "v_report" AS SELECT "id", "flag" FROM "air_inst"`,
		`COMMENT ON VIEW "v_report" IS '报表视图'`,
	}
	prepareFixture(t, fixture)
	defer cleanupFixture(t, fixture)

	ctx := context.Background()
	srcAdapter, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcAdapter.Close()
	tgtAdapter, err := db.NewDBAdapterContext(ctx, &fixture.target)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer tgtAdapter.Close()

	originalSchema, err := srcAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}
	tgtSchema, err := tgtAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read target schema: %v", err)
	}

	// C11：提取层拿到 CHECK 定义。
	srcTable := originalSchema.GetTable("air_inst")
	if srcTable == nil || len(srcTable.Checks) != 0 {
		t.Fatalf("source table must have no checks, got %+v", srcTable)
	}
	tgtTable := tgtSchema.GetTable("air_inst")
	if tgtTable == nil || len(tgtTable.Checks) != 1 || tgtTable.Checks["air_inst_flag_check"] == nil {
		t.Fatalf("target table must carry air_inst_flag_check, got %+v", tgtTable)
	}
	// C12：视图注释随 obj_description 提取。
	if view := tgtSchema.GetTable("v_report"); view == nil || view.Comment != "报表视图" {
		t.Fatalf("target view comment must be extracted, got %+v", view)
	}

	forwardDiff := pkgdiff.CompareSchemas(originalSchema, tgtSchema)
	if len(forwardDiff.TablesModified) != 2 {
		t.Fatalf("expected air_inst + v_report modified, got %+v", forwardDiff.TablesModified)
	}
	forwardStatements, err := pkgsql.GenerateSchemaSQLSafe(forwardDiff, consts.DBTypePostgres)
	if err != nil {
		t.Fatalf("generate forward SQL: %v", err)
	}
	if !containsStatementContaining(forwardStatements, `ADD CONSTRAINT "air_inst_flag_check" CHECK`) {
		t.Fatalf("forward must add the check constraint: %v", forwardStatements)
	}
	commentIdx := indexOfStatementContaining(forwardStatements, `COMMENT ON VIEW "DataSmith_App"."v_report" IS '报表视图';`)
	if commentIdx < 0 {
		t.Fatalf("forward must carry the view comment: %v", forwardStatements)
	}
	if containsStatementContaining(forwardStatements, "DROP VIEW") || containsStatementContaining(forwardStatements, "CREATE VIEW") {
		t.Fatalf("comment-only view diff must not rebuild the view: %v", forwardStatements)
	}

	srcConn := openConfig(t, fixture.source)
	for _, statement := range forwardStatements {
		if _, err := srcConn.Exec(statement); err != nil {
			t.Fatalf("execute forward statement %q: %v", statement, err)
		}
	}

	// 收敛：正向执行后与 target 的结构比对为空（含 CHECK 与视图注释）。
	srcAdapter2, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("reconnect source: %v", err)
	}
	defer srcAdapter2.Close()
	afterForward, err := srcAdapter2.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema after forward: %v", err)
	}
	if remaining := nonEmptyDiffParts(pkgdiff.CompareSchemas(afterForward, tgtSchema)); len(remaining) > 0 {
		t.Fatalf("forward execution did not converge: %v", remaining)
	}

	// 回滚对称：恢复到无 CHECK、无视图注释的初始状态。
	rollbackStatements, err := pkgsql.GenerateSchemaSQLSafe(pkgdiff.CompareSchemas(tgtSchema, originalSchema), consts.DBTypePostgres)
	if err != nil {
		t.Fatalf("generate rollback SQL: %v", err)
	}
	if !containsStatementContaining(rollbackStatements, `DROP CONSTRAINT IF EXISTS "air_inst_flag_check"`) {
		t.Fatalf("rollback must drop the check constraint: %v", rollbackStatements)
	}
	if !containsStatementContaining(rollbackStatements, `COMMENT ON VIEW "DataSmith_App"."v_report"`) {
		t.Fatalf("rollback must reset the view comment: %v", rollbackStatements)
	}
	for _, statement := range rollbackStatements {
		if _, err := srcConn.Exec(statement); err != nil {
			t.Fatalf("execute rollback statement %q: %v", statement, err)
		}
	}
	srcAdapter3, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("reconnect source after rollback: %v", err)
	}
	defer srcAdapter3.Close()
	afterRollback, err := srcAdapter3.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema after rollback: %v", err)
	}
	if remaining := nonEmptyDiffParts(pkgdiff.CompareSchemas(afterRollback, originalSchema)); len(remaining) > 0 {
		t.Fatalf("rollback did not restore original state: %v", remaining)
	}
}

// TestMySQLCheckConstraintsRoundTrip C11 验收（MySQL 8 同源）：内联命名
// CHECK 约束检出、ADD/DROP CONSTRAINT 语句可执行、闭环与回滚对称。
func TestMySQLCheckConstraintsRoundTrip(t *testing.T) {
	requireIntegration(t)

	fixture := mysqlFixture(t)
	fixture.setupSource = []string{
		"CREATE TABLE `t_item` (`id` BIGINT NOT NULL PRIMARY KEY, `flag` INT NOT NULL)",
	}
	fixture.setupTarget = []string{
		"CREATE TABLE `t_item` (`id` BIGINT NOT NULL PRIMARY KEY, `flag` INT NOT NULL, CONSTRAINT `t_item_flag_check` CHECK (`flag` >= 0))",
	}
	prepareFixture(t, fixture)
	defer cleanupFixture(t, fixture)

	ctx := context.Background()
	srcAdapter, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcAdapter.Close()
	tgtAdapter, err := db.NewDBAdapterContext(ctx, &fixture.target)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer tgtAdapter.Close()

	originalSchema, err := srcAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}
	tgtSchema, err := tgtAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read target schema: %v", err)
	}
	tgtTable := tgtSchema.GetTable("t_item")
	if tgtTable == nil || len(tgtTable.Checks) != 1 || tgtTable.Checks["t_item_flag_check"] == nil {
		t.Fatalf("target table must carry t_item_flag_check, got %+v", tgtTable)
	}

	forwardDiff := pkgdiff.CompareSchemas(originalSchema, tgtSchema)
	if len(forwardDiff.TablesModified) != 1 || len(forwardDiff.TablesModified[0].ChecksAdded) != 1 {
		t.Fatalf("expected check addition on t_item, got %+v", forwardDiff.TablesModified)
	}
	forwardStatements, err := pkgsql.GenerateSchemaSQLSafe(forwardDiff, consts.DBTypeMySQL)
	if err != nil {
		t.Fatalf("generate forward SQL: %v", err)
	}
	if !containsStatementContaining(forwardStatements, "ADD CONSTRAINT `t_item_flag_check` CHECK") {
		t.Fatalf("forward must add the check constraint: %v", forwardStatements)
	}

	srcConn := openConfig(t, fixture.source)
	for _, statement := range forwardStatements {
		if _, err := srcConn.Exec(statement); err != nil {
			t.Fatalf("execute forward statement %q: %v", statement, err)
		}
	}

	srcAdapter2, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("reconnect source: %v", err)
	}
	defer srcAdapter2.Close()
	afterForward, err := srcAdapter2.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema after forward: %v", err)
	}
	if remaining := nonEmptyDiffParts(pkgdiff.CompareSchemas(afterForward, tgtSchema)); len(remaining) > 0 {
		t.Fatalf("forward execution did not converge: %v", remaining)
	}

	rollbackStatements, err := pkgsql.GenerateSchemaSQLSafe(pkgdiff.CompareSchemas(tgtSchema, originalSchema), consts.DBTypeMySQL)
	if err != nil {
		t.Fatalf("generate rollback SQL: %v", err)
	}
	if !containsStatementContaining(rollbackStatements, "DROP CONSTRAINT `t_item_flag_check`") {
		t.Fatalf("rollback must drop the check constraint: %v", rollbackStatements)
	}
	for _, statement := range rollbackStatements {
		if _, err := srcConn.Exec(statement); err != nil {
			t.Fatalf("execute rollback statement %q: %v", statement, err)
		}
	}
	srcAdapter3, err := db.NewDBAdapterContext(ctx, &fixture.source)
	if err != nil {
		t.Fatalf("reconnect source after rollback: %v", err)
	}
	defer srcAdapter3.Close()
	afterRollback, err := srcAdapter3.ReadSchema()
	if err != nil {
		t.Fatalf("read source schema after rollback: %v", err)
	}
	if remaining := nonEmptyDiffParts(pkgdiff.CompareSchemas(afterRollback, originalSchema)); len(remaining) > 0 {
		t.Fatalf("rollback did not restore original state: %v", remaining)
	}
}

func indexOfStatementContaining(statements []string, needle string) int {
	for i, statement := range statements {
		if strings.Contains(statement, needle) {
			return i
		}
	}
	return -1
}

func containsStatementContaining(statements []string, needle string) bool {
	return indexOfStatementContaining(statements, needle) >= 0
}
