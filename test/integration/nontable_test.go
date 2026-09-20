//go:build integration

package integration_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	difflogic "github.com/jacktea/data-smith/internal/datasmith/diff"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

// TestPostgresNonTableObjectsRoundTrip 验证第 1 批能力在真实 PostgreSQL 上的
// 往返：C1 例程/序列参与结构比对且产物可执行；C2 视图依赖闭包先删后建使列
// 类型变更可直接执行；回滚对称。
func TestPostgresNonTableObjectsRoundTrip(t *testing.T) {
	requireIntegration(t)

	fixture := postgresFixture(t)
	fixture.setupSource = []string{
		`CREATE SEQUENCE old_seq START 1`,
		`CREATE TABLE "base" ("id" BIGINT NOT NULL PRIMARY KEY DEFAULT nextval('old_seq'), "amount" INTEGER NOT NULL)`,
		`INSERT INTO "base" ("id", "amount") VALUES (1, 10)`,
		`CREATE TABLE "orders" ("id" BIGINT NOT NULL PRIMARY KEY, "ref" TEXT NULL)`,
		`INSERT INTO "orders" ("id", "ref") VALUES (1, 'source')`,
		`CREATE OR REPLACE FUNCTION lookup_key(text) RETURNS integer LANGUAGE sql AS $f$ SELECT 1 $f$`,
		`CREATE VIEW "v_inner" AS SELECT b."id", b."amount" FROM "base" b`,
		`CREATE VIEW "v_outer" AS SELECT * FROM "v_inner"`,
	}
	fixture.setupTarget = []string{
		`CREATE SEQUENCE old_seq START 1`,
		`CREATE SEQUENCE air_ws_items_seq START 100`,
		`CREATE TABLE "base" ("id" BIGINT NOT NULL PRIMARY KEY DEFAULT nextval('old_seq'), "amount" BIGINT NOT NULL)`,
		`INSERT INTO "base" ("id", "amount") VALUES (1, 10)`,
		`CREATE TABLE "orders" ("id" BIGINT NOT NULL PRIMARY KEY, "ref" TEXT NULL)`,
		`INSERT INTO "orders" ("id", "ref") VALUES (1, 'source'), (2, 'added')`,
		`CREATE OR REPLACE FUNCTION lookup_key(text) RETURNS integer LANGUAGE sql AS $f$ SELECT 2 $f$`,
		`CREATE OR REPLACE FUNCTION has_revision(bigint) RETURNS boolean LANGUAGE sql AS $f$ SELECT $1 > 0 $f$`,
		`CREATE OR REPLACE PROCEDURE audit_proc(text) LANGUAGE plpgsql AS $p$ BEGIN END $p$`,
		`CREATE VIEW "v_inner" AS SELECT b."id", b."amount" FROM "base" b`,
		`CREATE VIEW "v_outer" AS SELECT * FROM "v_inner"`,
		`CREATE TABLE "late_table" ("id" BIGINT NOT NULL PRIMARY KEY, "note" TEXT NULL)`,
		`INSERT INTO "late_table" VALUES (1, 'late')`,
		`CREATE TABLE "with_serial" ("id" SERIAL PRIMARY KEY, "label" TEXT NOT NULL)`,
		`INSERT INTO "with_serial" ("label") VALUES ('serial')`,
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

	// C3：缺失表显式报错且直指表名。
	if _, err := srcAdapter.ExtractTable("definitely_missing"); !errors.Is(err, conn.ErrTableNotFound) {
		t.Fatalf("expected ErrTableNotFound for missing table, got %v", err)
	} else if !strings.Contains(err.Error(), "definitely_missing") {
		t.Fatalf("error must name the missing table, got %v", err)
	}

	originalSchema, err := srcAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read original source schema: %v", err)
	}
	tgtSchema, err := tgtAdapter.ReadSchema()
	if err != nil {
		t.Fatalf("read target schema: %v", err)
	}

	forwardDiff := pkgdiff.CompareSchemas(originalSchema, tgtSchema)
	assertDiffContains(t, forwardDiff)

	statements, err := pkgsql.GenerateSchemaSQLSafe(forwardDiff, consts.DBTypePostgres)
	if err != nil {
		t.Fatalf("generate forward SQL: %v", err)
	}
	srcDB := openConfig(t, fixture.source)
	defer srcDB.Close()
	execAll := func(statements []string, phase string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := srcDB.Exec(statement); err != nil {
				t.Fatalf("execute %s statement %q: %v", phase, statement, err)
			}
		}
	}
	// C2：不弹跳视图的 ALTER COLUMN TYPE 会被拒绝；弹跳后必须整组可执行。
	execAll(statements, "forward")

	// 收敛：正向执行后 source 与 target 的结构比对必须为空。
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

	// 回滚对称：按初始快照生成回滚（tgt→src），执行后回到原状。
	rollbackStatements, err := pkgsql.GenerateSchemaSQLSafe(pkgdiff.CompareSchemas(tgtSchema, originalSchema), consts.DBTypePostgres)
	if err != nil {
		t.Fatalf("generate rollback SQL: %v", err)
	}
	execAll(rollbackStatements, "rollback")

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

// TestPostgresDataDiffSkipsMissingTables 验证 diff-data 对晚建表的两种行为：
// 默认报 table not found 并直指表名；开启开关后跳过并给出清单。
func TestPostgresDataDiffSkipsMissingTables(t *testing.T) {
	requireIntegration(t)

	fixture := postgresFixture(t)
	fixture.setupSource = []string{
		`CREATE TABLE "present" ("id" BIGINT NOT NULL PRIMARY KEY, "v" TEXT NULL)`,
		`INSERT INTO "present" VALUES (1, 'a')`,
	}
	fixture.setupTarget = []string{
		`CREATE TABLE "present" ("id" BIGINT NOT NULL PRIMARY KEY, "v" TEXT NULL)`,
		`INSERT INTO "present" VALUES (1, 'b')`,
		`CREATE TABLE "late_table" ("id" BIGINT NOT NULL PRIMARY KEY, "note" TEXT NULL)`,
		`INSERT INTO "late_table" VALUES (1, 'late')`,
	}
	prepareFixture(t, fixture)
	defer cleanupFixture(t, fixture)

	rules := []pkgconfig.Rule{{Table: "present"}, {Table: "late_table"}}
	base := difflogic.DataDiffParams{
		Source:       &fixture.source,
		Target:       &fixture.target,
		Rules:        rules,
		BatchSize:    100,
		DMLBatchSize: 100,
	}

	_, err := difflogic.RunDataDiff(context.Background(), base, t.TempDir(), nil)
	if err == nil || !strings.Contains(err.Error(), "table not found") || !strings.Contains(err.Error(), "late_table") {
		t.Fatalf("expected table not found error naming late_table, got %v", err)
	}

	base.SkipMissingTables = true
	result, err := difflogic.RunDataDiff(context.Background(), base, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("skip-missing run failed: %v", err)
	}
	if !result.Complete {
		t.Fatal("skipped run must stay complete")
	}
	if len(result.SkippedTables) != 1 || result.SkippedTables[0] != "late_table" {
		t.Fatalf("expected late_table in skipped list, got %v", result.SkippedTables)
	}
}

func assertDiffContains(t *testing.T, d *pkgdiff.SchemaDiff) {
	t.Helper()
	if len(d.RoutinesAdded) != 2 {
		t.Fatalf("expected has_revision + audit_proc added, got %v", routineNames(d.RoutinesAdded))
	}
	if len(d.RoutinesModified) != 1 || d.RoutinesModified[0].Old.Name != "lookup_key" {
		t.Fatalf("expected lookup_key modified, got %v", d.RoutinesModified)
	}
	if len(d.SequencesAdded) != 2 {
		t.Fatalf("expected air_ws_items_seq + with_serial_id_seq added, got %v", sequenceNames(d.SequencesAdded))
	}
	var baseChanged bool
	for _, tblDiff := range d.TablesModified {
		if tblDiff.Table != nil && tblDiff.Table.Name == "base" && len(tblDiff.ColumnsModified) == 1 {
			baseChanged = true
		}
	}
	if !baseChanged {
		t.Fatalf("expected base.amount type change, got %v", d.TablesModified)
	}
	found := false
	for _, view := range d.ViewsAffected {
		if view.Name == "v_inner" || view.Name == "v_outer" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected views affected by column type change, got %v", d.ViewsAffected)
	}
}

func routineNames(routines []*conn.Routine) []string {
	names := make([]string, 0, len(routines))
	for _, routine := range routines {
		names = append(names, routine.Identity())
	}
	sort.Strings(names)
	return names
}

func sequenceNames(sequences []*conn.Sequence) []string {
	names := make([]string, 0, len(sequences))
	for _, sequence := range sequences {
		names = append(names, sequence.Name)
	}
	sort.Strings(names)
	return names
}

// nonEmptyDiffParts 列出仍存在差异的类别，用于收敛断言。
func nonEmptyDiffParts(d *pkgdiff.SchemaDiff) []string {
	var parts []string
	if len(d.TablesAdded) > 0 {
		parts = append(parts, "tables added")
	}
	if len(d.TablesDropped) > 0 {
		parts = append(parts, "tables dropped")
	}
	if len(d.TablesModified) > 0 {
		parts = append(parts, "tables modified")
	}
	if len(d.RoutinesAdded) > 0 || len(d.RoutinesDropped) > 0 || len(d.RoutinesModified) > 0 {
		parts = append(parts, "routines differ")
	}
	if len(d.SequencesAdded) > 0 || len(d.SequencesDropped) > 0 || len(d.SequencesModified) > 0 {
		parts = append(parts, "sequences differ")
	}
	if len(d.ViewsAffected) > 0 {
		parts = append(parts, "views affected")
	}
	return parts
}
