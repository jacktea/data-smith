//go:build integration

package diff

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
)

// C4a 集成验收（PostgreSQL fixture）：无物理主键的关联表
// （air_user_client_role 类）按 rules.comparisonKey 业务键比对——
// 检出 ADD / MODIFY / DROP 并生成可执行 DML；未配置业务键的无键表
// 显式报错并提示替代方案。
func TestRunDataDiffComparesKeylessTableByBusinessKeyOnPostgres(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("refusing live keyless-table test without the disposable fixture identity")
	}
	host := envDefault("DATASMITH_POSTGRES_HOST", "127.0.0.1")
	sourcePort := integrationPort(t, "DATASMITH_POSTGRES_SOURCE_PORT")
	targetPort := integrationPort(t, "DATASMITH_POSTGRES_TARGET_PORT")
	user := requiredIntegrationEnv(t, "DATASMITH_POSTGRES_USER")
	password := requiredIntegrationEnv(t, "DATASMITH_POSTGRES_PASSWORD")

	sourceAdmin := openShadowAdmin(t, host, sourcePort, user, password)
	targetAdmin := openShadowAdmin(t, host, targetPort, user, password)
	recreateShadowDatabases(t, sourceAdmin, shadowSrcDatabase)
	recreateShadowDatabases(t, targetAdmin, shadowTgtDatabase)
	t.Cleanup(func() { dropShadowDatabases(t, sourceAdmin, shadowSrcDatabase) })
	t.Cleanup(func() { dropShadowDatabases(t, targetAdmin, shadowTgtDatabase) })

	srcCfg := shadowConnConfig(host, sourcePort, user, password, shadowSrcDatabase)
	tgtCfg := shadowConnConfig(host, targetPort, user, password, shadowTgtDatabase)

	srcDB := openShadowDB(t, srcCfg)
	defer srcDB.Close()
	tgtDB := openShadowDB(t, tgtCfg)
	defer tgtDB.Close()

	execShadow(t, srcDB,
		"CREATE TABLE air_user_client_role (client_id BIGINT NOT NULL, role_id BIGINT NOT NULL, note TEXT)",
		"INSERT INTO air_user_client_role VALUES (1, 10, 'keep'), (2, 20, 'stale')",
	)
	execShadow(t, tgtDB,
		"CREATE TABLE air_user_client_role (client_id BIGINT NOT NULL, role_id BIGINT NOT NULL, note TEXT)",
		"INSERT INTO air_user_client_role VALUES (2, 20, 'changed'), (3, 30, 'new')",
	)

	dir := t.TempDir()
	result, err := RunDataDiff(context.Background(), DataDiffParams{
		Source: srcCfg,
		Target: tgtCfg,
		Rules: []pkgconfig.Rule{{
			Table:         "air_user_client_role",
			Columns:       []string{"note"},
			ComparisonKey: []string{"client_id", "role_id"},
		}},
		BatchSize: 100, DMLBatchSize: 100,
	}, dir, nil)
	if err != nil {
		t.Fatalf("RunDataDiff with business key: %v", err)
	}
	if !result.Complete || len(result.Tables) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	summary := result.Tables[0]
	if summary.Added != 1 || summary.Modified != 1 || summary.Dropped != 1 {
		t.Fatalf("counts = added %d / modified %d / dropped %d, want 1/1/1", summary.Added, summary.Modified, summary.Dropped)
	}

	forward, err := os.ReadFile(filepath.Join(dir, DataDiffForwardFile))
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(forward)
	for _, want := range []string{
		`DELETE FROM "public"."air_user_client_role" WHERE "client_id" = 1 AND "role_id" = 10;`,
		`UPDATE "public"."air_user_client_role" SET "note" = 'changed' WHERE "client_id" = 2 AND "role_id" = 20;`,
		`INSERT INTO "public"."air_user_client_role"`,
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("forward diff missing %q:\n%s", want, sqlText)
		}
	}

	// 未配置业务键的无键表：显式报错并提示 comparisonKey 替代方案。
	_, err = RunDataDiff(context.Background(), DataDiffParams{
		Source:    srcCfg,
		Target:    tgtCfg,
		Rules:     []pkgconfig.Rule{{Table: "air_user_client_role"}},
		BatchSize: 100, DMLBatchSize: 100,
	}, t.TempDir(), nil)
	if err == nil || !strings.Contains(err.Error(), "rules.comparisonKey") {
		t.Fatalf("keyless table without business key: err = %v, want comparisonKey guidance", err)
	}
}
