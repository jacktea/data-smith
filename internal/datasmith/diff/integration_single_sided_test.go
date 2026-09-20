//go:build integration

package diff

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/internal/datasmith/exec"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
)

const singleSidedMySQLDatabase = "datasmith_single_sided"

func TestFullDiffMySQLDirectSingleSidedExplicitAndWildcard(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("refusing live single-sided test without the disposable fixture identity")
	}
	host := envDefault("DATASMITH_MYSQL_HOST", "127.0.0.1")
	user := requiredIntegrationEnv(t, "DATASMITH_MYSQL_USER")
	password := requiredIntegrationEnv(t, "DATASMITH_MYSQL_PASSWORD")
	sourcePort := integrationPort(t, "DATASMITH_MYSQL_SOURCE_PORT")
	targetPort := integrationPort(t, "DATASMITH_MYSQL_TARGET_PORT")

	for _, port := range []int{sourcePort, targetPort} {
		port := port
		admin := openVerifiedMySQLFixture(t, host, port, user, password)
		execCLISetup(t, admin,
			"DROP DATABASE IF EXISTS `"+singleSidedMySQLDatabase+"`",
			"CREATE DATABASE `"+singleSidedMySQLDatabase+"`",
		)
		admin.Close()
		t.Cleanup(func() {
			admin := openVerifiedMySQLFixture(t, host, port, user, password)
			_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + singleSidedMySQLDatabase + "`")
			_ = admin.Close()
		})
	}

	source := openMySQLDatabase(t, host, sourcePort, user, password, singleSidedMySQLDatabase)
	target := openMySQLDatabase(t, host, targetPort, user, password, singleSidedMySQLDatabase)
	defer source.Close()
	defer target.Close()
	execCLISetup(t, source,
		"CREATE TABLE source_explicit (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		"CREATE TABLE source_wildcard (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		"INSERT INTO source_explicit VALUES (1, 'drop explicit')",
		"INSERT INTO source_wildcard VALUES (2, 'drop wildcard')",
	)
	execCLISetup(t, target,
		"CREATE TABLE target_explicit (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		"CREATE TABLE target_wildcard (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		"INSERT INTO target_explicit VALUES (11, 'insert explicit')",
		"INSERT INTO target_wildcard VALUES (12, 'insert wildcard')",
	)

	sourceCfg := &pkgconfig.ConnConfig{Type: consts.DBTypeMySQL, Host: host, Port: sourcePort, User: user, Password: password, DBName: singleSidedMySQLDatabase}
	targetCfg := &pkgconfig.ConnConfig{Type: consts.DBTypeMySQL, Host: host, Port: targetPort, User: user, Password: password, DBName: singleSidedMySQLDatabase}
	rules := []pkgconfig.Rule{
		{Table: "target_explicit"},
		{Table: "source_explicit"},
		{Table: "*_wildcard"},
	}
	ctx := context.Background()
	dir := t.TempDir()
	result, err := RunFullDiff(ctx, FullDiffParams{
		Source: sourceCfg, Target: targetCfg, Rules: rules, DataDiffMode: DataDiffModeDirect,
		BatchSize: 100, DMLBatchSize: 100,
	}, dir, func(message string) { t.Log(message) }, nil)
	if err != nil {
		t.Fatalf("MySQL direct full diff: %v", err)
	}
	assertSingleSidedSchema(t, result.Schema)
	assertTargetOnlyRows(t, result.Data, "target_explicit", "target_wildcard")
	if len(result.SkippedTables) != 1 || result.SkippedTables[0] != "source_explicit" {
		t.Fatalf("explicit source-only skips = %v, want [source_explicit]", result.SkippedTables)
	}
	forwardData := readArtifact(t, dir, DataDiffForwardFile)
	for _, value := range []string{"insert explicit", "insert wildcard"} {
		if !strings.Contains(forwardData, value) {
			t.Fatalf("data forward omitted target-only value %q:\n%s", value, forwardData)
		}
	}
	if strings.Contains(forwardData, "drop explicit") || strings.Contains(forwardData, "drop wildcard") {
		t.Fatalf("source-only rows must not enter data diff:\n%s", forwardData)
	}

	sourceAdapter, err := db.NewDBAdapterContext(ctx, sourceCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceAdapter.Close()
	applyFullForward(t, ctx, sourceAdapter, dir)

	closed, err := RunFullDiff(ctx, FullDiffParams{
		Source: sourceCfg, Target: targetCfg,
		Rules:        []pkgconfig.Rule{{Table: "target_explicit"}, {Table: "*_wildcard"}},
		DataDiffMode: DataDiffModeDirect, BatchSize: 100, DMLBatchSize: 100,
	}, t.TempDir(), nil, nil)
	if err != nil {
		t.Fatalf("second MySQL direct full diff: %v", err)
	}
	assertFullDiffClosed(t, closed)
}

func applyFullForward(t *testing.T, ctx context.Context, adapter conn.DBAdapter, dir string) {
	t.Helper()
	for _, name := range []string{SchemaDiffForwardFile, DataDiffForwardFile} {
		content := readArtifact(t, dir, name)
		if err := exec.ExecuteSQLContextForTarget(ctx, adapter, content, "source", false, false); err != nil {
			t.Fatalf("apply %s to disposable source: %v", filepath.Base(name), err)
		}
	}
}

func assertSingleSidedSchema(t *testing.T, summary SchemaDiffSummary) {
	t.Helper()
	if strings.Join(summary.TablesAdded, ",") != "target_explicit,target_wildcard" {
		t.Fatalf("tables added = %v", summary.TablesAdded)
	}
	if strings.Join(summary.TablesDropped, ",") != "source_explicit,source_wildcard" {
		t.Fatalf("tables dropped = %v", summary.TablesDropped)
	}
}

func assertTargetOnlyRows(t *testing.T, result DataDiffResult, names ...string) {
	t.Helper()
	byName := make(map[string]TableDiffSummary, len(result.Tables))
	for _, table := range result.Tables {
		byName[table.Table] = table
	}
	for _, name := range names {
		got, ok := byName[name]
		if !ok || got.Added != 1 || got.Modified != 0 || got.Dropped != 0 {
			t.Fatalf("target-only %s summary = %+v (present=%v), want +1", name, got, ok)
		}
	}
	for _, name := range []string{"source_explicit", "source_wildcard"} {
		if _, ok := byName[name]; ok {
			t.Fatalf("source-only %s must not enter data diff: %+v", name, result.Tables)
		}
	}
}

func assertFullDiffClosed(t *testing.T, result FullDiffResult) {
	t.Helper()
	if len(result.Schema.TablesAdded)+len(result.Schema.TablesDropped)+len(result.Schema.TablesModified) != 0 {
		t.Fatalf("closing schema diff = %+v, want empty", result.Schema)
	}
	for _, table := range result.Data.Tables {
		if table.Added+table.Modified+table.Dropped != 0 {
			t.Fatalf("closing data diff table = %+v, want empty", table)
		}
	}
}
