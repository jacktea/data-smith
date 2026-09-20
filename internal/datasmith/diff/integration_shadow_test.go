//go:build integration

package diff

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jacktea/data-smith/internal/datasmith/exec"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	_ "github.com/lib/pq"
)

// C5/C6 集成验收（PostgreSQL fixture）：
//   - C5：diff-full 影子事务两阶段——加列轮（2.2.0 场景）与删列轮（3.0.0 场景）
//     的数据比对跑在事务内对齐的结构上，up 一次执行通过；影子事务 ROLLBACK
//     后 source 库零残留。
//   - C6：规则省略（整库通配）展开为全部有行身份的表；默认排除账本表；
//     账本表不出现在任何产物中。

const (
	shadowSrcDatabase = "datasmith_shadow_src"
	shadowTgtDatabase = "datasmith_shadow_tgt"
)

func TestFullDiffShadowTwoPhaseOnPostgres(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("refusing live shadow test without the disposable fixture identity")
	}
	host := envDefault("DATASMITH_POSTGRES_HOST", "127.0.0.1")
	sourcePort := integrationPort(t, "DATASMITH_POSTGRES_SOURCE_PORT")
	targetPort := integrationPort(t, "DATASMITH_POSTGRES_TARGET_PORT")
	user := requiredIntegrationEnv(t, "DATASMITH_POSTGRES_USER")
	password := requiredIntegrationEnv(t, "DATASMITH_POSTGRES_PASSWORD")

	// source 与 target 是两个独立 PG 实例，各自重建一次性影子库。
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

	// target 领先：t_item 多出 notes 列（2.2.0 加列），另有 target 独有表 t_new。
	execShadow(t, tgtDB,
		"CREATE TABLE t_item (id BIGINT PRIMARY KEY, name TEXT NOT NULL, notes TEXT)",
		"CREATE TABLE t_new (id BIGINT PRIMARY KEY, label TEXT)",
		"INSERT INTO t_item VALUES (1, 'a', 'ta'), (2, 'b', 'tb')",
		"INSERT INTO t_new VALUES (7, 'n7')",
	)
	// source 落后一版且带账本表（migrate 引擎账本，必须与 diff 共存）。
	execShadow(t, srcDB,
		"CREATE TABLE t_item (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
		"CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())",
		"INSERT INTO t_item VALUES (1, 'a'), (9, 'legacy')",
		"INSERT INTO schema_migrations VALUES ('V1', now())",
	)

	ctx := context.Background()
	srcAdapter, err := db.NewDBAdapterContext(ctx, srcCfg)
	if err != nil {
		t.Fatalf("open source adapter: %v", err)
	}
	defer srcAdapter.Close()

	// —— 加列轮（2.2.0）：规则省略整库通配 + 影子事务两阶段 ——
	addDir := t.TempDir()
	result, err := RunFullDiff(ctx, FullDiffParams{
		Source:       srcCfg,
		Target:       tgtCfg,
		DataDiffMode: DataDiffModeShadow,
		BatchSize:    100,
		DMLBatchSize: 100,
	}, addDir, func(message string) { t.Log(message) }, nil)
	if err != nil {
		t.Fatalf("shadow full diff: %v", err)
	}
	assertShadowSchemaSummary(t, result)
	assertShadowDataSummary(t, result)
	assertLedgerAbsentFromArtifacts(t, addDir)

	forwardData := readArtifact(t, addDir, DataDiffForwardFile)
	if !strings.Contains(forwardData, "'tb'") {
		t.Fatal("data forward must carry the added row's new-column value")
	}
	if !strings.Contains(forwardData, "t_new") {
		t.Fatal("shadow data diff must cover the target-only table (aligned inside the transaction)")
	}

	// 影子事务必须零残留：source 结构与数据保持原状。
	if column, err := shadowHasColumn(srcDB, "t_item", "notes"); err != nil || column {
		t.Fatalf("source must be untouched after rollback (notes column exists=%v err=%v)", column, err)
	}
	if exists, err := shadowHasTable(srcDB, "t_new"); err != nil || exists {
		t.Fatalf("source must be untouched after rollback (t_new exists=%v err=%v)", exists, err)
	}
	if count := shadowCount(t, srcDB, "t_item WHERE id = 2"); count != 0 {
		t.Fatalf("source row 2 must not exist after rollback, got %d", count)
	}
	if count := shadowCount(t, srcDB, "schema_migrations WHERE version = 'V1'"); count != 1 {
		t.Fatalf("ledger row must survive the shadow diff, got %d", count)
	}

	// up 一次执行通过：结构 forward + 数据 forward 经既有执行通道落库。
	applyForward(t, ctx, srcAdapter, addDir)

	// 对齐后闭环：结构 0 语句、数据 0 DML（auto 模式下无结构差异 → 直接比对）。
	assertClosedLoop(t, ctx, srcCfg, tgtCfg)

	// —— 删列轮（3.0.0）：target 删列后 auto 模式影子两阶段一次对齐 ——
	execShadow(t, tgtDB, "ALTER TABLE t_item DROP COLUMN notes")
	dropDir := t.TempDir()
	dropped, err := RunFullDiff(ctx, FullDiffParams{
		Source:       srcCfg,
		Target:       tgtCfg,
		BatchSize:    100,
		DMLBatchSize: 100,
	}, dropDir, func(message string) { t.Log(message) }, nil)
	if err != nil {
		t.Fatalf("drop-column full diff: %v", err)
	}
	if !dropped.Schema.Destructive {
		t.Fatalf("drop-column diff must be destructive, got %+v", dropped.Schema)
	}
	if column, err := shadowHasColumn(srcDB, "t_item", "notes"); err != nil || !column {
		t.Fatalf("source must still own the notes column before applying up (exists=%v err=%v)", column, err)
	}
	applyForward(t, ctx, srcAdapter, dropDir)
	assertClosedLoop(t, ctx, srcCfg, tgtCfg)
}

// applyForward 把一轮 up（结构 forward + 数据 forward）经既有执行通道落到
// source，即验收标准中的「up 一次执行通过」。
func applyForward(t *testing.T, ctx context.Context, srcAdapter conn.DBAdapter, dir string) {
	t.Helper()
	for _, name := range []string{SchemaDiffForwardFile, DataDiffForwardFile} {
		content := readArtifact(t, dir, name)
		if err := exec.ExecuteSQLContextForTarget(ctx, srcAdapter, content, "source", false, true); err != nil {
			t.Fatalf("apply %s in one pass: %v", name, err)
		}
	}
}

// assertClosedLoop 验证对齐后整库重跑收敛：结构 0 语句、数据 0 DML。
func assertClosedLoop(t *testing.T, ctx context.Context, srcCfg, tgtCfg *pkgconfig.ConnConfig) {
	t.Helper()
	closed, err := RunFullDiff(ctx, FullDiffParams{
		Source:       srcCfg,
		Target:       tgtCfg,
		BatchSize:    100,
		DMLBatchSize: 100,
	}, t.TempDir(), func(message string) { t.Log(message) }, nil)
	if err != nil {
		t.Fatalf("closing full diff: %v", err)
	}
	if len(closed.Schema.TablesAdded)+len(closed.Schema.TablesDropped)+len(closed.Schema.TablesModified) != 0 {
		t.Fatalf("closing schema diff must be empty, got %+v", closed.Schema)
	}
	for _, table := range closed.Data.Tables {
		if table.Added+table.Modified+table.Dropped != 0 {
			t.Fatalf("closing data diff on %s = +%d ~%d -%d, want zero", table.Table, table.Added, table.Modified, table.Dropped)
		}
	}
}

func assertShadowSchemaSummary(t *testing.T, result FullDiffResult) {
	t.Helper()
	if len(result.Schema.TablesAdded) != 1 || result.Schema.TablesAdded[0] != "t_new" {
		t.Fatalf("tables added = %v, want [t_new]", result.Schema.TablesAdded)
	}
	if len(result.Schema.TablesModified) != 1 || result.Schema.TablesModified[0].Table != "t_item" {
		t.Fatalf("tables modified = %+v, want t_item", result.Schema.TablesModified)
	}
	if cols := result.Schema.TablesModified[0].ColumnsAdded; len(cols) != 1 || cols[0] != "notes" {
		t.Fatalf("columns added = %v, want [notes]", cols)
	}
	if len(result.Schema.TablesDropped) != 0 {
		t.Fatalf("tables dropped = %v, want none (schema_migrations is excluded)", result.Schema.TablesDropped)
	}
}

func assertShadowDataSummary(t *testing.T, result FullDiffResult) {
	t.Helper()
	byTable := map[string]TableDiffSummary{}
	for _, table := range result.Data.Tables {
		byTable[table.Table] = table
	}
	item, ok := byTable["t_item"]
	if !ok {
		t.Fatalf("data tables = %v, want t_item covered", result.Data.Tables)
	}
	// row 2 为新增行；row 9 为多余行；row 1 的 notes 列 NULL 与 'ta' 构成真实修改。
	if item.Added != 1 || item.Dropped != 1 || item.Modified != 1 {
		t.Fatalf("t_item diff = +%d ~%d -%d, want +1 ~1 -1", item.Added, item.Modified, item.Dropped)
	}
	fresh, ok := byTable["t_new"]
	if !ok || fresh.Added != 1 {
		t.Fatalf("t_new diff = %+v, want +1 (shadow mode covers target-only tables)", fresh)
	}
	// 账本表在结构阶段即被默认排除（filterTables），不会进入影子数据比对
	// 的候选，也不会出现在 forward DDL 中——这正是与 migrate-script 共存的
	// 关键。账本零出现由 assertLedgerAbsentFromArtifacts 与行存活断言兜底。
	if len(result.Data.ExcludedTables) != 0 {
		t.Fatalf("excluded = %v, want empty (ledger is filtered at the schema phase)", result.Data.ExcludedTables)
	}
	if len(result.Data.SkippedTables) != 0 {
		t.Fatalf("skipped = %v, want empty", result.Data.SkippedTables)
	}
}

// assertLedgerAbsentFromArtifacts 是 C6 的硬性验收：账本表不出现在任何产物。
func assertLedgerAbsentFromArtifacts(t *testing.T, dir string) {
	t.Helper()
	for _, name := range FullDiffFileNames() {
		if strings.Contains(readArtifact(t, dir, name), "schema_migrations") {
			t.Fatalf("%s must not reference the ledger table schema_migrations", name)
		}
	}
}

func shadowConnConfig(host string, port int, user, password, database string) *pkgconfig.ConnConfig {
	return &pkgconfig.ConnConfig{
		Type:           consts.DBTypePostgres,
		Host:           host,
		Port:           port,
		User:           user,
		Password:       password,
		DBName:         database,
		TableSchema:    "public",
		ConnectTimeout: 10 * time.Second,
	}
}

func shadowAdminDSN(host string, port int, user, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/datasmith_fixture?sslmode=disable", user, password, host, port)
}

func openShadowAdmin(t *testing.T, host string, port int, user, password string) *sql.DB {
	t.Helper()
	admin, err := sql.Open("postgres", shadowAdminDSN(host, port, user, password))
	if err != nil {
		t.Fatalf("open postgres admin: %v", err)
	}
	if err := admin.Ping(); err != nil {
		admin.Close()
		t.Fatalf("ping postgres admin: %v", err)
	}
	return admin
}

func recreateShadowDatabases(t *testing.T, admin *sql.DB, names ...string) {
	t.Helper()
	terminateShadowBackends(t, admin, names...)
	for _, name := range names {
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + name); err != nil {
			t.Fatalf("drop shadow database %s: %v", name, err)
		}
		if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
			t.Fatalf("create shadow database %s: %v", name, err)
		}
	}
}

func dropShadowDatabases(t *testing.T, admin *sql.DB, names ...string) {
	t.Helper()
	terminateShadowBackends(t, admin, names...)
	for _, name := range names {
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + name); err != nil {
			t.Errorf("drop shadow database %s: %v", name, err)
		}
	}
}

func terminateShadowBackends(t *testing.T, admin *sql.DB, names ...string) {
	t.Helper()
	literals := make([]string, 0, len(names))
	for _, name := range names {
		literals = append(literals, "'"+name+"'")
	}
	if _, err := admin.Exec(fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN (%s) AND pid <> pg_backend_pid()",
		strings.Join(literals, ", "))); err != nil {
		t.Fatalf("terminate shadow database backends: %v", err)
	}
}

func openShadowDB(t *testing.T, cfg *pkgconfig.ConnConfig) *sql.DB {
	t.Helper()
	database, err := sql.Open("postgres", fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName))
	if err != nil {
		t.Fatalf("open shadow database: %v", err)
	}
	if err := database.Ping(); err != nil {
		database.Close()
		t.Fatalf("ping shadow database: %v", err)
	}
	return database
}

func execShadow(t *testing.T, database *sql.DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := database.Exec(statement); err != nil {
			t.Fatalf("execute shadow setup %q: %v", statement, err)
		}
	}
}

func shadowHasColumn(database *sql.DB, table, column string) (bool, error) {
	var found bool
	err := database.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
	)`, table, column).Scan(&found)
	return found, err
}

func shadowHasTable(database *sql.DB, table string) (bool, error) {
	var found bool
	err := database.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1 AND table_type = 'BASE TABLE'
	)`, table).Scan(&found)
	return found, err
}

func shadowCount(t *testing.T, database *sql.DB, tableAndWhere string) int {
	t.Helper()
	var count int
	if err := database.QueryRow("SELECT count(*) FROM " + tableAndWhere).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", tableAndWhere, err)
	}
	return count
}

func readArtifact(t *testing.T, dir, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read artifact %s: %v", name, err)
	}
	return string(raw)
}
