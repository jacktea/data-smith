//go:build integration

package diff

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/consts"
)

// 回归场景:NOT NULL 的乐观锁字段被忽略后,生成的 INSERT 仍必须携带该字段,
// 否则增量 SQL 在执行阶段因缺列失败。忽略字段上的差异也不得触发 UPDATE。
func TestRunDataDiffInsertCarriesNotNullIgnoredColumn(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("refusing live test without the disposable fixture identity")
	}
	host := envDefault("DATASMITH_MYSQL_HOST", "127.0.0.1")
	user := requiredIntegrationEnv(t, "DATASMITH_MYSQL_USER")
	password := requiredIntegrationEnv(t, "DATASMITH_MYSQL_PASSWORD")
	sourcePort := integrationPort(t, "DATASMITH_MYSQL_SOURCE_PORT")
	targetPort := integrationPort(t, "DATASMITH_MYSQL_TARGET_PORT")

	dbName := "datasmith_ignore_cols"
	for _, port := range []int{sourcePort, targetPort} {
		port := port
		admin := openVerifiedMySQLFixture(t, host, port, user, password)
		if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`"); err != nil {
			admin.Close()
			t.Fatalf("drop prior ignore-cols fixture database: %v", err)
		}
		if _, err := admin.Exec("CREATE DATABASE `" + dbName + "`"); err != nil {
			admin.Close()
			t.Fatalf("create ignore-cols fixture database: %v", err)
		}
		admin.Close()
		t.Cleanup(func() {
			admin := openVerifiedMySQLFixture(t, host, port, user, password)
			if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`"); err != nil {
				t.Errorf("drop ignore-cols fixture database: %v", err)
			}
			admin.Close()
		})
	}

	source := openMySQLDatabase(t, host, sourcePort, user, password, dbName)
	target := openMySQLDatabase(t, host, targetPort, user, password, dbName)
	t.Cleanup(func() { source.Close() })
	t.Cleanup(func() { target.Close() })

	sourceCfg := &pkgconfig.ConnConfig{Type: consts.DBTypeMySQL, Host: host, Port: sourcePort, User: user, Password: password, DBName: dbName}
	targetCfg := &pkgconfig.ConnConfig{Type: consts.DBTypeMySQL, Host: host, Port: targetPort, User: user, Password: password, DBName: dbName}

	for _, chunkHash := range []bool{false, true} {
		t.Run(map[bool]string{false: "row-scan", true: "chunk-hash"}[chunkHash], func(t *testing.T) {
			seedIgnoreColsFixture(t, source, target)
			dir := t.TempDir()
			result, err := RunDataDiff(context.Background(), DataDiffParams{
				Source:       sourceCfg,
				Target:       targetCfg,
				Rules:        []pkgconfig.Rule{{Table: "orders", IgnoreColumns: []string{"lock_version"}}},
				BatchSize:    100,
				ChunkSize:    100,
				DMLBatchSize: 100,
				ChunkHash:    chunkHash,
			}, dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Complete || len(result.Tables) != 1 || result.Tables[0].Status != "ok" {
				t.Fatalf("unexpected diff result: %#v", result)
			}
			forwardBytes, err := os.ReadFile(filepath.Join(dir, dataDiffForwardFile))
			if err != nil {
				t.Fatal(err)
			}
			rollbackBytes, err := os.ReadFile(filepath.Join(dir, dataDiffRollbackFile))
			if err != nil {
				t.Fatal(err)
			}
			forward, rollback := string(forwardBytes), string(rollbackBytes)

			// 行 1 仅 lock_version 不同(9 vs 5):忽略后不得产生 UPDATE。
			if strings.Contains(forward, "UPDATE `orders`") {
				t.Fatalf("ignored-column-only difference must not produce UPDATE:\n%s", forward)
			}
			// 行 2 为 target 独有:forward INSERT 必须携带 NOT NULL 的 lock_version 及其值。
			if !insertLines(forward, "orders", "`lock_version`", "7") {
				t.Fatalf("forward INSERT must carry NOT NULL ignored column lock_version:\n%s", forward)
			}
			// 行 3 为 source 独有:forward DELETE 移除,回滚 INSERT 携带完整行。
			if !strings.Contains(forward, "DELETE FROM") || !strings.Contains(forward, "WHERE `id` = 3") {
				t.Fatalf("forward DELETE for source-only row missing:\n%s", forward)
			}
			if !insertLines(rollback, "orders", "`lock_version`", "1") {
				t.Fatalf("rollback INSERT must carry ignored column lock_version:\n%s", rollback)
			}

			// 执行 forward 于 source(文件头即 EXECUTE-ON: source):
			// 修复前 INSERT 缺 lock_version 列,会因 NOT NULL 无默认值而失败。
			execGeneratedSQLLines(t, source, forward)
			var amount, lockVersion int
			if err := source.QueryRow("SELECT `amount`, `lock_version` FROM `orders` WHERE `id` = 2").Scan(&amount, &lockVersion); err != nil {
				t.Fatalf("forward-executed row missing on source: %v", err)
			}
			if amount != 200 || lockVersion != 7 {
				t.Fatalf("inserted row = (%d, %d), want (200, 7)", amount, lockVersion)
			}
			var deleted int
			if err := source.QueryRow("SELECT COUNT(*) FROM `orders` WHERE `id` = 3").Scan(&deleted); err != nil {
				t.Fatal(err)
			}
			if deleted != 0 {
				t.Fatalf("source-only row id=3 must be deleted by forward SQL")
			}
		})
	}
}

func seedIgnoreColsFixture(t *testing.T, source, target *sql.DB) {
	t.Helper()
	for _, dbh := range []*sql.DB{source, target} {
		execCLISetup(t, dbh,
			"DROP TABLE IF EXISTS `orders`",
			"CREATE TABLE `orders` (`id` BIGINT NOT NULL PRIMARY KEY, `amount` BIGINT NOT NULL, `lock_version` INT NOT NULL)",
		)
	}
	execCLISetup(t, source, "INSERT INTO `orders` VALUES (1, 100, 9), (3, 300, 1)")
	execCLISetup(t, target, "INSERT INTO `orders` VALUES (1, 100, 5), (2, 200, 7)")
}

func execGeneratedSQLLines(t *testing.T, dbh *sql.DB, sqlText string) {
	t.Helper()
	for _, line := range strings.Split(sqlText, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		if _, err := dbh.Exec(trimmed); err != nil {
			t.Fatalf("execute generated SQL %q: %v", trimmed, err)
		}
	}
}
