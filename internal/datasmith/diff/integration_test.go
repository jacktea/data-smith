//go:build integration

package diff

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

const cliFixtureDatabase = "datasmith_cli_diff"

func TestCLICommandsGenerateLiveMySQLSchemaAndDataPairs(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("refusing live CLI test without the disposable fixture identity")
	}

	host := envDefault("DATASMITH_MYSQL_HOST", "127.0.0.1")
	user := requiredIntegrationEnv(t, "DATASMITH_MYSQL_USER")
	password := requiredIntegrationEnv(t, "DATASMITH_MYSQL_PASSWORD")
	sourcePort := integrationPort(t, "DATASMITH_MYSQL_SOURCE_PORT")
	targetPort := integrationPort(t, "DATASMITH_MYSQL_TARGET_PORT")

	for _, port := range []int{sourcePort, targetPort} {
		admin := openVerifiedMySQLFixture(t, host, port, user, password)
		recreateCLIDatabase(t, admin)
		admin.Close()
		port := port
		t.Cleanup(func() {
			admin := openVerifiedMySQLFixture(t, host, port, user, password)
			if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + cliFixtureDatabase + "`"); err != nil {
				t.Errorf("drop CLI fixture database: %v", err)
			}
			admin.Close()
		})
	}

	source := openMySQLDatabase(t, host, sourcePort, user, password, cliFixtureDatabase)
	target := openMySQLDatabase(t, host, targetPort, user, password, cliFixtureDatabase)
	t.Cleanup(func() { source.Close() })
	t.Cleanup(func() { target.Close() })
	execCLISetup(t, source,
		"CREATE TABLE `Order``Data` (`id` BIGINT NOT NULL PRIMARY KEY, `select` VARCHAR(32) NULL, `MixedCase` VARCHAR(32) NOT NULL)",
		"INSERT INTO `Order``Data` VALUES (9007199254740993, NULL, 'source'), (9007199254740995, '', 'drop')",
	)
	execCLISetup(t, target,
		"CREATE TABLE `Order``Data` (`id` BIGINT NOT NULL PRIMARY KEY, `select` VARCHAR(32) NULL, `MixedCase` VARCHAR(32) NOT NULL)",
		"INSERT INTO `Order``Data` VALUES (9007199254740993, '', 'target'), (9007199254740994, NULL, 'add')",
		"CREATE TABLE `Extra``Table` (`id` BIGINT NOT NULL PRIMARY KEY, `parent_id` BIGINT NULL)",
	)

	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")
	configText := fmt.Sprintf(`sourceDb:
  type: mysql
  host: %s
  port: %d
  user: %s
  password: %s
  dbname: %s
  extra:
    parseTime: "true"
targetDb:
  type: mysql
  host: %s
  port: %d
  user: %s
  password: %s
  dbname: %s
  extra:
    parseTime: "true"
`, host, sourcePort, user, password, cliFixtureDatabase, host, targetPort, user, password, cliFixtureDatabase)
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}
	rulesPath := filepath.Join(tempDir, "rules.json")
	rulesJSON, err := json.Marshal(map[string]any{"rules": []map[string]any{{"table": "Order`Data"}}})
	if err != nil {
		t.Fatalf("marshal CLI rules: %v", err)
	}
	if err := os.WriteFile(rulesPath, rulesJSON, 0o600); err != nil {
		t.Fatalf("write CLI rules: %v", err)
	}

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("enter CLI output directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldDir) })

	diffSchemaCmd.SetContext(context.Background())
	if err := diffSchemaCmd.Flags().Set("config", configPath); err != nil {
		t.Fatalf("set schema config flag: %v", err)
	}
	if err := runDiffSchema(diffSchemaCmd, nil); err != nil {
		t.Fatalf("run live diff-schema: %v", err)
	}
	assertGeneratedPair(t, filepath.Join(tempDir, "schema_diff.sql"), filepath.Join(tempDir, "schema_diff_rollback.sql"), "Extra``Table")

	forwardPath := filepath.Join(tempDir, "live_data_forward.sql")
	rollbackPath := filepath.Join(tempDir, "live_data_rollback.sql")
	diffDataCmd.SetContext(context.Background())
	for name, value := range map[string]string{
		"config":          configPath,
		"rules":           rulesPath,
		"output":          forwardPath,
		"rollback-output": rollbackPath,
		"batch-size":      "2",
		"dml-batch-size":  "2",
	} {
		if err := diffDataCmd.Flags().Set(name, value); err != nil {
			t.Fatalf("set data flag %s: %v", name, err)
		}
	}
	if err := runDiffData(diffDataCmd, nil); err != nil {
		t.Fatalf("run live diff-data: %v", err)
	}
	assertGeneratedPair(t, forwardPath, rollbackPath, "Order``Data")
}

func openVerifiedMySQLFixture(t *testing.T, host string, port int, user, password string) *sql.DB {
	t.Helper()
	database := openMySQLDatabase(t, host, port, user, password, "datasmith_fixture")
	var identity string
	if err := database.QueryRow("SELECT fixture_id FROM fixture_identity").Scan(&identity); err != nil {
		database.Close()
		t.Fatalf("verify CLI fixture identity: %v", err)
	}
	if identity != os.Getenv("DATASMITH_FIXTURE_ID") {
		database.Close()
		t.Fatalf("refusing destructive CLI setup for fixture identity %q", identity)
	}
	return database
}

func openMySQLDatabase(t *testing.T, host string, port int, user, password, databaseName string) *sql.DB {
	t.Helper()
	cfg := mysqlDriver.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.DBName = databaseName
	cfg.ParseTime = true
	database, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL CLI fixture: %v", err)
	}
	if err := database.Ping(); err != nil {
		database.Close()
		t.Fatalf("ping MySQL CLI fixture: %v", err)
	}
	return database
}

func recreateCLIDatabase(t *testing.T, admin *sql.DB) {
	t.Helper()
	if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + cliFixtureDatabase + "`"); err != nil {
		t.Fatalf("drop prior CLI fixture database: %v", err)
	}
	if _, err := admin.Exec("CREATE DATABASE `" + cliFixtureDatabase + "`"); err != nil {
		t.Fatalf("create CLI fixture database: %v", err)
	}
}

func execCLISetup(t *testing.T, database *sql.DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := database.Exec(statement); err != nil {
			t.Fatalf("execute CLI fixture setup: %v", err)
		}
	}
}

func assertGeneratedPair(t *testing.T, forwardPath, rollbackPath, escapedIdentifier string) {
	t.Helper()
	for _, path := range []string{forwardPath, rollbackPath} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read generated SQL %s: %v", filepath.Base(path), err)
		}
		text := string(contents)
		if !strings.Contains(text, executeOnSourceHeader) {
			t.Fatalf("generated SQL %s lacks execution marker", filepath.Base(path))
		}
		if !strings.Contains(text, escapedIdentifier) {
			t.Fatalf("generated SQL %s lacks escaped identifier %q", filepath.Base(path), escapedIdentifier)
		}
	}
}

func requiredIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func integrationPort(t *testing.T, name string) int {
	t.Helper()
	value := requiredIntegrationEnv(t, name)
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		t.Fatalf("%s must be a valid port", name)
	}
	return port
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
