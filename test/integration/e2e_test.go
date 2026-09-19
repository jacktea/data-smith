//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	"github.com/jacktea/data-smith/pkg/migrate"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

const (
	fixtureDatabase  = "datasmith_fixture"
	testDatabase     = "datasmith_e2e"
	migrateDatabase  = "datasmith_migrations"
	fixtureSchema    = "DataSmith_App"
	migrationSchema  = "DataSmith_Migrations"
	migrationLockKey = "data-smith:schema-migrations"
)

type engineFixture struct {
	name          string
	dbType        consts.DBType
	driver        string
	sourceAdmin   string
	targetAdmin   string
	source        config.ConnConfig
	target        config.ConnConfig
	migration     config.ConnConfig
	dataTable     string
	dataColumns   []string
	setupSource   []string
	setupTarget   []string
	migrationSQL  string
	retrySQL      string
	dependencySQL string
	driftSQL      string
}

func TestDualDatabaseEndToEnd(t *testing.T) {
	requireIntegration(t)

	fixtures := []engineFixture{mysqlFixture(t), postgresFixture(t)}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			prepareFixture(t, fixture)
			defer cleanupFixture(t, fixture)
			runDiffRoundTrip(t, fixture)
			runMigrationLifecycle(t, fixture)
		})
	}
}

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("set DATASMITH_INTEGRATION=1 and start the disposable fixtures")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") != "data-smith-integration-v1" {
		t.Fatal("DATASMITH_FIXTURE_ID must identify the disposable DataSmith fixture")
	}
}

func mysqlFixture(t *testing.T) engineFixture {
	t.Helper()
	host := envOr("DATASMITH_MYSQL_HOST", "127.0.0.1")
	sourcePort := envPort(t, "DATASMITH_MYSQL_SOURCE_PORT")
	targetPort := envPort(t, "DATASMITH_MYSQL_TARGET_PORT")
	user := requiredEnv(t, "DATASMITH_MYSQL_USER")
	password := requiredEnv(t, "DATASMITH_MYSQL_PASSWORD")

	admin := func(port int) string {
		cfg := mysqlDriver.NewConfig()
		cfg.User = user
		cfg.Passwd = password
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", host, port)
		cfg.DBName = fixtureDatabase
		cfg.ParseTime = true
		cfg.MultiStatements = false
		return cfg.FormatDSN()
	}
	connConfig := func(port int, database string) config.ConnConfig {
		return config.ConnConfig{
			Type:           consts.DBTypeMySQL,
			Host:           host,
			Port:           port,
			User:           user,
			Password:       password,
			DBName:         database,
			ConnectTimeout: 10 * time.Second,
			Extra:          config.DBParams{"parseTime": "true", "multiStatements": "true"},
		}
	}
	return engineFixture{
		name:        "mysql",
		dbType:      consts.DBTypeMySQL,
		driver:      "mysql",
		sourceAdmin: admin(sourcePort),
		targetAdmin: admin(targetPort),
		source:      connConfig(sourcePort, testDatabase),
		target:      connConfig(targetPort, testDatabase),
		migration:   connConfig(sourcePort, migrateDatabase),
		dataTable:   "Order`Data",
		dataColumns: []string{"id", "select", "MixedCase", "odd`col"},
		setupSource: []string{
			"CREATE TABLE `Order``Data` (`id` BIGINT NOT NULL, `select` VARCHAR(40) NULL, `MixedCase` VARCHAR(40) NOT NULL, `odd``col` VARCHAR(40) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			"INSERT INTO `Order``Data` (`id`, `select`, `MixedCase`, `odd``col`) VALUES (9007199254740993, NULL, 'source', ''), (9007199254740995, '', 'same', 'tick')",
		},
		setupTarget: []string{
			"CREATE TABLE `Order``Data` (`id` BIGINT NOT NULL, `select` VARCHAR(40) NULL, `MixedCase` VARCHAR(40) NOT NULL, `odd``col` VARCHAR(40) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			"INSERT INTO `Order``Data` (`id`, `select`, `MixedCase`, `odd``col`) VALUES (9007199254740993, '', 'target', ''), (9007199254740994, NULL, 'added', 'back`tick')",
			"CREATE TABLE `Parent``Table` (`id` BIGINT NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			"CREATE TABLE `Child``Table` (`id` BIGINT NOT NULL, `parent_id` BIGINT NOT NULL, PRIMARY KEY (`id`), CONSTRAINT `FK``Parent` FOREIGN KEY (`parent_id`) REFERENCES `Parent``Table` (`id`)) ENGINE=InnoDB",
			"CREATE VIEW `Mixed``View` AS SELECT c.`id`, p.`id` AS `ParentID` FROM `Child``Table` c JOIN `Parent``Table` p ON p.`id` = c.`parent_id`",
		},
		migrationSQL:  "CREATE TABLE migration_success (id BIGINT PRIMARY KEY)",
		retrySQL:      "INSERT INTO retry_dependency (id) VALUES (1)",
		dependencySQL: "CREATE TABLE retry_dependency (id BIGINT PRIMARY KEY)",
		driftSQL:      "CREATE TABLE migration_success_drift (id BIGINT PRIMARY KEY)",
	}
}

func postgresFixture(t *testing.T) engineFixture {
	t.Helper()
	host := envOr("DATASMITH_POSTGRES_HOST", "127.0.0.1")
	sourcePort := envPort(t, "DATASMITH_POSTGRES_SOURCE_PORT")
	targetPort := envPort(t, "DATASMITH_POSTGRES_TARGET_PORT")
	user := requiredEnv(t, "DATASMITH_POSTGRES_USER")
	password := requiredEnv(t, "DATASMITH_POSTGRES_PASSWORD")

	admin := func(port int) string {
		u := &url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: fmt.Sprintf("%s:%d", host, port), Path: fixtureDatabase}
		q := u.Query()
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
		return u.String()
	}
	connConfig := func(port int, database, schema string) config.ConnConfig {
		return config.ConnConfig{
			Type:           consts.DBTypePostgres,
			Host:           host,
			Port:           port,
			User:           user,
			Password:       password,
			DBName:         database,
			TableSchema:    schema,
			ConnectTimeout: 10 * time.Second,
			Extra:          config.DBParams{"sslmode": "disable"},
		}
	}
	return engineFixture{
		name:        "postgres",
		dbType:      consts.DBTypePostgres,
		driver:      "postgres",
		sourceAdmin: admin(sourcePort),
		targetAdmin: admin(targetPort),
		source:      connConfig(sourcePort, testDatabase, fixtureSchema),
		target:      connConfig(targetPort, testDatabase, fixtureSchema),
		migration:   connConfig(sourcePort, migrateDatabase, migrationSchema),
		dataTable:   `Order"Data`,
		dataColumns: []string{"id", "select", "MixedCase", `odd"col`},
		setupSource: []string{
			`CREATE TABLE "Order""Data" ("id" BIGINT NOT NULL, "select" TEXT NULL, "MixedCase" TEXT NOT NULL, "odd""col" TEXT NOT NULL, PRIMARY KEY ("id"))`,
			`INSERT INTO "Order""Data" ("id", "select", "MixedCase", "odd""col") VALUES (9007199254740993, NULL, 'source', ''), (9007199254740995, '', 'same', 'quote')`,
		},
		setupTarget: []string{
			`CREATE TABLE "Order""Data" ("id" BIGINT NOT NULL, "select" TEXT NULL, "MixedCase" TEXT NOT NULL, "odd""col" TEXT NOT NULL, PRIMARY KEY ("id"))`,
			`INSERT INTO "Order""Data" ("id", "select", "MixedCase", "odd""col") VALUES (9007199254740993, '', 'target', ''), (9007199254740994, NULL, 'added', 'double"quote')`,
			`CREATE TABLE "Parent""Table" ("id" BIGINT NOT NULL, PRIMARY KEY ("id"))`,
			`CREATE TABLE "Child""Table" ("id" BIGINT NOT NULL, "parent_id" BIGINT NOT NULL, PRIMARY KEY ("id"), CONSTRAINT "FK""Parent" FOREIGN KEY ("parent_id") REFERENCES "Parent""Table" ("id"))`,
			`CREATE VIEW "Mixed""View" AS SELECT c."id", p."id" AS "ParentID" FROM "Child""Table" c JOIN "Parent""Table" p ON p."id" = c."parent_id"`,
		},
		migrationSQL:  "CREATE TABLE migration_success (id BIGINT PRIMARY KEY)",
		retrySQL:      "INSERT INTO retry_dependency (id) VALUES (1)",
		dependencySQL: "CREATE TABLE retry_dependency (id BIGINT PRIMARY KEY)",
		driftSQL:      "CREATE TABLE migration_success_drift (id BIGINT PRIMARY KEY)",
	}
}

func prepareFixture(t *testing.T, fixture engineFixture) {
	t.Helper()
	for _, dsn := range []string{fixture.sourceAdmin, fixture.targetAdmin} {
		admin := openAndVerifyFixture(t, fixture.driver, dsn)
		recreateDatabase(t, admin, fixture.dbType, testDatabase)
		admin.Close()
	}

	admin := openAndVerifyFixture(t, fixture.driver, fixture.sourceAdmin)
	recreateDatabase(t, admin, fixture.dbType, migrateDatabase)
	admin.Close()

	if fixture.dbType == consts.DBTypePostgres {
		for _, cfg := range []config.ConnConfig{fixture.source, fixture.target, fixture.migration} {
			connection := openConfig(t, cfg)
			if _, err := connection.Exec(`CREATE SCHEMA ` + quotePostgresIdentifier(cfg.TableSchema)); err != nil {
				connection.Close()
				t.Fatalf("create PostgreSQL fixture schema: %v", err)
			}
			connection.Close()
		}
	}

	execStatements(t, openConfig(t, fixture.source), fixture.setupSource)
	execStatements(t, openConfig(t, fixture.target), fixture.setupTarget)
}

func cleanupFixture(t *testing.T, fixture engineFixture) {
	t.Helper()
	for _, dsn := range []string{fixture.sourceAdmin, fixture.targetAdmin} {
		admin := openAndVerifyFixture(t, fixture.driver, dsn)
		dropDatabase(t, admin, fixture.dbType, testDatabase)
		admin.Close()
	}
	admin := openAndVerifyFixture(t, fixture.driver, fixture.sourceAdmin)
	dropDatabase(t, admin, fixture.dbType, migrateDatabase)
	admin.Close()
}

func openAndVerifyFixture(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	database, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open fixture control database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		t.Fatalf("ping fixture control database: %v", err)
	}
	var identity string
	if err := database.QueryRowContext(ctx, "SELECT fixture_id FROM fixture_identity").Scan(&identity); err != nil {
		database.Close()
		t.Fatalf("verify fixture identity before destructive setup: %v", err)
	}
	if identity != os.Getenv("DATASMITH_FIXTURE_ID") {
		database.Close()
		t.Fatalf("refusing destructive setup for fixture identity %q", identity)
	}
	return database
}

func recreateDatabase(t *testing.T, admin *sql.DB, dbType consts.DBType, name string) {
	t.Helper()
	dropDatabase(t, admin, dbType, name)
	statement := "CREATE DATABASE " + quoteDatabase(dbType, name)
	if _, err := admin.Exec(statement); err != nil {
		t.Fatalf("create disposable database %s: %v", name, err)
	}
}

func dropDatabase(t *testing.T, admin *sql.DB, dbType consts.DBType, name string) {
	t.Helper()
	if name != testDatabase && name != migrateDatabase {
		t.Fatalf("refusing to drop non-fixture database %q", name)
	}
	if dbType == consts.DBTypePostgres {
		if _, err := admin.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", name); err != nil {
			t.Fatalf("terminate disposable PostgreSQL connections: %v", err)
		}
	}
	if _, err := admin.Exec("DROP DATABASE IF EXISTS " + quoteDatabase(dbType, name)); err != nil {
		t.Fatalf("drop disposable database %s: %v", name, err)
	}
}

func quoteDatabase(dbType consts.DBType, name string) string {
	if dbType == consts.DBTypeMySQL {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return quotePostgresIdentifier(name)
}

func quotePostgresIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func openConfig(t *testing.T, cfg config.ConnConfig) *sql.DB {
	t.Helper()
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	connection := adapter.GetConn()
	t.Cleanup(func() { adapter.Close() })
	return connection
}

func execStatements(t *testing.T, database *sql.DB, statements []string) {
	t.Helper()
	for i, statement := range statements {
		if _, err := database.Exec(statement); err != nil {
			t.Fatalf("execute fixture statement %d: %v", i+1, err)
		}
	}
}

func runDiffRoundTrip(t *testing.T, fixture engineFixture) {
	t.Helper()
	source := newAdapter(t, fixture.source)
	target := newAdapter(t, fixture.target)
	defer source.Close()
	defer target.Close()

	initialSchema := readSchema(t, source)
	initialRows := snapshotRows(t, source, fixture)
	targetSchema := readSchema(t, target)
	forwardDiff := pkgdiff.CompareSchemas(initialSchema, targetSchema)
	rollbackDiff := pkgdiff.CompareSchemas(targetSchema, initialSchema)
	forwardSchemaSQL := schemaSQL(t, forwardDiff, fixture.dbType)
	rollbackSchemaSQL := schemaSQL(t, rollbackDiff, fixture.dbType)
	if len(forwardSchemaSQL) < 3 || len(rollbackSchemaSQL) < 3 {
		t.Fatalf("expected dependency-bearing schema SQL, got forward=%d rollback=%d", len(forwardSchemaSQL), len(rollbackSchemaSQL))
	}
	execStatements(t, source.GetConn(), forwardSchemaSQL)

	sourceTable, err := source.ExtractTable(fixture.dataTable)
	if err != nil {
		t.Fatalf("extract source data table after schema apply: %v", err)
	}
	targetTable, err := target.ExtractTable(fixture.dataTable)
	if err != nil {
		t.Fatalf("extract target data table: %v", err)
	}
	forwardDataSQL, rollbackDataSQL := dataSQL(t, source, target, sourceTable, targetTable, fixture)
	if len(forwardDataSQL) < 3 {
		t.Fatalf("expected add/drop/modify data SQL, got %d statements", len(forwardDataSQL))
	}
	execStatements(t, source.GetConn(), forwardDataSQL)

	assertSchemaEmpty(t, source, target, "after forward apply")
	assertDataEmpty(t, source, target, targetTable, fixture, "after forward apply")

	reverseStrings(rollbackDataSQL)
	execStatements(t, source.GetConn(), rollbackDataSQL)
	execStatements(t, source.GetConn(), rollbackSchemaSQL)

	restoredSchema := readSchema(t, source)
	assertSchemaDiffValueEmpty(t, pkgdiff.CompareSchemas(initialSchema, restoredSchema), "restored source versus initial")
	assertSchemaDiffValueEmpty(t, pkgdiff.CompareSchemas(restoredSchema, initialSchema), "initial versus restored source")
	if restoredRows := snapshotRows(t, source, fixture); !reflect.DeepEqual(initialRows, restoredRows) {
		t.Fatalf("rollback did not restore original rows\ninitial=%v\nrestored=%v", initialRows, restoredRows)
	}
}

func newAdapter(t *testing.T, cfg config.ConnConfig) conn.DBAdapter {
	t.Helper()
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Fatalf("new %s adapter: %v", cfg.Type, err)
	}
	return adapter
}

func readSchema(t *testing.T, adapter conn.DBAdapter) *conn.DatabaseSchema {
	t.Helper()
	schema, err := adapter.ReadSchema()
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	return schema
}

func schemaSQL(t *testing.T, schemaDiff *pkgdiff.SchemaDiff, dbType consts.DBType) []string {
	t.Helper()
	statements, err := pkgsql.GenerateSchemaSQLSafe(schemaDiff, dbType)
	if err != nil {
		t.Fatalf("generate schema SQL: %v", err)
	}
	return statements
}

func dataSQL(t *testing.T, source, target conn.DBAdapter, sourceTable, targetTable *conn.Table, fixture engineFixture) ([]string, []string) {
	t.Helper()
	rule := &pkgdiff.AllFieldsEqualRule{Table: fixture.dataTable, Columns: fixture.dataColumns, ColumnsDef: targetTable.Columns}
	dialect := pkgsql.NewDialect(fixture.dbType)
	var forward, rollback []string
	err := pkgdiff.StreamCompareDataDetailedWithTableContext(context.Background(), source, target, rule, targetTable, 2, func(kind pkgdiff.DiffType, sourceRow, targetRow conn.Record, columns []string) error {
		switch kind {
		case pkgdiff.DiffTypeAdd:
			forward = appendSQL(forward, dialect.GenerateInsertSql(targetTable, targetRow))
			rollback = appendSQL(rollback, dialect.GenerateDeleteSql(targetTable, targetRow))
		case pkgdiff.DiffTypeDrop:
			forward = appendSQL(forward, dialect.GenerateDeleteSql(sourceTable, sourceRow))
			rollback = appendSQL(rollback, dialect.GenerateInsertSql(sourceTable, sourceRow))
		case pkgdiff.DiffTypeModify:
			forward = appendSQL(forward, dialect.GenerateUpdateSql(targetTable, targetRow, columns))
			rollback = appendSQL(rollback, dialect.GenerateUpdateSql(sourceTable, sourceRow, columns))
		default:
			return fmt.Errorf("unexpected diff type %q", kind)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream data diff: %v", err)
	}
	return forward, rollback
}

func appendSQL(statements []string, statement string) []string {
	if strings.TrimSpace(statement) == "" {
		return statements
	}
	return append(statements, statement)
}

func assertSchemaEmpty(t *testing.T, source, target conn.DBAdapter, phase string) {
	t.Helper()
	difference, err := pkgdiff.CompareSchemasWithAdapter(source, target)
	if err != nil {
		t.Fatalf("compare schemas %s: %v", phase, err)
	}
	assertSchemaDiffValueEmpty(t, difference, phase)
}

func assertSchemaDiffValueEmpty(t *testing.T, difference *pkgdiff.SchemaDiff, phase string) {
	t.Helper()
	if len(difference.TablesAdded)+len(difference.TablesDropped)+len(difference.TablesModified) != 0 {
		t.Fatalf("schema diff not empty %s: added=%d dropped=%d modified=%d", phase, len(difference.TablesAdded), len(difference.TablesDropped), len(difference.TablesModified))
	}
}

func assertDataEmpty(t *testing.T, source, target conn.DBAdapter, table *conn.Table, fixture engineFixture, phase string) {
	t.Helper()
	rule := &pkgdiff.AllFieldsEqualRule{Table: fixture.dataTable, Columns: fixture.dataColumns, ColumnsDef: table.Columns}
	count := 0
	err := pkgdiff.StreamCompareDataDetailedWithTableContext(context.Background(), source, target, rule, table, 2, func(pkgdiff.DiffType, conn.Record, conn.Record, []string) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("compare data %s: %v", phase, err)
	}
	if count != 0 {
		t.Fatalf("data diff not empty %s: %d differences", phase, count)
	}
}

func snapshotRows(t *testing.T, adapter conn.DBAdapter, fixture engineFixture) []string {
	t.Helper()
	rows, err := adapter.GetTableDataBatch(fixture.dataTable, fixture.dataColumns, []string{"id"}, nil, 100)
	if err != nil {
		t.Fatalf("snapshot fixture rows: %v", err)
	}
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, 0, len(fixture.dataColumns))
		for _, column := range fixture.dataColumns {
			value := row[column]
			if value == nil {
				values = append(values, "<NULL>")
				continue
			}
			if bytes, ok := value.([]byte); ok {
				value = string(bytes)
			}
			values = append(values, fmt.Sprintf("%T:%v", value, value))
		}
		result = append(result, strings.Join(values, "|"))
	}
	return result
}

func reverseStrings(values []string) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func runMigrationLifecycle(t *testing.T, fixture engineFixture) {
	t.Helper()
	adapter := newAdapter(t, fixture.migration)
	defer adapter.Close()
	if err := migrate.EnsureVersionTable(adapter); err != nil {
		t.Fatalf("ensure migration ledger: %v", err)
	}

	success := migrationFile(t, "001", "success", fixture.migrationSQL)
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{success}); err != nil {
		t.Fatalf("apply successful migration: %v", err)
	}
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{success}); err != nil {
		t.Fatalf("repeat successful migration: %v", err)
	}
	assertLedger(t, adapter.GetConn(), fixture.dbType, "001", "success", 1)

	retry := migrationFile(t, "002", "retry", fixture.retrySQL)
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{retry}); err == nil {
		t.Fatal("expected migration failure before dependency exists")
	}
	assertLedger(t, adapter.GetConn(), fixture.dbType, "002", "failed", 1)
	if _, err := adapter.GetConn().Exec(fixture.dependencySQL); err != nil {
		t.Fatalf("create retry dependency: %v", err)
	}
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{retry}); err != nil {
		t.Fatalf("retry failed migration with same checksum: %v", err)
	}
	assertLedger(t, adapter.GetConn(), fixture.dbType, "002", "success", 1)

	drift := migrationFile(t, "001", "success", fixture.driftSQL)
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{drift}); err == nil || !strings.Contains(err.Error(), "checksum drift") {
		t.Fatalf("expected checksum drift rejection, got %v", err)
	}

	locked := reserveMigrationLock(t, adapter, fixture.dbType)
	lockedFile := migrationFile(t, "003", "locked", "CREATE TABLE migration_locked (id BIGINT PRIMARY KEY)")
	if err := migrate.ApplyMigrations(adapter, []*migrate.MigrationFile{lockedFile}); err == nil || !strings.Contains(err.Error(), "another migration runner") {
		locked()
		t.Fatalf("expected migration lock rejection, got %v", err)
	}
	locked()
}

func migrationFile(t *testing.T, version, title, contents string) *migrate.MigrationFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), version+"__"+title+".sql")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write migration fixture: %v", err)
	}
	file := &migrate.MigrationFile{Version: version, Title: title, Direction: "up", Ext: "sql", Path: path}
	if err := migrate.PrepareMigrationFiles([]*migrate.MigrationFile{file}); err != nil {
		t.Fatalf("prepare migration fixture: %v", err)
	}
	return file
}

func assertLedger(t *testing.T, database *sql.DB, dbType consts.DBType, version, status string, wantCount int) {
	t.Helper()
	placeholder := "?"
	if dbType == consts.DBTypePostgres {
		placeholder = "$1"
	}
	var count int
	var gotStatus string
	err := database.QueryRow("SELECT COUNT(*), MAX(status) FROM schema_migrations WHERE version = "+placeholder, version).Scan(&count, &gotStatus)
	if err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	if count != wantCount || gotStatus != status {
		t.Fatalf("migration ledger version %s: count=%d status=%s, want count=%d status=%s", version, count, gotStatus, wantCount, status)
	}
}

func reserveMigrationLock(t *testing.T, adapter conn.DBAdapter, dbType consts.DBType) func() {
	t.Helper()
	connection, err := adapter.GetConn().Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve lock connection: %v", err)
	}
	if dbType == consts.DBTypeMySQL {
		var acquired sql.NullInt64
		if err := connection.QueryRowContext(context.Background(), "SELECT GET_LOCK(?, 0)", migrationLockKey).Scan(&acquired); err != nil || !acquired.Valid || acquired.Int64 != 1 {
			connection.Close()
			t.Fatalf("acquire MySQL migration lock: acquired=%v err=%v", acquired, err)
		}
		return func() {
			var released sql.NullInt64
			if err := connection.QueryRowContext(context.Background(), "SELECT RELEASE_LOCK(?)", migrationLockKey).Scan(&released); err != nil {
				t.Errorf("release MySQL migration lock: %v", err)
			}
			connection.Close()
		}
	}
	if _, err := connection.ExecContext(context.Background(), "SELECT pg_advisory_lock(hashtext($1))", migrationLockKey); err != nil {
		connection.Close()
		t.Fatalf("acquire PostgreSQL migration lock: %v", err)
	}
	return func() {
		if _, err := connection.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", migrationLockKey); err != nil {
			t.Errorf("release PostgreSQL migration lock: %v", err)
		}
		connection.Close()
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for integration tests", name)
	}
	return value
}

func envPort(t *testing.T, name string) int {
	t.Helper()
	value := requiredEnv(t, name)
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		t.Fatalf("%s must be a valid port", name)
	}
	return port
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func TestIntegrationGuardRejectsMissingIdentity(t *testing.T) {
	if os.Getenv("DATASMITH_INTEGRATION") != "1" {
		t.Skip("integration environment is intentionally absent")
	}
	if os.Getenv("DATASMITH_FIXTURE_ID") == "" {
		t.Fatal(errors.New("fixture identity guard is required"))
	}
}
