package migrate

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

type scriptAdapter struct {
	db  *sql.DB
	cfg *config.ConnConfig
}

func (a *scriptAdapter) ReadSchema() (*conn.DatabaseSchema, error) { return nil, nil }
func (a *scriptAdapter) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}
func (a *scriptAdapter) ExtractTable(string) (*conn.Table, error) { return nil, nil }
func (a *scriptAdapter) ExtractView(string) (*conn.Table, error)  { return nil, nil }
func (a *scriptAdapter) GetConn() *sql.DB                         { return a.db }
func (a *scriptAdapter) GetConfig() *config.ConnConfig            { return a.cfg }
func (a *scriptAdapter) Close() error                             { return nil }

func newScriptMock(t *testing.T, dbType consts.DBType) (*scriptAdapter, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return &scriptAdapter{db: database, cfg: &config.ConnConfig{Type: dbType}}, mock
}

func writeScript(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func expectPostgresLedgerSetup(mock sqlmock.Sqlmock) {
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS schema_migrations")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS execution_time")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS status")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS error_summary")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE UNIQUE INDEX IF NOT EXISTS schema_migrations_version_uq")).WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestRunMigrationsMissingDirFails(t *testing.T) {
	adapter, _ := newScriptMock(t, consts.DBTypePostgres)
	err := RunMigrations(t.Context(), adapter, filepath.Join(t.TempDir(), "missing"), false, "", nil)
	if err == nil {
		t.Fatal("expected missing directory error")
	}
}

func TestRollbackLatestMissingDirFails(t *testing.T) {
	adapter, _ := newScriptMock(t, consts.DBTypePostgres)
	_, err := RollbackLatest(t.Context(), adapter, filepath.Join(t.TempDir(), "missing"), nil)
	if err == nil {
		t.Fatal("expected missing directory error")
	}
}

func TestRunMigrationsAppliesPendingWithProgress(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "1__init.up.sql", "SELECT 1;")
	writeScript(t, dir, "1__init.down.sql", "SELECT 0;")

	var messages []string
	progress := func(message string) { messages = append(messages, message) }

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	expectPostgresLedgerSetup(mock)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, checksum FROM schema_migrations WHERE status = 'success'")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "checksum"}))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT checksum, status FROM schema_migrations WHERE version = $1")).WithArgs("1").
		WillReturnRows(sqlmock.NewRows([]string{"checksum", "status"}))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)")).
		WithArgs("1", "init", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT 1;")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'success', execution_time = $1,")).
		WithArgs(sqlmock.AnyArg(), "1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	if err := RunMigrations(t.Context(), adapter, dir, false, "", progress); err != nil {
		t.Fatalf("RunMigrations should succeed: %v", err)
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "待执行的迁移文件: 1") || !strings.Contains(joined, "迁移完成") {
		t.Fatalf("progress callbacks incomplete: %q", joined)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestAppliesDownScriptWithProgress(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "1__init.up.sql", "SELECT 1;")
	writeScript(t, dir, "1__init.down.sql", "SELECT 0;")

	var messages []string
	progress := func(message string) { messages = append(messages, message) }

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("1"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT 0;")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = $1,")).
		WithArgs(sqlmock.AnyArg(), "1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	rolled, err := RollbackLatest(t.Context(), adapter, dir, progress)
	if err != nil {
		t.Fatalf("RollbackLatest should succeed: %v", err)
	}
	if rolled != "1" {
		t.Fatalf("expected rolled version 1, got %q", rolled)
	}
	if !strings.Contains(strings.Join(messages, "\n"), "回退完成") {
		t.Fatalf("progress callbacks incomplete: %q", strings.Join(messages, "\n"))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
