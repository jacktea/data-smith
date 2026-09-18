package migrate

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

type mockAdapter struct {
	db  *sql.DB
	cfg *config.ConnConfig
}

func (a *mockAdapter) ReadSchema() (*conn.DatabaseSchema, error) { return nil, nil }
func (a *mockAdapter) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}
func (a *mockAdapter) ExtractTable(string) (*conn.Table, error) { return nil, nil }
func (a *mockAdapter) ExtractView(string) (*conn.Table, error)  { return nil, nil }
func (a *mockAdapter) GetConn() *sql.DB                         { return a.db }
func (a *mockAdapter) GetConfig() *config.ConnConfig            { return a.cfg }
func (a *mockAdapter) Close() error                             { return nil }

func newMockAdapter(t *testing.T, dbType consts.DBType) (*mockAdapter, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return &mockAdapter{db: database, cfg: &config.ConnConfig{Type: dbType}}, mock
}

func checksum(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func expectQuery(mock sqlmock.Sqlmock, query string) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(regexp.QuoteMeta(query))
}

func expectExec(mock sqlmock.Sqlmock, query string) *sqlmock.ExpectedExec {
	return mock.ExpectExec(regexp.QuoteMeta(query))
}

func TestMigrationFileReadContentReportsMissingFile(t *testing.T) {
	file := &MigrationFile{Path: "/definitely/missing/data-smith-migration.sql"}
	if _, err := file.ReadContent(); err == nil {
		t.Fatal("ReadContent must preserve file read errors")
	}
	if got := file.GetContent(); got != "" {
		t.Fatalf("deprecated GetContent compatibility changed: %q", got)
	}
}

func TestApplyMigrationsPreflightBeforeDatabaseMutation(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	tests := []struct {
		name string
		file *MigrationFile
		want string
	}{
		{"json", &MigrationFile{Version: "1", Ext: "json", Direction: "up", Content: `{}`}, "only SQL"},
		{"down", &MigrationFile{Version: "1", Ext: "sql", Direction: "down", Content: "SELECT 1"}, "accepts only"},
		{"missing", &MigrationFile{Version: "1", Ext: "sql", Direction: "up", Path: "/missing.sql"}, "read migration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ApplyMigrations(adapter, []*MigrationFile{test.file})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q error, got %v", test.want, err)
			}
		})
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database was touched during preflight: %v", err)
	}
}

func TestPrepareMigrationFilesRejectsDuplicateVersion(t *testing.T) {
	err := PrepareMigrationFiles([]*MigrationFile{
		{Version: "v1", Content: "SELECT 1"},
		{Version: "1", Content: "SELECT 2"},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
		t.Fatalf("expected duplicate version error, got %v", err)
	}
}

func TestApplyMySQLUsesQuestionMarkPlaceholdersAndStateMachine(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	content := "CREATE TABLE widgets (id INT)"
	sum := checksum(content)

	expectQuery(mock, "SELECT GET_LOCK(?, 0)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
	expectQuery(mock, "SELECT checksum, status FROM schema_migrations WHERE version = ?").WithArgs("1").
		WillReturnError(sql.ErrNoRows)
	expectExec(mock, `INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)
		VALUES (?, ?, ?, 'running', 0, NULL)
		ON DUPLICATE KEY UPDATE title = VALUES(title), checksum = VALUES(checksum), status = 'running',
		execution_time = 0, error_summary = NULL, applied_at = CURRENT_TIMESTAMP`).
		WithArgs("1", "widgets", sum).WillReturnResult(sqlmock.NewResult(1, 1))
	expectExec(mock, content).WillReturnResult(sqlmock.NewResult(0, 1))
	expectExec(mock, `UPDATE schema_migrations SET status = 'success', execution_time = ?,
		error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = ?`).
		WithArgs(sqlmock.AnyArg(), "1").WillReturnResult(sqlmock.NewResult(0, 1))
	expectQuery(mock, "SELECT RELEASE_LOCK(?)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

	err := ApplyMigrations(adapter, []*MigrationFile{{Version: "1", Title: "widgets", Content: content}})
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyPostgresMigrationAndSuccessAreAtomic(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	content := "CREATE TABLE widgets (id INTEGER)"
	sum := checksum(content)

	expectQuery(mock, "SELECT pg_try_advisory_lock(hashtext($1))").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	expectQuery(mock, "SELECT checksum, status FROM schema_migrations WHERE version = $1").WithArgs("1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectBegin()
	expectExec(mock, `INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)
		VALUES ($1, $2, $3, 'running', 0, NULL)
		ON CONFLICT (version) DO UPDATE SET title = EXCLUDED.title, checksum = EXCLUDED.checksum,
		status = 'running', execution_time = 0, error_summary = NULL, applied_at = CURRENT_TIMESTAMP`).
		WithArgs("1", "widgets", sum).WillReturnResult(sqlmock.NewResult(1, 1))
	expectExec(mock, content).WillReturnResult(sqlmock.NewResult(0, 1))
	expectExec(mock, `UPDATE schema_migrations SET status = 'success', execution_time = $1,
			error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = $2`).
		WithArgs(sqlmock.AnyArg(), "1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectQuery(mock, "SELECT pg_advisory_unlock(hashtext($1))").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(true))

	err := ApplyMigrations(adapter, []*MigrationFile{{Version: "1", Title: "widgets", Content: content}})
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyMigrationsRejectsChecksumDrift(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectQuery(mock, "SELECT pg_try_advisory_lock(hashtext($1))").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	expectQuery(mock, "SELECT checksum, status FROM schema_migrations WHERE version = $1").WithArgs("1").
		WillReturnRows(sqlmock.NewRows([]string{"checksum", "status"}).AddRow(strings.Repeat("a", 64), "success"))
	expectQuery(mock, "SELECT pg_advisory_unlock(hashtext($1))").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(true))

	err := ApplyMigrations(adapter, []*MigrationFile{{Version: "1", Content: "SELECT 1"}})
	if err == nil || !strings.Contains(err.Error(), "checksum drift") {
		t.Fatalf("expected checksum drift error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyMigrationsRejectsConcurrentRunner(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	expectQuery(mock, "SELECT GET_LOCK(?, 0)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(0))

	err := ApplyMigrations(adapter, []*MigrationFile{{Version: "1", Content: "SELECT 1"}})
	if err == nil || !strings.Contains(err.Error(), "another migration runner") {
		t.Fatalf("expected concurrent runner error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLFailureIsRecorded(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	content := "BROKEN SQL"
	expectQuery(mock, "SELECT GET_LOCK(?, 0)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
	expectQuery(mock, "SELECT checksum, status FROM schema_migrations WHERE version = ?").WithArgs("1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO schema_migrations").WillReturnResult(sqlmock.NewResult(1, 1))
	expectExec(mock, content).WillReturnError(errors.New("syntax error"))
	mock.ExpectExec("UPDATE schema_migrations SET status = 'failed'").
		WithArgs(sqlmock.AnyArg(), "syntax error", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	expectQuery(mock, "SELECT RELEASE_LOCK(?)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

	err := ApplyMigrations(adapter, []*MigrationFile{{Version: "1", Content: content}})
	if err == nil || !strings.Contains(err.Error(), "syntax error") {
		t.Fatalf("expected migration failure, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
