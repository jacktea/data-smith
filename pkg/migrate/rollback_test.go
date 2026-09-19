package migrate

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/consts"
)

func upFile(version, content string) *MigrationFile {
	return &MigrationFile{Version: version, Title: "t", Direction: "up", Ext: "sql", Content: content}
}

func downFile(version, content string) *MigrationFile {
	return &MigrationFile{Version: version, Title: "t", Direction: "down", Ext: "sql", Content: content}
}

func expectCurrentVersion(mock sqlmock.Sqlmock, version string) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(version))
}

func TestPrepareMigrationFilesAcceptsUpDownPair(t *testing.T) {
	files := []*MigrationFile{upFile("1.0", "CREATE TABLE a(id int);"), downFile("1.0", "DROP TABLE a;")}
	if err := PrepareMigrationFiles(files); err != nil {
		t.Fatalf("up/down pair must be accepted: %v", err)
	}
	if files[0].Checksum == "" || files[1].Checksum == "" {
		t.Fatal("checksums must be computed for both directions")
	}
}

func TestPrepareMigrationFilesRejectsDuplicateDirection(t *testing.T) {
	files := []*MigrationFile{upFile("1.0", "a;"), upFile("v1.0", "b;")}
	err := PrepareMigrationFiles(files)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate direction error, got %v", err)
	}
}

func TestPrepareMigrationFilesStillRejectsUnknownDirection(t *testing.T) {
	files := []*MigrationFile{{Version: "1.0", Direction: "sideways", Ext: "sql", Content: "a;"}}
	if err := PrepareMigrationFiles(files); err == nil {
		t.Fatal("unknown direction must be rejected")
	}
}

func TestForwardPathsRejectDownScripts(t *testing.T) {
	mixed := []*MigrationFile{upFile("1.0", "a;"), downFile("1.0", "b;")}
	err := prepareForwardFiles(mixed)
	if err == nil || !strings.Contains(err.Error(), "down script") {
		t.Fatalf("down script in forward set must be rejected, got %v", err)
	}
	upOnly := []*MigrationFile{upFile("1.0", "a;")}
	if err := prepareForwardFiles(upOnly); err != nil {
		t.Fatalf("up-only set must validate: %v", err)
	}
}

func TestRollbackLatestMigrationRequiresAppliedVersion(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectCurrentVersion(mock, "")
	_, err := RollbackLatestMigration(adapter, []*MigrationFile{downFile("1.0", "b;")})
	if err == nil || !strings.Contains(err.Error(), "no applied migration") {
		t.Fatalf("expected no-applied-migration error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestMigrationRequiresDownScript(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectCurrentVersion(mock, "2.0")
	_, err := RollbackLatestMigration(adapter, []*MigrationFile{upFile("2.0", "a;")})
	if err == nil || !strings.Contains(err.Error(), "no down script") {
		t.Fatalf("expected missing down script error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestMigrationPostgresExecutesDownInTransaction(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectCurrentVersion(mock, "V1.0")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs("V1.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE a;")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = $1,")).
		WithArgs(sqlmock.AnyArg(), "V1.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	rolled, err := RollbackLatestMigration(adapter, []*MigrationFile{downFile("1.0", "DROP TABLE a;")})
	if err != nil {
		t.Fatalf("rollback should succeed: %v", err)
	}
	if rolled != "V1.0" {
		t.Fatalf("expected rolled version V1.0, got %q", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestMigrationPostgresFailureKeepsLedger(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectCurrentVersion(mock, "1.0")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs("1.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("bad down").WillReturnError(errors.New("syntax error"))
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	_, err := RollbackLatestMigration(adapter, []*MigrationFile{downFile("1.0", "bad down;")})
	if err == nil || !strings.Contains(err.Error(), "rollback migration") {
		t.Fatalf("expected rollback failure, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestMigrationMySQLUpdatesLedger(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	expectCurrentVersion(mock, "1.0")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK(?, 0)"}).AddRow(1))
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE a;")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = ?,")).
		WithArgs(sqlmock.AnyArg(), "1.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	rolled, err := RollbackLatestMigration(adapter, []*MigrationFile{downFile("1.0", "DROP TABLE a;")})
	if err != nil {
		t.Fatalf("mysql rollback should succeed: %v", err)
	}
	if rolled != "1.0" {
		t.Fatalf("expected rolled version 1.0, got %q", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLatestMigrationFailsWhenLockBusy(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectCurrentVersion(mock, "1.0")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(false))
	_, err := RollbackLatestMigration(adapter, []*MigrationFile{downFile("1.0", "DROP TABLE a;")})
	if err == nil || !strings.Contains(err.Error(), "migration lock") {
		t.Fatalf("expected lock error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
