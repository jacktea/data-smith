package migrate

import (
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/consts"
)

func expectLedgerRows(mock sqlmock.Sqlmock, version, status string) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, status, checksum FROM schema_migrations")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "checksum"}).AddRow(version, status, "abc123"))
}

func expectEmptySuccessSet(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, checksum FROM schema_migrations WHERE status = 'success'")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "checksum"}))
}

func TestRemoveMigrationRecordDeletesRolledBackAndWarnsStackReorder(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "V1.0.1__init.up.sql", "SELECT 1;")

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	// planRemove 的账本读取与成功集合查询
	expectLedgerRows(mock, "V1.0.1", "rolled_back")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, checksum FROM schema_migrations WHERE status = 'success'")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "checksum"}).AddRow("V2.0.0", "x"))
	// DeleteMigrationRecord:加锁 → 复核账本 → 白名单 DELETE → 解锁
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectLedgerRows(mock, "V1.0.1", "rolled_back")
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM schema_migrations WHERE version = $1 AND status IN ('failed','rolled_back')")).
		WithArgs("V1.0.1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	deleted, warnings, err := RemoveMigrationRecord(t.Context(), adapter, dir, "1.0.1", nil)
	if err != nil {
		t.Fatalf("remove rolled_back record: %v", err)
	}
	if deleted != "V1.0.1" {
		t.Fatalf("deleted = %q, want V1.0.1", deleted)
	}
	reorderWarned := false
	for _, warning := range warnings {
		if strings.Contains(warning, "回退栈顶") {
			reorderWarned = true
		}
	}
	if !reorderWarned {
		t.Fatalf("expected stack-reorder warning, got %v", warnings)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRemoveMigrationRecordWarnsMissingUpScript(t *testing.T) {
	dir := t.TempDir()

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	expectLedgerRows(mock, "1", "failed")
	expectEmptySuccessSet(mock)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectLedgerRows(mock, "1", "failed")
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM schema_migrations WHERE version = $1 AND status IN ('failed','rolled_back')")).
		WithArgs("1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	deleted, warnings, err := RemoveMigrationRecord(t.Context(), adapter, dir, "V1", nil)
	if err != nil {
		t.Fatalf("remove failed record: %v", err)
	}
	if deleted != "1" {
		t.Fatalf("deleted = %q, want 1", deleted)
	}
	missingUpWarned := false
	for _, warning := range warnings {
		if strings.Contains(warning, "不存在版本 1 的 up 脚本") {
			missingUpWarned = true
		}
	}
	if !missingUpWarned {
		t.Fatalf("expected missing-up-script warning, got %v", warnings)
	}
}

func TestRemoveMigrationRecordNotFound(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "V1__init.up.sql", "SELECT 1;")

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, status, checksum FROM schema_migrations")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "checksum"}))

	if _, _, err := RemoveMigrationRecord(t.Context(), adapter, dir, "2.0", nil); err == nil || !strings.Contains(err.Error(), "不存在版本") {
		t.Fatalf("expected missing-record error, got %v", err)
	}
}
