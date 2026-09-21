package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/consts"
)

func ledgerFindRows(records ...MigrationRecord) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"version", "status", "checksum"})
	for _, record := range records {
		rows.AddRow(record.Version, record.Status, record.Checksum)
	}
	return rows
}

func expectLedgerFind(mock sqlmock.Sqlmock, records ...MigrationRecord) {
	mock.ExpectQuery("SELECT version, status, checksum FROM schema_migrations").
		WillReturnRows(ledgerFindRows(records...))
}

// sqlmock 按注册顺序匹配,加锁期望注册在首个账本查询前,解锁期望必须注册在
// DELETE 之后,故 acquire/release 拆成两个助手。
func expectPostgresLockAcquire(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT pg_try_advisory_lock\\(hashtext\\(\\$1\\)\\)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
}

func expectPostgresLockRelease(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT pg_advisory_unlock\\(hashtext\\(\\$1\\)\\)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))
}

func expectMySQLLockAcquire(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
}

func expectMySQLLockRelease(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT RELEASE_LOCK\\(\\?\\)").WithArgs(migrationLockName).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
}

func TestDeleteMigrationRecordRemovesRolledBackPostgres(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectPostgresLockAcquire(mock)
	expectLedgerFind(mock, MigrationRecord{Version: "V1.0.1", Status: "rolled_back", Checksum: "abc"})
	expectExec(mock, `DELETE FROM schema_migrations WHERE version = $1 AND status IN ('failed','rolled_back')`).
		WithArgs("V1.0.1").WillReturnResult(sqlmock.NewResult(1, 1))
	expectPostgresLockRelease(mock)

	deleted, err := DeleteMigrationRecord(adapter, "1.0.1")
	if err != nil {
		t.Fatalf("delete rolled_back record: %v", err)
	}
	if deleted != "V1.0.1" {
		t.Fatalf("deleted = %q, want exact ledger version V1.0.1", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteMigrationRecordRefusesSuccessAndRunning(t *testing.T) {
	tests := []struct {
		name   string
		status string
	}{
		{"success", "success"},
		{"running", "running"},
		{"rolling_back", "rolling_back"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
			expectPostgresLockAcquire(mock)
			expectLedgerFind(mock, MigrationRecord{Version: "1", Status: test.status, Checksum: "abc"})
			expectPostgresLockRelease(mock)

			_, err := DeleteMigrationRecord(adapter, "1")
			if err == nil || !strings.Contains(err.Error(), "仅 failed/rolled_back 记录允许删除") {
				t.Fatalf("expected whitelist refusal for %s, got %v", test.status, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations (a DELETE must not run): %v", err)
			}
		})
	}
}

func TestDeleteMigrationRecordNotFound(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectPostgresLockAcquire(mock)
	expectLedgerFind(mock)
	expectPostgresLockRelease(mock)

	if _, err := DeleteMigrationRecord(adapter, "9.9.9"); err == nil || !strings.Contains(err.Error(), "不存在版本") {
		t.Fatalf("expected missing-record error, got %v", err)
	}
}

func TestDeleteMigrationRecordAmbiguousVersions(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectPostgresLockAcquire(mock)
	expectLedgerFind(mock,
		MigrationRecord{Version: "V1", Status: "failed"},
		MigrationRecord{Version: "1", Status: "failed"},
	)
	expectPostgresLockRelease(mock)

	if _, err := DeleteMigrationRecord(adapter, "1"); err == nil || !strings.Contains(err.Error(), "多条记录") {
		t.Fatalf("expected ambiguity error, got %v", err)
	}
}

func TestDeleteMigrationRecordRefusesWhenStatusFlipsBeforeDelete(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectPostgresLockAcquire(mock)
	expectLedgerFind(mock, MigrationRecord{Version: "1", Status: "failed"})
	// The whitelist guard re-checked in DELETE itself: a concurrent runner
	// flipped the record between the read and the delete, so nothing matches.
	expectExec(mock, `DELETE FROM schema_migrations WHERE version = $1 AND status IN ('failed','rolled_back')`).
		WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 0))
	expectPostgresLockRelease(mock)

	if _, err := DeleteMigrationRecord(adapter, "1"); err == nil || !strings.Contains(err.Error(), "已拒绝删除") {
		t.Fatalf("expected concurrent-flip refusal, got %v", err)
	}
}

func TestDeleteMigrationRecordMySQLPlaceholders(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	expectMySQLLockAcquire(mock)
	expectLedgerFind(mock, MigrationRecord{Version: "1", Status: "failed", Checksum: "abc"})
	expectExec(mock, `DELETE FROM schema_migrations WHERE version = ? AND status IN ('failed','rolled_back')`).
		WithArgs("1").WillReturnResult(sqlmock.NewResult(1, 1))
	expectMySQLLockRelease(mock)

	deleted, err := DeleteMigrationRecord(adapter, "V1")
	if err != nil {
		t.Fatalf("delete failed record on MySQL: %v", err)
	}
	if deleted != "1" {
		t.Fatalf("deleted = %q, want 1", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestFindMigrationRecordEmptyVersionRejected(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	if _, _, err := FindMigrationRecord(context.Background(), adapter, "  "); err == nil {
		t.Fatal("expected empty version error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database was touched for an empty version: %v", err)
	}
}
