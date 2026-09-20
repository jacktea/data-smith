package migrate

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/consts"
)

func expectAppliedStack(mock sqlmock.Sqlmock, versions ...string) {
	rows := sqlmock.NewRows([]string{"version"})
	for _, v := range versions {
		rows.AddRow(v)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC")).
		WillReturnRows(rows)
}

func expectPostgresRollbackStep(mock sqlmock.Sqlmock, version, downSQL string) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs(version).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(downSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = $1,")).
		WithArgs(sqlmock.AnyArg(), version).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func TestPlanRollbackToRequiresAppliedTarget(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectAppliedStack(mock, "3.0", "2.0", "1.0")
	_, err := PlanRollbackTo(adapter, []*MigrationFile{downFile("1.0", "b;")}, "9.0")
	if err == nil || !strings.Contains(err.Error(), "not applied") {
		t.Fatalf("expected target-not-applied error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanRollbackToValidatesDownScriptsBeforeMutation(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	expectAppliedStack(mock, "3.0", "2.0", "1.0")
	// 2.0 缺 down 脚本:必须在动库前整体拒绝
	files := []*MigrationFile{
		upFile("1.0", "a;"), downFile("1.0", "drop one;"),
		upFile("2.0", "b;"),
		upFile("3.0", "c;"), downFile("3.0", "drop three;"),
	}
	_, err := PlanRollbackTo(adapter, files, "1.0")
	if err == nil || !strings.Contains(err.Error(), "2.0") || !strings.Contains(err.Error(), "no down script") {
		t.Fatalf("expected missing down script error naming 2.0, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationAlreadyAtTarget(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectAppliedStack(mock, "2.0", "1.0")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))
	rolled, err := RollbackToMigration(adapter, []*MigrationFile{downFile("2.0", "b;")}, "2.0")
	if err != nil {
		t.Fatalf("rollback to current version should be a no-op: %v", err)
	}
	if len(rolled) != 0 {
		t.Fatalf("expected empty rollback list, got %v", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationPostgresRollsBackNewestFirst(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectAppliedStack(mock, "V3.0", "V2.0", "V1.0")
	expectPostgresRollbackStep(mock, "V3.0", "DROP TABLE c;")
	expectPostgresRollbackStep(mock, "V2.0", "DROP TABLE b;")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	files := []*MigrationFile{
		downFile("v1.0", "DROP TABLE a;"),
		downFile("v2.0", "DROP TABLE b;"),
		downFile("v3.0", "DROP TABLE c;"),
	}
	rolled, err := RollbackToMigration(adapter, files, "V1.0")
	if err != nil {
		t.Fatalf("rollback to target should succeed: %v", err)
	}
	if len(rolled) != 2 || rolled[0] != "V3.0" || rolled[1] != "V2.0" {
		t.Fatalf("expected [V3.0 V2.0], got %v", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationBuildsAuthoritativePlanAfterLock(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	// 4.0 represents a version committed by a concurrent migration before this
	// rollback acquired the lock. The authoritative plan must observe it.
	expectAppliedStack(mock, "4.0", "3.0", "2.0", "1.0")
	expectPostgresRollbackStep(mock, "4.0", "DROP TABLE d;")
	expectPostgresRollbackStep(mock, "3.0", "DROP TABLE c;")
	expectPostgresRollbackStep(mock, "2.0", "DROP TABLE b;")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	files := []*MigrationFile{
		downFile("1.0", "DROP TABLE a;"),
		downFile("2.0", "DROP TABLE b;"),
		downFile("3.0", "DROP TABLE c;"),
		downFile("4.0", "DROP TABLE d;"),
	}
	rolled, err := RollbackToMigration(adapter, files, "1.0")
	if err != nil {
		t.Fatalf("rollback to target should use the locked ledger: %v", err)
	}
	want := []string{"4.0", "3.0", "2.0"}
	if strings.Join(rolled, ",") != strings.Join(want, ",") {
		t.Fatalf("rolled = %v, want %v", rolled, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationEmptyTargetRollsBackSingleStep(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectAppliedStack(mock, "2.0", "1.0")
	expectPostgresRollbackStep(mock, "2.0", "DROP TABLE b;")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	files := []*MigrationFile{downFile("1.0", "DROP TABLE a;"), downFile("2.0", "DROP TABLE b;")}
	rolled, err := RollbackToMigration(adapter, files, "")
	if err != nil {
		t.Fatalf("rollback latest should succeed: %v", err)
	}
	if len(rolled) != 1 || rolled[0] != "2.0" {
		t.Fatalf("expected single step [2.0], got %v", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationMySQLRollsBackEachStep(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK(?, 0)"}).AddRow(1))
	expectAppliedStack(mock, "2.0", "1.0")
	// 回退到 1.0 = 只回退其之上的 2.0,目标版本本身保留
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE b;")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = ?,")).
		WithArgs(sqlmock.AnyArg(), "2.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK(?)"}).AddRow(1))

	files := []*MigrationFile{downFile("1.0", "DROP TABLE a;"), downFile("2.0", "DROP TABLE b;")}
	rolled, err := RollbackToMigration(adapter, files, "1.0")
	if err != nil {
		t.Fatalf("mysql rollback to target should succeed: %v", err)
	}
	if len(rolled) != 1 || rolled[0] != "2.0" {
		t.Fatalf("expected [2.0], got %v", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMigrationStopsOnFailure(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	expectAppliedStack(mock, "3.0", "2.0", "1.0")
	// 第一步(3.0)执行失败,事务回滚,循环终止
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs("3.0").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("bad down").WillReturnError(errors.New("syntax error"))
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	files := []*MigrationFile{
		downFile("1.0", "DROP TABLE a;"),
		downFile("2.0", "DROP TABLE b;"),
		downFile("3.0", "bad down;"),
	}
	rolled, err := RollbackToMigration(adapter, files, "1.0")
	if err == nil || !strings.Contains(err.Error(), "rollback migration") {
		t.Fatalf("expected rollback failure, got %v", err)
	}
	if len(rolled) != 0 {
		t.Fatalf("no version should be reported rolled on failure, got %v", rolled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
