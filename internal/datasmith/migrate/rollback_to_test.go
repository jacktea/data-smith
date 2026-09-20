package migrate

import (
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/consts"
)

func expectPostgresRollbackStepOnScriptMock(mock sqlmock.Sqlmock, version, downSQL string) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1")).
		WithArgs(version).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(downSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE schema_migrations SET status = 'rolled_back', execution_time = $1,")).
		WithArgs(sqlmock.AnyArg(), version).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func TestRollbackToAppliesDownScriptsUntilTargetWithProgress(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "1__init.up.sql", "SELECT 1;")
	writeScript(t, dir, "1__init.down.sql", "DROP TABLE a;")
	writeScript(t, dir, "2__add.up.sql", "SELECT 2;")
	writeScript(t, dir, "2__add.down.sql", "DROP TABLE b;")
	writeScript(t, dir, "3__idx.up.sql", "SELECT 3;")
	writeScript(t, dir, "3__idx.down.sql", "DROP INDEX i3;")

	var messages []string
	progress := func(message string) { messages = append(messages, message) }

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("3").AddRow("2").AddRow("1"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	// 回退到 1:依次回退 3、2,目标版本 1 保留
	expectPostgresRollbackStepOnScriptMock(mock, "3", "DROP INDEX i3;")
	expectPostgresRollbackStepOnScriptMock(mock, "2", "DROP TABLE b;")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtext($1))")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"pg_advisory_unlock"}).AddRow(true))

	rolled, err := RollbackTo(t.Context(), adapter, dir, "1", progress)
	if err != nil {
		t.Fatalf("RollbackTo should succeed: %v", err)
	}
	if len(rolled) != 2 || rolled[0] != "3" || rolled[1] != "2" {
		t.Fatalf("expected rolled [3 2], got %v", rolled)
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "开始回退到版本 1") || !strings.Contains(joined, "回退完成") {
		t.Fatalf("progress callbacks incomplete: %q", joined)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackToMissingDirFails(t *testing.T) {
	adapter, _ := newScriptMock(t, consts.DBTypePostgres)
	_, err := RollbackTo(t.Context(), adapter, "missing-dir", "1", nil)
	if err == nil {
		t.Fatal("expected missing directory error")
	}
}

func TestPlanRollbackListsVersionsWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "1__init.up.sql", "SELECT 1;")
	writeScript(t, dir, "1__init.down.sql", "DROP TABLE a;")
	writeScript(t, dir, "2__add.up.sql", "SELECT 2;")
	writeScript(t, dir, "2__add.down.sql", "DROP TABLE b;")

	var messages []string
	progress := func(message string) { messages = append(messages, message) }

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("2").AddRow("1"))

	planned, err := PlanRollback(t.Context(), adapter, dir, "1", progress)
	if err != nil {
		t.Fatalf("PlanRollback should succeed: %v", err)
	}
	if len(planned) != 1 || planned[0] != "2" {
		t.Fatalf("expected plan [2], got %v", planned)
	}
	if !strings.Contains(strings.Join(messages, "\n"), "将依次回退 1 个版本") {
		t.Fatalf("progress callbacks incomplete: %q", strings.Join(messages, "\n"))
	}
	// 只读计划:不得获取迁移锁或触碰账本
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanRollbackEmptyTargetPlansSingleStep(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "1__init.up.sql", "SELECT 1;")
	writeScript(t, dir, "1__init.down.sql", "DROP TABLE a;")
	writeScript(t, dir, "2__add.up.sql", "SELECT 2;")
	writeScript(t, dir, "2__add.down.sql", "DROP TABLE b;")

	adapter, mock := newScriptMock(t, consts.DBTypePostgres)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("2").AddRow("1"))

	planned, err := PlanRollback(t.Context(), adapter, dir, "", nil)
	if err != nil {
		t.Fatalf("PlanRollback should succeed: %v", err)
	}
	if len(planned) != 1 || planned[0] != "2" {
		t.Fatalf("expected single-step plan [2], got %v", planned)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
