package exec

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/lib/pq"
)

func TestExecuteSQLContextHonorsDeadline(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "SELECT pg_sleep(10)"
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := ExecuteSQLContext(ctx, adapter, sqlText, false, false)
	if !errors.Is(err, context.DeadlineExceeded) && (err == nil || !strings.Contains(err.Error(), "cancel")) {
		t.Fatalf("got %v, want context cancellation", err)
	}
}

func TestExecuteSQLContextRejectsMarkedScriptWithoutTargetIdentity(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)

	err := ExecuteSQLContext(
		context.Background(),
		adapter,
		"-- DATASMITH EXECUTE-ON: source\nDELETE FROM users;",
		false,
		false,
	)
	if err == nil || !strings.Contains(err.Error(), "执行目标身份") {
		t.Fatalf("ExecuteSQLContext() error = %v, want missing target identity rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("marked SQL reached the database without a target identity: %v", err)
	}
}

func TestExecuteSQLContextForTargetRejectsInvalidTargetIdentity(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)

	err := ExecuteSQLContextForTarget(
		context.Background(),
		adapter,
		"DELETE FROM users;",
		"primary",
		false,
		false,
	)
	if err == nil || !strings.Contains(err.Error(), "source 或 target") {
		t.Fatalf("ExecuteSQLContextForTarget() error = %v, want invalid target identity rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL reached the database with an invalid target identity: %v", err)
	}
}

type execMockAdapter struct {
	db  *sql.DB
	cfg *config.ConnConfig
}

func (a *execMockAdapter) ReadSchema() (*conn.DatabaseSchema, error) { return nil, nil }
func (a *execMockAdapter) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}
func (a *execMockAdapter) ExtractTable(string) (*conn.Table, error) { return nil, nil }
func (a *execMockAdapter) ExtractView(string) (*conn.Table, error)  { return nil, nil }
func (a *execMockAdapter) GetConn() *sql.DB                         { return a.db }
func (a *execMockAdapter) GetConfig() *config.ConnConfig            { return a.cfg }
func (a *execMockAdapter) Close() error                             { return nil }

func newExecMockAdapter(t *testing.T, dbType consts.DBType) (*execMockAdapter, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return &execMockAdapter{db: database, cfg: &config.ConnConfig{Type: dbType}}, mock
}

// 事务/dry-run 模式按扫描出的语句逐条执行（同一事务，整文件原子）；
// 前导注释不参与执行文本。
func TestExecuteSQLPostgresDryRunExecutesOriginalSQLAndRollsBack(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	statement := "INSERT INTO t VALUES ('BEGIN;');"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	if err := ExecuteSQL(adapter, "  -- keep leading text\n"+statement+"\n", true, false); err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLTransactionExecutesOriginalSQLAndCommits(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	statement := "UPDATE t SET note = 'COMMIT;';"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := ExecuteSQL(adapter, "\n"+statement+" -- preserve me\n", false, true); err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLRejectsExplicitTransactionBeforeBegin(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	err := ExecuteSQL(adapter, "BEGIN; INSERT INTO t VALUES (1); COMMIT;", false, true)
	if err == nil || !strings.Contains(err.Error(), "BEGIN") {
		t.Fatalf("ExecuteSQL() error = %v, want explicit transaction rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database operation: %v", err)
	}
}

func TestExecuteSQLMySQLDryRunRejectsDDLBeforeMutation(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypeMySQL)
	err := ExecuteSQL(adapter, "INSERT INTO audit VALUES (1); CREATE TABLE unsafe (id INT);", true, false)
	if err == nil || !strings.Contains(err.Error(), "rejects DDL") {
		t.Fatalf("ExecuteSQL() error = %v, want DDL rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database was mutated before validation: %v", err)
	}
}

func TestExecuteSQLMySQLTransactionRejectsExecutableCommentBeforeBegin(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypeMySQL)
	err := ExecuteSQL(adapter, "/*! COMMIT */; INSERT INTO t VALUES (1);", false, true)
	if err == nil || !strings.Contains(err.Error(), "可执行注释") {
		t.Fatalf("ExecuteSQL() error = %v, want executable-comment rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database operation: %v", err)
	}
}

func TestExecuteSQLMySQLDryRunRejectsModeDependentQuoteBeforeBegin(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypeMySQL)
	sqlText := `INSERT INTO t VALUES ('safe\'); DROP TABLE t; -- close ');`
	err := ExecuteSQL(adapter, sqlText, true, false)
	if err == nil || !strings.Contains(err.Error(), "反斜杠转义模式") {
		t.Fatalf("ExecuteSQL() error = %v, want mode-dependent quote rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database operation: %v", err)
	}
}

func TestExecuteSQLMySQLDryRunAllowsTransactionalDML(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypeMySQL)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO t VALUES (1);")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE t SET n = 2;")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	if err := ExecuteSQL(adapter, "INSERT INTO t VALUES (1); UPDATE t SET n = 2;", true, false); err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// C9：事务模式逐条执行，中途失败的脚本直接定位到失败语句（含文本预览），
// 不依赖驱动位置信息；失败后回滚整个事务。
func TestExecuteSQLTransactionAttributesFailingStatement(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	driverErr := errors.New("relation \"missing\" does not exist")
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO t VALUES (1);")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE missing SET n = 2;")).WillReturnError(driverErr)
	mock.ExpectRollback()

	sqlText := "INSERT INTO t VALUES (1);\nUPDATE missing SET n = 2;"
	err := ExecuteSQL(adapter, sqlText, false, true)
	if !errors.Is(err, driverErr) {
		t.Fatalf("ExecuteSQL() error = %v, want wrapped driver error", err)
	}
	if !strings.Contains(err.Error(), "第 2/2 条语句") {
		t.Fatalf("error = %v, want failing statement index", err)
	}
	if !strings.Contains(err.Error(), "起始于脚本第 2 行第 1 列: UPDATE missing SET n = 2;") {
		t.Fatalf("error = %v, want failing statement preview", err)
	}
	if !strings.Contains(err.Error(), "已回滚") {
		t.Fatalf("error = %v, want rollback notice", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL was not rolled back exactly once: %v", err)
	}
}

// C9：驱动给出 PostgreSQL 位置时，错误附带所在语句的文本预览。
func TestExecutionErrorIncludesStatementPreviewOnPostgresPosition(t *testing.T) {
	sqlText := "SELECT '你';\nUPDATE accounts SET balance = 0 WHERE id = 7;"
	position := utf8.RuneCountInString(sqlText[:strings.Index(sqlText, "UPDATE")]) + 1
	scan, err := scanSQL(sqlText)
	if err != nil {
		t.Fatal(err)
	}
	driverErr := &pq.Error{Message: "syntax error", Position: strconv.Itoa(position)}
	got := executionError("执行 SQL 失败", driverErr, sqlText, scan)
	for _, want := range []string{"第 2 条语句，脚本第 2 行第 1 列: UPDATE accounts SET balance = 0 WHERE id = 7;"} {
		if !strings.Contains(got.Error(), want) {
			t.Fatalf("error = %v, want %q", got, want)
		}
	}
}

func TestExecuteSQLFailureDoesNotReexecuteSQL(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "INSERT INTO t VALUES (1);\nUPDATE missing SET n = 2;"
	driverErr := errors.New("driver syntax failure")
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnError(driverErr)

	err := ExecuteSQL(adapter, sqlText, false, false)
	if !errors.Is(err, driverErr) {
		t.Fatalf("ExecuteSQL() error = %v, want wrapped driver error", err)
	}
	if !strings.Contains(err.Error(), "语句范围 1-2") {
		t.Fatalf("ExecuteSQL() error = %v, want safe statement range", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL was executed more than once: %v", err)
	}
}

func TestExecuteSQLUsesPostgresCharacterPosition(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "SELECT '你';\nBROKEN;"
	position := utf8.RuneCountInString(sqlText[:strings.Index(sqlText, "BROKEN")]) + 1
	driverErr := &pq.Error{Message: "syntax error", Position: strconv.Itoa(position)}
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnError(driverErr)

	err := ExecuteSQL(adapter, sqlText, false, false)
	if err == nil || !strings.Contains(err.Error(), "第 2 条语句，脚本第 2 行第 1 列") {
		t.Fatalf("ExecuteSQL() error = %v, want Unicode-safe PostgreSQL position", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLNonTransactionStillExecutesWhenDiagnosticScanFails(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "INSERT INTO t VALUES ('unterminated"
	driverErr := errors.New("driver parse failure")
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnError(driverErr)

	err := ExecuteSQL(adapter, sqlText, false, false)
	if !errors.Is(err, driverErr) || !strings.Contains(err.Error(), "无法安全推导语句位置") {
		t.Fatalf("ExecuteSQL() error = %v, want one-shot driver error with unavailable location", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL was not executed exactly once: %v", err)
	}
}
