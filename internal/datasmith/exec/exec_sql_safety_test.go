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

func TestExecuteSQLPostgresDryRunExecutesOriginalSQLAndRollsBack(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "  -- keep leading text\nINSERT INTO t VALUES ('BEGIN;');\n"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	if err := ExecuteSQL(adapter, sqlText, true, false); err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSQLTransactionExecutesOriginalSQLAndCommits(t *testing.T) {
	adapter, mock := newExecMockAdapter(t, consts.DBTypePostgres)
	sqlText := "\nUPDATE t SET note = 'COMMIT;'; -- preserve me\n"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := ExecuteSQL(adapter, sqlText, false, true); err != nil {
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
	sqlText := "INSERT INTO t VALUES (1); UPDATE t SET n = 2;"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectRollback()

	if err := ExecuteSQL(adapter, sqlText, true, false); err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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
