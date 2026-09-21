package server

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/conn"
)

// newLedgerDeleteFixture 建好"脚本库 + 空目录 + MySQL 桩连接"的删除入口测试
// 底座,返回 server、front 与两个 ID;sqlmock 由调用方持有以布置期望。
func newLedgerDeleteFixture(t *testing.T) (*Server, *httptest.Server, string, string, sqlmock.Sqlmock) {
	t.Helper()
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	connCreated := createConnection(t, front, "db", 33096)
	connID, _ := connCreated["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{33096: adapter})
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "lib"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	return srv, front, libID, connID, mock
}

func TestMigrateLedgerDeleteRequiresConfirmation(t *testing.T) {
	_, front, libID, connID, _ := newLedgerDeleteFixture(t)
	resp := mustJSON(t, front, http.MethodPost, "/api/migrate/ledger/delete", map[string]any{
		"libraryId": libID, "connectionId": connID, "version": "1",
	}, http.StatusBadRequest)
	if msg, _ := resp["error"].(string); !strings.Contains(msg, "confirmed=true") {
		t.Fatalf("expected confirmation error, got %v", resp["error"])
	}
}

func TestMigrateLedgerDeleteRejectsSuccessRecord(t *testing.T) {
	_, front, libID, connID, mock := newLedgerDeleteFixture(t)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, status, checksum FROM schema_migrations")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "checksum"}).AddRow("1", "success", "abc"))

	resp := mustJSON(t, front, http.MethodPost, "/api/migrate/ledger/delete", map[string]any{
		"libraryId": libID, "connectionId": connID, "version": "1", "confirmed": true,
	}, http.StatusBadRequest)
	if msg, _ := resp["error"].(string); !strings.Contains(msg, "仅 failed/rolled_back") {
		t.Fatalf("expected whitelist refusal, got %v", resp["error"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a DELETE must not run against a success record: %v", err)
	}
}

func TestMigrateLedgerDeleteDeletesRolledBackRecord(t *testing.T) {
	_, front, libID, connID, mock := newLedgerDeleteFixture(t)
	// planRemove:账本读取 + 成功集合
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, status, checksum FROM schema_migrations")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "checksum"}).AddRow("V1", "rolled_back", "abc"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, checksum FROM schema_migrations WHERE status = 'success'")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "checksum"}))
	// DeleteMigrationRecord:加锁 → 复核 → 白名单 DELETE → 解锁
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, status, checksum FROM schema_migrations")).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "checksum"}).AddRow("V1", "rolled_back", "abc"))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM schema_migrations WHERE version = ? AND status IN ('failed','rolled_back')")).
		WithArgs("V1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT RELEASE_LOCK(?)")).WithArgs("data-smith:schema-migrations").
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))

	resp := mustJSON(t, front, http.MethodPost, "/api/migrate/ledger/delete", map[string]any{
		"libraryId": libID, "connectionId": connID, "version": "1", "confirmed": true,
	}, http.StatusOK)
	if resp["version"] != "V1" {
		t.Fatalf("version = %v, want exact ledger version V1", resp["version"])
	}
	// 脚本库为空目录:必须给出缺 up 脚本提示
	warnings, _ := resp["warnings"].([]any)
	if len(warnings) == 0 {
		t.Fatalf("expected missing-up-script warning, got %v", resp["warnings"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
