package server

import (
	"net/http"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

func TestQueryGuardAcceptsReadOnly(t *testing.T) {
	cases := []struct {
		sql    string
		dbType consts.DBType
	}{
		{"SELECT 1", consts.DBTypeMySQL},
		{"  select * from t where x = 1", consts.DBTypeMySQL},
		{"SELECT * FROM t WHERE name = 'a;b'", consts.DBTypeMySQL},
		{"-- 查询用户\nSELECT id FROM users", consts.DBTypeMySQL},
		{"/* lead */ SELECT 1 /* trailing */", consts.DBTypeMySQL},
		{"WITH x AS (SELECT 1 AS n) SELECT * FROM x", consts.DBTypeMySQL},
		{"SHOW TABLES;", consts.DBTypeMySQL},
		{"EXPLAIN SELECT 1", consts.DBTypeMySQL},
		{"DESC t", consts.DBTypeMySQL},
		{"DESCRIBE t", consts.DBTypeMySQL},
		{"# note\nSELECT 1", consts.DBTypeMySQL},
		{"SELECT 1;", consts.DBTypePostgres},
		{"SELECT * FROM t WHERE note = $$semi;colons;$$", consts.DBTypePostgres},
		{"SELECT $tag$a;b$tag$", consts.DBTypePostgres},
		{"WITH a AS (SELECT 1) SELECT * FROM a", consts.DBTypePostgres},
		{"SHOW search_path", consts.DBTypePostgres},
		{"EXPLAIN ANALYZE SELECT 1", consts.DBTypePostgres},
	}
	for _, tc := range cases {
		if err := checkQueryStatement(tc.sql, tc.dbType); err != nil {
			t.Errorf("guard rejected %q (%s): %v", tc.sql, tc.dbType, err)
		}
	}
}

func TestQueryGuardRejects(t *testing.T) {
	cases := []struct {
		sql    string
		dbType consts.DBType
	}{
		{"", consts.DBTypeMySQL},
		{"   \n\t ", consts.DBTypeMySQL},
		{"-- only comment", consts.DBTypeMySQL},
		{"SELECT 1; SELECT 2", consts.DBTypeMySQL},
		{"UPDATE t SET x = 1", consts.DBTypeMySQL},
		{"INSERT INTO t VALUES (1)", consts.DBTypeMySQL},
		{"DELETE FROM t", consts.DBTypeMySQL},
		{"DROP TABLE t", consts.DBTypeMySQL},
		{"CREATE TABLE t (a INT)", consts.DBTypeMySQL},
		{"TRUNCATE TABLE t", consts.DBTypeMySQL},
		{"ALTER TABLE t ADD c INT", consts.DBTypeMySQL},
		{"GRANT ALL ON *.* TO u", consts.DBTypeMySQL},
		{"SET NAMES utf8", consts.DBTypeMySQL},
		{"; SELECT 1", consts.DBTypeMySQL},
		{"SELECT 'unterminated", consts.DBTypeMySQL},
		{"/* unterminated", consts.DBTypeMySQL},
		{"DESC t", consts.DBTypePostgres},           // MySQL-only
		{"DESCRIBE t", consts.DBTypePostgres},       // MySQL-only
		{"# note\nSELECT 1", consts.DBTypePostgres}, // # is not a PG comment
		{"UPDATE t SET x = 1", consts.DBTypePostgres},
	}
	for _, tc := range cases {
		if err := checkQueryStatement(tc.sql, tc.dbType); err == nil {
			t.Errorf("guard accepted %q (%s)", tc.sql, tc.dbType)
		}
	}
}

func TestSQLQueryEndpoint(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	created := createConnection(t, front, "q", 33061)
	connID, _ := created["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{
		33061: adapter,
	})

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(int64(1), "alice").
		AddRow(int64(2), nil)
	mock.ExpectQuery("SELECT id, name FROM users").WillReturnRows(rows)

	resp := mustJSON(t, front, http.MethodPost, "/api/sql/query", map[string]any{
		"connectionId": connID,
		"sql":          "SELECT id, name FROM users",
	}, http.StatusOK)
	if resp["rowCount"].(float64) != 2 || resp["truncated"] != false {
		t.Fatalf("unexpected result: %v", resp)
	}
	columns := resp["columns"].([]any)
	if len(columns) != 2 || columns[0] != "id" || columns[1] != "name" {
		t.Fatalf("columns wrong: %v", columns)
	}
	rowList := resp["rows"].([]any)
	second := rowList[1].([]any)
	if second[1] != nil {
		t.Fatalf("NULL not preserved: %v", rowList)
	}
	if _, has := resp["elapsedMs"]; !has {
		t.Fatalf("elapsedMs missing: %v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	// Guard rejection is a 400 and never hits the database.
	mustJSON(t, front, http.MethodPost, "/api/sql/query", map[string]any{
		"connectionId": connID,
		"sql":          "DELETE FROM users",
	}, http.StatusBadRequest)

	// Row cap: 1001 rows → 1000 returned with truncated=true.
	capped := sqlmock.NewRows([]string{"n"})
	for i := 0; i < queryRowLimit+1; i++ {
		capped.AddRow(int64(i))
	}
	mock.ExpectQuery("SELECT n FROM big").WillReturnRows(capped)
	resp = mustJSON(t, front, http.MethodPost, "/api/sql/query", map[string]any{
		"connectionId": connID,
		"sql":          "SELECT n FROM big",
	}, http.StatusOK)
	if resp["rowCount"].(float64) != queryRowLimit || resp["truncated"] != true {
		t.Fatalf("truncation wrong: rowCount=%v truncated=%v", resp["rowCount"], resp["truncated"])
	}

	// Unknown connection.
	mustJSON(t, front, http.MethodPost, "/api/sql/query", map[string]any{
		"connectionId": "missing", "sql": "SELECT 1",
	}, http.StatusNotFound)
}
