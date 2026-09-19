package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/conn"
)

// TestTestConnectionReportsFailure exercises POST /api/connections/{id}/test
// against an unreachable address; the endpoint answers 200 with ok=false.
func TestTestConnectionReportsFailure(t *testing.T) {
	_, front := newTestServer(t)
	created := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "dead", "type": "mysql", "host": "127.0.0.1", "port": 1, "dbname": "x",
	}, http.StatusOK)
	started := time.Now()
	resp := mustJSON(t, front, http.MethodPost, "/api/connections/"+created["id"].(string)+"/test", nil, http.StatusOK)
	if resp["ok"] != false {
		t.Fatalf("ok = %v, want false: %v", resp["ok"], resp)
	}
	if _, has := resp["error"]; !has {
		t.Fatalf("error missing: %v", resp)
	}
	if elapsed := time.Since(started); elapsed > testConnectionTimeout {
		t.Fatalf("test connection took %s, unbounded", elapsed)
	}
}

func TestResetPreviewValidatesTarget(t *testing.T) {
	_, front := newTestServer(t)

	ok := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "biz", "type": "mysql", "host": "h", "port": 3306, "dbname": "shop",
	}, http.StatusOK)
	resp := mustJSON(t, front, http.MethodPost, "/api/reset/preview", map[string]any{
		"connectionId": ok["id"],
	}, http.StatusOK)
	sqlText, _ := resp["sql"].(string)
	if !strings.Contains(sqlText, "DROP DATABASE") || !strings.Contains(sqlText, "CREATE DATABASE") {
		t.Fatalf("reset SQL wrong: %q", sqlText)
	}

	// MySQL system database refused (ValidateResetTarget via BuildResetSQL).
	sys := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "sys", "type": "mysql", "host": "h", "port": 3306, "dbname": "mysql",
	}, http.StatusOK)
	if status := statusOf(t, front, http.MethodPost, "/api/reset/preview", map[string]any{
		"connectionId": sys["id"],
	}); status != http.StatusBadRequest {
		t.Fatalf("system db should be refused: %d", status)
	}

	// Postgres valid target (non-system db/schema) previews fine.
	pg := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "pg", "type": "postgres", "host": "h", "port": 5432, "dbname": "appdb", "tableSchema": "public",
	}, http.StatusOK)
	mustJSON(t, front, http.MethodPost, "/api/reset/preview", map[string]any{"connectionId": pg["id"]}, http.StatusOK)

	// Unknown connection.
	if status := statusOf(t, front, http.MethodPost, "/api/reset/preview", map[string]any{
		"connectionId": "missing",
	}); status != http.StatusBadRequest {
		t.Fatalf("unknown connection should 400: %d", status)
	}
}

func TestResetJobConfirmedGate(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	created := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "biz", "type": "mysql", "host": "h", "port": 33101, "dbname": "shop",
	}, http.StatusOK)
	connID, _ := created["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{33101: adapter})

	// Missing confirmed → 400, no job created.
	if status := statusOf(t, front, http.MethodPost, "/api/jobs/reset", map[string]any{
		"connectionId": connID,
	}); status != http.StatusBadRequest {
		t.Fatalf("unconfirmed reset should 400: %d", status)
	}
	if status := statusOf(t, front, http.MethodPost, "/api/jobs/reset", map[string]any{
		"connectionId": connID, "confirmed": false,
	}); status != http.StatusBadRequest {
		t.Fatalf("confirmed=false should 400: %d", status)
	}

	// confirmed=true → job runs; engine executes the reset statement.
	mock.ExpectExec("DROP DATABASE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	resp := mustJSON(t, front, http.MethodPost, "/api/jobs/reset", map[string]any{
		"connectionId": connID, "confirmed": true,
	}, http.StatusOK)
	jobObj, _ := resp["job"].(map[string]any)
	if jobObj == nil {
		t.Fatalf("job missing: %v", resp)
	}
	job := srv.jobs.Get(jobObj["id"].(string))
	if job == nil {
		t.Fatal("job not in registry")
	}
	waitForJobStatus(t, job, jobStatusSucceeded)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	view := job.View()
	if view.Summary["ok"] != true {
		t.Fatalf("reset summary wrong: %v", view.Summary)
	}
}

func TestMigrateAndRollbackSubmitGates(t *testing.T) {
	srv, front := newTestServer(t)
	created := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "biz", "type": "mysql", "host": "h", "port": 33102, "dbname": "shop",
	}, http.StatusOK)
	connID, _ := created["id"].(string)

	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "lib"}, http.StatusOK)
	libID, _ := lib["id"].(string)

	// Unknown library / connection rejected before any job creation.
	if status := statusOf(t, front, http.MethodPost, "/api/jobs/migrate", map[string]any{
		"libraryId": "missing", "connectionId": connID, "dryRun": true,
	}); status != http.StatusBadRequest {
		t.Fatalf("unknown library should 400: %d", status)
	}
	if status := statusOf(t, front, http.MethodPost, "/api/jobs/migrate", map[string]any{
		"libraryId": libID, "connectionId": "missing",
	}); status != http.StatusBadRequest {
		t.Fatalf("unknown connection should 400: %d", status)
	}

	// Rollback requires confirmed=true.
	if status := statusOf(t, front, http.MethodPost, "/api/jobs/rollback", map[string]any{
		"libraryId": libID, "connectionId": connID,
	}); status != http.StatusBadRequest {
		t.Fatalf("unconfirmed rollback should 400: %d", status)
	}

	// Confirmed migrate reaches the adapter; an adapter open failure surfaces
	// as job failure (engine wiring smoke).
	resp := mustJSON(t, front, http.MethodPost, "/api/jobs/migrate", map[string]any{
		"libraryId": libID, "connectionId": connID, "dryRun": true,
	}, http.StatusOK)
	jobObj, _ := resp["job"].(map[string]any)
	job := srv.jobs.Get(jobObj["id"].(string))
	if job == nil {
		t.Fatal("job not in registry")
	}
	waitForJobStatus(t, job, jobStatusFailed)
	view := job.View()
	if view.Error == "" {
		t.Fatal("adapter failure not surfaced in job error")
	}

	// Confirmed rollback likewise reaches the adapter.
	resp = mustJSON(t, front, http.MethodPost, "/api/jobs/rollback", map[string]any{
		"libraryId": libID, "connectionId": connID, "confirmed": true,
	}, http.StatusOK)
	jobObj, _ = resp["job"].(map[string]any)
	job = srv.jobs.Get(jobObj["id"].(string))
	waitForJobStatus(t, job, jobStatusFailed)
}

func TestConnectionsTablesEndpoint(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, _ := newMySQLStub(t)
	created := createConnection(t, front, "db", 33103)
	connID, _ := created["id"].(string)

	// Schema-reading stub: replace the default empty one.
	schema := &stubSchemaAdapter{stubAdapter: adapter}
	installStubAdapters(srv, map[int]conn.DBAdapter{33103: schema})

	tables := mustJSON(t, front, http.MethodGet, "/api/connections/"+connID+"/tables", nil, http.StatusOK)
	list, _ := tables["tables"].([]any)
	if len(list) != 1 {
		t.Fatalf("kind=default should hide views: %v", tables)
	}
	only, _ := list[0].(map[string]any)
	if only["name"] != "users" || only["type"] != "table" {
		t.Fatalf("table wrong: %v", only)
	}
	columns, _ := only["columns"].([]any)
	first, _ := columns[0].(map[string]any)
	if first["name"] != "id" || first["primaryKey"] != true || first["position"] != float64(1) {
		t.Fatalf("columns wrong: %v", columns)
	}

	all := mustJSON(t, front, http.MethodGet, "/api/connections/"+connID+"/tables?kind=all", nil, http.StatusOK)
	list, _ = all["tables"].([]any)
	if len(list) != 2 {
		t.Fatalf("kind=all should include views: %v", all)
	}
	views := mustJSON(t, front, http.MethodGet, "/api/connections/"+connID+"/tables?kind=view", nil, http.StatusOK)
	list, _ = views["tables"].([]any)
	if len(list) != 1 {
		t.Fatalf("kind=view wrong: %v", views)
	}
	if status := statusOf(t, front, http.MethodGet, "/api/connections/"+connID+"/tables?kind=bogus", nil); status != http.StatusBadRequest {
		t.Fatalf("bad kind should 400: %d", status)
	}
}

func TestStaticFallbackServesIndex(t *testing.T) {
	_, front := newTestServer(t)
	status, raw := doRequest(t, front, http.MethodGet, "/", nil)
	if status != http.StatusOK {
		t.Fatalf("index status = %d", status)
	}
	if !strings.Contains(string(raw), "DataSmith") {
		t.Fatalf("placeholder index wrong: %s", raw)
	}
	// SPA fallback: unknown non-API path also serves index.html.
	status, raw = doRequest(t, front, http.MethodGet, "/connections", nil)
	if status != http.StatusOK || !strings.Contains(string(raw), "DataSmith") {
		t.Fatalf("SPA fallback failed: %d %s", status, raw)
	}
	// Health endpoint.
	resp := mustJSON(t, front, http.MethodGet, "/api/health", nil, http.StatusOK)
	if resp["ok"] != true {
		t.Fatalf("health wrong: %v", resp)
	}
}
