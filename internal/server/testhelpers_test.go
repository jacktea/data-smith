package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

// newTestServer builds a Server over a temp data dir with an httptest front.
func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		front.Close()
		srv.Close()
	})
	return srv, front
}

// doRequest performs a request and returns status plus raw body.
func doRequest(t *testing.T, front *httptest.Server, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, front.URL+path, reader)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	return resp.StatusCode, raw
}

func decodeJSONBody(t *testing.T, method, path string, raw []byte) any {
	t.Helper()
	if len(bytes.TrimSpace(raw)) == 0 {
		t.Fatalf("%s %s: empty response body", method, path)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s %s: response is not JSON: %s", method, path, raw)
	}
	return v
}

// mustJSON performs a request, asserts the status, and returns a decoded
// object response.
func mustJSON(t *testing.T, front *httptest.Server, method, path string, body any, wantStatus int) map[string]any {
	t.Helper()
	status, raw := doRequest(t, front, method, path, body)
	if status != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d (body: %s)", method, path, status, wantStatus, raw)
	}
	decoded := decodeJSONBody(t, method, path, raw)
	obj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("%s %s: want JSON object, got %T", method, path, decoded)
	}
	return obj
}

// mustJSONArray performs a request, asserts the status, and returns a decoded
// array response.
func mustJSONArray(t *testing.T, front *httptest.Server, method, path string, body any, wantStatus int) []any {
	t.Helper()
	status, raw := doRequest(t, front, method, path, body)
	if status != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d (body: %s)", method, path, status, wantStatus, raw)
	}
	decoded := decodeJSONBody(t, method, path, raw)
	arr, ok := decoded.([]any)
	if !ok {
		t.Fatalf("%s %s: want JSON array, got %T", method, path, decoded)
	}
	return arr
}

// statusOf performs a request and returns only the status.
func statusOf(t *testing.T, front *httptest.Server, method, path string, body any) int {
	t.Helper()
	status, _ := doRequest(t, front, method, path, body)
	return status
}

// createConnection posts a MySQL connection with a known password.
func createConnection(t *testing.T, front *httptest.Server, name string, port int) map[string]any {
	t.Helper()
	body := map[string]any{
		"name": name, "type": "mysql", "host": "127.0.0.1", "port": port,
		"user": "root", "password": "secret-" + name, "dbname": "appdb",
	}
	return mustJSON(t, front, http.MethodPost, "/api/connections", body, http.StatusOK)
}

// --- stub adapter backed by sqlmock -----------------------------------------

type stubAdapter struct {
	db  *sql.DB
	cfg *pkgconfig.ConnConfig
}

func (a *stubAdapter) ReadSchema() (*conn.DatabaseSchema, error) {
	return &conn.DatabaseSchema{Tables: map[string]*conn.Table{}}, nil
}

func (a *stubAdapter) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a *stubAdapter) ExtractTable(tableName string) (*conn.Table, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a *stubAdapter) ExtractView(viewName string) (*conn.Table, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a *stubAdapter) GetConn() *sql.DB                 { return a.db }
func (a *stubAdapter) GetConfig() *pkgconfig.ConnConfig { return a.cfg }
func (a *stubAdapter) Close() error                     { return nil }

func newMySQLStub(t *testing.T) (*stubAdapter, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New(): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg := &pkgconfig.ConnConfig{Type: "mysql", Host: "127.0.0.1", Port: 3306, DBName: "appdb"}
	return &stubAdapter{db: db, cfg: cfg}, mock
}

// stubSchemaAdapter returns a fixed schema (users table + v_users view).
type stubSchemaAdapter struct {
	*stubAdapter
}

func (a *stubSchemaAdapter) ReadSchema() (*conn.DatabaseSchema, error) {
	return &conn.DatabaseSchema{Tables: map[string]*conn.Table{
		"users": {
			Name: "users", Type: conn.TableTypeTable,
			Columns: map[string]*conn.Column{
				"id":   {Name: "id", DataType: "int", Position: 1},
				"name": {Name: "name", DataType: "varchar", Nullable: true, Position: 2},
			},
			PrimaryKey: &conn.PrimaryKey{Name: "PRIMARY", Columns: []string{"id"}},
		},
		"v_users": {
			Name: "v_users", Type: conn.TableTypeView,
			Columns: map[string]*conn.Column{},
		},
	}}, nil
}

// installStubAdapters routes adapter opens to per-port stubs.
func installStubAdapters(s *Server, byPort map[int]conn.DBAdapter) {
	s.setAdapterOpener(func(ctx context.Context, cfg *pkgconfig.ConnConfig) (conn.DBAdapter, error) {
		if adapter, ok := byPort[cfg.Port]; ok {
			return adapter, nil
		}
		return nil, fmt.Errorf("测试桩未配置端口 %d 的适配器", cfg.Port)
	})
}

// waitForJobStatus polls until the job reaches status or times out.
func waitForJobStatus(t *testing.T, job *Job, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if job.Status() == status {
			return
		}
		if time.Now().After(deadline) {
			view := job.View()
			t.Fatalf("job %s: status = %s, want %s (error: %q, log: %v)", job.ID(), view.Status, status, view.Error, view.Log)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForCondition polls until fn returns true or times out.
func waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
