package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
)

func TestStoreAtomicWriteAndPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	conn := &StoredConnection{ID: "conn_1", Name: "a", Type: "mysql", Host: "h", Port: 3306, Password: "p"}
	if err := store.PutConnection(conn); err != nil {
		t.Fatalf("PutConnection: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat store: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("store.json perms = %v, want 0600", perm)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".store-") {
			t.Fatalf("temporary store file left behind: %s", entry.Name())
		}
	}
}

func TestStoreMissingFileStartsEmpty(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "store.json"))
	if err != nil {
		t.Fatalf("OpenStore on missing file: %v", err)
	}
	if conns := store.ListConnections(); len(conns) != 0 {
		t.Fatalf("connections = %d, want 0", len(conns))
	}
}

func TestStoreCorruptFileRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if _, err := OpenStore(path); err == nil {
		t.Fatal("OpenStore on corrupt file should fail")
	}
}

func TestStoreRoundTripPreservesSecrets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	proxy := &StoredProxy{Host: "jump", Port: 22, User: "u", Type: "pass", Pass: "proxy-pass", RsaKeyPath: "/k"}
	if err := store.PutConnection(&StoredConnection{
		ID: "conn_x", Name: "n", Type: "mysql", Host: "h", Port: 1, Password: "pw",
		Proxy: proxy, ConnectTimeoutMs: 3000, MaxOpenConns: 7,
	}); err != nil {
		t.Fatalf("PutConnection: %v", err)
	}
	// Reload from disk: secrets must survive.
	reloaded, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := reloaded.GetConnection("conn_x")
	if !ok {
		t.Fatal("connection missing after reload")
	}
	if got.Password != "pw" || got.Proxy == nil || got.Proxy.Pass != "proxy-pass" {
		t.Fatalf("secrets lost after reload: %+v", got)
	}
	if got.ConnectTimeoutMs != 3000 || got.MaxOpenConns != 7 {
		t.Fatalf("numeric fields lost: %+v", got)
	}
}

func TestConnConfigProjection(t *testing.T) {
	stored := &StoredConnection{
		ID: "c", Type: "postgres", Host: "h", Port: 5432, User: "u", Password: "p",
		DBName: "db", TableSchema: "public", ConnectTimeoutMs: 2500, MaxOpenConns: 4,
		Proxy: &StoredProxy{Host: "j", Port: 22, User: "ju", Type: "rsa", RsaKey: "KEY"},
	}
	cfg := stored.ConnConfig()
	if cfg.Type != "postgres" || cfg.ConnectTimeout != 2500*time.Millisecond || cfg.MaxOpenConns != 4 {
		t.Fatalf("bad config projection: %+v", cfg)
	}
	proxy, ok := cfg.Proxy.(*pkgconfig.SSHProxy)
	if !ok || proxy.Host != "j" || proxy.RsaKey != "KEY" {
		t.Fatalf("proxy projection wrong: %+v, ok=%v", cfg.Proxy, ok)
	}
}

// TestConnectionsMaskingAndKeepPassword covers the masking contract and the
// PUT keep-password semantics end to end.
func TestConnectionsMaskingAndKeepPassword(t *testing.T) {
	srv, front := newTestServer(t)

	created := mustJSON(t, front, http.MethodPost, "/api/connections", map[string]any{
		"name": "prod", "type": "mysql", "host": "db", "port": 3306,
		"user": "root", "password": "super-secret", "dbname": "app",
		"proxy": map[string]any{
			"host": "jump", "port": 22, "user": "ju", "type": "pass",
			"pass": "proxy-secret", "rsaKeyPath": "/keys/id_rsa",
		},
	}, http.StatusOK)
	connID, _ := created["id"].(string)
	if connID == "" {
		t.Fatalf("create connection: missing id: %v", created)
	}
	if created["passwordSet"] != true {
		t.Fatalf("passwordSet = %v, want true", created["passwordSet"])
	}
	raw, _ := json.Marshal(created)
	if strings.Contains(string(raw), "super-secret") || strings.Contains(string(raw), "proxy-secret") {
		t.Fatalf("response leaks secrets: %s", raw)
	}
	proxyView, ok := created["proxy"].(map[string]any)
	if !ok {
		t.Fatalf("proxy view missing: %v", created)
	}
	if proxyView["passSet"] != true || proxyView["rsaKeyPathSet"] != true {
		t.Fatalf("proxy set flags wrong: %v", proxyView)
	}
	if _, has := proxyView["pass"]; has {
		t.Fatalf("proxy view exposes pass: %v", proxyView)
	}

	// PUT without password/proxy secrets: values must be preserved server-side.
	updated := mustJSON(t, front, http.MethodPut, "/api/connections/"+connID, map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "dbname": "app",
		"proxy": map[string]any{
			"host": "jump", "port": 22, "user": "ju", "type": "pass",
			"rsaKeyPath": "/keys/id_rsa",
		},
	}, http.StatusOK)
	if updated["passwordSet"] != true {
		t.Fatalf("password lost on update: %v", updated)
	}
	stored, ok := srv.store.GetConnection(connID)
	if !ok {
		t.Fatal("stored connection missing")
	}
	if stored.Password != "super-secret" {
		t.Fatalf("stored password = %q, want original", stored.Password)
	}
	if stored.Proxy == nil || stored.Proxy.Pass != "proxy-secret" {
		t.Fatalf("proxy pass lost on update: %+v", stored.Proxy)
	}
	if stored.Host != "db2" || stored.Port != 3307 {
		t.Fatalf("update not applied: %+v", stored)
	}

	// Explicit empty password clears it.
	mustJSON(t, front, http.MethodPut, "/api/connections/"+connID, map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "password": "", "dbname": "app",
	}, http.StatusOK)
	stored, _ = srv.store.GetConnection(connID)
	if stored.Password != "" {
		t.Fatalf("explicit empty password not applied: %q", stored.Password)
	}
	if stored.Proxy == nil || stored.Proxy.Pass != "proxy-secret" {
		t.Fatalf("proxy pass should survive separate update: %+v", stored.Proxy)
	}

	// Explicit "proxy": null clears the proxy; absent field keeps it.
	mustJSON(t, front, http.MethodPut, "/api/connections/"+connID, map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "dbname": "app",
		"proxy": map[string]any{"host": "jump2", "port": 2222, "user": "ju2", "type": "pass"},
	}, http.StatusOK)
	stored, _ = srv.store.GetConnection(connID)
	if stored.Proxy == nil || stored.Proxy.Host != "jump2" {
		t.Fatalf("proxy replacement failed: %+v", stored.Proxy)
	}
	// New proxy had no pass in request and no old proxy pass semantics apply
	// only to same-proxy merges; here old pass was kept from previous proxy.
	if stored.Proxy.Pass != "proxy-secret" {
		t.Fatalf("proxy pass should carry over when absent: %+v", stored.Proxy)
	}

	rawBodyPut, err := json.Marshal(map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "dbname": "app", "proxy": nil,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, front.URL+"/api/connections/"+connID, bytes.NewReader(rawBodyPut))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := front.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("explicit null PUT status = %d", putResp.StatusCode)
	}
	stored, _ = srv.store.GetConnection(connID)
	if stored.Proxy != nil {
		t.Fatalf("explicit null proxy should clear: %+v", stored.Proxy)
	}
	// Absent proxy field still keeps (nothing to keep now → nil).
	mustJSON(t, front, http.MethodPut, "/api/connections/"+connID, map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "dbname": "app",
	}, http.StatusOK)
	stored, _ = srv.store.GetConnection(connID)
	if stored.Proxy != nil {
		t.Fatalf("absent proxy should keep (nil): %+v", stored.Proxy)
	}

	// Malformed proxy object rejected.
	if status := statusOf(t, front, http.MethodPut, "/api/connections/"+connID, map[string]any{
		"name": "prod-2", "type": "mysql", "host": "db2", "port": 3307,
		"user": "root", "dbname": "app", "proxy": map[string]any{"bogus": 1},
	}); status != http.StatusBadRequest {
		t.Fatalf("malformed proxy should 400: %d", status)
	}

	// Validation errors.
	badType := map[string]any{"name": "x", "type": "oracle", "host": "h", "port": 1}
	if status := statusOf(t, front, http.MethodPost, "/api/connections", badType); status != http.StatusBadRequest {
		t.Fatalf("bad type: status = %d", status)
	}

	// 404 paths.
	if status := statusOf(t, front, http.MethodGet, "/api/connections/nope", nil); status != http.StatusNotFound {
		t.Fatalf("missing GET: status = %d", status)
	}

	// Delete then double-delete.
	mustJSON(t, front, http.MethodDelete, "/api/connections/"+connID, nil, http.StatusOK)
	if status := statusOf(t, front, http.MethodDelete, "/api/connections/"+connID, nil); status != http.StatusNotFound {
		t.Fatalf("double delete: status = %d", status)
	}
}
