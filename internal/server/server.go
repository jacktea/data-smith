package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"sync"

	difflogic "github.com/jacktea/data-smith/internal/datasmith/diff"
	"github.com/jacktea/data-smith/internal/server/webfs"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/logger"
)

// request body cap: exec-sql scripts may be large, everything else is small.
const maxRequestBody = 32 << 20

// Server wires the HTTP API to the store, job registry, and web assets.
type Server struct {
	dataDir string
	store   *Store
	jobs    *Registry
	webFS   fs.FS
	mux     *http.ServeMux

	// libraryLocks serializes script-file and version-metadata mutations per
	// library without blocking independent libraries.
	libraryLocks sync.Map

	// Internal seams replace local dependencies in tests; production uses the
	// filesystem, JSON store, and full-diff engine adapters below.
	listLibraryScripts    func(string) ([]scriptInfo, error)
	deleteVersionMeta     func(string, string) error
	runFullDiffEngine     func(context.Context, difflogic.FullDiffParams, string, func(string), func(difflogic.TableProgress)) (difflogic.FullDiffResult, error)
	beforeLibraryMutation func(string, string)

	// openAdapter creates a DB adapter for a stored connection config. Tests
	// override it with a stub; production uses the pkg/db factory.
	openAdapter func(ctx context.Context, cfg *pkgconfig.ConnConfig) (conn.DBAdapter, error)
}

// New builds the server: loads the store, sweeps orphan job directories, and
// registers all routes.
func New(dataDir string) (*Server, error) {
	store, err := OpenStore(storePath(dataDir))
	if err != nil {
		return nil, err
	}
	registry, err := NewRegistry(dataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		dataDir:            dataDir,
		store:              store,
		jobs:               registry,
		webFS:              webfs.Dist(),
		mux:                http.NewServeMux(),
		listLibraryScripts: listScripts,
		deleteVersionMeta:  store.DeleteVersionMeta,
		runFullDiffEngine:  difflogic.RunFullDiff,
	}
	s.openAdapter = db.NewDBAdapterContext
	s.routes()
	if err := s.sweepOrphanVersionMetaAtStartup(); err != nil {
		registry.Close()
		return nil, err
	}
	return s, nil
}

// sweepOrphanVersionMetaAtStartup 在启动时执行一次 store 一致性自检，把
// 已无 up 可执行入口的孤儿版本登记清掉并输出告警（修复历史遗留不一致）。
func (s *Server) sweepOrphanVersionMetaAtStartup() error {
	removed, err := s.SweepOrphanVersionMeta()
	for libID, versions := range removed {
		logger.Warnf("脚本库 %s 清理孤儿版本登记(已无 up 可执行入口): %s", libID, strings.Join(versions, ", "))
	}
	if err != nil {
		return fmt.Errorf("清理孤儿版本登记: %w", err)
	}
	return nil
}

// Close releases background resources (janitor, in-flight job contexts).
func (s *Server) Close() {
	s.jobs.Close()
}

func storePath(dataDir string) string { return joinPath(dataDir, "store.json") }

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("GET /api/health", s.handleHealth)

	m.HandleFunc("GET /api/connections", s.handleListConnections)
	m.HandleFunc("POST /api/connections", s.handleCreateConnection)
	m.HandleFunc("GET /api/connections/{id}", s.handleGetConnection)
	m.HandleFunc("PUT /api/connections/{id}", s.handleUpdateConnection)
	m.HandleFunc("DELETE /api/connections/{id}", s.handleDeleteConnection)
	m.HandleFunc("POST /api/connections/{id}/test", s.handleTestConnection)
	m.HandleFunc("GET /api/connections/{id}/tables", s.handleListTables)

	m.HandleFunc("GET /api/schemes", s.handleListSchemes)
	m.HandleFunc("POST /api/schemes", s.handleCreateScheme)
	m.HandleFunc("GET /api/schemes/{id}", s.handleGetScheme)
	m.HandleFunc("PUT /api/schemes/{id}", s.handleUpdateScheme)
	m.HandleFunc("DELETE /api/schemes/{id}", s.handleDeleteScheme)

	m.HandleFunc("POST /api/jobs/diff-schema", s.handleDiffSchemaSubmit)
	m.HandleFunc("POST /api/jobs/diff-data", s.handleDiffDataSubmit)
	m.HandleFunc("POST /api/jobs/diff-full", s.handleDiffFullSubmit)
	m.HandleFunc("POST /api/jobs/exec-sql", s.handleExecSQLSubmit)
	m.HandleFunc("POST /api/reset/preview", s.handleResetPreview)
	m.HandleFunc("POST /api/jobs/reset", s.handleResetSubmit)
	m.HandleFunc("POST /api/jobs/migrate", s.handleMigrateSubmit)
	m.HandleFunc("POST /api/jobs/rollback", s.handleRollbackSubmit)
	m.HandleFunc("POST /api/migrate/ledger/delete", s.handleMigrateLedgerDelete)
	m.HandleFunc("GET /api/jobs", s.handleListJobs)
	m.HandleFunc("GET /api/jobs/{id}", s.handleGetJob)
	m.HandleFunc("POST /api/jobs/{id}/cancel", s.handleCancelJob)
	m.HandleFunc("GET /api/jobs/{id}/artifacts", s.handleListArtifacts)
	m.HandleFunc("GET /api/jobs/{id}/artifacts/download", s.handleDownloadArtifactsZip)
	m.HandleFunc("GET /api/jobs/{id}/artifacts/{name}", s.handleDownloadArtifact)

	m.HandleFunc("POST /api/sql/query", s.handleSQLQuery)

	m.HandleFunc("GET /api/libraries", s.handleListLibraries)
	m.HandleFunc("POST /api/libraries", s.handleCreateLibrary)
	m.HandleFunc("DELETE /api/libraries/{id}", s.handleDeleteLibrary)
	m.HandleFunc("GET /api/libraries/{id}/scripts", s.handleListScripts)
	m.HandleFunc("POST /api/libraries/{id}/scripts", s.handleCreateScript)
	m.HandleFunc("GET /api/libraries/{id}/scripts/{fileName}", s.handleGetScript)
	m.HandleFunc("PUT /api/libraries/{id}/scripts/{fileName}", s.handlePutScript)
	m.HandleFunc("DELETE /api/libraries/{id}/scripts/{fileName}", s.handleDeleteScript)
	m.HandleFunc("GET /api/libraries/{id}/scripts/{fileName}/download", s.handleDownloadScript)
	m.HandleFunc("POST /api/libraries/{id}/scripts/copy", s.handleCopyScripts)

	m.HandleFunc("POST /api/libraries/{id}/copy", s.handleCopyLibrary)
	m.HandleFunc("POST /api/libraries/{id}/versions", s.handleRegisterVersion)
	m.HandleFunc("GET /api/migrate/plan", s.handleMigratePlan)

	m.HandleFunc("GET /", s.handleStatic)
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) lockLibrary(id string) func() {
	value, _ := s.libraryLocks.LoadOrStore(id, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// setAdapterOpener overrides the adapter factory; used by tests to stub
// database backends.
func (s *Server) setAdapterOpener(fn func(ctx context.Context, cfg *pkgconfig.ConnConfig) (conn.DBAdapter, error)) {
	s.openAdapter = fn
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// --- JSON helpers -----------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("请求体 JSON 解析失败: %w", err)
	}
	return nil
}

func strictUnmarshal(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func marshalIndent(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- static assets ----------------------------------------------------------

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "不支持的方法")
		return
	}
	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "" || name == "." {
		name = "index.html"
	}
	if s.serveAsset(w, r, name) {
		return
	}
	// SPA fallback: unknown paths render the app shell.
	if !s.serveAsset(w, r, "index.html") {
		http.NotFound(w, r)
	}
}

func (s *Server) serveAsset(w http.ResponseWriter, r *http.Request, name string) bool {
	f, err := s.webFS.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		return false
	}
	seeker, ok := f.(io.ReadSeeker)
	if !ok {
		return false
	}
	ctype := mime.TypeByExtension(path.Ext(name))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ctype)
	http.ServeContent(w, r, path.Base(name), info.ModTime(), seeker)
	return true
}
