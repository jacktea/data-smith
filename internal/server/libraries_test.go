package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/conn"
)

// createLibraryAndJob prepares a library plus a succeeded diff-schema job with
// the given artifacts, returning (libraryID, jobID).
func createLibraryAndJob(t *testing.T, srv *Server, front *httptest.Server, artifacts map[string]string) (string, string) {
	t.Helper()
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "主库迁移"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	job := srv.jobs.Submit(JobDiffSchema, nil, func(ctx context.Context, j *Job) error {
		for name, content := range artifacts {
			if err := os.WriteFile(filepath.Join(j.Dir(), name), []byte(content), 0o644); err != nil {
				return err
			}
		}
		return nil
	})
	waitForJobStatus(t, job, jobStatusSucceeded)
	return libID, job.ID()
}

// createLibraryAndDiffJobs prepares a library plus a succeeded diff-schema job
// and a succeeded diff-data job, returning (libraryID, schemaJobID, dataJobID).
func createLibraryAndDiffJobs(t *testing.T, srv *Server, front *httptest.Server) (string, string, string) {
	t.Helper()
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "主库迁移"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	schemaJob := srv.jobs.Submit(JobDiffSchema, nil, func(ctx context.Context, j *Job) error {
		for name, content := range map[string]string{
			"schema_diff.sql":          regSchemaForward,
			"schema_diff_rollback.sql": regSchemaRollback,
		} {
			if err := os.WriteFile(filepath.Join(j.Dir(), name), []byte(content), 0o644); err != nil {
				return err
			}
		}
		return nil
	})
	dataJob := srv.jobs.Submit(JobDiffData, nil, func(ctx context.Context, j *Job) error {
		for name, content := range map[string]string{
			"data_diff.sql":          regDataForward,
			"data_diff_rollback.sql": regDataRollback,
		} {
			if err := os.WriteFile(filepath.Join(j.Dir(), name), []byte(content), 0o644); err != nil {
				return err
			}
		}
		return nil
	})
	waitForJobStatus(t, schemaJob, jobStatusSucceeded)
	waitForJobStatus(t, dataJob, jobStatusSucceeded)
	return libID, schemaJob.ID(), dataJob.ID()
}

func TestLibraryLifecycle(t *testing.T) {
	srv, front := newTestServer(t)
	libs := mustJSONArray(t, front, http.MethodGet, "/api/libraries", nil, http.StatusOK)
	if len(libs) != 0 {
		t.Fatalf("expected empty library list, got %v", libs)
	}
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "订单库"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	if libID == "" {
		t.Fatalf("library id missing: %v", lib)
	}
	if status := statusOf(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": ""}); status != http.StatusBadRequest {
		t.Fatalf("empty name: status = %d", status)
	}

	// Script create/list/get/put/delete.
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+libID+"/scripts", map[string]any{
		"fileName": "V1__init.up.sql", "content": "CREATE TABLE a (id INT);",
	}, http.StatusOK)
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+libID+"/scripts", map[string]any{
		"fileName": "2__no_direction.sql", "content": "CREATE TABLE b (id INT);",
	}, http.StatusOK)

	scripts := mustJSONArray(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts", nil, http.StatusOK)
	if len(scripts) != 2 {
		t.Fatalf("scripts = %d, want 2", len(scripts))
	}
	byName := map[string]map[string]any{}
	for _, s := range scripts {
		m, _ := s.(map[string]any)
		byName[m["fileName"].(string)] = m
	}
	if byName["V1__init.up.sql"]["direction"] != "up" {
		t.Fatalf("direction wrong: %v", byName)
	}
	if byName["2__no_direction.sql"]["direction"] != "" {
		t.Fatalf("implicit up should show empty direction: %v", byName)
	}

	got := mustJSON(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts/V1__init.up.sql", nil, http.StatusOK)
	if got["content"] != "CREATE TABLE a (id INT);" {
		t.Fatalf("content wrong: %v", got)
	}

	mustJSON(t, front, http.MethodPut, "/api/libraries/"+libID+"/scripts/V1__init.up.sql", map[string]any{
		"content": "CREATE TABLE a2 (id INT);",
	}, http.StatusOK)
	got = mustJSON(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts/V1__init.up.sql", nil, http.StatusOK)
	if got["content"] != "CREATE TABLE a2 (id INT);" {
		t.Fatalf("replace failed: %v", got)
	}

	// Guards: down rejected, bad names rejected, traversal rejected.
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+libID+"/scripts", map[string]any{
		"fileName": "V2__bad.down.sql", "content": "x",
	}); status != http.StatusBadRequest {
		t.Fatalf("down should be rejected: %d", status)
	}
	if status := statusOf(t, front, http.MethodPut, "/api/libraries/"+libID+"/scripts/V2__bad.down.sql", map[string]any{
		"content": "x",
	}); status != http.StatusBadRequest {
		t.Fatalf("down PUT should be rejected: %d", status)
	}
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+libID+"/scripts", map[string]any{
		"fileName": "not-a-migration.sql", "content": "x",
	}); status != http.StatusBadRequest {
		t.Fatalf("bad filename should be rejected: %d", status)
	}
	if status := statusOf(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts/..%2F..%2Fstore.json", nil); status != http.StatusBadRequest {
		t.Fatalf("traversal should be rejected: %d", status)
	}
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+libID+"/scripts", map[string]any{
		"fileName": "V1__init.up.sql", "content": "dup",
	}); status != http.StatusBadRequest {
		t.Fatalf("duplicate should be rejected: %d", status)
	}

	// Download endpoint serves the raw file.
	resp, err := front.Client().Get(front.URL + "/api/libraries/" + libID + "/scripts/V1__init.up.sql/download")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d", resp.StatusCode)
	}

	mustJSON(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V1__init.up.sql", nil, http.StatusOK)
	scripts = mustJSONArray(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts", nil, http.StatusOK)
	if len(scripts) != 1 {
		t.Fatalf("after delete scripts = %d, want 1", len(scripts))
	}

	// Library delete removes the directory.
	dir := filepath.Join(srv.dataDir, "libraries", libID)
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("library dir missing: %v", err)
	}
	mustJSON(t, front, http.MethodDelete, "/api/libraries/"+libID, nil, http.StatusOK)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("library dir not removed: %v", err)
	}
}

const (
	regSchemaForward  = "-- DATASMITH EXECUTE-ON: source\nALTER TABLE users ADD COLUMN age INT;\n"
	regSchemaRollback = "-- DATASMITH EXECUTE-ON: source\nALTER TABLE users DROP COLUMN age;\n"
	regDataForward    = "-- DATASMITH EXECUTE-ON: source\nINSERT INTO `users` (`id`,`name`) VALUES (1,'a');\n"
	regDataRollback   = "-- DATASMITH EXECUTE-ON: source\nDELETE FROM `users` WHERE `id`=1;\n"
)

func TestRegisterVersionConcatenatesAndDeduplicatesHeader(t *testing.T) {
	srv, front := newTestServer(t)
	libID, schemaJobID, dataJobID := createLibraryAndDiffJobs(t, srv, front)

	resp := mustJSON(t, front, http.MethodPost, "/api/libraries/"+libID+"/versions", map[string]any{
		"sourceJobId": schemaJobID, "companionJobId": dataJobID, "includeSchema": true, "includeData": true,
		"version": "1.0.0", "title": "初始化", "expectedConnectionId": "conn_x",
	}, http.StatusOK)
	if resp["upFile"] != "V1.0.0__初始化.up.sql" || resp["downFile"] != "V1.0.0__初始化.down.sql" {
		t.Fatalf("file names wrong: %v", resp)
	}

	up, err := os.ReadFile(filepath.Join(srv.dataDir, "libraries", libID, "V1.0.0__初始化.up.sql"))
	if err != nil {
		t.Fatalf("read up: %v", err)
	}
	upText := string(up)
	if got := strings.Count(upText, "DATASMITH EXECUTE-ON"); got != 1 {
		t.Fatalf("up header count = %d, want 1:\n%s", got, upText)
	}
	if !strings.HasPrefix(upText, "-- DATASMITH EXECUTE-ON: source\n") {
		t.Fatalf("up must start with single header:\n%s", upText)
	}
	if strings.Index(upText, "-- ==== schema forward ====") > strings.Index(upText, "-- ==== data forward ====") {
		t.Fatalf("up section order wrong:\n%s", upText)
	}
	if !strings.Contains(upText, "ALTER TABLE users ADD COLUMN age INT;") ||
		!strings.Contains(upText, "INSERT INTO `users`") {
		t.Fatalf("up content missing statements:\n%s", upText)
	}

	down, err := os.ReadFile(filepath.Join(srv.dataDir, "libraries", libID, "V1.0.0__初始化.down.sql"))
	if err != nil {
		t.Fatalf("read down: %v", err)
	}
	downText := string(down)
	if got := strings.Count(downText, "DATASMITH EXECUTE-ON"); got != 1 {
		t.Fatalf("down header count = %d, want 1:\n%s", got, downText)
	}
	if strings.Index(downText, "-- ==== data rollback ====") > strings.Index(downText, "-- ==== schema rollback ====") {
		t.Fatalf("down section order wrong (data rollback must come first):\n%s", downText)
	}

	// Duplicate version rejected.
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+libID+"/versions", map[string]any{
		"sourceJobId": schemaJobID, "includeSchema": true, "version": "v1.0.0", "title": "again",
	}); status != http.StatusBadRequest {
		t.Fatalf("duplicate version should be rejected: %d", status)
	}
	// Different version works and records metadata.
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+libID+"/versions", map[string]any{
		"sourceJobId": schemaJobID, "includeSchema": true, "version": "2", "title": "second",
		"expectedConnectionId": "conn_other",
	}, http.StatusOK)
	lib, ok := srv.store.GetLibrary(libID)
	if !ok {
		t.Fatal("library missing")
	}
	if lib.Versions["1.0.0"].ExpectedConnectionID != "conn_x" {
		t.Fatalf("first version meta wrong: %+v", lib.Versions)
	}
	if _, known := lib.Versions["2"]; !known {
		t.Fatalf("second version meta missing: %+v", lib.Versions)
	}
}

func TestRegisterVersionValidation(t *testing.T) {
	srv, front := newTestServer(t)
	libID, _ := createLibraryAndJob(t, srv, front, map[string]string{
		"schema_diff.sql": regSchemaForward, "schema_diff_rollback.sql": regSchemaRollback,
	})

	post := func(body map[string]any) int {
		return statusOf(t, front, http.MethodPost, "/api/libraries/"+libID+"/versions", body)
	}
	base := map[string]any{"sourceJobId": "nonexistent", "includeSchema": true, "version": "1.0.0", "title": "t"}
	if post(base) != http.StatusBadRequest {
		t.Fatalf("unknown job should 400")
	}

	// Queued job rejected: block on ctx so cancel transitions to cancelled.
	blocked := srv.jobs.Submit(JobDiffSchema, nil, func(ctx context.Context, j *Job) error {
		<-ctx.Done()
		return ctx.Err()
	})
	srv.jobs.Cancel(blocked.ID())
	waitForJobStatus(t, blocked, jobStatusCancelled)
	if status := post(map[string]any{"sourceJobId": blocked.ID(), "includeSchema": true, "version": "1", "title": "t"}); status != http.StatusBadRequest {
		t.Fatalf("cancelled job should 400: %d", status)
	}

	// Non-diff job type rejected.
	wrongType := srv.jobs.Submit(JobExecSQL, nil, func(ctx context.Context, j *Job) error { return nil })
	waitForJobStatus(t, wrongType, jobStatusSucceeded)
	if status := post(map[string]any{"sourceJobId": wrongType.ID(), "includeSchema": true, "version": "1", "title": "t"}); status != http.StatusBadRequest {
		t.Fatalf("exec-sql job should 400: %d", status)
	}

	// Both includes false.
	_, okJobID := createLibraryAndJob(t, srv, front, map[string]string{"schema_diff.sql": regSchemaForward})
	if status := post(map[string]any{"sourceJobId": okJobID, "version": "3", "title": "t"}); status != http.StatusBadRequest {
		t.Fatalf("no includes should 400: %d", status)
	}

	// Missing artifact.
	if status := post(map[string]any{"sourceJobId": okJobID, "includeData": true, "version": "3", "title": "t"}); status != http.StatusBadRequest {
		t.Fatalf("missing data artifact should 400: %d", status)
	}

	// Bad version / title.
	if status := post(map[string]any{"sourceJobId": okJobID, "includeSchema": true, "version": "abc", "title": "t"}); status != http.StatusBadRequest {
		t.Fatalf("bad version should 400: %d", status)
	}
	if status := post(map[string]any{"sourceJobId": okJobID, "includeSchema": true, "version": "3", "title": "a.b"}); status != http.StatusBadRequest {
		t.Fatalf("dotted title should 400: %d", status)
	}
}

func TestMigratePlanWithLedger(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	connCreated := createConnection(t, front, "db", 33091)
	connID, _ := connCreated["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{33091: adapter})

	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "lib"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	libDir := filepath.Join(srv.dataDir, "libraries", libID)
	// down 脚本由版本登记生成,这里直接落盘验证扫描兼容性。
	// 注意:引擎文件名正则要求多段版本带 V 前缀。
	files := map[string]string{
		"1__init.up.sql": "SELECT 1;", "1__init.down.sql": "SELECT 0;",
		"V2.0__add_log.up.sql": "SELECT 2;", "V2.0__add_log.down.sql": "SELECT 0;",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(libDir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("seed script: %v", err)
		}
	}

	exists := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(exists)
	ledger := sqlmock.NewRows([]string{"version", "title", "checksum", "status", "applied_at", "execution_time", "error_summary"}).
		AddRow("1", "init", "abc123", "success", "2026-01-02T03:04:05Z", int64(120), nil).
		AddRow("2.0", "add_log", "def456", "failed", "2026-01-02T03:06:07Z", int64(80), "duplicate column")
	mock.ExpectQuery("SELECT version, title, checksum, status, applied_at, execution_time, error_summary FROM schema_migrations ORDER BY id").
		WillReturnRows(ledger)

	resp := mustJSON(t, front, http.MethodGet, fmt.Sprintf("/api/migrate/plan?libraryId=%s&connectionId=%s", libID, connID), nil, http.StatusOK)

	latest, _ := resp["latestApplied"].(map[string]any)
	if latest == nil || latest["version"] != "1" {
		t.Fatalf("latestApplied wrong: %v", resp["latestApplied"])
	}
	applied, _ := resp["applied"].([]any)
	if len(applied) != 2 {
		t.Fatalf("applied rows = %d, want 2", len(applied))
	}
	failedRow, _ := applied[1].(map[string]any)
	if failedRow["status"] != "failed" || failedRow["errorSummary"] != "duplicate column" {
		t.Fatalf("failed row wrong: %v", failedRow)
	}
	pending, _ := resp["pending"].([]any)
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1 (2.0): %v", len(pending), pending)
	}
	pending0, _ := pending[0].(map[string]any)
	if pending0["version"] != "2.0" || pending0["title"] != "add_log" {
		t.Fatalf("pending wrong: %v", pending0)
	}
	if resp["nextVersion"] != "2.1" {
		t.Fatalf("nextVersion = %v, want 2.1", resp["nextVersion"])
	}

	// targetVersion filter: plan only up to 1 → nothing pending.
	exists2 := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(exists2)
	mock.ExpectQuery("SELECT version, title, checksum").WillReturnRows(
		sqlmock.NewRows([]string{"version", "title", "checksum", "status", "applied_at", "execution_time", "error_summary"}).
			AddRow("1", "init", "abc123", "success", "2026-01-02T03:04:05Z", int64(120), nil))
	resp = mustJSON(t, front, http.MethodGet, fmt.Sprintf("/api/migrate/plan?libraryId=%s&connectionId=%s&targetVersion=1", libID, connID), nil, http.StatusOK)
	pending, _ = resp["pending"].([]any)
	if len(pending) != 0 {
		t.Fatalf("target-filtered pending = %d, want 0", len(pending))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
}

func TestMigratePlanEmptyLedgerAndEmptyLibrary(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	connCreated := createConnection(t, front, "db", 33092)
	connID, _ := connCreated["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{33092: adapter})

	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "empty"}, http.StatusOK)
	libID, _ := lib["id"].(string)

	// Ledger table missing → applied=[].
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(0)))
	resp := mustJSON(t, front, http.MethodGet, fmt.Sprintf("/api/migrate/plan?libraryId=%s&connectionId=%s", libID, connID), nil, http.StatusOK)
	applied, _ := resp["applied"].([]any)
	if len(applied) != 0 {
		t.Fatalf("applied = %d, want 0", len(applied))
	}
	if resp["latestApplied"] != nil {
		t.Fatalf("latestApplied should be null: %v", resp["latestApplied"])
	}
	if resp["nextVersion"] != "1.0.0" {
		t.Fatalf("empty library nextVersion = %v, want 1.0.0", resp["nextVersion"])
	}
}

// 老版本建的账本缺 checksum 列时,执行计划应先自愈表结构再重试读取。
func TestMigratePlanHealsLegacyLedger(t *testing.T) {
	srv, front := newTestServer(t)
	adapter, mock := newMySQLStub(t)
	connCreated := createConnection(t, front, "db", 33094)
	connID, _ := connCreated["id"].(string)
	installStubAdapters(srv, map[int]conn.DBAdapter{33094: adapter})

	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "lib"}, http.StatusOK)
	libID, _ := lib["id"].(string)

	// 老形状账本:表存在但 SELECT checksum 直接报错。
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT version, title, checksum, status, applied_at, execution_time, error_summary FROM schema_migrations ORDER BY id").
		WillReturnError(errors.New("Error 1054 (42S22): Unknown column 'checksum' in 'field list'"))

	// EnsureVersionTable 自愈:建表(幂等)+ 逐列/索引检查(均已存在)。
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").WillReturnResult(sqlmock.NewResult(0, 0))
	for _, col := range []string{"checksum", "execution_time", "status", "error_summary"} {
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS").WithArgs(col).
			WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1)))
	}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.STATISTICS").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1)))

	// 重试读取成功(readLedger 先再查一次表存在)。
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(int64(1)))
	ledgerSelect := "SELECT version, title, checksum, status, applied_at, execution_time, error_summary FROM schema_migrations ORDER BY id"
	mock.ExpectQuery(ledgerSelect).
		WillReturnRows(sqlmock.NewRows([]string{"version", "title", "checksum", "status", "applied_at", "execution_time", "error_summary"}).
			AddRow("1", "init", "abc123", "success", "2026-01-02T03:04:05Z", int64(120), nil))

	resp := mustJSON(t, front, http.MethodGet, fmt.Sprintf("/api/migrate/plan?libraryId=%s&connectionId=%s", libID, connID), nil, http.StatusOK)
	applied, _ := resp["applied"].([]any)
	if len(applied) != 1 {
		t.Fatalf("applied = %d, want 1 after heal", len(applied))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
}

func TestMigratePlanConnectionFailureIs400(t *testing.T) {
	_, front := newTestServer(t)
	connCreated := createConnection(t, front, "db", 33093)
	connID, _ := connCreated["id"].(string)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "lib"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	// No stub installed → openAdapter fails.
	if status := statusOf(t, front, http.MethodGet, fmt.Sprintf("/api/migrate/plan?libraryId=%s&connectionId=%s", libID, connID), nil); status != http.StatusBadRequest {
		t.Fatalf("connection failure should be 400, got %d", status)
	}
}
