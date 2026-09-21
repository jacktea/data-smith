package server

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	difflogic "github.com/jacktea/data-smith/internal/datasmith/diff"
)

// C8：up 是迁移版本的可执行入口；删除 up 后即使 down 仍保留，也必须
// 清理 LibraryMeta.Versions 中的版本登记。
func TestDeleteScriptCleansOrphanVersionMeta(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	dir := srv.libraryDirUnchecked(libID)

	writeLibScript(t, dir, "V3.6.0__sync.up.sql", "SELECT 1;")
	writeLibScript(t, dir, "V3.6.0__sync.down.sql", "SELECT 0;")
	writeLibScript(t, dir, "V3.7.1__sync.up.sql", "SELECT 1;")
	writeLibScript(t, dir, "V3.7.1__sync.down.sql", "SELECT 0;")
	if err := srv.store.SetVersionMeta(libID, "3.6.0", VersionMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{ExpectedConnectionID: "conn-1"}); err != nil {
		t.Fatal(err)
	}

	// 删除 3.7.1 的 up：同版本仍有 down，但已无可执行入口，登记清理。
	if status, _ := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.up.sql", nil); status != http.StatusOK {
		t.Fatalf("delete up: status = %d", status)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)

	// 删除 3.7.1 的 down：版本已无任何脚本，登记清理。
	if status, _ := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.down.sql", nil); status != http.StatusOK {
		t.Fatalf("delete down: status = %d", status)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)
	// 未触碰的 3.6.0 登记保持原样。
	assertVersionMeta(t, srv, libID, "3.6.0", true)
}

func TestDeleteDownKeepsVersionMetadataWhileUpExists(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	dir := srv.libraryDirUnchecked(libID)
	writeLibScript(t, dir, "V3.7.1__sync.up.sql", "SELECT 1;")
	writeLibScript(t, dir, "V3.7.1__sync.down.sql", "SELECT 0;")
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}

	if status, body := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.down.sql", nil); status != http.StatusOK {
		t.Fatalf("delete down: status=%d body=%s", status, body)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", true)

	// up 成为最后一个脚本；删除后版本登记必须清理。
	if status, body := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.up.sql", nil); status != http.StatusOK {
		t.Fatalf("delete last up: status=%d body=%s", status, body)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)
}

func TestDeleteScriptReportsMetadataCleanupScanFailure(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	dir := srv.libraryDirUnchecked(libID)
	fileName := "V3.7.1__sync.up.sql"
	writeLibScript(t, dir, fileName, "SELECT 1;")
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}
	srv.listLibraryScripts = func(string) ([]scriptInfo, error) {
		return nil, errors.New("scan failed")
	}

	status, body := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/"+fileName, nil)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", status, http.StatusInternalServerError, body)
	}
	if got := string(body); !strings.Contains(got, "脚本已删除") || !strings.Contains(got, "scan failed") {
		t.Fatalf("response must expose partial success and scan failure, got %s", got)
	}
	if _, err := os.Stat(filepath.Join(dir, fileName)); !os.IsNotExist(err) {
		t.Fatalf("script must already be deleted, stat err=%v", err)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", true)
}

func TestDeleteScriptReportsVersionMetadataPersistenceFailure(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	dir := srv.libraryDirUnchecked(libID)
	fileName := "V3.7.1__sync.up.sql"
	writeLibScript(t, dir, fileName, "SELECT 1;")
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}
	srv.deleteVersionMeta = func(string, string) error {
		return errors.New("persist failed")
	}

	status, body := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/"+fileName, nil)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", status, http.StatusInternalServerError, body)
	}
	if got := string(body); !strings.Contains(got, "脚本已删除") || !strings.Contains(got, "persist failed") {
		t.Fatalf("response must expose partial success and persistence failure, got %s", got)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", true)
}

func TestDeleteAndConcurrentRegistrationSerializeVersionMetadataPerLibrary(t *testing.T) {
	srv, front := newTestServer(t)
	libID, jobID := createLibraryAndJob(t, srv, front, map[string]string{
		difflogic.SchemaDiffForwardFile:  regSchemaForward,
		difflogic.SchemaDiffRollbackFile: regSchemaRollback,
	})
	dir := srv.libraryDirUnchecked(libID)
	oldUp := "V1.0.0__old.up.sql"
	writeLibScript(t, dir, oldUp, "-- DATASMITH EXECUTE-ON: source\nSELECT 1;")
	if err := srv.store.SetVersionMeta(libID, "1.0.0", VersionMeta{ExpectedConnectionID: "conn-old"}); err != nil {
		t.Fatal(err)
	}

	deleteScanned := make(chan struct{})
	allowDelete := make(chan struct{})
	srv.listLibraryScripts = func(dir string) ([]scriptInfo, error) {
		scripts, err := listScripts(dir)
		close(deleteScanned)
		<-allowDelete
		return scripts, err
	}
	registerAttempted := make(chan struct{})
	srv.beforeLibraryMutation = func(id, operation string) {
		if id == libID && operation == "register-version" {
			close(registerAttempted)
		}
	}

	type response struct {
		status int
		body   []byte
	}
	deleteResult := make(chan response, 1)
	go func() {
		status, body := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/"+oldUp, nil)
		deleteResult <- response{status: status, body: body}
	}()
	select {
	case <-deleteScanned:
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not reach the post-delete metadata scan barrier")
	}

	registerResult := make(chan response, 1)
	go func() {
		status, body := doRequest(t, front, http.MethodPost, "/api/libraries/"+libID+"/versions", map[string]any{
			"sourceJobId": jobID, "includeSchema": true, "version": "1.0.0", "title": "new",
			"expectedConnectionId": "conn-new",
		})
		registerResult <- response{status: status, body: body}
	}()
	select {
	case <-registerAttempted:
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not reach the per-library lock barrier")
	}
	select {
	case got := <-registerResult:
		t.Fatalf("registration bypassed the delete lock: status=%d body=%s", got.status, got.body)
	default:
	}

	close(allowDelete)
	for name, results := range map[string]<-chan response{"delete": deleteResult, "register": registerResult} {
		select {
		case got := <-results:
			if got.status != http.StatusOK {
				t.Fatalf("%s status=%d body=%s", name, got.status, got.body)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s request did not finish", name)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "V1.0.0__new.up.sql")); err != nil {
		t.Fatalf("new up script missing after serialized registration: %v", err)
	}
	lib, ok := srv.store.GetLibrary(libID)
	if !ok || lib.Versions["1.0.0"].ExpectedConnectionID != "conn-new" {
		t.Fatalf("new up script must retain its version metadata: %+v", lib)
	}
}

// C8：store 一致性自检——「现网 store.json 孤儿条目」由启动自检清理，
// 有效登记不受影响。
func TestSweepOrphanVersionMetaRemovesEntriesWithoutScripts(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	dir := srv.libraryDirUnchecked(libID)

	writeLibScript(t, dir, "V3.6.0__sync.up.sql", "SELECT 1;")
	writeLibScript(t, dir, "V3.6.0__sync.down.sql", "SELECT 0;")
	if err := srv.store.SetVersionMeta(libID, "3.6.0", VersionMeta{}); err != nil {
		t.Fatal(err)
	}
	// 模拟历史遗留孤儿：版本登记在、脚本文件已不在。
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}

	removed, err := srv.SweepOrphanVersionMeta()
	if err != nil {
		t.Fatalf("SweepOrphanVersionMeta(): %v", err)
	}
	if got := fmt.Sprint(removed[libID]); got != "[3.7.1]" {
		t.Fatalf("removed = %v, want only [3.7.1]", removed)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)
	assertVersionMeta(t, srv, libID, "3.6.0", true)

	// 重复清理不报错且不重复报告已删除的版本。
	removed, err = srv.SweepOrphanVersionMeta()
	if err != nil {
		t.Fatalf("second SweepOrphanVersionMeta(): %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("second sweep removed = %v, want empty", removed)
	}
}

func TestSweepOrphanVersionMetaReportsDirectoryScanFailure(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}
	srv.listLibraryScripts = func(string) ([]scriptInfo, error) {
		return nil, errors.New("scan failed")
	}

	removed, err := srv.SweepOrphanVersionMeta()
	if err == nil || !strings.Contains(err.Error(), "scan failed") {
		t.Fatalf("err = %v, want scan failure (removed=%v)", err, removed)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", true)
}

func TestSweepOrphanVersionMetaTreatsImplicitDirectionAsUp(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "legacy"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	writeLibScript(t, srv.libraryDirUnchecked(libID), "V1.0__init.sql", "SELECT 1;")
	if err := srv.store.SetVersionMeta(libID, "1.0", VersionMeta{}); err != nil {
		t.Fatal(err)
	}

	removed, err := srv.SweepOrphanVersionMeta()
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("implicit up script must keep metadata, removed=%v", removed)
	}
	assertVersionMeta(t, srv, libID, "1.0", true)
}

// C8：启动自检在 New 时即执行——带孤儿登记的 store.json 加载后被修复，
// 且修复结果持久化到磁盘。
func TestServerNewRunsOrphanVersionSweep(t *testing.T) {
	srv, front := newTestServer(t)
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": "airedge"}, http.StatusOK)
	libID, _ := lib["id"].(string)
	if err := srv.store.SetVersionMeta(libID, "3.7.1", VersionMeta{}); err != nil {
		t.Fatal(err)
	}

	rebuilt, err := New(srv.dataDir)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer rebuilt.Close()
	got, ok := rebuilt.store.GetLibrary(libID)
	if !ok {
		t.Fatal("library disappeared after restart")
	}
	if _, orphan := got.Versions["3.7.1"]; orphan {
		t.Fatalf("orphan version 3.7.1 survived startup sweep: %v", got.Versions)
	}
}

func writeLibScript(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertVersionMeta(t *testing.T, srv *Server, libID, version string, want bool) {
	t.Helper()
	lib, ok := srv.store.GetLibrary(libID)
	if !ok {
		t.Fatalf("library %s missing", libID)
	}
	if _, has := lib.Versions[version]; has != want {
		t.Fatalf("version %s meta exists = %v, want %v (versions=%v)", version, has, want, lib.Versions)
	}
}
