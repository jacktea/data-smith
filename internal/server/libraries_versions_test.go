package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// C8：删除脚本文件后，LibraryMeta.Versions 中已无任何脚本的孤儿版本登记
// 必须同步清理；仍存在脚本的版本保持登记。
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

	// 删除 3.7.1 的 up：同版本仍有 down，登记保留。
	if status, _ := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.up.sql", nil); status != http.StatusOK {
		t.Fatalf("delete up: status = %d", status)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", true)

	// 删除 3.7.1 的 down：版本已无任何脚本，登记清理。
	if status, _ := doRequest(t, front, http.MethodDelete, "/api/libraries/"+libID+"/scripts/V3.7.1__sync.down.sql", nil); status != http.StatusOK {
		t.Fatalf("delete down: status = %d", status)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)
	// 未触碰的 3.6.0 登记保持原样。
	assertVersionMeta(t, srv, libID, "3.6.0", true)
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

	removed := srv.SweepOrphanVersionMeta()
	if got := fmt.Sprint(removed[libID]); got != "[3.7.1]" {
		t.Fatalf("removed = %v, want only [3.7.1]", removed)
	}
	assertVersionMeta(t, srv, libID, "3.7.1", false)
	assertVersionMeta(t, srv, libID, "3.6.0", true)
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
