package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// seedScripts writes script files directly into a library directory; direct
// writes are how down scripts come into being outside version registration.
func seedScripts(t *testing.T, srv *Server, libID string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(srv.dataDir, "libraries", libID, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("seed script %s: %v", name, err)
		}
	}
}

func listScriptNames(t *testing.T, front *httptest.Server, libID string) map[string]string {
	t.Helper()
	scripts := mustJSONArray(t, front, http.MethodGet, "/api/libraries/"+libID+"/scripts", nil, http.StatusOK)
	out := make(map[string]string, len(scripts))
	for _, s := range scripts {
		m, _ := s.(map[string]any)
		name, _ := m["fileName"].(string)
		out[name] = fmt.Sprint(m["size"])
	}
	return out
}

func createLibraryNamed(t *testing.T, front *httptest.Server, name string) string {
	t.Helper()
	lib := mustJSON(t, front, http.MethodPost, "/api/libraries", map[string]any{"name": name}, http.StatusOK)
	id, _ := lib["id"].(string)
	if id == "" {
		t.Fatalf("library id missing: %v", lib)
	}
	return id
}

func TestCopyLibraryCopiesScriptsAndVersionMeta(t *testing.T) {
	srv, front := newTestServer(t)
	srcID := createLibraryNamed(t, front, "订单库")
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts", map[string]any{
		"fileName": "V1__init.up.sql", "content": "CREATE TABLE a (id INT);",
	}, http.StatusOK)
	seedScripts(t, srv, srcID, map[string]string{
		"V1__init.down.sql":   "DROP TABLE a;",
		"2__no_direction.sql": "CREATE TABLE b (id INT);",
	})
	if err := srv.store.SetVersionMeta(srcID, "1", VersionMeta{ExpectedConnectionID: "conn_x"}); err != nil {
		t.Fatalf("SetVersionMeta: %v", err)
	}

	resp := mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/copy",
		map[string]any{"name": "订单库-副本"}, http.StatusOK)
	copiedID, _ := resp["id"].(string)
	if copiedID == "" || copiedID == srcID {
		t.Fatalf("copy must get a fresh id, got %q", copiedID)
	}
	if resp["name"] != "订单库-副本" {
		t.Fatalf("copy name wrong: %v", resp)
	}

	want := map[string]string{
		"V1__init.up.sql":     "CREATE TABLE a (id INT);",
		"V1__init.down.sql":   "DROP TABLE a;",
		"2__no_direction.sql": "CREATE TABLE b (id INT);",
	}
	for name, content := range want {
		got := mustJSON(t, front, http.MethodGet,
			"/api/libraries/"+copiedID+"/scripts/"+name, nil, http.StatusOK)
		if got["content"] != content {
			t.Fatalf("copied script %s content = %v, want %q", name, got["content"], content)
		}
	}
	// 源库不受影响。
	if got := listScriptNames(t, front, srcID); len(got) != len(want) {
		t.Fatalf("source scripts changed: %v", got)
	}

	srcLib, ok := srv.store.GetLibrary(srcID)
	if !ok {
		t.Fatal("source library missing")
	}
	copiedLib, ok := srv.store.GetLibrary(copiedID)
	if !ok {
		t.Fatal("copied library missing from store")
	}
	if len(copiedLib.Versions) != 1 || copiedLib.Versions["1"].ExpectedConnectionID != "conn_x" {
		t.Fatalf("version meta not cloned: %+v", copiedLib.Versions)
	}
	if copiedLib.CreatedAt.Equal(srcLib.CreatedAt) {
		t.Fatalf("copied library must get a fresh createdAt")
	}
}

func TestCopyLibraryValidation(t *testing.T) {
	_, front := newTestServer(t)
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/lib_missing/copy",
		map[string]any{"name": "x"}); status != http.StatusNotFound {
		t.Fatalf("copy missing library: status = %d, want 404", status)
	}
	srcID := createLibraryNamed(t, front, "订单库")
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+srcID+"/copy",
		map[string]any{"name": "  "}); status != http.StatusBadRequest {
		t.Fatalf("empty name: status = %d, want 400", status)
	}
}

func TestCopyScriptsBetweenLibraries(t *testing.T) {
	srv, front := newTestServer(t)
	srcID := createLibraryNamed(t, front, "库A")
	dstID := createLibraryNamed(t, front, "库B")
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts", map[string]any{
		"fileName": "V1__init.up.sql", "content": "CREATE TABLE a (id INT);",
	}, http.StatusOK)
	seedScripts(t, srv, srcID, map[string]string{
		"V1__init.down.sql":    "DROP TABLE a;",
		"V2.0__add_log.up.sql": "ALTER TABLE a ADD log TEXT;",
	})

	resp := mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts/copy", map[string]any{
		"targetLibraryId": dstID,
		"fileNames":       []string{"V1__init.up.sql", "V1__init.down.sql"},
	}, http.StatusOK)
	if resp["copied"] != float64(2) {
		t.Fatalf("copied = %v, want 2", resp["copied"])
	}
	// down 脚本忠实复制；未选中的脚本留在源库。
	names := listScriptNames(t, front, dstID)
	if _, ok := names["V1__init.up.sql"]; !ok {
		t.Fatalf("up script missing in target: %v", names)
	}
	if _, ok := names["V1__init.down.sql"]; !ok {
		t.Fatalf("down script missing in target: %v", names)
	}
	if len(names) != 2 {
		t.Fatalf("target scripts = %v, want exactly the copied pair", names)
	}
	if got := listScriptNames(t, front, srcID); len(got) != 3 {
		t.Fatalf("source scripts changed: %v", got)
	}

	// 登记元数据不随脚本复制：目标库 Versions 保持为空。
	dstLib, ok := srv.store.GetLibrary(dstID)
	if !ok {
		t.Fatal("target library missing")
	}
	if len(dstLib.Versions) != 0 {
		t.Fatalf("target version meta should stay empty: %+v", dstLib.Versions)
	}

	// 重复请求同一批 → 同名冲突整批拒绝。
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts/copy", map[string]any{
		"targetLibraryId": dstID,
		"fileNames":       []string{"V1__init.up.sql"},
	}); status != http.StatusBadRequest {
		t.Fatalf("re-copy same name: status = %d, want 400", status)
	}
}

func TestCopyScriptsAllOrNothingOnConflict(t *testing.T) {
	srv, front := newTestServer(t)
	srcID := createLibraryNamed(t, front, "库A")
	dstID := createLibraryNamed(t, front, "库B")
	seedScripts(t, srv, srcID, map[string]string{
		"V1__a.up.sql": "SELECT 1;",
		"V2__b.up.sql": "SELECT 2;",
	})
	seedScripts(t, srv, dstID, map[string]string{
		"V1__c.up.sql": "SELECT 3;",
	})

	// V1__a 与目标库 V1__c 版本号相同、标题不同 → 版本链冲突，整批拒绝。
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts/copy", map[string]any{
		"targetLibraryId": dstID,
		"fileNames":       []string{"V1__a.up.sql", "V2__b.up.sql"},
	}); status != http.StatusBadRequest {
		t.Fatalf("version conflict: status = %d, want 400", status)
	}
	if got := listScriptNames(t, front, dstID); len(got) != 1 {
		t.Fatalf("target must stay untouched on conflict: %v", got)
	}

	// 同名冲突同样整批拒绝。
	seedScripts(t, srv, dstID, map[string]string{"V2__b.up.sql": "old"})
	if status := statusOf(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts/copy", map[string]any{
		"targetLibraryId": dstID,
		"fileNames":       []string{"V2__b.up.sql"},
	}); status != http.StatusBadRequest {
		t.Fatalf("same-name conflict: status = %d, want 400", status)
	}

	// up/down 同版本同标题互为配套，允许复制补齐。
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts", map[string]any{
		"fileName": "V3__pair.up.sql", "content": "SELECT 3;",
	}, http.StatusOK)
	seedScripts(t, srv, dstID, map[string]string{"V3__pair.down.sql": "SELECT 0;"})
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts/copy", map[string]any{
		"targetLibraryId": dstID,
		"fileNames":       []string{"V3__pair.up.sql"},
	}, http.StatusOK)
}

func TestCopyScriptsValidation(t *testing.T) {
	_, front := newTestServer(t)
	srcID := createLibraryNamed(t, front, "库A")
	dstID := createLibraryNamed(t, front, "库B")
	mustJSON(t, front, http.MethodPost, "/api/libraries/"+srcID+"/scripts", map[string]any{
		"fileName": "V1__init.up.sql", "content": "SELECT 1;",
	}, http.StatusOK)

	post := func(target, srcLib string, names []string) int {
		return statusOf(t, front, http.MethodPost, "/api/libraries/"+srcLib+"/scripts/copy", map[string]any{
			"targetLibraryId": target,
			"fileNames":       names,
		})
	}
	if status := post(srcID, srcID, []string{"V1__init.up.sql"}); status != http.StatusBadRequest {
		t.Fatalf("same library: status = %d, want 400", status)
	}
	if status := post("", srcID, []string{"V1__init.up.sql"}); status != http.StatusBadRequest {
		t.Fatalf("empty target: status = %d, want 400", status)
	}
	if status := post(dstID, srcID, nil); status != http.StatusBadRequest {
		t.Fatalf("empty selection: status = %d, want 400", status)
	}
	if status := post(dstID, srcID, []string{"V9__nope.up.sql"}); status != http.StatusBadRequest {
		t.Fatalf("missing script: status = %d, want 400", status)
	}
	if status := post(dstID, srcID, []string{"../../store.json"}); status != http.StatusBadRequest {
		t.Fatalf("traversal name: status = %d, want 400", status)
	}
	if status := post("lib_missing", srcID, []string{"V1__init.up.sql"}); status != http.StatusBadRequest {
		t.Fatalf("missing target library: status = %d, want 400", status)
	}
	if status := post(dstID, "lib_missing", []string{"V1__init.up.sql"}); status != http.StatusBadRequest {
		t.Fatalf("missing source library: status = %d, want 400", status)
	}
	// 校验失败不得写入目标库。
	if got := listScriptNames(t, front, dstID); len(got) != 0 {
		t.Fatalf("target must stay empty: %v", got)
	}
}

// 并发对向复制（A→B 与 B→A）按 ID 升序加锁，race 下不得死锁。每轮使用
// 互不重叠的版本号与文件名，全部请求都应成功，最终两库各拥有全部脚本。
func TestCopyScriptsConcurrentOppositeDirections(t *testing.T) {
	_, front := newTestServer(t)
	libA := createLibraryNamed(t, front, "库A")
	libB := createLibraryNamed(t, front, "库B")
	const rounds = 10
	// 库A 持有版本 11..20，库B 持有版本 1..10，避免版本链冲突。
	for i := 1; i <= rounds; i++ {
		mustJSON(t, front, http.MethodPost, "/api/libraries/"+libA+"/scripts", map[string]any{
			"fileName": fmt.Sprintf("V%d__from_a.up.sql", rounds+i), "content": fmt.Sprintf("SELECT %d;", rounds+i),
		}, http.StatusOK)
		mustJSON(t, front, http.MethodPost, "/api/libraries/"+libB+"/scripts", map[string]any{
			"fileName": fmt.Sprintf("V%d__from_b.up.sql", i), "content": fmt.Sprintf("SELECT %d;", i),
		}, http.StatusOK)
	}

	var wg sync.WaitGroup
	errs := make([]error, rounds)
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			status, raw := doRequest(t, front, http.MethodPost, "/api/libraries/"+libA+"/scripts/copy", map[string]any{
				"targetLibraryId": libB,
				"fileNames":       []string{fmt.Sprintf("V%d__from_a.up.sql", rounds+i+1)},
			})
			if status != http.StatusOK {
				errs[i] = fmt.Errorf("A→B round %d: status %d: %s", i, status, raw)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			status, raw := doRequest(t, front, http.MethodPost, "/api/libraries/"+libB+"/scripts/copy", map[string]any{
				"targetLibraryId": libA,
				"fileNames":       []string{fmt.Sprintf("V%d__from_b.up.sql", i+1)},
			})
			if status != http.StatusOK {
				errs[i] = fmt.Errorf("B→A round %d: status %d: %s", i, status, raw)
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("%v", err)
		}
	}
	if got := listScriptNames(t, front, libA); len(got) != 2*rounds {
		t.Fatalf("library A scripts = %d, want %d: %v", len(got), 2*rounds, got)
	}
	if got := listScriptNames(t, front, libB); len(got) != 2*rounds {
		t.Fatalf("library B scripts = %d, want %d: %v", len(got), 2*rounds, got)
	}
}
