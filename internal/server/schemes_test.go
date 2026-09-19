package server

import (
	"net/http"
	"testing"
)

func TestSchemeUpdateAndDeleteLifecycle(t *testing.T) {
	_, front := newTestServer(t)

	created := mustJSON(t, front, http.MethodPost, "/api/schemes", map[string]any{
		"name": "全量比对",
		"tables": []map[string]any{
			{"table": "orders"},
		},
	}, http.StatusOK)
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("create scheme: missing id: %v", created)
	}
	createdAt, _ := created["updatedAt"].(string)
	if createdAt == "" {
		t.Fatalf("create scheme: missing updatedAt: %v", created)
	}

	updated := mustJSON(t, front, http.MethodPut, "/api/schemes/"+id, map[string]any{
		"name": "订单库比对",
		"tables": []map[string]any{
			{"table": "orders", "columns": []string{"amount"}, "ignoreColumns": []string{"lock_version"}},
			{"table": "users"},
		},
	}, http.StatusOK)
	if got, _ := updated["id"].(string); got != id {
		t.Fatalf("update scheme: id changed: %q -> %q", id, got)
	}
	if got, _ := updated["name"].(string); got != "订单库比对" {
		t.Fatalf("update scheme: name = %q", got)
	}
	if got, _ := updated["updatedAt"].(string); got < createdAt {
		t.Fatalf("update scheme: updatedAt not bumped: %s -> %s", createdAt, got)
	}
	tables, _ := updated["tables"].([]any)
	if len(tables) != 2 {
		t.Fatalf("update scheme: tables = %v", updated["tables"])
	}
	first, _ := tables[0].(map[string]any)
	ignores, _ := first["ignoreColumns"].([]any)
	if len(ignores) != 1 || ignores[0] != "lock_version" {
		t.Fatalf("update scheme: ignoreColumns = %v", first["ignoreColumns"])
	}

	// 更新与删除后,列表与单读必须反映最新状态。
	list := mustJSONArray(t, front, http.MethodGet, "/api/schemes", nil, http.StatusOK)
	if len(list) != 1 {
		t.Fatalf("list schemes after update: %v", list)
	}
	got := mustJSON(t, front, http.MethodGet, "/api/schemes/"+id, nil, http.StatusOK)
	if name, _ := got["name"].(string); name != "订单库比对" {
		t.Fatalf("get scheme after update: name = %q", name)
	}

	mustJSON(t, front, http.MethodDelete, "/api/schemes/"+id, nil, http.StatusOK)
	if status := statusOf(t, front, http.MethodGet, "/api/schemes/"+id, nil); status != http.StatusNotFound {
		t.Fatalf("get deleted scheme: status = %d, want 404", status)
	}
	if status := statusOf(t, front, http.MethodDelete, "/api/schemes/"+id, nil); status != http.StatusNotFound {
		t.Fatalf("delete deleted scheme: status = %d, want 404", status)
	}
}

func TestSchemeUpdateValidationAndMissingID(t *testing.T) {
	_, front := newTestServer(t)

	if status := statusOf(t, front, http.MethodPut, "/api/schemes/scheme_missing", map[string]any{
		"name":   "任意",
		"tables": []map[string]any{{"table": "orders"}},
	}); status != http.StatusNotFound {
		t.Fatalf("update missing scheme: status = %d, want 404", status)
	}

	created := mustJSON(t, front, http.MethodPost, "/api/schemes", map[string]any{
		"name":  "空名校验",
		"tables": []map[string]any{{"table": "orders"}},
	}, http.StatusOK)
	id, _ := created["id"].(string)

	// 空名称与重复表名必须被拒绝,且不得破坏原方案。
	if status := statusOf(t, front, http.MethodPut, "/api/schemes/"+id, map[string]any{
		"name":   "  ",
		"tables": []map[string]any{{"table": "orders"}},
	}); status != http.StatusBadRequest {
		t.Fatalf("update with blank name: status = %d, want 400", status)
	}
	if status := statusOf(t, front, http.MethodPut, "/api/schemes/"+id, map[string]any{
		"name":   "重复表",
		"tables": []map[string]any{{"table": "orders"}, {"table": "orders"}},
	}); status != http.StatusBadRequest {
		t.Fatalf("update with duplicate tables: status = %d, want 400", status)
	}
	got := mustJSON(t, front, http.MethodGet, "/api/schemes/"+id, nil, http.StatusOK)
	if name, _ := got["name"].(string); name != "空名校验" {
		t.Fatalf("rejected update must not modify scheme: name = %q", name)
	}
}
