//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jacktea/data-smith/internal/server"
)

// webHarness 内嵌 Web 控制台 server（临时数据目录），经 HTTP API 驱动。
type webHarness struct {
	t       *testing.T
	front   *httptest.Server
	srv     *server.Server
	sourceN string
	targetN string
}

func newWebHarness(t *testing.T) *webHarness {
	t.Helper()
	srv, err := server.New(t.TempDir())
	if err != nil {
		t.Fatalf("server.New(): %v", err)
	}
	t.Cleanup(srv.Close)
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)
	return &webHarness{t: t, front: front, srv: srv}
}

func (h *webHarness) post(path string, body any) (int, map[string]any) {
	h.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(h.front.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatalf("decode response of %s: %v", path, err)
	}
	return resp.StatusCode, out
}

// addConnection 经 API 登记一条连接，返回连接 ID。
func (h *webHarness) addConnection(name, typ, host string, port int, user, password, dbname, schema string) string {
	h.t.Helper()
	status, out := h.post("/api/connections", map[string]any{
		"name": name, "type": typ, "host": host, "port": port,
		"user": user, "password": &password, "dbname": dbname,
		"tableSchema": schema,
	})
	if status != 200 {
		h.t.Fatalf("create connection %s: status %d body %v", name, status, out)
	}
	id, _ := out["id"].(string)
	if id == "" {
		h.t.Fatalf("create connection %s: missing id in %v", name, out)
	}
	return id
}

// submitDiffFull 提交完全比对任务并轮询至终态，返回任务视图。
func (h *webHarness) submitDiffFull(sourceID, targetID, dataDiffMode string) map[string]any {
	h.t.Helper()
	status, out := h.post("/api/jobs/diff-full", map[string]any{
		"sourceId":     sourceID,
		"targetId":     targetID,
		"tables":       []map[string]any{{"table": "t_item"}},
		"dataDiffMode": dataDiffMode,
	})
	if status != 200 {
		h.t.Fatalf("submit diff-full (mode %s): status %d body %v", dataDiffMode, status, out)
	}
	job, _ := out["job"].(map[string]any)
	id, _ := job["id"].(string)
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := http.Get(h.front.URL + "/api/jobs/" + id)
		if err != nil {
			h.t.Fatalf("get job: %v", err)
		}
		var view map[string]any
		err = json.NewDecoder(resp.Body).Decode(&view)
		resp.Body.Close()
		if err != nil {
			h.t.Fatalf("decode job: %v", err)
		}
		st, _ := view["status"].(string)
		if st == "succeeded" || st == "failed" || st == "cancelled" {
			return view
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("job (mode %s) not terminal in time: %v", dataDiffMode, view)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func webLog(view map[string]any) string {
	h := view["log"]
	raw, _ := json.Marshal(h)
	return string(raw)
}

// TestWebFullDiffDataDiffMode C5 的 Web 开关验收：完全比对任务经 API 暴露
// 数据比对模式（auto/shadow/direct），三值在真实库上行为与 CLI 语义一致——
// auto/shadow 走影子两阶段、direct 与 MySQL auto 回退直接比对、MySQL 强制
// shadow 提交成功但执行期被引擎拒绝。Web 不重写任何影子逻辑。
func TestWebFullDiffDataDiffMode(t *testing.T) {
	requireIntegration(t)

	pg := postgresFixture(t)
	// 结构漂移取「target 独有表」形态（列级漂移在 direct 模式下读取源行会
	// 硬失败，属引擎文档化的公共列容错边界）；数据漂移在 t_item 上。
	pg.setupSource = []string{
		`CREATE TABLE "t_item" ("id" BIGINT NOT NULL PRIMARY KEY, "flag" INTEGER NOT NULL)`,
		`INSERT INTO "t_item" VALUES (1, 1)`,
	}
	pg.setupTarget = []string{
		`CREATE TABLE "t_item" ("id" BIGINT NOT NULL PRIMARY KEY, "flag" INTEGER NOT NULL)`,
		`INSERT INTO "t_item" VALUES (1, 1), (2, 2)`,
		`CREATE TABLE "t_extra" ("id" BIGINT NOT NULL PRIMARY KEY)`,
	}
	prepareFixture(t, pg)
	defer cleanupFixture(t, pg)

	my := mysqlFixture(t)
	my.setupSource = []string{
		"CREATE TABLE `t_item` (`id` BIGINT NOT NULL PRIMARY KEY, `flag` INT NOT NULL)",
		"INSERT INTO `t_item` VALUES (1, 1)",
	}
	my.setupTarget = []string{
		"CREATE TABLE `t_item` (`id` BIGINT NOT NULL PRIMARY KEY, `flag` INT NOT NULL)",
		"INSERT INTO `t_item` VALUES (1, 1), (2, 2)",
		"CREATE TABLE `t_extra` (`id` BIGINT NOT NULL PRIMARY KEY)",
	}
	prepareFixture(t, my)
	defer cleanupFixture(t, my)

	h := newWebHarness(t)
	pgSrc := h.addConnection("webmode-pg-src", "postgres", pg.source.Host, pg.source.Port, pg.source.User, pg.source.Password, pg.source.DBName, pg.source.TableSchema)
	pgTgt := h.addConnection("webmode-pg-tgt", "postgres", pg.target.Host, pg.target.Port, pg.target.User, pg.target.Password, pg.target.DBName, pg.target.TableSchema)
	mySrc := h.addConnection("webmode-my-src", "mysql", my.source.Host, my.source.Port, my.source.User, my.source.Password, my.source.DBName, "")

	// PG auto：有结构差异 → 影子两阶段，任务成功且零残留（影子 ROLLBACK）。
	view := h.submitDiffFull(pgSrc, pgTgt, "auto")
	if view["status"] != "succeeded" {
		t.Fatalf("pg auto: job must succeed, got %v (error %v)", view["status"], view["error"])
	}
	if log := webLog(view); !strings.Contains(log, "影子结构对齐") {
		t.Fatalf("pg auto: expected shadow alignment in job log, got %s", log)
	}

	// PG shadow（显式强制）：同样走影子。
	view = h.submitDiffFull(pgSrc, pgTgt, "shadow")
	if view["status"] != "succeeded" || !strings.Contains(webLog(view), "影子结构对齐") {
		t.Fatalf("pg shadow: expected success with shadow alignment, got %v log %s", view["status"], webLog(view))
	}

	// PG direct：禁用影子，表级漂移在公共列容错下直接比对可完成。
	view = h.submitDiffFull(pgSrc, pgTgt, "direct")
	if view["status"] != "succeeded" {
		t.Fatalf("pg direct: job must succeed, got %v (error %v)", view["status"], view["error"])
	}
	if log := webLog(view); strings.Contains(log, "影子结构对齐") || !strings.Contains(log, "数据比对模式: direct") {
		t.Fatalf("pg direct: expected direct-mode warning without shadow, got %s", log)
	}

	// MySQL auto：无事务性 DDL，自动回退直接比对并告警。
	view = h.submitDiffFull(mySrc, h.addConnection("webmode-my-tgt", "mysql", my.target.Host, my.target.Port, my.target.User, my.target.Password, my.target.DBName, ""), "auto")
	if view["status"] != "succeeded" || !strings.Contains(webLog(view), "数据比对模式: direct") {
		t.Fatalf("mysql auto: expected success with direct fallback, got %v log %s", view["status"], webLog(view))
	}

	// MySQL 强制 shadow：提交期合法，执行期被引擎拒绝（DDL 隐式提交保护）。
	view = h.submitDiffFull(mySrc, h.addConnection("webmode-my-tgt2", "mysql", my.target.Host, my.target.Port, my.target.User, my.target.Password, my.target.DBName, ""), "shadow")
	errText, _ := view["error"].(string)
	if view["status"] != "failed" || !(strings.Contains(errText, "PostgreSQL source") || strings.Contains(webLog(view), "PostgreSQL source")) {
		t.Fatalf("mysql shadow: expected engine rejection naming PostgreSQL source, got %v error %q log %s", view["status"], errText, webLog(view))
	}
}
