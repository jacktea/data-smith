package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	difflogic "github.com/jacktea/data-smith/internal/datasmith/diff"
)

// putWebDiffConnection registers a connection whose database lives nowhere;
// runs fail fast at connect time, which is enough to observe submit-time
// validation and the persisted job params.
func putWebDiffConnection(t *testing.T, srv *Server, id, name, typ string) {
	t.Helper()
	err := srv.store.PutConnection(&StoredConnection{
		ID:               id,
		Name:             name,
		Type:             typ,
		Host:             "127.0.0.1",
		Port:             1, // 快速连接失败,不影响提交期断言
		User:             "u",
		Password:         "p",
		DBName:           "db",
		TableSchema:      "public",
		ConnectTimeoutMs: 300,
	})
	if err != nil {
		t.Fatalf("put connection %s: %v", id, err)
	}
}

type webJobView struct {
	Status  string         `json:"status"`
	Error   string         `json:"error"`
	Log     []string       `json:"log"`
	Params  map[string]any `json:"params"`
	Summary map[string]any `json:"summary"`
}

// waitWebJobTerminal 轮询任务直至进入终态（succeeded/failed/cancelled）。
func waitWebJobTerminal(t *testing.T, frontURL, jobID string) webJobView {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(frontURL + "/api/jobs/" + jobID)
		if err != nil {
			t.Fatalf("GET job %s: %v", jobID, err)
		}
		var view webJobView
		err = json.NewDecoder(resp.Body).Decode(&view)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("decode job view: %v", err)
		}
		if view.Status == "succeeded" || view.Status == "failed" || view.Status == "cancelled" {
			return view
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not reach a terminal state in time, last status %s (log %v)", jobID, view.Status, view.Log)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// postWebJSON 发送 JSON POST 并返回状态码与响应体。
func postWebJSON(t *testing.T, frontURL, path string, body any) (int, string) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(frontURL+path, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(respBody)
}

// Web 完全比对的数据比对模式开关：非法取值提交期 400；合法取值归一后
// 记入任务参数并写进任务日志（透传引擎语义，Web 不重写影子逻辑）。
func TestDiffFullSubmitValidatesDataDiffMode(t *testing.T) {
	srv, front := newTestServer(t)
	putWebDiffConnection(t, srv, "conn-src", "src", "postgres")
	putWebDiffConnection(t, srv, "conn-tgt", "tgt", "postgres")

	post := func(body map[string]any) (int, string) {
		return postWebJSON(t, front.URL, "/api/jobs/diff-full", body)
	}

	// 非法取值：提交期拒绝，不产生任务（连接校验先于模式校验通过）。
	if status, body := post(map[string]any{
		"sourceId":     "conn-src",
		"targetId":     "conn-tgt",
		"tables":       []map[string]any{{"table": "t_item"}},
		"dataDiffMode": "bogus",
	}); status != 400 || !strings.Contains(body, "dataDiffMode") {
		t.Fatalf("bogus dataDiffMode must be rejected with 400 naming the field, got %d: %s", status, body)
	}

	// 合法取值：归一记录 + 任务日志可见；任务本体因连接不可达而失败属预期。
	for _, mode := range []string{"shadow", "direct", ""} {
		body := map[string]any{
			"sourceId":     "conn-src",
			"targetId":     "conn-tgt",
			"tables":       []map[string]any{{"table": "t_item"}},
			"dataDiffMode": mode,
		}
		status, respBody := post(body)
		if status != 200 {
			t.Fatalf("mode %q: expected submit 200, got %d: %s", mode, status, respBody)
		}
		var created struct {
			Job struct {
				ID string `json:"id"`
			} `json:"job"`
		}
		if err := json.Unmarshal([]byte(respBody), &created); err != nil {
			t.Fatalf("decode submit response: %v", err)
		}
		view := waitWebJobTerminal(t, front.URL, created.Job.ID)
		want := mode
		if want == "" {
			want = "auto"
		}
		if got, _ := view.Params["requestedDataDiffMode"].(string); got != want {
			t.Fatalf("mode %q: params.requestedDataDiffMode = %v, want %q", mode, view.Params["requestedDataDiffMode"], want)
		}
		found := false
		for _, line := range view.Log {
			if strings.Contains(line, "requested data diff mode="+want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("mode %q: job log must record the effective mode, got %v", mode, view.Log)
		}
		if view.Status != "failed" {
			t.Fatalf("job against an unreachable DB must fail, got %s (error: %s)", view.Status, view.Error)
		}
	}
}

func TestDiffFullTaskModeIsConsistentAcrossAPIArtifactAndLog(t *testing.T) {
	srv, front := newTestServer(t)
	putWebDiffConnection(t, srv, "conn-src", "src", "postgres")
	putWebDiffConnection(t, srv, "conn-tgt", "tgt", "postgres")

	tests := []struct {
		name      string
		requested string
		effective string
	}{
		{name: "auto to shadow", requested: "auto", effective: "shadow"},
		{name: "auto to direct", requested: "auto", effective: "direct"},
		{name: "explicit shadow", requested: "shadow", effective: "shadow"},
		{name: "explicit direct", requested: "direct", effective: "direct"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv.runFullDiffEngine = func(_ context.Context, _ difflogic.FullDiffParams, _ string, progress func(string), _ func(difflogic.TableProgress)) (difflogic.FullDiffResult, error) {
				progress("数据比对模式: requested=" + tt.requested + " effective=" + tt.effective)
				return difflogic.FullDiffResult{DataDiffMode: difflogic.DataDiffModeSelection{Requested: tt.requested, Effective: tt.effective}}, nil
			}
			status, respBody := postWebJSON(t, front.URL, "/api/jobs/diff-full", map[string]any{
				"sourceId": "conn-src", "targetId": "conn-tgt",
				"tables": []map[string]any{{"table": "t_item"}}, "dataDiffMode": tt.requested,
			})
			if status != http.StatusOK {
				t.Fatalf("submit status=%d body=%s", status, respBody)
			}
			var created struct {
				Job struct {
					ID string `json:"id"`
				} `json:"job"`
			}
			if err := json.Unmarshal([]byte(respBody), &created); err != nil {
				t.Fatal(err)
			}
			view := waitWebJobTerminal(t, front.URL, created.Job.ID)
			if view.Status != "succeeded" {
				t.Fatalf("status=%s error=%s log=%v", view.Status, view.Error, view.Log)
			}
			if got := view.Params["requestedDataDiffMode"]; got != tt.requested {
				t.Fatalf("params.requestedDataDiffMode=%v, want %s", got, tt.requested)
			}
			mode, _ := view.Summary["dataDiffMode"].(map[string]any)
			if mode["requested"] != tt.requested || mode["effective"] != tt.effective {
				t.Fatalf("API summary mode=%v, want requested=%s effective=%s", mode, tt.requested, tt.effective)
			}
			job := srv.jobs.Get(created.Job.ID)
			raw, err := os.ReadFile(filepath.Join(job.Dir(), summaryJSONFile))
			if err != nil {
				t.Fatal(err)
			}
			var persisted difflogic.FullDiffResult
			if err := json.Unmarshal(raw, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.DataDiffMode.Requested != tt.requested || persisted.DataDiffMode.Effective != tt.effective {
				t.Fatalf("persisted mode=%+v, want requested=%s effective=%s", persisted.DataDiffMode, tt.requested, tt.effective)
			}
			wantLog := "requested=" + tt.requested + " effective=" + tt.effective
			if !logContains(view.Log, wantLog) {
				t.Fatalf("log must contain %q, got %v", wantLog, view.Log)
			}
		})
	}
}

func logContains(lines []string, want string) bool {
	for _, line := range lines {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}
