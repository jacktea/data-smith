package server

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobLifecycleSuccess(t *testing.T) {
	srv, front := newTestServer(t)
	job := srv.jobs.Submit(JobDiffSchema, map[string]any{"sourceId": "s"}, func(ctx context.Context, j *Job) error {
		j.Logf("step one")
		j.Summary(map[string]any{"files": []string{"schema_diff.sql"}})
		return nil
	})
	if job.Status() != jobStatusQueued && job.Status() != jobStatusSucceeded {
		t.Fatalf("initial status = %s", job.Status())
	}
	waitForJobStatus(t, job, jobStatusSucceeded)

	view := mustJSON(t, front, http.MethodGet, "/api/jobs/"+job.ID(), nil, http.StatusOK)
	if view["status"] != jobStatusSucceeded || view["type"] != "diff-schema" {
		t.Fatalf("job view wrong: %v", view)
	}
	logView, _ := view["log"].([]any)
	found := false
	for _, line := range logView {
		if strings.Contains(line.(string), "step one") {
			found = true
		}
	}
	if !found {
		t.Fatalf("log missing step one: %v", logView)
	}
	summary, _ := view["summary"].(map[string]any)
	if summary == nil {
		t.Fatal("summary missing")
	}
	if _, ok := view["params"].(map[string]any); !ok {
		t.Fatalf("params missing: %v", view)
	}
}

func TestJobListEndpointNewestFirst(t *testing.T) {
	srv, front := newTestServer(t)
	release := make(chan struct{})
	for i := 0; i < 3; i++ {
		srv.jobs.Submit(JobExecSQL, map[string]any{"n": i}, func(ctx context.Context, j *Job) error {
			<-release
			return nil
		})
	}
	// Exactly the capacity-limited number may run; the rest queue.
	waitForCondition(t, func() bool {
		running, queued := 0, 0
		for _, j := range srv.jobs.List(10) {
			switch j.Status() {
			case jobStatusRunning:
				running++
			case jobStatusQueued:
				queued++
			}
		}
		return running == semaphoreCapacity && queued == 1
	})
	close(release)
	for _, j := range srv.jobs.List(10) {
		waitForJobStatus(t, j, jobStatusSucceeded)
	}
	jobs := mustJSONArray(t, front, http.MethodGet, "/api/jobs", nil, http.StatusOK)
	if len(jobs) != 3 {
		t.Fatalf("jobs = %d, want 3", len(jobs))
	}
	first, _ := jobs[0].(map[string]any)
	last, _ := jobs[2].(map[string]any)
	if first["params"].(map[string]any)["n"] != 2.0 {
		t.Fatalf("list not newest first: %v", jobs)
	}
	if last["params"].(map[string]any)["n"] != 0.0 {
		t.Fatalf("list order wrong: %v", jobs)
	}
	// limit applies.
	capped := mustJSONArray(t, front, http.MethodGet, "/api/jobs?limit=2", nil, http.StatusOK)
	if len(capped) != 2 {
		t.Fatalf("limit not applied: %d", len(capped))
	}
}

func TestJobFailureCarriesError(t *testing.T) {
	srv, front := newTestServer(t)
	job := srv.jobs.Submit(JobReset, nil, func(ctx context.Context, j *Job) error {
		return errBad("boom")
	})
	waitForJobStatus(t, job, jobStatusFailed)
	view := mustJSON(t, front, http.MethodGet, "/api/jobs/"+job.ID(), nil, http.StatusOK)
	if view["error"] != "boom" {
		t.Fatalf("error not surfaced: %v", view)
	}
	if _, ok := view["finishedAt"]; !ok {
		t.Fatalf("finishedAt missing: %v", view)
	}
}

func TestJobCancelRunning(t *testing.T) {
	srv, front := newTestServer(t)
	started := make(chan struct{})
	job := srv.jobs.Submit(JobDiffData, nil, func(ctx context.Context, j *Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	mustJSON(t, front, http.MethodPost, "/api/jobs/"+job.ID()+"/cancel", nil, http.StatusOK)
	waitForJobStatus(t, job, jobStatusCancelled)
}

func TestJobCancelQueued(t *testing.T) {
	srv, _ := newTestServer(t)
	// Fill the semaphore with blocking jobs.
	blockers := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(semaphoreCapacity)
	for i := 0; i < semaphoreCapacity; i++ {
		srv.jobs.Submit(JobExecSQL, nil, func(ctx context.Context, j *Job) error {
			ready.Done()
			<-blockers
			return nil
		})
	}
	ready.Wait()
	queued := srv.jobs.Submit(JobMigrate, nil, func(ctx context.Context, j *Job) error { return nil })
	time.Sleep(20 * time.Millisecond)
	if queued.Status() != jobStatusQueued {
		t.Fatalf("third job status = %s, want queued", queued.Status())
	}
	if !srv.jobs.Cancel(queued.ID()) {
		t.Fatal("cancel queued returned false")
	}
	waitForJobStatus(t, queued, jobStatusCancelled)
	close(blockers)
}

func TestSemaphoreCapacityTwo(t *testing.T) {
	srv, _ := newTestServer(t)
	blockers := make(chan struct{})
	for i := 0; i < semaphoreCapacity+1; i++ {
		srv.jobs.Submit(JobExecSQL, nil, func(ctx context.Context, j *Job) error {
			<-blockers
			return nil
		})
	}
	waitForCondition(t, func() bool {
		running, queued := 0, 0
		for _, j := range srv.jobs.List(10) {
			switch j.Status() {
			case jobStatusRunning:
				running++
			case jobStatusQueued:
				queued++
			}
		}
		return running == semaphoreCapacity && queued == 1
	})
	close(blockers)
}

func TestOrphanJobDirsSweptOnStart(t *testing.T) {
	dir := t.TempDir()
	jobsDir := filepath.Join(dir, "jobs")
	if err := os.MkdirAll(filepath.Join(jobsDir, "stale-job"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(jobsDir, "stale-job", "schema_diff.sql"), []byte("-- x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer srv.Close()
	if _, err := os.Stat(filepath.Join(jobsDir, "stale-job")); !os.IsNotExist(err) {
		t.Fatalf("orphan dir survived startup sweep: %v", err)
	}
}

func TestJobArtifactsListDownloadZip(t *testing.T) {
	srv, front := newTestServer(t)
	job := srv.jobs.Submit(JobDiffSchema, nil, func(ctx context.Context, j *Job) error {
		if err := os.WriteFile(filepath.Join(j.Dir(), "schema_diff.sql"), []byte("-- forward"), 0o644); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(j.Dir(), "schema_diff_rollback.sql"), []byte("-- rollback"), 0o644)
	})
	waitForJobStatus(t, job, jobStatusSucceeded)

	entries := mustJSONArray(t, front, http.MethodGet, "/api/jobs/"+job.ID()+"/artifacts", nil, http.StatusOK)
	if len(entries) != 2 {
		t.Fatalf("artifacts = %d, want 2", len(entries))
	}
	first, _ := entries[0].(map[string]any)
	if first["name"] != "schema_diff.sql" || first["size"] != float64(10) {
		t.Fatalf("artifact listing wrong: %v", entries)
	}

	// Single artifact download with attachment disposition.
	resp, err := front.Client().Get(front.URL + "/api/jobs/" + job.ID() + "/artifacts/schema_diff.sql")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download status = %d", resp.StatusCode)
	}
	if disp := resp.Header.Get("Content-Disposition"); !strings.Contains(disp, "attachment") {
		t.Fatalf("disposition missing: %q", disp)
	}
	content, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(content)) != "-- forward" {
		t.Fatalf("download content wrong: %q", string(content))
	}

	// Zip download contains both files.
	zipResp, err := front.Client().Get(front.URL + "/api/jobs/" + job.ID() + "/artifacts/download")
	if err != nil {
		t.Fatalf("zip: %v", err)
	}
	defer zipResp.Body.Close()
	if ct := zipResp.Header.Get("Content-Type"); ct != "application/zip" {
		t.Fatalf("zip content type = %q", ct)
	}
	zipBytes, _ := io.ReadAll(zipResp.Body)
	if len(zipBytes) < 100 || string(zipBytes[:2]) != "PK" {
		t.Fatalf("zip payload wrong: len=%d", len(zipBytes))
	}

	// Path escape rejected.
	if status := statusOf(t, front, http.MethodGet, "/api/jobs/"+job.ID()+"/artifacts/..%2F..%2Fstore.json", nil); status != http.StatusBadRequest {
		t.Fatalf("path escape: status = %d", status)
	}

	// Unknown job.
	if status := statusOf(t, front, http.MethodGet, "/api/jobs/missing/artifacts", nil); status != http.StatusNotFound {
		t.Fatalf("unknown job artifacts: status = %d", status)
	}
}

// 每个任务必须有自己的产物子目录:否则不同任务的产物互相可见/覆盖,
// 数据比对详情里会列出结构比对的 SQL。
func TestJobArtifactDirsAreIsolated(t *testing.T) {
	srv, front := newTestServer(t)
	schemaJob := srv.jobs.Submit(JobDiffSchema, nil, func(ctx context.Context, j *Job) error {
		return os.WriteFile(filepath.Join(j.Dir(), "schema_diff.sql"), []byte("-- schema"), 0o644)
	})
	dataJob := srv.jobs.Submit(JobDiffData, nil, func(ctx context.Context, j *Job) error {
		return os.WriteFile(filepath.Join(j.Dir(), "data_diff.sql"), []byte("-- data"), 0o644)
	})
	waitForJobStatus(t, schemaJob, jobStatusSucceeded)
	waitForJobStatus(t, dataJob, jobStatusSucceeded)

	if schemaJob.Dir() == dataJob.Dir() {
		t.Fatalf("jobs share one artifact dir: %s", schemaJob.Dir())
	}
	schemaEntries := mustJSONArray(t, front, http.MethodGet, "/api/jobs/"+schemaJob.ID()+"/artifacts", nil, http.StatusOK)
	if len(schemaEntries) != 1 || schemaEntries[0].(map[string]any)["name"] != "schema_diff.sql" {
		t.Fatalf("schema job artifacts polluted: %v", schemaEntries)
	}
	dataEntries := mustJSONArray(t, front, http.MethodGet, "/api/jobs/"+dataJob.ID()+"/artifacts", nil, http.StatusOK)
	if len(dataEntries) != 1 || dataEntries[0].(map[string]any)["name"] != "data_diff.sql" {
		t.Fatalf("data job artifacts polluted: %v", dataEntries)
	}
}

func TestTTLJanitorSweep(t *testing.T) {
	dir := t.TempDir()
	srv, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer srv.Close()
	jobsDir := filepath.Join(dir, "jobs")
	oldDir := filepath.Join(jobsDir, "old-job")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	past := time.Now().Add(-jobTTL - time.Hour)
	if err := os.Chtimes(oldDir, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	freshDir := filepath.Join(jobsDir, "fresh-job")
	if err := os.MkdirAll(freshDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv.jobs.sweepExpired(time.Now())
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("expired dir survived: %v", err)
	}
	if _, err := os.Stat(freshDir); err != nil {
		t.Fatalf("fresh dir removed: %v", err)
	}
}
