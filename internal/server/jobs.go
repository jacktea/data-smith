package server

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// JobType enumerates the supported long-running operations.
type JobType string

const (
	JobDiffSchema JobType = "diff-schema"
	JobDiffData   JobType = "diff-data"
	JobExecSQL    JobType = "exec-sql"
	JobReset      JobType = "reset"
	JobMigrate    JobType = "migrate"
	JobRollback   JobType = "rollback"
)

const (
	jobStatusQueued    = "queued"
	jobStatusRunning   = "running"
	jobStatusSucceeded = "succeeded"
	jobStatusFailed    = "failed"
	jobStatusCancelled = "cancelled"
)

const (
	semaphoreCapacity = 2
	jobLogLimit       = 200
	jobTTL            = 7 * 24 * time.Hour
	janitorInterval   = time.Hour
)

// JobProgress tracks data-diff table progress; nil for other job types.
type JobProgress struct {
	CurrentTable string `json:"currentTable,omitempty"`
	TablesDone   int    `json:"tablesDone"`
	TablesTotal  int    `json:"tablesTotal"`
}

// Runner executes the payload of a job. Returning an error fails the job
// unless the context was cancelled, in which case the job is cancelled.
type Runner func(ctx context.Context, job *Job) error

// Job is one long-running operation instance with log ring buffer and
// artifacts directory (<data-dir>/jobs/<id>/).
type Job struct {
	mu         sync.Mutex
	id         string
	typ        JobType
	status     string
	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time
	params     map[string]any
	summary    map[string]any
	errMsg     string
	log        []string
	progress   *JobProgress
	cancel     context.CancelFunc
	dir        string
}

// newJob creates a job with its own artifact subdirectory under jobsDir, so
// concurrent jobs never see each other's files.
func newJob(typ JobType, params map[string]any, jobsDir string) *Job {
	id := newID("job")
	return &Job{
		id:        id,
		typ:       typ,
		status:    jobStatusQueued,
		createdAt: time.Now(),
		params:    params,
		log:       []string{},
		dir:       filepath.Join(jobsDir, id),
	}
}

func (j *Job) ID() string    { return j.id }
func (j *Job) Type() JobType { return j.typ }
func (j *Job) Dir() string   { return j.dir }

// Status returns the current status string.
func (j *Job) Status() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.status
}

// Logf appends a timestamped line to the ring buffer (last jobLogLimit kept).
func (j *Job) Logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	j.mu.Lock()
	defer j.mu.Unlock()
	j.log = append(j.log, time.Now().Format("15:04:05")+" "+msg)
	if len(j.log) > jobLogLimit {
		j.log = j.log[len(j.log)-jobLogLimit:]
	}
}

// SetProgressTotal initializes the data-diff progress denominator.
func (j *Job) SetProgressTotal(total int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.progress == nil {
		j.progress = &JobProgress{}
	}
	j.progress.TablesTotal = total
}

// BeginTable marks the current table at the start of its comparison.
func (j *Job) BeginTable(table string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.progress == nil {
		j.progress = &JobProgress{}
	}
	j.progress.CurrentTable = table
}

// FinishTable marks one table done and logs the outcome.
func (j *Job) FinishTable(table, errText string) {
	j.mu.Lock()
	if j.progress != nil {
		j.progress.TablesDone++
	}
	if j.progress != nil {
		j.progress.CurrentTable = ""
	}
	j.mu.Unlock()
	if errText != "" {
		j.Logf("表 %s 比对失败: %s", table, errText)
	} else {
		j.Logf("表 %s 比对完成", table)
	}
}

func (j *Job) setSummaryLocked(summary map[string]any) {
	j.summary = summary
}

// Summary sets the job summary payload.
func (j *Job) Summary(summary map[string]any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.setSummaryLocked(summary)
}

func (j *Job) markRunning() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusQueued {
		return
	}
	j.status = jobStatusRunning
	j.startedAt = time.Now()
}

func (j *Job) markCancelledQueued() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status != jobStatusQueued {
		return
	}
	j.status = jobStatusCancelled
	j.finishedAt = time.Now()
	j.errMsg = "任务在排队中被取消"
}

// finish records the terminal state; only a running (or stuck queued) job can
// be finalized, terminal states are immutable.
func (j *Job) finish(err error, ctxCancelled bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.status == jobStatusSucceeded || j.status == jobStatusFailed || j.status == jobStatusCancelled {
		return
	}
	j.finishedAt = time.Now()
	switch {
	case err == nil:
		j.status = jobStatusSucceeded
	case ctxCancelled:
		j.status = jobStatusCancelled
		j.errMsg = "任务已取消"
	default:
		j.status = jobStatusFailed
		j.errMsg = err.Error()
	}
}

// JobView is the JSON projection of a job.
type JobView struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Status     string         `json:"status"`
	CreatedAt  time.Time      `json:"createdAt"`
	StartedAt  *time.Time     `json:"startedAt,omitempty"`
	FinishedAt *time.Time     `json:"finishedAt,omitempty"`
	Params     map[string]any `json:"params"`
	Summary    map[string]any `json:"summary"`
	Error      string         `json:"error"`
	Log        []string       `json:"log"`
	Progress   *JobProgress   `json:"progress"`
}

// View snapshots the job under its lock.
func (j *Job) View() JobView {
	j.mu.Lock()
	defer j.mu.Unlock()
	view := JobView{
		ID:        j.id,
		Type:      string(j.typ),
		Status:    j.status,
		CreatedAt: j.createdAt,
		Params:    j.params,
		Summary:   j.summary,
		Error:     j.errMsg,
		Progress:  j.progress,
	}
	if !j.startedAt.IsZero() {
		started := j.startedAt
		view.StartedAt = &started
	}
	if !j.finishedAt.IsZero() {
		finished := j.finishedAt
		view.FinishedAt = &finished
	}
	view.Log = append([]string(nil), j.log...)
	if view.Log == nil {
		view.Log = []string{}
	}
	return view
}

// Registry keeps jobs in memory, gates heavy work behind a capacity-2
// semaphore, and sweeps expired/orphaned artifact directories.
type Registry struct {
	mu      sync.Mutex
	dataDir string
	jobs    map[string]*Job
	order   []string
	sem     chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewRegistry creates the registry, removes every orphaned artifact directory
// left by previous runs, and starts the hourly TTL janitor.
func NewRegistry(dataDir string) (*Registry, error) {
	jobsDir := filepath.Join(dataDir, "jobs")
	if err := os.MkdirAll(jobsDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建任务目录: %w", err)
	}
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return nil, fmt.Errorf("读取任务目录: %w", err)
	}
	// The registry starts empty, so every persisted directory is an orphan
	// whose owning job no longer exists in memory.
	for _, entry := range entries {
		_ = os.RemoveAll(filepath.Join(jobsDir, entry.Name()))
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{
		dataDir: dataDir,
		jobs:    make(map[string]*Job),
		sem:     make(chan struct{}, semaphoreCapacity),
		ctx:     ctx,
		cancel:  cancel,
	}
	go r.janitor()
	return r, nil
}

// Close stops the janitor and cancels every queued/running job context.
func (r *Registry) Close() {
	r.cancel()
}

// Submit registers a job and starts its runner goroutine. Params must already
// be masked (no secrets).
func (r *Registry) Submit(typ JobType, params map[string]any, runner Runner) *Job {
	job := newJob(typ, params, filepath.Join(r.dataDir, "jobs"))
	_ = os.MkdirAll(job.dir, 0o755)

	r.mu.Lock()
	r.jobs[job.id] = job
	r.order = append(r.order, job.id)
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(r.ctx)
	job.cancel = cancel
	go r.run(job, ctx, runner)
	return job
}

func (r *Registry) run(job *Job, ctx context.Context, runner Runner) {
	defer func() {
		if job.cancel != nil {
			job.cancel()
		}
	}()
	select {
	case r.sem <- struct{}{}:
		defer func() { <-r.sem }()
	case <-ctx.Done():
		job.markCancelledQueued()
		return
	}
	if ctx.Err() != nil {
		job.markCancelledQueued()
		return
	}
	job.markRunning()
	err := runner(ctx, job)
	job.finish(err, ctx.Err() != nil)
}

// Get returns the job with id, or nil.
func (r *Registry) Get(id string) *Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs[id]
}

// List returns job views newest-first, up to limit (0 = default 50).
func (r *Registry) List(limit int) []*Job {
	if limit <= 0 {
		limit = 50
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Job, 0, len(r.order))
	for i := len(r.order) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, r.jobs[r.order[i]])
	}
	return out
}

// Cancel cancels a queued or running job; reports whether a cancel was issued.
func (r *Registry) Cancel(id string) bool {
	job := r.Get(id)
	if job == nil {
		return false
	}
	switch job.Status() {
	case jobStatusQueued:
		job.markCancelledQueued()
		if job.cancel != nil {
			job.cancel()
		}
		return true
	case jobStatusRunning:
		if job.cancel != nil {
			job.cancel()
		}
		return true
	default:
		return false
	}
}

// janitor removes artifact directories older than jobTTL every hour.
func (r *Registry) janitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.sweepExpired(time.Now())
		}
	}
}

// sweepExpired removes artifact directories (and registry entries) whose
// creation is older than jobTTL.
func (r *Registry) sweepExpired(now time.Time) {
	jobsDir := filepath.Join(r.dataDir, "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return
	}
	expired := make(map[string]bool)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.IsDir() {
			continue
		}
		if now.Sub(info.ModTime()) > jobTTL {
			expired[entry.Name()] = true
			_ = os.RemoveAll(filepath.Join(jobsDir, entry.Name()))
		}
	}
	if len(expired) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.order[:0]
	for _, id := range r.order {
		if expired[id] {
			delete(r.jobs, id)
			continue
		}
		kept = append(kept, id)
	}
	r.order = kept
}

// artifactEntry is one artifact file listing row.
type artifactEntry struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// listArtifacts returns the sorted files inside the job directory.
func (s *Server) listArtifacts(job *Job) ([]artifactEntry, error) {
	entries, err := os.ReadDir(job.Dir())
	if err != nil {
		if os.IsNotExist(err) {
			return []artifactEntry{}, nil
		}
		return nil, err
	}
	out := make([]artifactEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, artifactEntry{Name: entry.Name(), Size: info.Size()})
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out, nil
}

// safeArtifactPath resolves a requested file name under dir, rejecting path
// escapes and separators.
func safeArtifactPath(dir, name string) (string, error) {
	if name == "" || name != filepath.Base(name) || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return "", fmt.Errorf("非法的产物文件名: %s", name)
	}
	return filepath.Join(dir, name), nil
}

func serveFileAttachment(w http.ResponseWriter, r *http.Request, path, downloadName string) {
	f, err := os.Open(path)
	if err != nil {
		writeError(w, http.StatusNotFound, "文件不存在: %s", downloadName)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		writeError(w, http.StatusNotFound, "文件不存在: %s", downloadName)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", urlPathEscape(downloadName)))
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, downloadName, info.ModTime(), f)
}

// --- job HTTP handlers ------------------------------------------------------

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 50)
	jobs := s.jobs.List(limit)
	out := make([]JobView, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job.View())
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job := s.jobs.Get(r.PathValue("id"))
	if job == nil {
		writeError(w, http.StatusNotFound, "任务不存在")
		return
	}
	writeJSON(w, http.StatusOK, job.View())
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	job := s.jobs.Get(r.PathValue("id"))
	if job == nil {
		writeError(w, http.StatusNotFound, "任务不存在")
		return
	}
	ok := s.jobs.Cancel(job.ID())
	writeJSON(w, http.StatusOK, map[string]any{"ok": ok})
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	job := s.jobs.Get(r.PathValue("id"))
	if job == nil {
		writeError(w, http.StatusNotFound, "任务不存在")
		return
	}
	entries, err := s.listArtifacts(job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取任务产物失败: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleDownloadArtifact(w http.ResponseWriter, r *http.Request) {
	job := s.jobs.Get(r.PathValue("id"))
	if job == nil {
		writeError(w, http.StatusNotFound, "任务不存在")
		return
	}
	name := r.PathValue("name")
	path, err := safeArtifactPath(job.Dir(), name)
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	serveFileAttachment(w, r, path, name)
}

func (s *Server) handleDownloadArtifactsZip(w http.ResponseWriter, r *http.Request) {
	job := s.jobs.Get(r.PathValue("id"))
	if job == nil {
		writeError(w, http.StatusNotFound, "任务不存在")
		return
	}
	entries, err := s.listArtifacts(job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "读取任务产物失败: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", urlPathEscape(job.ID()+"-artifacts.zip")))
	zipWriter := zip.NewWriter(w)
	for _, entry := range entries {
		path, err := safeArtifactPath(job.Dir(), entry.Name)
		if err != nil {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		writer, err := zipWriter.Create(entry.Name)
		if err == nil {
			_, err = io.Copy(writer, f)
		}
		f.Close()
		if err != nil {
			return
		}
	}
	_ = zipWriter.Close()
}
