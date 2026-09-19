package server

import (
	"context"
	"net/http"

	pkgmigrate "github.com/jacktea/data-smith/pkg/migrate"
)

type resetPreviewRequest struct {
	ConnectionID string `json:"connectionId"`
}

type resetJobRequest struct {
	ConnectionID string `json:"connectionId"`
	Confirmed    *bool  `json:"confirmed"`
}

// handleResetPreview returns the validated reset SQL without connecting.
func (s *Server) handleResetPreview(w http.ResponseWriter, r *http.Request) {
	var req resetPreviewRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "连接不存在")
		return
	}
	sql, err := pkgmigrate.BuildResetSQL(stored.ConnConfig())
	if err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sql": sql})
}

func (s *Server) handleResetSubmit(w http.ResponseWriter, r *http.Request) {
	var req resetJobRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "连接不存在")
		return
	}
	if req.Confirmed == nil || !*req.Confirmed {
		writeError(w, http.StatusBadRequest, "重置为破坏性操作,必须显式确认(confirmed=true)")
		return
	}
	job := s.jobs.Submit(JobReset, map[string]any{
		"connectionId": stored.ID,
		"confirmed":    true,
	}, func(ctx context.Context, job *Job) error {
		return s.runReset(ctx, job, stored.ID)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runReset(ctx context.Context, job *Job, connectionID string) error {
	stored, ok := s.store.GetConnection(connectionID)
	if !ok {
		return errBad("连接已被删除")
	}
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		return err
	}
	defer adapter.Close()

	// 复用引擎安全逻辑:BuildResetSQL 内含 ValidateResetTarget(系统库/空目标拒绝)。
	if _, err := pkgmigrate.BuildResetSQL(stored.ConnConfig()); err != nil {
		return err
	}
	job.Logf("开始重置数据库 %s(%s)", stored.Name, stored.DBName)
	if err := pkgmigrate.ResetDatabaseContext(ctx, adapter); err != nil {
		return err
	}
	job.Summary(map[string]any{"ok": true})
	job.Logf("数据库重置完成")
	return nil
}
