package server

import (
	"context"
	"net/http"
	"strings"

	execlogic "github.com/jacktea/data-smith/internal/datasmith/exec"
)

type execSQLRequest struct {
	ConnectionID string `json:"connectionId"`
	Content      string `json:"content"`
	Mode         string `json:"mode"`
	TargetRole   string `json:"targetRole"`
}

// exec-sql 执行模式:dryrun=事务中执行后回滚;tx=显式事务提交;direct=直连执行。
const (
	execModeDryRun = "dryrun"
	execModeTx     = "tx"
	execModeDirect = "direct"
)

func (s *Server) handleExecSQLSubmit(w http.ResponseWriter, r *http.Request) {
	var req execSQLRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "连接不存在")
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		writeError(w, http.StatusBadRequest, "SQL 内容不能为空")
		return
	}
	switch req.Mode {
	case execModeDryRun, execModeTx, execModeDirect:
	default:
		writeError(w, http.StatusBadRequest, "mode 仅支持 dryrun、tx 或 direct")
		return
	}
	if err := execlogic.ValidateExecutionTarget(req.Content, req.TargetRole); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	job := s.jobs.Submit(JobExecSQL, map[string]any{
		"connectionId": stored.ID,
		"content":      req.Content,
		"mode":         req.Mode,
		"targetRole":   req.TargetRole,
	}, func(ctx context.Context, job *Job) error {
		return s.runExecSQL(ctx, job, stored.ID, req.Content, req.Mode, req.TargetRole)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runExecSQL(ctx context.Context, job *Job, connectionID, content, mode, targetRole string) error {
	stored, ok := s.store.GetConnection(connectionID)
	if !ok {
		return errBad("连接已被删除")
	}
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		return err
	}
	defer adapter.Close()

	dryRun := mode == execModeDryRun
	useTx := mode == execModeTx || dryRun
	job.Logf("开始执行 SQL: mode=%s targetRole=%s", mode, targetRole)
	if err := execlogic.ExecuteSQLContextForTarget(ctx, adapter, content, targetRole, dryRun, useTx); err != nil {
		return err
	}
	job.Summary(map[string]any{"mode": mode, "ok": true})
	job.Logf("SQL 执行完成")
	return nil
}
