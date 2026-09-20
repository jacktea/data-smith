package server

import (
	"context"
	"net/http"
	"strings"

	migratelogic "github.com/jacktea/data-smith/internal/datasmith/migrate"
	local "github.com/jacktea/data-smith/internal/datasmith/migrate/local"
	pkgmigrate "github.com/jacktea/data-smith/pkg/migrate"
)

type migrateJobRequest struct {
	LibraryID     string `json:"libraryId"`
	ConnectionID  string `json:"connectionId"`
	TargetVersion string `json:"targetVersion"`
	DryRun        bool   `json:"dryRun"`
}

type rollbackJobRequest struct {
	LibraryID     string `json:"libraryId"`
	ConnectionID  string `json:"connectionId"`
	TargetVersion string `json:"targetVersion"`
	Confirmed     *bool  `json:"confirmed"`
}

func (s *Server) handleMigrateSubmit(w http.ResponseWriter, r *http.Request) {
	var req migrateJobRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	if _, ok := s.store.GetLibrary(req.LibraryID); !ok {
		writeError(w, http.StatusBadRequest, "脚本库不存在")
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "连接不存在")
		return
	}
	job := s.jobs.Submit(JobMigrate, map[string]any{
		"libraryId":     req.LibraryID,
		"connectionId":  stored.ID,
		"targetVersion": req.TargetVersion,
		"dryRun":        req.DryRun,
	}, func(ctx context.Context, job *Job) error {
		return s.runMigrate(ctx, job, req.LibraryID, stored.ID, req.TargetVersion, req.DryRun)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runMigrate(ctx context.Context, job *Job, libraryID, connectionID, targetVersion string, dryRun bool) error {
	libraryDir, err := s.libraryDir(libraryID)
	if err != nil {
		return err
	}
	stored, ok := s.store.GetConnection(connectionID)
	if !ok {
		return errBad("连接已被删除")
	}
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		return err
	}
	defer adapter.Close()

	// 与引擎一致的准备步骤:先建/校验账本,再读成功集合与待执行清单。
	if err := pkgmigrate.EnsureVersionTable(adapter); err != nil {
		return err
	}
	files, err := local.ScanMigrations(libraryDir)
	if err != nil {
		return err
	}
	local.SortMigrations(files)
	successSet, err := pkgmigrate.SuccessfulMigrations(adapter)
	if err != nil {
		return err
	}

	var pending []*localMigrationFile
	for _, f := range files {
		if f.Direction != "up" {
			continue
		}
		if targetVersion != "" && local.CompareVersion(f.Version, targetVersion) > 0 {
			continue
		}
		if _, applied := successSet[normalizedVersion(f.Version)]; applied {
			continue
		}
		pending = append(pending, &localMigrationFile{Version: f.Version, Title: f.Title})
	}
	if err := s.checkExpectedConnections(job, libraryID, connectionID, pending); err != nil {
		return err
	}

	applied := make([]map[string]any, 0, len(pending))
	for _, f := range pending {
		applied = append(applied, map[string]any{"version": f.Version, "title": f.Title})
	}
	job.Summary(map[string]any{
		"dryRun":        dryRun,
		"applied":       applied,
		"targetVersion": targetVersion,
	})
	if dryRun {
		job.Logf("开始迁移(试跑模式),待执行 %d 个版本", len(pending))
	} else {
		job.Logf("开始迁移(执行模式),待执行 %d 个版本", len(pending))
	}
	if err := migratelogic.RunMigrations(ctx, adapter, libraryDir, dryRun, targetVersion, func(msg string) { job.Logf("%s", msg) }); err != nil {
		return err
	}
	job.Logf("迁移完成")
	return nil
}

// localMigrationFile is the minimal version/title pair used in summaries.
type localMigrationFile struct {
	Version string
	Title   string
}

// checkExpectedConnections enforces the version-registration contract: a
// version registered on another connection of the same dialect warns;
// a dialect mismatch hard-fails before any script runs.
func (s *Server) checkExpectedConnections(job *Job, libraryID, connectionID string, pending []*localMigrationFile) error {
	library, ok := s.store.GetLibrary(libraryID)
	if !ok {
		return errBad("脚本库不存在")
	}
	execConn, ok := s.store.GetConnection(connectionID)
	if !ok {
		return errBad("连接已被删除")
	}
	for _, f := range pending {
		meta, known := library.Versions[normalizedVersion(f.Version)]
		if !known || meta.ExpectedConnectionID == "" || meta.ExpectedConnectionID == connectionID {
			continue
		}
		expected, ok := s.store.GetConnection(meta.ExpectedConnectionID)
		if !ok {
			job.Logf("警告: 版本 %s 登记时的预期执行库已删除,跳过校验", f.Version)
			continue
		}
		if !strings.EqualFold(expected.Type, execConn.Type) {
			return errBad("版本 %s 登记于连接 %s(%s),与当前执行连接方言(%s)不一致,拒绝执行",
				f.Version, expected.Name, expected.Type, execConn.Type)
		}
		job.Logf("警告: 版本 %s 登记于连接 %s(%s),当前为跨库执行,已放行", f.Version, expected.Name, expected.Type)
	}
	return nil
}

func (s *Server) handleRollbackSubmit(w http.ResponseWriter, r *http.Request) {
	var req rollbackJobRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	if _, ok := s.store.GetLibrary(req.LibraryID); !ok {
		writeError(w, http.StatusBadRequest, "脚本库不存在")
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "连接不存在")
		return
	}
	if req.Confirmed == nil || !*req.Confirmed {
		writeError(w, http.StatusBadRequest, "回退为破坏性操作,必须显式确认(confirmed=true)")
		return
	}
	job := s.jobs.Submit(JobRollback, map[string]any{
		"libraryId":     req.LibraryID,
		"connectionId":  stored.ID,
		"targetVersion": req.TargetVersion,
		"confirmed":     true,
	}, func(ctx context.Context, job *Job) error {
		return s.runRollback(ctx, job, req.LibraryID, stored.ID, req.TargetVersion)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runRollback(ctx context.Context, job *Job, libraryID, connectionID, targetVersion string) error {
	libraryDir, err := s.libraryDir(libraryID)
	if err != nil {
		return err
	}
	stored, ok := s.store.GetConnection(connectionID)
	if !ok {
		return errBad("连接已被删除")
	}
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		return err
	}
	defer adapter.Close()

	if targetVersion == "" {
		job.Logf("开始回退最新已应用版本")
		version, err := migratelogic.RollbackLatest(ctx, adapter, libraryDir, func(msg string) { job.Logf("%s", msg) })
		if err != nil {
			return err
		}
		job.Summary(map[string]any{"version": version, "versions": []string{version}, "rolled": true})
		job.Logf("版本 %s 已回退", version)
		return nil
	}

	job.Logf("开始回退到版本 %s(从最新版本依次回退)", targetVersion)
	rolled, err := migratelogic.RollbackTo(ctx, adapter, libraryDir, targetVersion, func(msg string) { job.Logf("%s", msg) })
	if err != nil {
		return err
	}
	if rolled == nil {
		rolled = []string{}
	}
	job.Summary(map[string]any{"versions": rolled, "targetVersion": targetVersion, "rolled": true})
	if len(rolled) == 0 {
		job.Logf("当前已处于版本 %s, 无需回退", targetVersion)
		return nil
	}
	job.Logf("已依次回退 %d 个版本, 当前版本 %s", len(rolled), targetVersion)
	return nil
}
