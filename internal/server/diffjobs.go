package server

import (
	"context"
	"net/http"
	"os"
	"path/filepath"

	difflogic "github.com/jacktea/data-smith/internal/datasmith/diff"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	pkgdb "github.com/jacktea/data-smith/pkg/db"
)

const summaryJSONFile = "summary.json"

// diff-schema --------------------------------------------------------------

type diffSchemaRequest struct {
	SourceID      string   `json:"sourceId"`
	TargetID      string   `json:"targetId"`
	IncludeTables []string `json:"includeTables"`
	ExcludeTables []string `json:"excludeTables"`
}

func (s *Server) handleDiffSchemaSubmit(w http.ResponseWriter, r *http.Request) {
	var req diffSchemaRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	source, ok := s.store.GetConnection(req.SourceID)
	if !ok {
		writeError(w, http.StatusBadRequest, "source 连接不存在")
		return
	}
	target, ok := s.store.GetConnection(req.TargetID)
	if !ok {
		writeError(w, http.StatusBadRequest, "target 连接不存在")
		return
	}
	job := s.jobs.Submit(JobDiffSchema, map[string]any{
		"sourceId":      source.ID,
		"targetId":      target.ID,
		"includeTables": req.IncludeTables,
		"excludeTables": req.ExcludeTables,
	}, func(ctx context.Context, job *Job) error {
		return s.runDiffSchema(ctx, job, source.ID, target.ID, req.IncludeTables, req.ExcludeTables)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runDiffSchema(ctx context.Context, job *Job, sourceID, targetID string, includeTables, excludeTables []string) error {
	source, ok := s.store.GetConnection(sourceID)
	if !ok {
		return errBad("source 连接已被删除")
	}
	target, ok := s.store.GetConnection(targetID)
	if !ok {
		return errBad("target 连接已被删除")
	}
	job.Logf("开始结构比对: source=%s(%s) target=%s(%s)", source.Name, source.ID, target.Name, target.ID)
	summary, err := difflogic.RunSchemaDiff(ctx, difflogic.SchemaDiffParams{
		Source:        source.ConnConfig(),
		Target:        target.ConnConfig(),
		IncludeTables: includeTables,
		ExcludeTables: excludeTables,
	}, job.Dir(), func(msg string) { job.Logf("%s", msg) })
	if err != nil {
		return err
	}
	job.Summary(map[string]any{
		"schema": summary,
		"files":  difflogic.SchemaDiffFileNames(),
	})
	job.Logf("结构比对完成: 新增 %d 张表, 删除 %d 张表, 修改 %d 张表",
		len(summary.TablesAdded), len(summary.TablesDropped), len(summary.TablesModified))
	return nil
}

// diff-data ----------------------------------------------------------------

type diffDataRequest struct {
	SourceID     string        `json:"sourceId"`
	TargetID     string        `json:"targetId"`
	SchemeID     string        `json:"schemeId"`
	Tables       []SchemeTable `json:"tables"`
	BatchSize    int           `json:"batchSize"`
	ChunkSize    int           `json:"chunkSize"`
	DMLBatchSize int           `json:"dmlBatchSize"`
	ChunkHash    bool          `json:"chunkHash"`
	BestEffort   bool          `json:"bestEffort"`
}

// data-diff defaults mirror the CLI flag defaults.
const (
	defaultBatchSize = 1000
	defaultChunkSize = 10000
	maxDMLBatchSize  = 10000
)

func (s *Server) handleDiffDataSubmit(w http.ResponseWriter, r *http.Request) {
	var req diffDataRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	source, ok := s.store.GetConnection(req.SourceID)
	if !ok {
		writeError(w, http.StatusBadRequest, "source 连接不存在")
		return
	}
	target, ok := s.store.GetConnection(req.TargetID)
	if !ok {
		writeError(w, http.StatusBadRequest, "target 连接不存在")
		return
	}
	tables := req.Tables
	if len(tables) == 0 && req.SchemeID != "" {
		scheme, ok := s.store.GetScheme(req.SchemeID)
		if !ok {
			writeError(w, http.StatusBadRequest, "比对方案不存在")
			return
		}
		tables = scheme.Tables
	}
	if len(tables) == 0 {
		writeError(w, http.StatusBadRequest, "请选择至少一张数据表")
		return
	}
	for _, table := range tables {
		if table.Table == "" {
			writeError(w, http.StatusBadRequest, "数据表名不能为空")
			return
		}
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	dmlBatchSize := req.DMLBatchSize
	if dmlBatchSize <= 0 {
		dmlBatchSize = defaultBatchSize
	}
	if dmlBatchSize > maxDMLBatchSize {
		writeError(w, http.StatusBadRequest, "dmlBatchSize 不能超过 %d", maxDMLBatchSize)
		return
	}

	params := map[string]any{
		"sourceId":     source.ID,
		"targetId":     target.ID,
		"schemeId":     req.SchemeID,
		"tables":       tables,
		"batchSize":    batchSize,
		"chunkSize":    chunkSize,
		"dmlBatchSize": dmlBatchSize,
		"chunkHash":    req.ChunkHash,
		"bestEffort":   req.BestEffort,
	}
	job := s.jobs.Submit(JobDiffData, params, func(ctx context.Context, job *Job) error {
		return s.runDiffData(ctx, job, source.ID, target.ID, tables, batchSize, chunkSize, dmlBatchSize, req.ChunkHash, req.BestEffort)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runDiffData(ctx context.Context, job *Job, sourceID, targetID string, tables []SchemeTable, batchSize, chunkSize, dmlBatchSize int, chunkHash, bestEffort bool) error {
	source, ok := s.store.GetConnection(sourceID)
	if !ok {
		return errBad("source 连接已被删除")
	}
	target, ok := s.store.GetConnection(targetID)
	if !ok {
		return errBad("target 连接已被删除")
	}
	// 数据比对只覆盖两侧都存在的表;单侧缺失的表属于结构差异,归结构比对处理。
	srcConn, err := pkgdb.NewDBAdapterContext(ctx, source.ConnConfig())
	if err != nil {
		return errBad("连接 source 失败: %s", err.Error())
	}
	defer srcConn.Close()
	tgtConn, err := pkgdb.NewDBAdapterContext(ctx, target.ConnConfig())
	if err != nil {
		return errBad("连接 target 失败: %s", err.Error())
	}
	defer tgtConn.Close()
	rules := make([]pkgconfig.Rule, 0, len(tables))
	for _, table := range tables {
		// ExtractTable 对不存在的表返回空表对象而非错误,以"无列"判定缺失。
		srcTbl, srcErr := srcConn.ExtractTable(table.Table)
		tgtTbl, tgtErr := tgtConn.ExtractTable(table.Table)
		switch {
		case srcErr != nil || srcTbl == nil || len(srcTbl.Columns) == 0:
			job.Logf("跳过表 %s: source 侧不存在(结构差异请使用结构比对)", table.Table)
			continue
		case tgtErr != nil || tgtTbl == nil || len(tgtTbl.Columns) == 0:
			job.Logf("跳过表 %s: target 侧不存在(结构差异请使用结构比对)", table.Table)
			continue
		}
		rules = append(rules, pkgconfig.Rule{
			Table:         table.Table,
			Columns:       table.Columns,
			IgnoreColumns: table.IgnoreColumns,
		})
	}
	if len(rules) == 0 {
		return errBad("所选数据表在两侧均不存在,无可比对内容")
	}
	job.SetProgressTotal(len(rules))
	job.Logf("开始数据比对: source=%s(%s) target=%s(%s), 共 %d 张表",
		source.Name, source.ID, target.Name, target.ID, len(rules))

	result, err := difflogic.RunDataDiff(ctx, difflogic.DataDiffParams{
		Source:       source.ConnConfig(),
		Target:       target.ConnConfig(),
		Rules:        rules,
		BatchSize:    batchSize,
		ChunkSize:    chunkSize,
		DMLBatchSize: dmlBatchSize,
		ChunkHash:    chunkHash,
		BestEffort:   bestEffort,
	}, job.Dir(), func(event difflogic.TableProgress) {
		switch event.Phase {
		case "start":
			job.BeginTable(event.Table)
			job.Logf("开始比对表 %s", event.Table)
		case "done":
			job.FinishTable(event.Table, event.Error)
		}
	})
	if err != nil {
		return err
	}
	if result.Tables == nil {
		result.Tables = []difflogic.TableDiffSummary{}
	}
	if err := s.writeSummaryJSON(job, result); err != nil {
		return err
	}
	files := append(difflogic.DataDiffFileNames(), summaryJSONFile)
	job.Summary(map[string]any{
		"complete": result.Complete,
		"tables":   result.Tables,
		"files":    files,
	})
	job.Logf("数据比对完成: complete=%v", result.Complete)
	return nil
}

func (s *Server) writeSummaryJSON(job *Job, result difflogic.DataDiffResult) error {
	raw, err := marshalIndent(result)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(job.Dir(), summaryJSONFile), raw, 0o644)
}
