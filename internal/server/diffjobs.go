package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

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

// diff-full ----------------------------------------------------------------

// diffFullRegisterRequest opts a diff-full job into one-step registration:
// after the run succeeds its four artifacts are merged into a
// V{version}__{title} up/down pair inside the library. An empty version gets
// the next free library version.
type diffFullRegisterRequest struct {
	LibraryID            string `json:"libraryId"`
	Version              string `json:"version"`
	Title                string `json:"title"`
	ExpectedConnectionID string `json:"expectedConnectionId"`
}

type diffFullRequest struct {
	SourceID      string                   `json:"sourceId"`
	TargetID      string                   `json:"targetId"`
	IncludeTables []string                 `json:"includeTables"`
	ExcludeTables []string                 `json:"excludeTables"`
	SchemeID      string                   `json:"schemeId"`
	Tables        []SchemeTable            `json:"tables"`
	BatchSize     int                      `json:"batchSize"`
	ChunkSize     int                      `json:"chunkSize"`
	DMLBatchSize  int                      `json:"dmlBatchSize"`
	ChunkHash     bool                     `json:"chunkHash"`
	BestEffort    bool                     `json:"bestEffort"`
	DataDiffMode  string                   `json:"dataDiffMode"`
	Register      *diffFullRegisterRequest `json:"register"`
}

func (s *Server) handleDiffFullSubmit(w http.ResponseWriter, r *http.Request) {
	var req diffFullRequest
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
	if req.Register != nil {
		if err := s.validateRegisterRequest(*req.Register); err != nil {
			writeError(w, http.StatusBadRequest, "%s", err.Error())
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
	// 数据比对模式经引擎既有校验与语义执行（auto/shadow/direct），Web 不重写
	// 任何影子机制逻辑；空串按引擎默认归一为 auto，任务记录与日志可见。
	if !difflogic.IsValidDataDiffMode(req.DataDiffMode) {
		writeError(w, http.StatusBadRequest, "dataDiffMode 取值必须为 auto、shadow 或 direct")
		return
	}
	dataDiffMode := difflogic.NormalizeDataDiffMode(req.DataDiffMode)

	params := map[string]any{
		"sourceId":              source.ID,
		"targetId":              target.ID,
		"includeTables":         req.IncludeTables,
		"excludeTables":         req.ExcludeTables,
		"schemeId":              req.SchemeID,
		"tables":                tables,
		"batchSize":             batchSize,
		"chunkSize":             chunkSize,
		"dmlBatchSize":          dmlBatchSize,
		"chunkHash":             req.ChunkHash,
		"bestEffort":            req.BestEffort,
		"requestedDataDiffMode": dataDiffMode,
	}
	if req.Register != nil {
		params["register"] = map[string]any{
			"libraryId":            req.Register.LibraryID,
			"version":              req.Register.Version,
			"title":                req.Register.Title,
			"expectedConnectionId": req.Register.ExpectedConnectionID,
		}
	}
	job := s.jobs.Submit(JobDiffFull, params, func(ctx context.Context, job *Job) error {
		return s.runDiffFull(ctx, job, source.ID, target.ID, req.IncludeTables, req.ExcludeTables,
			tables, batchSize, chunkSize, dmlBatchSize, req.ChunkHash, req.BestEffort, dataDiffMode, req.Register)
	})
	writeJSON(w, http.StatusOK, map[string]any{"job": job.View()})
}

func (s *Server) runDiffFull(ctx context.Context, job *Job, sourceID, targetID string, includeTables, excludeTables []string,
	tables []SchemeTable, batchSize, chunkSize, dmlBatchSize int, chunkHash, bestEffort bool, dataDiffMode string, register *diffFullRegisterRequest) error {
	source, ok := s.store.GetConnection(sourceID)
	if !ok {
		return errBad("source 连接已被删除")
	}
	target, ok := s.store.GetConnection(targetID)
	if !ok {
		return errBad("target 连接已被删除")
	}
	rules := make([]pkgconfig.Rule, 0, len(tables))
	for _, table := range tables {
		rules = append(rules, pkgconfig.Rule{
			Table:         table.Table,
			Columns:       table.Columns,
			IgnoreColumns: table.IgnoreColumns,
		})
	}
	// Web 侧保持显式选择语义：整库通配展开（空规则 = 全表）仅 CLI/引擎层开放。
	if len(rules) == 0 {
		return errBad("请选择数据表或比对方案")
	}
	job.SetProgressTotal(len(rules))
	job.Logf("开始完全比对: source=%s(%s) target=%s(%s), 结构范围 %d 张表, 数据 %d 张表, requested data diff mode=%s",
		source.Name, source.ID, target.Name, target.ID, len(includeTables)+len(excludeTables), len(rules), dataDiffMode)

	result, err := s.runFullDiffEngine(ctx, difflogic.FullDiffParams{
		Source:        source.ConnConfig(),
		Target:        target.ConnConfig(),
		IncludeTables: includeTables,
		ExcludeTables: excludeTables,
		Rules:         rules,
		DataDiffMode:  dataDiffMode,
		BatchSize:     batchSize,
		ChunkSize:     chunkSize,
		DMLBatchSize:  dmlBatchSize,
		ChunkHash:     chunkHash,
		BestEffort:    bestEffort,
	}, job.Dir(), func(msg string) { job.Logf("%s", msg) }, func(event difflogic.TableProgress) {
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
	if err := s.writeFullSummaryJSON(job, result); err != nil {
		return err
	}
	files := append(difflogic.FullDiffFileNames(), summaryJSONFile)
	summary := map[string]any{
		"schema":        result.Schema,
		"complete":      result.Data.Complete,
		"tables":        result.Data.Tables,
		"skippedTables": result.SkippedTables,
		"files":         files,
		"dataDiffMode":  result.DataDiffMode,
	}
	job.Summary(summary)
	job.Logf("完全比对完成: complete=%v", result.Data.Complete)

	if register != nil {
		version, upFile, downFile, err := s.registerJobVersion(job, register)
		if err != nil {
			return err
		}
		job.Summary(map[string]any{
			"schema":            result.Schema,
			"complete":          result.Data.Complete,
			"tables":            result.Data.Tables,
			"skippedTables":     result.SkippedTables,
			"files":             files,
			"registeredVersion": version,
			"upFile":            upFile,
			"downFile":          downFile,
			"dataDiffMode":      result.DataDiffMode,
		})
		job.Logf("已登记为迁移版本 %s (%s, %s)", version, upFile, downFile)
	}
	return nil
}

func (s *Server) writeFullSummaryJSON(job *Job, result difflogic.FullDiffResult) error {
	raw, err := marshalIndent(result)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(job.Dir(), summaryJSONFile), raw, 0o644)
}

// validateRegisterRequest mirrors the register-version endpoint's input rules
// so invalid one-step registrations fail at submit time.
func (s *Server) validateRegisterRequest(reg diffFullRegisterRequest) error {
	if _, ok := s.store.GetLibrary(reg.LibraryID); !ok {
		return errBad("脚本库不存在")
	}
	if reg.Title == "" || strings.ContainsAny(reg.Title, ".") {
		return errBad("版本标题不能为空且不能包含点号")
	}
	if _, err := normalizeRegistrationVersion(reg.Version); reg.Version != "" && err != nil {
		return err
	}
	return nil
}

// registerJobVersion imports the finished diff job's four artifacts into the
// library as one V{version}__{title} up/down pair.
func (s *Server) registerJobVersion(job *Job, reg *diffFullRegisterRequest) (version, upFile, downFile string, err error) {
	if s.beforeLibraryMutation != nil {
		s.beforeLibraryMutation(reg.LibraryID, "register-job-version")
	}
	unlock := s.lockLibrary(reg.LibraryID)
	defer unlock()
	dir, err := s.libraryDir(reg.LibraryID)
	if err != nil {
		return "", "", "", err
	}
	if reg.Version == "" {
		version = nextLibraryVersion(dir)
	} else {
		if version, err = normalizeRegistrationVersion(reg.Version); err != nil {
			return "", "", "", err
		}
	}
	if err := s.checkVersionUnique(dir, version); err != nil {
		return "", "", "", err
	}
	readArtifact := func(name string) (string, error) {
		raw, err := os.ReadFile(filepath.Join(job.Dir(), name))
		if err != nil {
			if os.IsNotExist(err) {
				return "", errBad("任务缺少产物 %s, 无法登记", name)
			}
			return "", fmt.Errorf("读取任务产物 %s: %w", name, err)
		}
		return string(raw), nil
	}
	schemaForward, err := readArtifact(difflogic.SchemaDiffForwardFile)
	if err != nil {
		return "", "", "", err
	}
	schemaRollback, err := readArtifact(difflogic.SchemaDiffRollbackFile)
	if err != nil {
		return "", "", "", err
	}
	dataForward, err := readArtifact(difflogic.DataDiffForwardFile)
	if err != nil {
		return "", "", "", err
	}
	dataRollback, err := readArtifact(difflogic.DataDiffRollbackFile)
	if err != nil {
		return "", "", "", err
	}
	upContent, downContent := difflogic.AssembleVersionScripts(true, true, schemaForward, schemaRollback, dataForward, dataRollback)
	upFile = fmt.Sprintf("V%s__%s.up.sql", version, reg.Title)
	downFile = fmt.Sprintf("V%s__%s.down.sql", version, reg.Title)
	if err := os.WriteFile(filepath.Join(dir, upFile), []byte(upContent), 0o644); err != nil {
		return "", "", "", fmt.Errorf("写入 up 脚本失败: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, downFile), []byte(downContent), 0o644); err != nil {
		return "", "", "", fmt.Errorf("写入 down 脚本失败: %w", err)
	}
	if err := s.store.SetVersionMeta(reg.LibraryID, version, VersionMeta{ExpectedConnectionID: reg.ExpectedConnectionID}); err != nil {
		return "", "", "", err
	}
	return version, upFile, downFile, nil
}
