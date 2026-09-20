package diff

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

type tableModels struct {
	target        *conn.Table
	source        *conn.Table
	effectiveCols []string
}

const maxDMLBatchSize = 10000

const executeOnSourceHeader = "-- DATASMITH EXECUTE-ON: source"

func validateDMLBatchSize(size int) error {
	if size <= 0 {
		return fmt.Errorf("DML batch size must be greater than zero")
	}
	if size > maxDMLBatchSize {
		return fmt.Errorf("DML batch size must not exceed %d", maxDMLBatchSize)
	}
	return nil
}

type prepareTableModelsFunc func(pkgconfig.Rule) (*tableModels, error)

type streamTableCompareFunc func(pkgconfig.Rule, *tableModels, pkgdiff.DetailedDiffErrorHandler) error

// errTableSkipped 标记「按配置跳过」的表（如 --skip-missing-tables 下不存在的
// 表）：不是失败，不进入失败清单，也不生成 DML 分节。
var errTableSkipped = errors.New("table skipped")

type tableModelCache struct {
	db     conn.DBAdapter
	models map[string]*conn.Table
	errors map[string]error
}

func newTableModelCache(db conn.DBAdapter) *tableModelCache {
	return &tableModelCache{db: db, models: make(map[string]*conn.Table), errors: make(map[string]error)}
}

func (c *tableModelCache) get(table string) (*conn.Table, error) {
	if model, ok := c.models[table]; ok {
		return model, nil
	}
	if err, ok := c.errors[table]; ok {
		return nil, err
	}
	model, err := c.db.ExtractTable(table)
	if err != nil {
		c.errors[table] = err
		return nil, err
	}
	c.models[table] = model
	return model, nil
}

type rollbackSpool struct {
	table string
	path  string
}

type tableDiffStats struct {
	added    int
	modified int
	dropped  int
}

func generateStreamingDataDiffOutputs(
	forward io.Writer,
	rollback io.Writer,
	spoolParent string,
	rules []pkgconfig.Rule,
	dialect pkgsql.IDialect,
	dmlBatchSize int,
	bestEffort bool,
	prepare prepareTableModelsFunc,
	compare streamTableCompareFunc,
	onTableDone func(rule pkgconfig.Rule, stats tableDiffStats, tableErr error),
) (resultFailures []tableDiffFailure, resultErr error) {
	if err := validateDMLBatchSize(dmlBatchSize); err != nil {
		return nil, err
	}
	if prepare == nil || compare == nil {
		return nil, fmt.Errorf("streaming table preparation and comparison are required")
	}
	if _, err := fmt.Fprintln(forward, executeOnSourceHeader); err != nil {
		return nil, fmt.Errorf("write forward execution target: %w", err)
	}
	if _, err := fmt.Fprintln(rollback, executeOnSourceHeader); err != nil {
		return nil, fmt.Errorf("write rollback execution target: %w", err)
	}
	if spoolParent == "" {
		spoolParent = os.TempDir()
	}
	spoolDir, err := os.MkdirTemp(spoolParent, ".datasmith-diff-spool-*")
	if err != nil {
		return nil, fmt.Errorf("create rollback spool directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(spoolDir); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove diff spool directory: %w", err))
		}
	}()

	var failures []tableDiffFailure
	var spools []rollbackSpool
	for index, rule := range rules {
		models, err := prepare(rule)
		if err != nil {
			if errors.Is(err, errTableSkipped) {
				if onTableDone != nil {
					onTableDone(rule, tableDiffStats{}, nil)
				}
				continue
			}
			failures, err = recordTableFailure(failures, rule.Table, err, bestEffort)
			if err != nil {
				return failures, err
			}
			continue
		}

		rollbackFile, err := os.CreateTemp(spoolDir, fmt.Sprintf("%06d-rollback-*", index))
		if err != nil {
			return failures, fmt.Errorf("create rollback spool for table %s: %w", rule.Table, err)
		}
		rollbackPath := rollbackFile.Name()
		var tableForward io.Writer = forward
		var forwardFile *os.File
		if bestEffort {
			forwardFile, err = os.CreateTemp(spoolDir, fmt.Sprintf("%06d-forward-*", index))
			if err != nil {
				return failures, errors.Join(
					fmt.Errorf("create forward spool for table %s: %w", rule.Table, err),
					rollbackFile.Close(),
					os.Remove(rollbackPath),
				)
			}
			tableForward = forwardFile
		}

		stream := newTableDMLStream(tableForward, rollbackFile, dialect, models, dmlBatchSize)
		if _, err = fmt.Fprintf(tableForward, "--- diff %s \n", rule.Table); err == nil {
			_, err = fmt.Fprintf(rollbackFile, "--- rollback %s \n", rule.Table)
		}
		if err == nil {
			err = compare(rule, models, stream.handle)
		}
		if err == nil {
			err = stream.flush()
		}
		err = errors.Join(err, rollbackFile.Sync(), rollbackFile.Close())
		if forwardFile != nil {
			err = errors.Join(err, forwardFile.Sync(), forwardFile.Close())
		}
		if err != nil {
			_ = os.Remove(rollbackPath)
			if forwardFile != nil {
				_ = os.Remove(forwardFile.Name())
			}
			if onTableDone != nil {
				onTableDone(rule, tableDiffStats{}, err)
			}
			failures, err = recordTableFailure(failures, rule.Table, err, bestEffort)
			if err != nil {
				return failures, err
			}
			continue
		}
		if onTableDone != nil {
			onTableDone(rule, stream.stats(), nil)
		}

		if forwardFile != nil {
			if err := copySpool(forward, forwardFile.Name()); err != nil {
				return failures, fmt.Errorf("publish successful forward spool for table %s: %w", rule.Table, err)
			}
			if err := os.Remove(forwardFile.Name()); err != nil {
				return failures, fmt.Errorf("remove forward spool for table %s: %w", rule.Table, err)
			}
		}
		spools = append(spools, rollbackSpool{table: rule.Table, path: rollbackPath})
	}

	for i := len(spools) - 1; i >= 0; i-- {
		if err := copySpool(rollback, spools[i].path); err != nil {
			return failures, fmt.Errorf("merge rollback spool for table %s: %w", spools[i].table, err)
		}
		if err := os.Remove(spools[i].path); err != nil {
			return failures, fmt.Errorf("remove rollback spool for table %s: %w", spools[i].table, err)
		}
	}
	if err := writeCompletionReport(forward, bestEffort, failures); err != nil {
		return failures, fmt.Errorf("write forward completion report: %w", err)
	}
	if err := writeCompletionReport(rollback, bestEffort, failures); err != nil {
		return failures, fmt.Errorf("write rollback completion report: %w", err)
	}
	return failures, nil
}

func recordTableFailure(failures []tableDiffFailure, table string, cause error, bestEffort bool) ([]tableDiffFailure, error) {
	failures = append(failures, tableDiffFailure{table: table, err: cause})
	if !bestEffort {
		return failures, fmt.Errorf("diff table %s: %w", table, cause)
	}
	return failures, nil
}

func copySpool(dst io.Writer, path string) error {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, file)
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

type dmlKind uint8

const (
	dmlNone dmlKind = iota
	dmlInsert
	dmlDelete
)

type dmlBatchWriter struct {
	writer  io.Writer
	dialect pkgsql.IDialect
	table   *conn.Table
	limit   int
	kind    dmlKind
	rows    []conn.Record
}

func (w *dmlBatchWriter) add(kind dmlKind, row conn.Record) error {
	if w.kind != dmlNone && w.kind != kind {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.kind = kind
	w.rows = append(w.rows, row)
	if len(w.rows) >= w.limit {
		return w.flush()
	}
	return nil
}

func (w *dmlBatchWriter) addForTable(kind dmlKind, table *conn.Table, row conn.Record) error {
	if w.table != table && len(w.rows) > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.table = table
	return w.add(kind, row)
}

func (w *dmlBatchWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	if batchDialect, ok := w.dialect.(pkgsql.IDataBatchDialect); ok {
		var statement string
		if w.kind == dmlInsert {
			statement = batchDialect.GenerateInsertBatchSql(w.table, w.rows)
		} else {
			statement = batchDialect.GenerateDeleteBatchSql(w.table, w.rows)
		}
		if statement != "" {
			if _, err := fmt.Fprintln(w.writer, statement); err != nil {
				return err
			}
		}
	} else {
		for _, row := range w.rows {
			statement := w.dialect.GenerateInsertSql(w.table, row)
			if w.kind == dmlDelete {
				statement = w.dialect.GenerateDeleteSql(w.table, row)
			}
			if _, err := fmt.Fprintln(w.writer, statement); err != nil {
				return err
			}
		}
	}
	w.rows = w.rows[:0]
	w.kind = dmlNone
	return nil
}

type tableDMLStream struct {
	forward       dmlBatchWriter
	rollback      dmlBatchWriter
	target        *conn.Table
	source        *conn.Table
	effectiveCols []string
	added         int
	modified      int
	dropped       int
}

func (s *tableDMLStream) stats() tableDiffStats {
	return tableDiffStats{added: s.added, modified: s.modified, dropped: s.dropped}
}

func newTableDMLStream(forward, rollback io.Writer, dialect pkgsql.IDialect, models *tableModels, batchSize int) *tableDMLStream {
	source := models.source
	if source == nil {
		source = models.target
	}
	return &tableDMLStream{
		forward:       dmlBatchWriter{writer: forward, dialect: dialect, table: models.target, limit: batchSize, rows: make([]conn.Record, 0, batchSize)},
		rollback:      dmlBatchWriter{writer: rollback, dialect: dialect, table: source, limit: batchSize, rows: make([]conn.Record, 0, batchSize)},
		target:        models.target,
		source:        source,
		effectiveCols: models.effectiveCols,
	}
}

func (s *tableDMLStream) handle(kind pkgdiff.DiffType, srcRow, tgtRow conn.Record, diffCols []string) error {
	switch kind {
	case pkgdiff.DiffTypeAdd:
		s.added++
		if err := s.forward.add(dmlInsert, tgtRow); err != nil {
			return err
		}
		return s.rollback.addForTable(dmlDelete, s.target, tgtRow)
	case pkgdiff.DiffTypeDrop:
		s.dropped++
		if err := s.forward.add(dmlDelete, srcRow); err != nil {
			return err
		}
		return s.rollback.addForTable(dmlInsert, s.source, srcRow)
	case pkgdiff.DiffTypeModify:
		s.modified++
		if err := s.forward.flush(); err != nil {
			return err
		}
		if err := s.rollback.flush(); err != nil {
			return err
		}
		cols := diffCols
		if len(cols) == 0 {
			cols = s.effectiveCols
		}
		if statement := s.forward.dialect.GenerateUpdateSql(s.target, tgtRow, cols); statement != "" {
			if _, err := fmt.Fprintln(s.forward.writer, statement); err != nil {
				return err
			}
		}
		if statement := s.rollback.dialect.GenerateUpdateSql(s.target, srcRow, cols); statement != "" {
			if _, err := fmt.Fprintln(s.rollback.writer, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *tableDMLStream) flush() error {
	return errors.Join(s.forward.flush(), s.rollback.flush())
}
