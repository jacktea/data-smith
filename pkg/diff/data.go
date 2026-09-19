package diff

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/jacktea/data-smith/pkg/chunk"
	"github.com/jacktea/data-smith/pkg/conn"
)

type DetailedDiffHandler func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string)

// DetailedDiffErrorHandler is the error-aware streaming callback used by
// callers that write each difference as it is discovered. Returning an error
// stops both database iterators immediately.
type DetailedDiffErrorHandler func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) error

func StreamCompareData(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int, handle func(diffType DiffType, srcRow, tgtRow conn.Record)) error {
	if handle == nil {
		return fmt.Errorf("diff handler is required")
	}
	return StreamCompareDataDetailed(srcDB, tgtDB, rule, batchSize, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) {
		handle(diffType, srcRow, tgtRow)
	})
}

func StreamCompareDataDetailed(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int, handle DetailedDiffHandler) error {
	if err := validateCompareInputs(srcDB, tgtDB, rule, batchSize, handle); err != nil {
		return err
	}
	tbl, err := getTableModel(tgtDB, rule.GetTable())
	if err != nil {
		return err
	}
	return StreamCompareDataDetailedWithTable(srcDB, tgtDB, rule, tbl, batchSize, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) error {
		handle(diffType, srcRow, tgtRow, diffCols)
		return nil
	})
}

// StreamCompareDataDetailedWithTable streams differences while reusing a
// caller-owned table model. It avoids repeated metadata extraction within one
// run and propagates callback failures without reading another batch.
func StreamCompareDataDetailedWithTable(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, tbl *conn.Table, batchSize int, handle DetailedDiffErrorHandler) error {
	return StreamCompareDataDetailedWithTableContext(context.Background(), srcDB, tgtDB, rule, tbl, batchSize, handle)
}

// StreamCompareDataDetailedWithTableContext is the cancellable counterpart of
// StreamCompareDataDetailedWithTable. The legacy API remains unchanged.
func StreamCompareDataDetailedWithTableContext(ctx context.Context, srcDB, tgtDB conn.DBAdapter, rule ICompareRule, tbl *conn.Table, batchSize int, handle DetailedDiffErrorHandler) error {
	if ctx == nil {
		return fmt.Errorf("comparison context is required")
	}
	if err := validateCompareInputsWithErrorHandler(srcDB, tgtDB, rule, batchSize, handle); err != nil {
		return err
	}
	cols, pks, colTypes, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		return err
	}
	srcIter := newRowBatchIterator(ctx, srcDB, rule.GetTable(), cols, pks, batchSize)
	tgtIter := newRowBatchIterator(ctx, tgtDB, rule.GetTable(), cols, pks, batchSize)
	defer srcIter.Close()
	defer tgtIter.Close()

	var srcBuf, tgtBuf []conn.Record
	var srcIdx, tgtIdx int
	var srcDone, tgtDone bool
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if srcIdx >= len(srcBuf) && !srcDone {
			batch, err := srcIter.NextBatch()
			if err != nil {
				return err
			}
			if batch == nil {
				srcDone = true
			} else {
				srcBuf = batch
				srcIdx = 0
			}
		}
		if tgtIdx >= len(tgtBuf) && !tgtDone {
			batch, err := tgtIter.NextBatch()
			if err != nil {
				return err
			}
			if batch == nil {
				tgtDone = true
			} else {
				tgtBuf = batch
				tgtIdx = 0
			}
		}
		if (srcDone || srcIdx >= len(srcBuf)) && (tgtDone || tgtIdx >= len(tgtBuf)) {
			break
		}
		var srcRow, tgtRow conn.Record
		if srcIdx < len(srcBuf) {
			srcRow = srcBuf[srcIdx]
		}
		if tgtIdx < len(tgtBuf) {
			tgtRow = tgtBuf[tgtIdx]
		}
		var cmp int
		if srcRow == nil {
			cmp = 1
		} else if tgtRow == nil {
			cmp = -1
		} else {
			cmp = comparePKRecord(srcRow, tgtRow, pks, colTypes)
		}
		if cmp < 0 {
			// src 存在而 tgt 不存在：目标版本删除了此记录
			if err := handle(DiffTypeDrop, srcRow, nil, nil); err != nil {
				return err
			}
			srcIdx++
		} else if cmp > 0 {
			// tgt 存在而 src 不存在：目标版本新增了此记录
			if err := handle(DiffTypeAdd, nil, tgtRow, nil); err != nil {
				return err
			}
			tgtIdx++
		} else {
			var isDiff bool
			var diffCols []string
			if detailedRule, ok := rule.(IDetailedCompareRule); ok {
				var equal bool
				equal, diffCols = detailedRule.DiffColumns(srcRow, tgtRow)
				isDiff = !equal
			} else {
				isDiff = !rule.IsEqual(srcRow, tgtRow)
			}
			if isDiff {
				if err := handle(DiffTypeModify, srcRow, tgtRow, diffCols); err != nil {
					return err
				}
			}
			srcIdx++
			tgtIdx++
		}
	}
	return nil
}

func StreamCompareDataToDiff(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int) (*DataDiff, error) {
	diff := &DataDiff{}
	err := StreamCompareDataDetailed(srcDB, tgtDB, rule, batchSize, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) {
		switch diffType {
		case DiffTypeAdd:
			diff.Added = append(diff.Added, tgtRow)
		case DiffTypeDrop:
			diff.Dropped = append(diff.Dropped, srcRow)
		case DiffTypeModify:
			diff.Modified = append(diff.Modified, ModifiedRow{
				Old:          srcRow,
				New:          tgtRow,
				ModifiedCols: diffCols,
			})
		}
	})
	if err != nil {
		return nil, err
	}
	return diff, nil
}

// StreamCompareDataWithChunkFilter 带有分块哈希预过滤的流式数据比对
func StreamCompareDataWithChunkFilter(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize, chunkSize int, handle DetailedDiffHandler) error {
	if err := validateCompareInputs(srcDB, tgtDB, rule, batchSize, handle); err != nil {
		return err
	}
	if chunkSize <= 0 {
		return fmt.Errorf("chunk size must be greater than zero")
	}
	tbl, err := getTableModel(tgtDB, rule.GetTable())
	if err != nil {
		return err
	}
	return StreamCompareDataWithChunkFilterAndTable(srcDB, tgtDB, rule, tbl, batchSize, chunkSize, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) error {
		handle(diffType, srcRow, tgtRow, diffCols)
		return nil
	})
}

// StreamCompareDataWithChunkFilterAndTable is the metadata-reusing,
// error-aware variant of StreamCompareDataWithChunkFilter.
func StreamCompareDataWithChunkFilterAndTable(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, tbl *conn.Table, batchSize, chunkSize int, handle DetailedDiffErrorHandler) error {
	return StreamCompareDataWithChunkFilterAndTableContext(context.Background(), srcDB, tgtDB, rule, tbl, batchSize, chunkSize, handle)
}

// StreamCompareDataWithChunkFilterAndTableContext adds cancellation to the
// optimized comparison path while preserving the legacy wrapper.
func StreamCompareDataWithChunkFilterAndTableContext(ctx context.Context, srcDB, tgtDB conn.DBAdapter, rule ICompareRule, tbl *conn.Table, batchSize, chunkSize int, handle DetailedDiffErrorHandler) error {
	if ctx == nil {
		return fmt.Errorf("comparison context is required")
	}
	if err := validateCompareInputsWithErrorHandler(srcDB, tgtDB, rule, batchSize, handle); err != nil {
		return err
	}
	if chunkSize <= 0 {
		return fmt.Errorf("chunk size must be greater than zero")
	}
	srcCfg := srcDB.GetConfig()
	tgtCfg := tgtDB.GetConfig()

	if srcCfg != nil && tgtCfg != nil && srcCfg.Type == tgtCfg.Type {
		if err := ctx.Err(); err != nil {
			return err
		}
		srcHasher, srcOk := srcDB.(chunk.VerifiedChunkHasher)
		tgtHasher, tgtOk := tgtDB.(chunk.VerifiedChunkHasher)
		if srcOk && tgtOk {
			cols, pks, colTypes, err := tableColumnsAndTypes(tbl, rule)
			if err != nil {
				return err
			}
			if len(pks) == 1 {
				pk := pks[0]
				srcStats, err := srcHasher.GetChunkStats(rule.GetTable(), pk)
				if err != nil {
					return fmt.Errorf("get source chunk stats: %w", err)
				}
				tgtStats, err := tgtHasher.GetChunkStats(rule.GetTable(), pk)
				if err != nil {
					return fmt.Errorf("get target chunk stats: %w", err)
				}
				statsMatch := srcStats.Count == tgtStats.Count &&
					CompareValues(srcStats.MinPK, tgtStats.MinPK, colTypes[pk]) == 0 &&
					CompareValues(srcStats.MaxPK, tgtStats.MaxPK, colTypes[pk]) == 0
				if statsMatch && tgtStats.Count == 0 {
					log.Printf("Table %s: matching empty-table statistics, skipped row-level scan", rule.GetTable())
					return nil
				}
				if statsMatch {
					var ranges []chunk.ChunkRange
					if ranger, ok := tgtHasher.(chunk.StatsAwareChunkRanger); ok {
						ranges, err = ranger.GetChunkRangesWithStats(rule.GetTable(), pk, chunkSize, tgtStats)
					} else {
						ranges, err = tgtHasher.GetChunkRanges(rule.GetTable(), pk, chunkSize)
					}
					if err != nil {
						return fmt.Errorf("get target chunk ranges: %w", err)
					}
					if len(ranges) == 0 {
						return fmt.Errorf("non-empty table %s returned no chunk ranges", rule.GetTable())
					}
					allMatched := true
					for _, r := range ranges {
						if err := ctx.Err(); err != nil {
							return err
						}
						srcHash, err := srcHasher.GetChunkHash(rule.GetTable(), cols, pk, r.MinPK, r.MaxPK, r.IsLast)
						if err != nil {
							return fmt.Errorf("hash source chunk %d: %w", r.ChunkIndex, err)
						}
						tgtHash, err := tgtHasher.GetChunkHash(rule.GetTable(), cols, pk, r.MinPK, r.MaxPK, r.IsLast)
						if err != nil {
							return fmt.Errorf("hash target chunk %d: %w", r.ChunkIndex, err)
						}
						if srcHash != tgtHash {
							allMatched = false
							break
						}
					}
					if allMatched {
						log.Printf("Table %s: count/min/max and all %d probabilistic chunk fingerprints matched; skipped row-level scan", rule.GetTable(), len(ranges))
						return nil
					}
				}
			}
		}
	}

	return StreamCompareDataDetailedWithTableContext(ctx, srcDB, tgtDB, rule, tbl, batchSize, handle)
}

func StreamCompareDataToDiffWithChunkFilter(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize, chunkSize int) (*DataDiff, error) {
	diff := &DataDiff{}
	err := StreamCompareDataWithChunkFilter(srcDB, tgtDB, rule, batchSize, chunkSize, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) {
		switch diffType {
		case DiffTypeAdd:
			diff.Added = append(diff.Added, tgtRow)
		case DiffTypeDrop:
			diff.Dropped = append(diff.Dropped, srcRow)
		case DiffTypeModify:
			diff.Modified = append(diff.Modified, ModifiedRow{
				Old:          srcRow,
				New:          tgtRow,
				ModifiedCols: diffCols,
			})
		}
	})
	if err != nil {
		return nil, err
	}
	return diff, nil
}

func newRowBatchIterator(ctx context.Context, db conn.DBAdapter, table string, cols, pk []string, batchSize int) *rowBatchIterator {
	return &rowBatchIterator{
		ctx:   ctx,
		db:    db,
		table: table,
		cols:  cols,
		pk:    pk,
		limit: batchSize,
	}
}

type rowBatchIterator struct {
	ctx    context.Context
	db     conn.DBAdapter
	table  string
	cols   []string
	pk     []string
	limit  int
	lastPK []any
	buf    []conn.Record
	idx    int
	closed bool
}

func (it *rowBatchIterator) NextBatch() ([]conn.Record, error) {
	if it.closed {
		return nil, nil
	}
	if it.buf != nil && it.idx < len(it.buf) {
		batch := it.buf[it.idx:]
		it.idx = len(it.buf)
		return batch, nil
	}
	batch, err := conn.GetTableDataBatchContext(it.ctx, it.db, it.table, it.cols, it.pk, it.lastPK, it.limit)
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		it.closed = true
		return nil, nil
	}
	it.buf = batch
	it.idx = len(batch)
	if len(batch) > 0 {
		it.lastPK = extractPK(batch[len(batch)-1], it.pk)
	}
	return batch, nil
}

func (it *rowBatchIterator) Close() error {
	it.closed = true
	return nil
}

// comparePKRecord 比较主键值，使用类型感知的强类型比较器
func comparePKRecord(a, b conn.Record, pk []string, colTypes map[string]string) int {
	for _, k := range pk {
		var colType string
		if colTypes != nil {
			colType = colTypes[k]
		}
		cmp := CompareValues(a[k], b[k], colType)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// extractPK 生成主键值
func extractPK(row conn.Record, pk []string) []any {
	var res []any
	for _, k := range pk {
		res = append(res, row[k])
	}
	return res
}

func getTableModel(db conn.DBAdapter, table string) (*conn.Table, error) {
	tbl, err := db.ExtractTable(table)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, fmt.Errorf("table %s not found", table)
	}
	return tbl, nil
}

func tableColumnsAndTypes(tbl *conn.Table, rule ICompareRule) ([]string, []string, map[string]string, error) {
	if tbl == nil {
		return nil, nil, nil, fmt.Errorf("table %s not found", rule.GetTable())
	}
	// 行读取必须跟随比对列集(忽略列/对比列),同时保留主键用于行匹配;
	// 否则忽略单侧不存在的列会直接报错。
	allowed := make(map[string]bool)
	if cr, ok := rule.(interface{ GetCompareColumns() []string }); ok {
		for _, name := range cr.GetCompareColumns() {
			allowed[name] = true
		}
	}
	var cols []string
	colTypes := make(map[string]string)
	for _, col := range tbl.GetColumnsByPosition() {
		if col == nil {
			continue
		}
		name := col.Name
		if len(allowed) > 0 && !allowed[name] {
			continue
		}
		cols = append(cols, name)
		colTypes[name] = col.DataType
	}
	var pks []string
	if tbl.PrimaryKey != nil && len(tbl.PrimaryKey.Columns) > 0 {
		pks = tbl.PrimaryKey.Columns
	} else if tbl.Indexes != nil {
		// 回退查找非空唯一索引
		indexNames := make([]string, 0, len(tbl.Indexes))
		for name := range tbl.Indexes {
			indexNames = append(indexNames, name)
		}
		sort.Strings(indexNames)
		for _, name := range indexNames {
			idx := tbl.Indexes[name]
			if idx.Unique && len(idx.Columns) > 0 {
				allNotNull := true
				for _, colName := range idx.Columns {
					if c := tbl.Columns[colName]; c != nil && c.Nullable {
						allNotNull = false
						break
					}
				}
				if allNotNull {
					pks = idx.Columns
					break
				}
			}
		}
	}
	if len(pks) == 0 {
		return nil, nil, nil, fmt.Errorf("primary key or not-null unique index required for table %s", rule.GetTable())
	}
	// 主键必须参与行读取(可能不在比对列集中)
	for _, pk := range pks {
		found := false
		for _, name := range cols {
			if name == pk {
				found = true
				break
			}
		}
		if !found {
			if col := tbl.Columns[pk]; col != nil {
				cols = append(cols, pk)
				colTypes[pk] = col.DataType
			}
		}
	}
	return cols, pks, colTypes, nil
}

func validateCompareInputs(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int, handle DetailedDiffHandler) error {
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if srcDB == nil || tgtDB == nil {
		return fmt.Errorf("source and target database adapters are required")
	}
	if rule == nil {
		return fmt.Errorf("comparison rule is required")
	}
	if rule.GetTable() == "" {
		return fmt.Errorf("comparison table is required")
	}
	if handle == nil {
		return fmt.Errorf("diff handler is required")
	}
	return nil
}

func validateCompareInputsWithErrorHandler(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int, handle DetailedDiffErrorHandler) error {
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if srcDB == nil || tgtDB == nil {
		return fmt.Errorf("source and target database adapters are required")
	}
	if rule == nil {
		return fmt.Errorf("comparison rule is required")
	}
	if rule.GetTable() == "" {
		return fmt.Errorf("comparison table is required")
	}
	if handle == nil {
		return fmt.Errorf("diff handler is required")
	}
	return nil
}
