package diff

import (
	"fmt"
	"log"
	"sort"

	"github.com/jacktea/data-smith/pkg/chunk"
	"github.com/jacktea/data-smith/pkg/conn"
)

type DetailedDiffHandler func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string)

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
	cols, pks, colTypes, err := getTableColumnsAndTypes(tgtDB, rule.GetTable())
	if err != nil {
		return err
	}
	srcIter := newRowBatchIterator(srcDB, rule.GetTable(), cols, pks, batchSize)
	tgtIter := newRowBatchIterator(tgtDB, rule.GetTable(), cols, pks, batchSize)
	defer srcIter.Close()
	defer tgtIter.Close()

	var srcBuf, tgtBuf []conn.Record
	var srcIdx, tgtIdx int
	var srcDone, tgtDone bool
	for {
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
			handle(DiffTypeDrop, srcRow, nil, nil)
			srcIdx++
		} else if cmp > 0 {
			// tgt 存在而 src 不存在：目标版本新增了此记录
			handle(DiffTypeAdd, nil, tgtRow, nil)
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
				handle(DiffTypeModify, srcRow, tgtRow, diffCols)
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
	srcCfg := srcDB.GetConfig()
	tgtCfg := tgtDB.GetConfig()

	if srcCfg != nil && tgtCfg != nil && srcCfg.Type == tgtCfg.Type {
		srcHasher, srcOk := srcDB.(chunk.VerifiedChunkHasher)
		tgtHasher, tgtOk := tgtDB.(chunk.VerifiedChunkHasher)
		if srcOk && tgtOk {
			cols, pks, colTypes, err := getTableColumnsAndTypes(tgtDB, rule.GetTable())
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
					ranges, err := tgtHasher.GetChunkRanges(rule.GetTable(), pk, chunkSize)
					if err != nil {
						return fmt.Errorf("get target chunk ranges: %w", err)
					}
					if len(ranges) == 0 {
						return fmt.Errorf("non-empty table %s returned no chunk ranges", rule.GetTable())
					}
					allMatched := true
					for _, r := range ranges {
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

	return StreamCompareDataDetailed(srcDB, tgtDB, rule, batchSize, handle)
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

func newRowBatchIterator(db conn.DBAdapter, table string, cols, pk []string, batchSize int) *rowBatchIterator {
	return &rowBatchIterator{
		db:    db,
		table: table,
		cols:  cols,
		pk:    pk,
		limit: batchSize,
	}
}

type rowBatchIterator struct {
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
	batch, err := it.db.GetTableDataBatch(it.table, it.cols, it.pk, it.lastPK, it.limit)
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

// getTableColumnsAndTypes 获取表的列、主键及字段类型映射
func getTableColumnsAndTypes(db conn.DBAdapter, table string) ([]string, []string, map[string]string, error) {
	if db == nil {
		return nil, nil, nil, fmt.Errorf("database adapter is required")
	}
	if table == "" {
		return nil, nil, nil, fmt.Errorf("table name is required")
	}
	tbl, err := db.ExtractTable(table)
	if err != nil {
		return nil, nil, nil, err
	}
	if tbl == nil {
		return nil, nil, nil, fmt.Errorf("table %s not found", table)
	}
	var cols []string
	colTypes := make(map[string]string)
	for _, col := range tbl.GetColumnsByPosition() {
		name := col.Name
		cols = append(cols, name)
		if col != nil {
			colTypes[name] = col.DataType
		}
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
		return nil, nil, nil, fmt.Errorf("primary key or not-null unique index required for table %s", table)
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

// getTableColumns 获取表的列和主键（保留向后兼容）
func getTableColumns(db conn.DBAdapter, table string) ([]string, []string, error) {
	cols, pks, _, err := getTableColumnsAndTypes(db, table)
	return cols, pks, err
}
