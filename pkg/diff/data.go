package diff

import (
	"fmt"

	"github.com/jacktea/data-smith/pkg/conn"
)

func StreamCompareData(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int, handle func(diffType DiffType, srcRow, tgtRow conn.Record)) error {
	cols, pks, err := getTableColumns(tgtDB, rule.GetTable())
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
			cmp = comparePKRecord(srcRow, tgtRow, pks)
		}
		if cmp < 0 {
			handle(DiffTypeAdd, srcRow, nil)
			srcIdx++
		} else if cmp > 0 {
			handle(DiffTypeDrop, nil, tgtRow)
			tgtIdx++
		} else {
			if !rule.IsEqual(srcRow, tgtRow) {
				handle(DiffTypeModify, srcRow, tgtRow)
			}
			srcIdx++
			tgtIdx++
		}
	}
	return nil
}

func StreamCompareDataToDiff(srcDB, tgtDB conn.DBAdapter, rule ICompareRule, batchSize int) (*DataDiff, error) {
	diff := &DataDiff{}
	err := StreamCompareData(srcDB, tgtDB, rule, batchSize, func(diffType DiffType, srcRow, tgtRow conn.Record) {
		switch diffType {
		case DiffTypeAdd:
			diff.Added = append(diff.Added, srcRow)
		case DiffTypeDrop:
			diff.Dropped = append(diff.Dropped, tgtRow)
		case DiffTypeModify:
			diff.Modified = append(diff.Modified, ModifiedRow{Old: tgtRow, New: srcRow})
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

// comparePKRecord 比较主键值
func comparePKRecord(a, b conn.Record, pk []string) int {
	for _, k := range pk {
		av := fmt.Sprintf("%v", a[k])
		bv := fmt.Sprintf("%v", b[k])
		if av < bv {
			return -1
		} else if av > bv {
			return 1
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

// getTableColumns 获取表的列和主键
func getTableColumns(db conn.DBAdapter, table string) ([]string, []string, error) {
	tbl, err := db.ExtractTable(table)
	if err != nil {
		return nil, nil, err
	}
	var cols []string
	for name := range tbl.Columns {
		cols = append(cols, name)
	}
	var pks []string
	if tbl.PrimaryKey != nil {
		pks = tbl.PrimaryKey.Columns
	}
	return cols, pks, nil
}
