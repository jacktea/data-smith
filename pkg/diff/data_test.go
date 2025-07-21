package diff

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

type mockDB struct {
	rows []conn.Record
	cols []string
	pk   []string
}

func (m *mockDB) ReadSchema() (*conn.DatabaseSchema, error) {
	tbl := &conn.Table{Columns: map[string]*conn.Column{}}
	for _, c := range m.cols {
		tbl.Columns[c] = &conn.Column{Name: c}
	}
	schema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"t": tbl}}
	return schema, nil
}

func (m *mockDB) Close() error { return nil }

func (m *mockDB) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	start := 0
	if lastPK != nil && len(lastPK) > 0 {
		for i, row := range m.rows {
			match := true
			for j, k := range pk {
				if row[k] != lastPK[j] {
					match = false
					break
				}
			}
			if match {
				start = i + 1
				break
			}
		}
	}
	end := start + limit
	if end > len(m.rows) {
		end = len(m.rows)
	}
	return m.rows[start:end], nil
}

func (m *mockDB) ExtractTable(tableName string) (*conn.Table, error) {
	return nil, nil
}

func (m *mockDB) ExtractView(viewName string) (*conn.Table, error) {
	return nil, nil
}

func (m *mockDB) GetConn() *sql.DB {
	return nil
}

func (m *mockDB) GetConfig() *config.ConnConfig {
	return nil
}

func TestStreamCompareData(t *testing.T) {
	cols := []string{"id", "val"}
	pk := []string{"id"}
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"val"})

	tests := []struct {
		name    string
		srcRows []conn.Record
		tgtRows []conn.Record
		expect  []string
	}{
		{
			"all add", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}}, nil, []string{"ADD:1", "ADD:2"},
		},
		{
			"all drop", nil, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}}, []string{"DROP:1", "DROP:2"},
		},
		{
			"mod and add", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}}, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "B"}}, []string{"MODIFY:2", "ADD:3"},
		},
		{
			"unbalanced batch", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 3, "val": "c"}, {"id": 4, "val": "D"}}, []string{"ADD:1", "ADD:2", "MODIFY:4"},
		},
		{
			"unbalanced batch with overlap", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}, {"id": 5, "val": "e"}}, []conn.Record{{"id": 2, "val": "b"}, {"id": 3, "val": "C"}, {"id": 5, "val": "e"}}, []string{"ADD:1", "MODIFY:3", "ADD:4"},
		},
		{
			"target more with overlap", []conn.Record{{"id": 2, "val": "b"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "D"}}, []string{"DROP:1", "DROP:3", "MODIFY:4"},
		},
		{
			"head tail overlap", []conn.Record{{"id": 1, "val": "A"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 1, "val": "A"}, {"id": 4, "val": "D"}}, []string{"ADD:2", "ADD:3", "MODIFY:4"},
		},
		{
			"interleaved", []conn.Record{{"id": 1, "val": "a"}, {"id": 3, "val": "c"}, {"id": 5, "val": "e"}}, []conn.Record{{"id": 2, "val": "b"}, {"id": 3, "val": "C"}, {"id": 4, "val": "d"}}, []string{"ADD:1", "DROP:2", "MODIFY:3", "DROP:4", "ADD:5"},
		},
	}

	for _, tt := range tests {
		src := &mockDB{rows: tt.srcRows, cols: cols, pk: pk}
		tgt := &mockDB{rows: tt.tgtRows, cols: cols, pk: pk}
		var got []string
		handle := func(diffType DiffType, srcRow, tgtRow conn.Record) {
			var id any
			if srcRow != nil {
				id = srcRow["id"]
			} else if tgtRow != nil {
				id = tgtRow["id"]
			}
			got = append(got, fmt.Sprintf("%s:%v", diffType, id))
		}
		err := StreamCompareData(src, tgt, rule, 2, handle)
		if err != nil {
			t.Fatalf("%s: err=%v", tt.name, err)
		}
		if !reflect.DeepEqual(got, tt.expect) {
			t.Errorf("%s: got %v, expect %v", tt.name, got, tt.expect)
		}
	}
}

func TestStreamCompareData_SinglePKMultiCompare(t *testing.T) {
	cols := []string{"id", "a", "b"}
	pk := []string{"id"}
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"a", "b"})

	tests := []struct {
		name    string
		srcRows []conn.Record
		tgtRows []conn.Record
		expect  []string
	}{
		{
			"single pk multi compare - mod", []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 2, "a": "x", "b": "z"}}, []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 2, "a": "x", "b": "y"}}, []string{"MODIFY:2"},
		},
		{
			"single pk multi compare - add drop", []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 3, "a": "a", "b": "b"}}, []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 2, "a": "x", "b": "z"}}, []string{"DROP:2", "ADD:3"},
		},
		{
			"single pk multi compare - all mod", []conn.Record{{"id": 1, "a": "A", "b": "B"}, {"id": 2, "a": "C", "b": "D"}}, []conn.Record{{"id": 1, "a": "a", "b": "b"}, {"id": 2, "a": "c", "b": "d"}}, []string{"MODIFY:1", "MODIFY:2"},
		},
		{
			"single pk multi compare - no change", []conn.Record{{"id": 1, "a": "x", "b": "y"}}, []conn.Record{{"id": 1, "a": "x", "b": "y"}}, []string{},
		},
		{
			"single pk multi compare - interleaved", []conn.Record{{"id": 1, "a": "a", "b": "b"}, {"id": 3, "a": "c", "b": "d"}}, []conn.Record{{"id": 2, "a": "x", "b": "y"}, {"id": 3, "a": "C", "b": "d"}}, []string{"ADD:1", "DROP:2", "MODIFY:3"},
		},
	}
	for _, tt := range tests {
		src := &mockDB{rows: tt.srcRows, cols: cols, pk: pk}
		tgt := &mockDB{rows: tt.tgtRows, cols: cols, pk: pk}
		var got []string
		handle := func(diffType DiffType, srcRow, tgtRow conn.Record) {
			var id any
			if srcRow != nil {
				id = srcRow["id"]
			} else if tgtRow != nil {
				id = tgtRow["id"]
			}
			got = append(got, fmt.Sprintf("%s:%v", diffType, id))
		}
		err := StreamCompareData(src, tgt, rule, 2, handle)
		if err != nil {
			t.Fatalf("%s: err=%v", tt.name, err)
		}
		gotStr := strings.Join(got, ",")
		expectStr := strings.Join(tt.expect, ",")
		if gotStr != expectStr {
			t.Errorf("%s: got [%v], expect [%v]", tt.name, gotStr, expectStr)
		}
	}
}

func TestStreamCompareData_MultiPKMultiCompare(t *testing.T) {
	cols := []string{"id", "sub", "a", "b"}
	pk := []string{"id", "sub"}
	rule := CreateCompareRule(&conn.Table{Name: "t"}, []string{"a", "b"})

	tests := []struct {
		name    string
		srcRows []conn.Record
		tgtRows []conn.Record
		expect  []string
	}{
		{
			"multi pk multi compare - mod", []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 1, "sub": 2, "a": "x", "b": "z"}}, []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 1, "sub": 2, "a": "x", "b": "y"}}, []string{"MODIFY:1:2"},
		},
		{
			"multi pk multi compare - add drop", []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 2, "sub": 1, "a": "a", "b": "b"}}, []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 1, "sub": 2, "a": "x", "b": "z"}}, []string{"DROP:1:2", "ADD:2:1"},
		},
		{
			"multi pk multi compare - all mod", []conn.Record{{"id": 1, "sub": 1, "a": "A", "b": "B"}, {"id": 2, "sub": 2, "a": "C", "b": "D"}}, []conn.Record{{"id": 1, "sub": 1, "a": "a", "b": "b"}, {"id": 2, "sub": 2, "a": "c", "b": "d"}}, []string{"MODIFY:1:1", "MODIFY:2:2"},
		},
		{
			"multi pk multi compare - no change", []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}}, []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}}, []string{},
		},
		{
			"multi pk multi compare - interleaved", []conn.Record{{"id": 1, "sub": 1, "a": "a", "b": "b"}, {"id": 2, "sub": 2, "a": "c", "b": "d"}}, []conn.Record{{"id": 1, "sub": 2, "a": "C", "b": "d"}, {"id": 2, "sub": 1, "a": "c", "b": "d"}}, []string{"ADD:1:1", "DROP:1:2", "DROP:2:1", "ADD:2:2"},
		},
	}
	for _, tt := range tests {
		src := &mockDB{rows: tt.srcRows, cols: cols, pk: pk}
		tgt := &mockDB{rows: tt.tgtRows, cols: cols, pk: pk}
		var got []string
		handle := func(diffType DiffType, srcRow, tgtRow conn.Record) {
			var id1, id2 any
			if srcRow != nil {
				id1 = srcRow["id"]
				id2 = srcRow["sub"]
			} else if tgtRow != nil {
				id1 = tgtRow["id"]
				id2 = tgtRow["sub"]
			}
			got = append(got, fmt.Sprintf("%s:%v:%v", diffType, id1, id2))
		}
		err := StreamCompareData(src, tgt, rule, 2, handle)
		if err != nil {
			t.Fatalf("%s: err=%v", tt.name, err)
		}
		gotStr := strings.Join(got, ",")
		expectStr := strings.Join(tt.expect, ",")
		if gotStr != expectStr {
			t.Errorf("%s: got [%v], expect [%v]", tt.name, gotStr, expectStr)
		}
	}
}
