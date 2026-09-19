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
	rows     []conn.Record
	cols     []string
	pk       []string
	colTypes map[string]string
}

func (m *mockDB) ReadSchema() (*conn.DatabaseSchema, error) {
	tbl := &conn.Table{Columns: map[string]*conn.Column{}}
	for _, c := range m.cols {
		tbl.Columns[c] = &conn.Column{Name: c, DataType: m.colTypes[c]}
	}
	schema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"t": tbl}}
	return schema, nil
}

func (m *mockDB) Close() error { return nil }

func (m *mockDB) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	start := 0
	if len(lastPK) > 0 {
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
	tbl := &conn.Table{
		Name:    tableName,
		Columns: make(map[string]*conn.Column),
		PrimaryKey: &conn.PrimaryKey{
			Name:    "pk",
			Columns: m.pk,
		},
	}
	for _, c := range m.cols {
		tbl.Columns[c] = &conn.Column{Name: c, DataType: m.colTypes[c]}
	}
	return tbl, nil
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
			"all drop", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}}, nil, []string{"DROP:1", "DROP:2"},
		},
		{
			"all add", nil, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}}, []string{"ADD:1", "ADD:2"},
		},
		{
			"mod and drop", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}}, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "B"}}, []string{"MODIFY:2", "DROP:3"},
		},
		{
			"unbalanced batch", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 3, "val": "c"}, {"id": 4, "val": "D"}}, []string{"DROP:1", "DROP:2", "MODIFY:4"},
		},
		{
			"unbalanced batch with overlap", []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}, {"id": 5, "val": "e"}}, []conn.Record{{"id": 2, "val": "b"}, {"id": 3, "val": "C"}, {"id": 5, "val": "e"}}, []string{"DROP:1", "MODIFY:3", "DROP:4"},
		},
		{
			"target more with overlap", []conn.Record{{"id": 2, "val": "b"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "D"}}, []string{"ADD:1", "ADD:3", "MODIFY:4"},
		},
		{
			"head tail overlap", []conn.Record{{"id": 1, "val": "A"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}, {"id": 4, "val": "d"}}, []conn.Record{{"id": 1, "val": "A"}, {"id": 4, "val": "D"}}, []string{"DROP:2", "DROP:3", "MODIFY:4"},
		},
		{
			"interleaved", []conn.Record{{"id": 1, "val": "a"}, {"id": 3, "val": "c"}, {"id": 5, "val": "e"}}, []conn.Record{{"id": 2, "val": "b"}, {"id": 3, "val": "C"}, {"id": 4, "val": "d"}}, []string{"DROP:1", "ADD:2", "MODIFY:3", "ADD:4", "DROP:5"},
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
			"single pk multi compare - add drop", []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 3, "a": "a", "b": "b"}}, []conn.Record{{"id": 1, "a": "x", "b": "y"}, {"id": 2, "a": "x", "b": "z"}}, []string{"ADD:2", "DROP:3"},
		},
		{
			"single pk multi compare - all mod", []conn.Record{{"id": 1, "a": "A", "b": "B"}, {"id": 2, "a": "C", "b": "D"}}, []conn.Record{{"id": 1, "a": "a", "b": "b"}, {"id": 2, "a": "c", "b": "d"}}, []string{"MODIFY:1", "MODIFY:2"},
		},
		{
			"single pk multi compare - no change", []conn.Record{{"id": 1, "a": "x", "b": "y"}}, []conn.Record{{"id": 1, "a": "x", "b": "y"}}, []string{},
		},
		{
			"single pk multi compare - interleaved", []conn.Record{{"id": 1, "a": "a", "b": "b"}, {"id": 3, "a": "c", "b": "d"}}, []conn.Record{{"id": 2, "a": "x", "b": "y"}, {"id": 3, "a": "C", "b": "d"}}, []string{"DROP:1", "ADD:2", "MODIFY:3"},
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
			"multi pk multi compare - add drop", []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 2, "sub": 1, "a": "a", "b": "b"}}, []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}, {"id": 1, "sub": 2, "a": "x", "b": "z"}}, []string{"ADD:1:2", "DROP:2:1"},
		},
		{
			"multi pk multi compare - all mod", []conn.Record{{"id": 1, "sub": 1, "a": "A", "b": "B"}, {"id": 2, "sub": 2, "a": "C", "b": "D"}}, []conn.Record{{"id": 1, "sub": 1, "a": "a", "b": "b"}, {"id": 2, "sub": 2, "a": "c", "b": "d"}}, []string{"MODIFY:1:1", "MODIFY:2:2"},
		},
		{
			"multi pk multi compare - no change", []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}}, []conn.Record{{"id": 1, "sub": 1, "a": "x", "b": "y"}}, []string{},
		},
		{
			"multi pk multi compare - interleaved", []conn.Record{{"id": 1, "sub": 1, "a": "a", "b": "b"}, {"id": 2, "sub": 2, "a": "c", "b": "d"}}, []conn.Record{{"id": 1, "sub": 2, "a": "C", "b": "d"}, {"id": 2, "sub": 1, "a": "c", "b": "d"}}, []string{"DROP:1:1", "ADD:1:2", "ADD:2:1", "DROP:2:2"},
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

func TestStreamCompareData_NumericPKOrdering(t *testing.T) {
	cols := []string{"id", "val"}
	pk := []string{"id"}
	rule := CreateCompareRule(&conn.Table{
		Name: "t",
		Columns: map[string]*conn.Column{
			"id":  {Name: "id", DataType: "bigint"},
			"val": {Name: "val", DataType: "varchar"},
		},
	}, []string{"val"})

	srcRows := []conn.Record{
		{"id": 9, "val": "nine"},
		{"id": 10, "val": "ten"},
		{"id": 11, "val": "eleven"},
	}
	tgtRows := []conn.Record{
		{"id": 9, "val": "nine"},
		{"id": 10, "val": "ten_mod"},
		{"id": 12, "val": "twelve"},
	}

	src := &mockDB{rows: srcRows, cols: cols, pk: pk}
	tgt := &mockDB{rows: tgtRows, cols: cols, pk: pk}

	var got []string
	handle := func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) {
		var id any
		if srcRow != nil {
			id = srcRow["id"]
		} else if tgtRow != nil {
			id = tgtRow["id"]
		}
		got = append(got, fmt.Sprintf("%s:%v", diffType, id))
	}

	err := StreamCompareDataDetailed(src, tgt, rule, 2, handle)
	if err != nil {
		t.Fatalf("StreamCompareDataDetailed err=%v", err)
	}

	expect := []string{"MODIFY:10", "DROP:11", "ADD:12"}
	if !reflect.DeepEqual(got, expect) {
		t.Errorf("NumericPKOrdering: got %v, expect %v", got, expect)
	}
}

func TestValueComparator_Semantics(t *testing.T) {
	// 1. JSON 乱序键相等性比对
	json1 := `{"name": "alice", "age": 30}`
	json2 := `{"age": 30, "name": "alice"}`
	if !AreValuesEqual(json1, json2, "jsonb") {
		t.Errorf("expected JSON values to be equal regardless of key order")
	}

	// 2. 数值 Decimal 尾零抹平比对
	if !AreValuesEqual("10.50", "10.5", "decimal") {
		t.Errorf("expected decimal 10.50 and 10.5 to be equal")
	}

	// 3. 布尔值多形式比对
	if !AreValuesEqual(true, "1", "boolean") {
		t.Errorf("expected true and '1' to be equal for boolean")
	}
	if !AreValuesEqual(false, "0", "bool") {
		t.Errorf("expected false and '0' to be equal for bool")
	}

	// 4. 二进制相等性比对
	if !AreValuesEqual([]byte("binary_data"), []byte("binary_data"), "bytea") {
		t.Errorf("expected binary data to be equal")
	}
}

func TestStreamCompareData_ModifiedCols(t *testing.T) {
	cols := []string{"id", "title", "status", "count"}
	pk := []string{"id"}
	rule := CreateCompareRule(&conn.Table{
		Name: "items",
		Columns: map[string]*conn.Column{
			"id":     {Name: "id", DataType: "int"},
			"title":  {Name: "title", DataType: "varchar"},
			"status": {Name: "status", DataType: "varchar"},
			"count":  {Name: "count", DataType: "int"},
		},
	}, nil)

	srcRows := []conn.Record{
		{"id": 1, "title": "Old Title", "status": "active", "count": 10},
	}
	tgtRows := []conn.Record{
		{"id": 1, "title": "New Title", "status": "active", "count": 10},
	}

	src := &mockDB{rows: srcRows, cols: cols, pk: pk}
	tgt := &mockDB{rows: tgtRows, cols: cols, pk: pk}

	diff, err := StreamCompareDataToDiff(src, tgt, rule, 10)
	if err != nil {
		t.Fatalf("StreamCompareDataToDiff failed: %v", err)
	}

	if len(diff.Modified) != 1 {
		t.Fatalf("expected 1 modified row, got %d", len(diff.Modified))
	}

	mod := diff.Modified[0]
	if !reflect.DeepEqual(mod.ModifiedCols, []string{"title"}) {
		t.Errorf("expected ModifiedCols to be [title], got %v", mod.ModifiedCols)
	}
	if mod.Old["title"] != "Old Title" {
		t.Errorf("expected mod.Old title to be Old Title, got %v", mod.Old["title"])
	}
	if mod.New["title"] != "New Title" {
		t.Errorf("expected mod.New title to be New Title, got %v", mod.New["title"])
	}
}

func TestReversibleDataDiff(t *testing.T) {
	cols := []string{"id", "val"}
	pk := []string{"id"}
	rule := CreateCompareRule(&conn.Table{
		Name: "test_rev",
		Columns: map[string]*conn.Column{
			"id":  {Name: "id", DataType: "int"},
			"val": {Name: "val", DataType: "varchar"},
		},
	}, []string{"val"})

	srcRows := []conn.Record{
		{"id": 1, "val": "stay_old"},
		{"id": 2, "val": "will_drop"},
	}
	tgtRows := []conn.Record{
		{"id": 1, "val": "stay_new"},
		{"id": 3, "val": "will_add"},
	}

	src := &mockDB{rows: srcRows, cols: cols, pk: pk}
	tgt := &mockDB{rows: tgtRows, cols: cols, pk: pk}

	diff, err := StreamCompareDataToDiff(src, tgt, rule, 10)
	if err != nil {
		t.Fatalf("StreamCompareDataToDiff err=%v", err)
	}

	// 验证 Diff 收集
	// 1. Added 包含 target 新增的 id=3
	if len(diff.Added) != 1 || diff.Added[0]["id"] != 3 {
		t.Errorf("expected Added to contain id 3, got %v", diff.Added)
	}
	// 2. Dropped 包含 source 删除的 id=2
	if len(diff.Dropped) != 1 || diff.Dropped[0]["id"] != 2 {
		t.Errorf("expected Dropped to contain id 2, got %v", diff.Dropped)
	}
	// 3. Modified 包含 id=1, Old="stay_old", New="stay_new"
	if len(diff.Modified) != 1 {
		t.Fatalf("expected 1 modified row, got %d", len(diff.Modified))
	}
	if diff.Modified[0].Old["val"] != "stay_old" || diff.Modified[0].New["val"] != "stay_new" {
		t.Errorf("expected Old=stay_old, New=stay_new, got Old=%v, New=%v",
			diff.Modified[0].Old["val"], diff.Modified[0].New["val"])
	}
}

func TestCreateCompareRule_IgnoreColumns(t *testing.T) {
	table := &conn.Table{
		Name: "audit_test",
		Columns: map[string]*conn.Column{
			"id":         {Name: "id", DataType: "int"},
			"content":    {Name: "content", DataType: "varchar"},
			"updated_at": {Name: "updated_at", DataType: "timestamp"},
			"created_at": {Name: "created_at", DataType: "timestamp"},
		},
	}

	// 1. 全表字段但忽略 updated_at 和 created_at
	rule := CreateCompareRule(table, nil, []string{"updated_at", "created_at"})
	allRule, ok := rule.(*AllFieldsEqualRule)
	if !ok {
		t.Fatalf("expected AllFieldsEqualRule")
	}

	for _, c := range allRule.Columns {
		if c == "updated_at" || c == "created_at" {
			t.Errorf("expected %s to be ignored in Columns", c)
		}
	}

	// 2. 验证即便 updated_at 发生变更，但由于被忽略，两行记录仍判定为相等
	rowA := conn.Record{"id": 1, "content": "same", "updated_at": "2026-01-01 00:00:00"}
	rowB := conn.Record{"id": 1, "content": "same", "updated_at": "2026-09-18 12:00:00"}
	if !rule.IsEqual(rowA, rowB) {
		t.Errorf("expected rows to be equal when only ignored columns differ")
	}
}
