package diff

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func TestTableColumnsAndTypesFetchesCompareSetPlusPK(t *testing.T) {
	tbl := &conn.Table{
		Name: "app_config",
		Columns: map[string]*conn.Column{
			"id":        {Name: "id", DataType: "int", Position: 1},
			"cfg_key":   {Name: "cfg_key", DataType: "text", Position: 2},
			"cfg_value": {Name: "cfg_value", DataType: "text", Position: 3},
			"env":       {Name: "env", DataType: "text", Position: 4},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rule := CreateCompareRule(tbl, nil, []string{"env"})
	cols, pks, colTypes, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		t.Fatal(err)
	}
	// 忽略字段不参与比对,但行读取必须取值:生成的 INSERT 依赖完整行数据。
	foundEnv := false
	for _, name := range cols {
		if name == "env" {
			foundEnv = true
		}
	}
	if !foundEnv {
		t.Fatalf("ignored column %q must still be fetched for INSERT generation: %v", "env", cols)
	}
	if len(cols) != 4 {
		t.Fatalf("expected id/cfg_key/cfg_value/env fetched, got %v", cols)
	}
	if len(pks) != 1 || pks[0] != "id" {
		t.Fatalf("primary key must be preserved: %v", pks)
	}
	if colTypes["cfg_value"] != "text" || colTypes["env"] != "text" {
		t.Fatalf("unexpected colTypes: %v", colTypes)
	}
	compareSet := rule.(*AllFieldsEqualRule).GetCompareColumns()
	for _, name := range compareSet {
		if name == "env" {
			t.Fatalf("ignored column %q must not join the compare set: %v", name, compareSet)
		}
	}
}

func TestTableColumnsAndTypesSkipsUnknownIgnoredColumn(t *testing.T) {
	tbl := &conn.Table{
		Name: "app_config",
		Columns: map[string]*conn.Column{
			"id":      {Name: "id", DataType: "int", Position: 1},
			"cfg_key": {Name: "cfg_key", DataType: "text", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	// "ghost" 不在该表上:单侧忽略列不得进入行读取列集。
	rule := CreateCompareRule(tbl, nil, []string{"ghost"})
	cols, _, _, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range cols {
		if name == "ghost" {
			t.Fatalf("ignored column missing from table must not be fetched: %v", cols)
		}
	}
	if len(cols) != 2 {
		t.Fatalf("expected id/cfg_key fetched, got %v", cols)
	}
}

func TestChunkHashColumnsExcludesIgnored(t *testing.T) {
	tbl := &conn.Table{
		Name: "app_config",
		Columns: map[string]*conn.Column{
			"id":  {Name: "id", DataType: "int", Position: 1},
			"key": {Name: "key", DataType: "text", Position: 2},
			"env": {Name: "env", DataType: "text", Position: 3},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rule := CreateCompareRule(tbl, nil, []string{"env"})
	cols := []string{"id", "key", "env"}
	hashCols := chunkHashColumns(cols, rule)
	if len(hashCols) != 2 || hashCols[0] != "id" || hashCols[1] != "key" {
		t.Fatalf("chunk hash must exclude ignored column, got %v", hashCols)
	}
	// 忽略列覆盖全部列时退回完整列集。
	allIgnored := CreateCompareRule(tbl, nil, []string{"id", "key", "env"})
	if got := chunkHashColumns(cols, allIgnored); len(got) != len(cols) {
		t.Fatalf("expected fallback to full column set, got %v", got)
	}
	// 无忽略字段时原样返回。
	plain := CreateCompareRule(tbl, nil, nil)
	if got := chunkHashColumns(cols, plain); len(got) != len(cols) {
		t.Fatalf("expected unchanged column set without ignores, got %v", got)
	}
}

func TestTableColumnsAndTypesKeepsPKWhenOutsideCompareSet(t *testing.T) {
	tbl := &conn.Table{
		Name: "items",
		Columns: map[string]*conn.Column{
			"id":    {Name: "id", DataType: "int", Position: 1},
			"name":  {Name: "name", DataType: "text", Position: 2},
			"extra": {Name: "extra", DataType: "text", Position: 3},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rule := CreateCompareRule(tbl, []string{"name"}, nil)
	cols, pks, _, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		t.Fatal(err)
	}
	foundID := false
	for _, name := range cols {
		if name == "id" {
			foundID = true
		}
		if name == "extra" {
			t.Fatalf("column outside compare set must not be fetched: %v", cols)
		}
	}
	if !foundID {
		t.Fatalf("primary key must still be fetched for row matching: %v", cols)
	}
	if len(pks) != 1 || pks[0] != "id" {
		t.Fatalf("unexpected pks: %v", pks)
	}
}
