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
	for _, name := range cols {
		if name == "env" {
			t.Fatalf("ignored column %q must not be fetched", name)
		}
	}
	if len(cols) != 3 {
		t.Fatalf("expected id/cfg_key/cfg_value fetched, got %v", cols)
	}
	if len(pks) != 1 || pks[0] != "id" {
		t.Fatalf("primary key must be preserved: %v", pks)
	}
	if colTypes["cfg_value"] != "text" || colTypes["env"] != "" {
		t.Fatalf("unexpected colTypes: %v", colTypes)
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
