package diff

import (
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

// C4a：无物理主键表支持 rules.comparisonKey 业务键比对。物理行身份
// （主键 → 非空唯一索引）仍是首选判定，业务键只在两者皆缺时生效，且
// 业务键列必须存在并全部 NOT NULL。

func keylessTable() *conn.Table {
	tbl := &conn.Table{
		Name:    "air_user_client_role",
		Columns: map[string]*conn.Column{},
		Indexes: map[string]*conn.Index{},
	}
	tbl.Columns["client_id"] = &conn.Column{Name: "client_id", DataType: "int8"}
	tbl.Columns["role_id"] = &conn.Column{Name: "role_id", DataType: "int8"}
	tbl.Columns["note"] = &conn.Column{Name: "note", DataType: "text"}
	return tbl
}

func TestTableColumnsAndTypesBusinessKeyIdentity(t *testing.T) {
	tbl := keylessTable()
	rule := CreateCompareRule(tbl, []string{"client_id", "role_id"})

	cols, pks, _, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		t.Fatalf("business key identity rejected: %v", err)
	}
	if strings.Join(pks, ",") != "client_id,role_id" {
		t.Fatalf("pks = %v, want business key columns", pks)
	}
	for _, name := range pks {
		found := false
		for _, col := range cols {
			if col == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("business key column %s missing from row columns %v", name, cols)
		}
	}
}

func TestTableColumnsAndTypesPhysicalIdentityBeatsBusinessKey(t *testing.T) {
	tbl := keylessTable()
	tbl.PrimaryKey = &conn.PrimaryKey{Name: "pk", Columns: []string{"client_id"}}
	rule := CreateCompareRule(tbl, []string{"role_id"})

	_, pks, _, err := tableColumnsAndTypes(tbl, rule)
	if err != nil {
		t.Fatalf("physical identity rejected: %v", err)
	}
	if strings.Join(pks, ",") != "client_id" {
		t.Fatalf("pks = %v, want primary key to keep precedence over business key", pks)
	}
}

func TestTableColumnsAndTypesBusinessKeyValidation(t *testing.T) {
	tests := []struct {
		name        string
		businessKey []string
		wantErr     []string
	}{
		{
			name:        "nullable business key column",
			businessKey: []string{"client_id", "note"},
			wantErr:     []string{"nullable", "rules.comparisonKey"},
		},
		{
			name:        "unknown business key column",
			businessKey: []string{"ghost"},
			wantErr:     []string{"not found", "ghost"},
		},
		{
			name:        "empty business key on keyless table",
			businessKey: nil,
			wantErr:     []string{"rules.comparisonKey", "NOT NULL business key"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := keylessTable()
			tbl.Columns["note"].Nullable = true
			rule := CreateCompareRule(tbl, tt.businessKey)
			_, _, _, err := tableColumnsAndTypes(tbl, rule)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not mention %q", err.Error(), want)
				}
			}
		})
	}
}

func TestStreamCompareDataWithBusinessKeyIdentity(t *testing.T) {
	// 双侧均为无键关联表（air_user_client_role 类）：以业务键 client_id
	// 定位行，可检出 ADD / MODIFY / DROP。
	src := &mockDB{cols: []string{"client_id", "role_id", "note"}, colTypes: map[string]string{
		"client_id": "int8", "role_id": "int8", "note": "text",
	}}
	tgt := &mockDB{cols: []string{"client_id", "role_id", "note"}, colTypes: map[string]string{
		"client_id": "int8", "role_id": "int8", "note": "text",
	}}
	src.rows = []conn.Record{
		{"client_id": int64(1), "role_id": int64(10), "note": "keep"},
		{"client_id": int64(2), "role_id": int64(20), "note": "stale"},
	}
	tgt.rows = []conn.Record{
		{"client_id": int64(2), "role_id": int64(21), "note": "stale"},
		{"client_id": int64(3), "role_id": int64(30), "note": "new"},
	}

	tbl, err := tgt.ExtractTable("t")
	if err != nil {
		t.Fatal(err)
	}
	rule := CreateCompareRuleColumns(tbl, []string{"role_id", "note"}, []string{"client_id"}, nil)

	var events []string
	err = StreamCompareDataDetailedWithTable(src, tgt, rule, tbl, 1, func(diffType DiffType, srcRow, tgtRow conn.Record, diffCols []string) error {
		switch diffType {
		case DiffTypeAdd:
			events = append(events, "ADD")
		case DiffTypeDrop:
			events = append(events, "DROP")
		case DiffTypeModify:
			events = append(events, "MODIFY:"+strings.Join(diffCols, "|"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("StreamCompareDataDetailedWithTable: %v", err)
	}
	if strings.Join(events, ",") != "DROP,MODIFY:role_id,ADD" {
		t.Fatalf("events = %v, want DROP,MODIFY:role_id,ADD", events)
	}
}
