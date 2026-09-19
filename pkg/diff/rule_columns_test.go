package diff

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func columnsOf(t *testing.T, rule ICompareRule) []string {
	t.Helper()
	allRule, ok := rule.(*AllFieldsEqualRule)
	if !ok {
		t.Fatalf("expected *AllFieldsEqualRule, got %T", rule)
	}
	return allRule.Columns
}

func assertColumns(t *testing.T, got []string, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d columns %v, got %v", len(want), want, got)
	}
	for _, c := range got {
		if !want[c] {
			t.Fatalf("unexpected column %q in %v", c, got)
		}
	}
}

func testRuleTable() *conn.Table {
	return &conn.Table{
		Name: "items",
		Columns: map[string]*conn.Column{
			"id":     {Name: "id", DataType: "int"},
			"name":   {Name: "name", DataType: "varchar"},
			"note":   {Name: "note", DataType: "varchar"},
			"synced": {Name: "synced", DataType: "timestamp"},
		},
	}
}

func TestCreateCompareRuleColumns_PrecedenceOverComparisonKey(t *testing.T) {
	rule := CreateCompareRuleColumns(testRuleTable(), []string{"id", "name"}, []string{"id"}, nil)
	assertColumns(t, columnsOf(t, rule), map[string]bool{"id": true, "name": true})
}

func TestCreateCompareRuleColumns_ColumnsMinusIgnored(t *testing.T) {
	rule := CreateCompareRuleColumns(testRuleTable(), []string{"id", "name", "synced"}, nil, []string{"synced"})
	assertColumns(t, columnsOf(t, rule), map[string]bool{"id": true, "name": true})
}

func TestCreateCompareRuleColumns_FallsBackToComparisonKey(t *testing.T) {
	rule := CreateCompareRuleColumns(testRuleTable(), nil, []string{"id"}, []string{"note"})
	assertColumns(t, columnsOf(t, rule), map[string]bool{"id": true})
}

func TestCreateCompareRuleColumns_FallsBackToAllColumns(t *testing.T) {
	rule := CreateCompareRuleColumns(testRuleTable(), nil, nil, []string{"synced"})
	assertColumns(t, columnsOf(t, rule), map[string]bool{"id": true, "name": true, "note": true})
}

func TestCreateCompareRuleColumns_EmptyColumnsFallsBack(t *testing.T) {
	rule := CreateCompareRuleColumns(testRuleTable(), []string{}, []string{"id", "note"}, nil)
	assertColumns(t, columnsOf(t, rule), map[string]bool{"id": true, "note": true})
}
