package diff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

func ignoredColumnTestTable(name string, withTargetOnly bool) *conn.Table {
	columns := map[string]*conn.Column{
		"id":           {Name: "id", DataType: "int", Position: 1},
		"amount":       {Name: "amount", DataType: "int", Position: 2},
		"lock_version": {Name: "lock_version", DataType: "int", Position: 3},
	}
	if withTargetOnly {
		columns["tgt_only"] = &conn.Column{Name: "tgt_only", DataType: "varchar", Position: 4}
	}
	return &conn.Table{
		Name:       name,
		Columns:    columns,
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
}

func TestDataDiffKeepColumnsCarriesBothSidedIgnoredColumns(t *testing.T) {
	tgt := ignoredColumnTestTable("orders", true)
	src := ignoredColumnTestTable("orders", false)
	keep := dataDiffKeepColumns(tgt, src, []string{"amount"}, []string{"lock_version", "tgt_only", "ghost"})
	if !keep["id"] || !keep["amount"] || !keep["lock_version"] {
		t.Fatalf("keep must contain compare columns, PK and both-sided ignored columns: %v", keep)
	}
	if keep["tgt_only"] || keep["ghost"] {
		t.Fatalf("single-sided or unknown ignored columns must stay out of the SQL column set: %v", keep)
	}
}

func TestDataDiffKeepColumnsNilTableSafety(t *testing.T) {
	tbl := ignoredColumnTestTable("orders", false)
	if keep := dataDiffKeepColumns(nil, tbl, []string{"amount"}, []string{"lock_version"}); len(keep) != 1 || !keep["amount"] {
		t.Fatalf("nil target table must yield effective columns only: %v", keep)
	}
	if keep := dataDiffKeepColumns(tbl, nil, []string{"amount"}, []string{"lock_version"}); keep["lock_version"] {
		t.Fatalf("nil source table must exclude ignored columns: %v", keep)
	}
}

func TestCompareRuleRebuiltFromSlimTableStillExcludesIgnored(t *testing.T) {
	// prepareTable 加宽 slim 表后,compareTable 用同一忽略清单重建比对规则,
	// 忽略字段不得重新进入比对列集。
	slim := ignoredColumnTestTable("orders", false)
	rule := pkgdiff.CreateCompareRuleColumns(slim, nil, nil, []string{"lock_version"})
	all, ok := rule.(*pkgdiff.AllFieldsEqualRule)
	if !ok {
		t.Fatalf("expected AllFieldsEqualRule, got %T", rule)
	}
	for _, c := range all.Columns {
		if c == "lock_version" {
			t.Fatalf("ignored column re-entered the compare set after slim table widening: %v", all.Columns)
		}
	}
	if len(all.IgnoredColumns) != 1 || all.IgnoredColumns[0] != "lock_version" {
		t.Fatalf("rebuilt rule must record ignored columns: %v", all.IgnoredColumns)
	}
}

func TestStreamingSQLCarriesIgnoredColumns(t *testing.T) {
	tbl := ignoredColumnTestTable("orders", false)
	rules := []config.Rule{{Table: "orders"}}
	prepare := func(config.Rule) (*tableModels, error) {
		return &tableModels{target: tbl, source: tbl, effectiveCols: []string{"amount"}}, nil
	}
	compare := func(_ config.Rule, _ *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		if err := handle(pkgdiff.DiffTypeAdd, nil, conn.Record{"id": 1, "amount": 10, "lock_version": 7}, nil); err != nil {
			return err
		}
		if err := handle(pkgdiff.DiffTypeModify,
			conn.Record{"id": 2, "amount": 1, "lock_version": 3},
			conn.Record{"id": 2, "amount": 2, "lock_version": 9},
			[]string{"amount"},
		); err != nil {
			return err
		}
		return handle(pkgdiff.DiffTypeDrop, conn.Record{"id": 3, "amount": 5, "lock_version": 4}, nil, nil)
	}
	var forward, rollback bytes.Buffer
	if _, err := generateStreamingDataDiffOutputs(&forward, &rollback, t.TempDir(), rules, pkgsql.NewDialect(consts.DBTypeMySQL), 10, false, prepare, compare, nil); err != nil {
		t.Fatal(err)
	}
	fwd := forward.String()
	rb := rollback.String()

	// ADD:forward INSERT 必须携带忽略字段及其值,否则 NOT NULL 忽略字段缺列会执行失败。
	if !insertLines(fwd, "orders", "`lock_version`", "7") {
		t.Fatalf("forward INSERT must carry ignored column lock_version with its value:\n%s", fwd)
	}
	// MODIFY:UPDATE SET 只含比对出差异的列,忽略字段不回写。
	if strings.Contains(fwd, "`lock_version` =") {
		t.Fatalf("UPDATE SET must not include ignored column:\n%s", fwd)
	}
	if !strings.Contains(fwd, "`amount` = 2") {
		t.Fatalf("UPDATE SET must include the differing compare column:\n%s", fwd)
	}
	// DROP:回滚 INSERT 需要完整行数据。
	if !insertLines(rb, "orders", "`lock_version`", "4") {
		t.Fatalf("rollback INSERT for dropped row must carry ignored column with its value:\n%s", rb)
	}
}

// insertLines 断言目标 SQL 文本中存在对 table 的 INSERT 语句,
// 且该语句同时包含 wantCol 与 wantVal。表名允许带库名前缀
// (真实运行时 MySQL 方言会输出 `db`.`table`)。
func insertLines(sqlText, table, wantCol, wantVal string) bool {
	for _, line := range strings.Split(sqlText, "\n") {
		if strings.Contains(line, "INSERT INTO") &&
			strings.Contains(line, "`"+table+"`") &&
			strings.Contains(line, wantCol) &&
			strings.Contains(line, wantVal) {
			return true
		}
	}
	return false
}
