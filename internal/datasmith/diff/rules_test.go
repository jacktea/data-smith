package diff

import (
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

func table(name string, pkCols ...string) *conn.Table {
	tbl := &conn.Table{Name: name, Type: conn.TableTypeTable, Columns: map[string]*conn.Column{}}
	for _, col := range append([]string{"id"}, pkCols...) {
		tbl.Columns[col] = &conn.Column{Name: col, DataType: "bigint"}
	}
	if len(pkCols) > 0 {
		tbl.PrimaryKey = &conn.PrimaryKey{Name: name + "_pkey", Columns: pkCols}
	} else {
		tbl.PrimaryKey = &conn.PrimaryKey{Name: name + "_pkey", Columns: []string{"id"}}
	}
	return tbl
}

func TestMatchTablePattern(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"air_sys_*", "air_sys_list_item", true},
		{"air_sys_*", "air_order", false},
		{"*", "anything", true},
		{"air_?", "air_x", true},
		{"air_?", "air_xy", false},
		{"AIR_SYS_*", "air_sys_list_item", true},
		{"air_*", "AIR_ORDER", true},
	}
	for _, tc := range cases {
		if got := matchTablePattern(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matchTablePattern(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

func TestHasWildcardRules(t *testing.T) {
	if !hasWildcardRules(nil) {
		t.Fatal("empty rules must trigger whole-database expansion")
	}
	if !hasWildcardRules([]pkgconfig.Rule{{Table: "air_sys_*"}}) {
		t.Fatal("wildcard entry must trigger expansion")
	}
	if hasWildcardRules([]pkgconfig.Rule{{Table: "orders"}}) {
		t.Fatal("explicit rules must not trigger expansion")
	}
}

func TestValidateWildcardRulesRejectsFields(t *testing.T) {
	if err := validateWildcardRules([]pkgconfig.Rule{{Table: "a_*", Columns: []string{"id"}}}); err == nil {
		t.Fatal("wildcard with columns must be rejected")
	}
	if err := validateWildcardRules([]pkgconfig.Rule{{Table: "a_*", ComparisonKey: []string{"id"}}}); err == nil {
		t.Fatal("wildcard with comparisonKey must be rejected")
	}
	if err := validateWildcardRules([]pkgconfig.Rule{{Table: "a_**", IgnoreColumns: nil}}); err != nil {
		t.Fatalf("bare wildcard must be accepted, got %v", err)
	}
}

func TestExpandRulesDirectModeIntersection(t *testing.T) {
	src := map[string]*conn.Table{
		"orders":         table("orders", "id"),
		"src_only":       table("src_only", "id"),
		"no_identity":    {Name: "no_identity", Type: conn.TableTypeTable, Columns: map[string]*conn.Column{"v": {Name: "v", Nullable: true}}},
		"no_id_src_only": table("no_id_src_only", "id"),
	}
	// src 侧 no_identity 无行身份（无主键且列可空）
	src["no_identity"].PrimaryKey = nil
	tgt := map[string]*conn.Table{
		"orders":      table("orders", "id"),
		"tgt_only":    table("tgt_only", "id"),
		"no_identity": {Name: "no_identity", Type: conn.TableTypeTable, Columns: map[string]*conn.Column{"v": {Name: "v", Nullable: true}}},
	}
	tgt["no_identity"].PrimaryKey = nil

	var logs []string
	rules, skipped := expandRules([]pkgconfig.Rule{{Table: "*"}}, src, tgt, false, func(msg string) { logs = append(logs, msg) })

	if len(rules) != 1 || rules[0].Table != "orders" {
		t.Fatalf("expanded rules = %#v, want [orders]", rules)
	}
	joined := strings.Join(skipped, ",")
	for _, name := range []string{"src_only", "tgt_only", "no_identity"} {
		if !strings.Contains(joined, name) {
			t.Fatalf("skipped = %v, want it to contain %s", skipped, name)
		}
	}
	if len(logs) == 0 {
		t.Fatal("expansion must report skips")
	}
}

func TestExpandRulesExplicitRulesPassThrough(t *testing.T) {
	src := map[string]*conn.Table{"orders": table("orders", "id")}
	tgt := map[string]*conn.Table{"orders": table("orders", "id")}
	rules, skipped := expandRules([]pkgconfig.Rule{{Table: "orders", ComparisonKey: []string{"id"}}}, src, tgt, false, nil)
	if len(rules) != 1 || rules[0].ComparisonKey == nil {
		t.Fatalf("explicit rules must pass through unchanged, got %#v", rules)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %v, want empty", skipped)
	}
}

func TestExpandRulesShadowModeIncludesTargetOnlyTables(t *testing.T) {
	src := map[string]*conn.Table{"orders": table("orders", "id")}
	tgt := map[string]*conn.Table{
		"orders":  table("orders", "id"),
		"tgt_new": table("tgt_new", "id"),
		"tgt_bad": {Name: "tgt_bad", Type: conn.TableTypeTable, Columns: map[string]*conn.Column{"v": {Name: "v", Nullable: true}}},
	}
	tgt["tgt_bad"].PrimaryKey = nil

	rules, skipped := expandRules([]pkgconfig.Rule{{Table: "*"}}, src, tgt, true, nil)
	if len(rules) != 2 {
		t.Fatalf("shadow expansion = %#v, want orders+tgt_new (target-only table is aligned by the shadow DDL)", rules)
	}
	if rules[0].Table != "orders" || rules[1].Table != "tgt_new" {
		t.Fatalf("shadow expansion order = %s,%s, want deterministic orders,tgt_new", rules[0].Table, rules[1].Table)
	}
	if len(skipped) != 1 || skipped[0] != "tgt_bad" {
		t.Fatalf("skipped = %v, want [tgt_bad]", skipped)
	}
}

func TestFilterRulesByExcludesDefaultsToLedgerTables(t *testing.T) {
	rules := []pkgconfig.Rule{
		{Table: "orders"},
		{Table: "schema_migrations"},
		{Table: "SCHEMA_MIGRATIONS"},
		{Table: "flyway_schema_history"},
		{Table: "users"},
	}
	kept, excluded := filterRulesByExcludes(rules, pkgconfig.EffectiveExcludeTables(nil))
	if len(kept) != 2 || kept[0].Table != "orders" || kept[1].Table != "users" {
		t.Fatalf("kept = %#v, want orders+users", kept)
	}
	if len(excluded) != 3 {
		t.Fatalf("excluded = %v, want the three ledger entries", excluded)
	}
}

func TestResolveDataRulesExpandsAndExcludesFromSnapshot(t *testing.T) {
	// snapshot 非空时不触碰适配器，传 nil 即可。
	src := map[string]*conn.Table{
		"orders":            table("orders", "id"),
		"schema_migrations": table("schema_migrations", "version"),
		"air_sys_items":     table("air_sys_items", "id"),
	}
	tgt := map[string]*conn.Table{
		"orders":            table("orders", "id"),
		"schema_migrations": table("schema_migrations", "version"),
		"air_sys_items":     table("air_sys_items", "id"),
	}
	params := DataDiffParams{Rules: []pkgconfig.Rule{{Table: "*"}, {Table: "orders"}}}
	rules, excluded, err := resolveDataRules(nil, nil, params, dataPhaseOptions{schemaSnapshot: &schemaPair{src: src, tgt: tgt}}, func(string) {})
	if err != nil {
		t.Fatalf("resolveDataRules: %v", err)
	}
	// 全库通配命中三张表；orders 同时是显式规则（不得重复展开）；账本表
	// 命中默认排除清单被滤掉。
	if len(rules) != 2 || rules[0].Table != "air_sys_items" || rules[1].Table != "orders" {
		t.Fatalf("rules = %#v, want air_sys_items+orders", rules)
	}
	if len(excluded) != 1 || excluded[0] != "schema_migrations" {
		t.Fatalf("excluded = %v, want [schema_migrations]", excluded)
	}
}

func TestDecideShadowDataDiff(t *testing.T) {
	pg, mysql := consts.DBTypePostgres, consts.DBTypeMySQL
	forward := []string{"CREATE TABLE x(id int);"}

	if shadow, err := decideShadowDataDiff(DataDiffModeAuto, pg, forward); err != nil || !shadow {
		t.Fatalf("auto+pg+forward: shadow=%v err=%v, want true,nil", shadow, err)
	}
	if shadow, err := decideShadowDataDiff(DataDiffModeAuto, pg, nil); err != nil || shadow {
		t.Fatalf("auto+pg+no-forward: shadow=%v err=%v, want false,nil", shadow, err)
	}
	if shadow, err := decideShadowDataDiff(DataDiffModeAuto, mysql, forward); err != nil || shadow {
		t.Fatalf("auto+mysql: shadow=%v err=%v, want false,nil (no transactional DDL)", shadow, err)
	}
	if shadow, err := decideShadowDataDiff(DataDiffModeShadow, pg, nil); err != nil || !shadow {
		t.Fatalf("forced shadow+pg: shadow=%v err=%v, want true,nil", shadow, err)
	}
	if _, err := decideShadowDataDiff(DataDiffModeShadow, mysql, nil); err == nil {
		t.Fatal("forced shadow+mysql must be rejected: MySQL DDL auto-commits inside the shadow transaction")
	}
	if shadow, err := decideShadowDataDiff(DataDiffModeDirect, pg, forward); err != nil || shadow {
		t.Fatalf("direct: shadow=%v err=%v, want false,nil", shadow, err)
	}
	if err := ValidateDataDiffMode("bogus"); err == nil {
		t.Fatal("bogus mode must be rejected")
	}
}

func TestStatementPreviewCollapsesWhitespace(t *testing.T) {
	preview := statementPreview("ALTER TABLE\n  air_sys_list_item\n  ADD COLUMN parent_id bigint;")
	if strings.ContainsAny(preview, "\n\t") {
		t.Fatalf("preview must be single-line: %q", preview)
	}
	long := strings.Repeat("x", 300)
	if got := statementPreview(long); len(got) != 123 {
		t.Fatalf("long preview length = %d, want 120+ellipsis", len(got))
	}
}

func TestFilterSequencesByOwnedTable(t *testing.T) {
	sequences := map[string]*conn.Sequence{
		"schema_migrations_id_seq": {Name: "schema_migrations_id_seq", OwnedBy: "schema_migrations.id"},
		"air_items_seq":            {Name: "air_items_seq", OwnedBy: "air_items.id"},
		"standalone_seq":           {Name: "standalone_seq"},
	}
	filtered := filterSequencesByOwnedTable(sequences, pkgconfig.EffectiveExcludeTables(nil))
	if _, ok := filtered["schema_migrations_id_seq"]; ok {
		t.Fatal("sequence owned by an excluded ledger table must be excluded")
	}
	if _, ok := filtered["air_items_seq"]; !ok {
		t.Fatal("sequence owned by a business table must be kept")
	}
	if _, ok := filtered["standalone_seq"]; !ok {
		t.Fatal("standalone sequence must be kept")
	}
}
