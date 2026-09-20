package diff

import (
	"reflect"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

func TestFilterTables_DefaultToolTablesExcluded(t *testing.T) {
	tables := map[string]*conn.Table{
		"users":                 {Name: "users"},
		"orders":                {Name: "orders"},
		"flyway_schema_history": {Name: "flyway_schema_history"},
		"schema_migrations":     {Name: "schema_migrations"},
	}

	effectiveExcludes := pkgconfig.EffectiveExcludeTables(nil)
	filtered := filterTables(tables, nil, effectiveExcludes)

	if _, ok := filtered["flyway_schema_history"]; ok {
		t.Errorf("expected flyway_schema_history to be excluded")
	}
	if _, ok := filtered["schema_migrations"]; ok {
		t.Errorf("expected schema_migrations to be excluded")
	}
	if _, ok := filtered["users"]; !ok {
		t.Errorf("expected users to be retained")
	}
	if _, ok := filtered["orders"]; !ok {
		t.Errorf("expected orders to be retained")
	}
	if len(filtered) != 2 {
		t.Errorf("expected 2 tables, got %d", len(filtered))
	}
}

func TestFilterTables_CaseInsensitiveExclusion(t *testing.T) {
	tables := map[string]*conn.Table{
		"users":                 {Name: "users"},
		"FLYWAY_SCHEMA_HISTORY": {Name: "FLYWAY_SCHEMA_HISTORY"},
		"Schema_Migrations":     {Name: "Schema_Migrations"},
		"CUSTOM_TOOL_TABLE":     {Name: "CUSTOM_TOOL_TABLE"},
	}

	userExcludes := []string{"custom_tool_table"}
	effectiveExcludes := pkgconfig.EffectiveExcludeTables(userExcludes)
	filtered := filterTables(tables, nil, effectiveExcludes)

	if _, ok := filtered["FLYWAY_SCHEMA_HISTORY"]; ok {
		t.Errorf("expected FLYWAY_SCHEMA_HISTORY to be excluded (case-insensitive)")
	}
	if _, ok := filtered["Schema_Migrations"]; ok {
		t.Errorf("expected Schema_Migrations to be excluded (case-insensitive)")
	}
	if _, ok := filtered["CUSTOM_TOOL_TABLE"]; ok {
		t.Errorf("expected CUSTOM_TOOL_TABLE to be excluded (case-insensitive)")
	}
	if _, ok := filtered["users"]; !ok {
		t.Errorf("expected users to be retained")
	}
	if len(filtered) != 1 {
		t.Errorf("expected 1 table, got %d", len(filtered))
	}
}

func TestFilterTables_WithIncludesAndExcludes(t *testing.T) {
	tables := map[string]*conn.Table{
		"users":                 {Name: "users"},
		"orders":                {Name: "orders"},
		"logs":                  {Name: "logs"},
		"flyway_schema_history": {Name: "flyway_schema_history"},
	}

	// 仅选择 users, orders, logs，但 logs 同时在自定义排除列表中，flyway_schema_history 在默认排除列表中
	includes := []string{"users", "orders", "logs", "flyway_schema_history"}
	effectiveExcludes := pkgconfig.EffectiveExcludeTables([]string{"logs"})

	filtered := filterTables(tables, includes, effectiveExcludes)

	expectedKeys := map[string]bool{
		"users":  true,
		"orders": true,
	}

	gotKeys := make(map[string]bool, len(filtered))
	for k := range filtered {
		gotKeys[k] = true
	}

	if !reflect.DeepEqual(gotKeys, expectedKeys) {
		t.Errorf("got %v, want %v", gotKeys, expectedKeys)
	}
}
