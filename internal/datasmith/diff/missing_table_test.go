package diff

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

// missingAwareDB 模拟一个缺表报错（conn.ErrTableNotFound）的适配器，
// 用于验证 --skip-missing-tables 的判定与登记。
type missingAwareDB struct {
	tables   map[string]*conn.Table
	extracts map[string]int
}

func (d *missingAwareDB) ReadSchema() (*conn.DatabaseSchema, error) {
	return nil, nil
}

func (d *missingAwareDB) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	return nil, nil
}

func (d *missingAwareDB) ExtractTable(tableName string) (*conn.Table, error) {
	d.extracts[tableName]++
	if tbl, ok := d.tables[tableName]; ok {
		return tbl, nil
	}
	// 与真实驱动一致：哨兵错误上附 schema.table 限定名。
	return nil, fmt.Errorf("%w: %s.%s", conn.ErrTableNotFound, "public", tableName)
}

func (d *missingAwareDB) ExtractView(viewName string) (*conn.Table, error) {
	return d.ExtractTable(viewName)
}

func (d *missingAwareDB) GetConn() *sql.DB                 { return nil }
func (d *missingAwareDB) GetConfig() *pkgconfig.ConnConfig { return &pkgconfig.ConnConfig{} }
func (d *missingAwareDB) Close() error                     { return nil }

func pkTable(name string) *conn.Table {
	return &conn.Table{
		Name:   name,
		Schema: "public",
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"id": {Name: "id", DataType: "bigint", Position: 1},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
}

func TestBuildPrepareTableFuncSkipsMissingTablesWhenEnabled(t *testing.T) {
	source := &missingAwareDB{tables: map[string]*conn.Table{"present": pkTable("present")}, extracts: map[string]int{}}
	target := &missingAwareDB{tables: map[string]*conn.Table{"present": pkTable("present")}, extracts: map[string]int{}}

	var skipped []string
	prepare := buildPrepareTableFunc(
		DataDiffParams{SkipMissingTables: true},
		newTableModelCache((source)),
		newTableModelCache((target)),
		func(rule pkgconfig.Rule, side string) {
			skipped = append(skipped, rule.Table+"@"+side)
		},
	)

	models, err := prepare(pkgconfig.Rule{Table: "present"})
	if err != nil || models == nil {
		t.Fatalf("existing table must prepare fine, got models=%v err=%v", models, err)
	}
	models, err = prepare(pkgconfig.Rule{Table: "air_late_table"})
	if !errors.Is(err, errTableSkipped) || models != nil {
		t.Fatalf("missing table must yield errTableSkipped, got models=%v err=%v", models, err)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0], "air_late_table") {
		t.Fatalf("missing table must be reported as skipped, got %v", skipped)
	}
}

func TestBuildPrepareTableFuncFailsOnMissingTablesByDefault(t *testing.T) {
	source := &missingAwareDB{tables: map[string]*conn.Table{}, extracts: map[string]int{}}
	target := &missingAwareDB{tables: map[string]*conn.Table{}, extracts: map[string]int{}}

	prepare := buildPrepareTableFunc(
		DataDiffParams{},
		newTableModelCache((source)),
		newTableModelCache((target)),
		func(rule pkgconfig.Rule, side string) { t.Fatal("no table should be marked skipped") },
	)

	_, err := prepare(pkgconfig.Rule{Table: "air_late_table"})
	if err == nil || errors.Is(err, errTableSkipped) {
		t.Fatalf("default behavior must fail on missing table, got %v", err)
	}
	if !errors.Is(err, conn.ErrTableNotFound) || !strings.Contains(err.Error(), "air_late_table") {
		t.Fatalf("error must name the missing table via ErrTableNotFound, got %v", err)
	}
}
