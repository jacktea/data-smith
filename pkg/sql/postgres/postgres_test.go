package postgres

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
)

func TestGenerateTableDDL(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Type:        consts.DBTypePostgres,
		Port:        5432,
		User:        "postgres",
		Password:    "air20220401",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Skipf("Skipping live database test: %v", err)
	}
	table, err := adapter.ExtractTable("air_inst_document")
	if err != nil {
		t.Skipf("Failed to extract table detail: %v", err)
	}

	dbDialect := NewPostgreDialect()

	ddl := dbDialect.GenerateTableDDL(table)
	t.Logf("Table DDL: %s", ddl)
}

func TestExtractViewDetail(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Type:        consts.DBTypePostgres,
		Port:        5432,
		User:        "postgres",
		Password:    "air20220401",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Skipf("Skipping live database test: %v", err)
	}
	view, err := adapter.ExtractView("air_inst_document_tab_whereused")
	if err != nil {
		t.Skipf("Failed to extract view detail: %v", err)
	}
	dbDialect := NewPostgreDialect()
	ddl := dbDialect.GenerateViewDDL(view)
	t.Logf("View DDL: %s", ddl)
}

func TestPostgreDialect_DataSqlGeneration(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name: "users",
		Columns: map[string]*conn.Column{
			"id":        {Name: "id", DataType: "int", Position: 1},
			"name":      {Name: "name", DataType: "varchar", Position: 2},
			"is_active": {Name: "is_active", DataType: "boolean", Position: 3},
			"avatar":    {Name: "avatar", DataType: "bytea", Position: 4},
			"profile":   {Name: "profile", DataType: "jsonb", Position: 5},
		},
		PrimaryKey: &conn.PrimaryKey{
			Name:    "users_pkey",
			Columns: []string{"id"},
		},
	}

	row := conn.Record{
		"id":        10,
		"name":      "O'Connor",
		"is_active": true,
		"avatar":    []byte{0xde, 0xad, 0xbe, 0xef},
		"profile":   `{"theme":"dark"}`,
	}

	// 1. 测试增量 UPDATE（只更新 name 和 is_active）
	updateSql := dialect.GenerateUpdateSql(table, row, []string{"name", "is_active"})
	expectedUpdate := `UPDATE users SET "name" = 'O''Connor', "is_active" = TRUE WHERE "id" = 10;`
	if updateSql != expectedUpdate {
		t.Errorf("GenerateUpdateSql mismatch:\ngot:  %s\nwant: %s", updateSql, expectedUpdate)
	}

	// 2. 测试 Bytea 转义
	updateByteaSql := dialect.GenerateUpdateSql(table, row, []string{"avatar"})
	expectedByteaUpdate := `UPDATE users SET "avatar" = E'\\xdeadbeef'::bytea WHERE "id" = 10;`
	if updateByteaSql != expectedByteaUpdate {
		t.Errorf("GenerateUpdateSql bytea mismatch:\ngot:  %s\nwant: %s", updateByteaSql, expectedByteaUpdate)
	}

	// 3. 测试 DELETE
	delSql := dialect.GenerateDeleteSql(table, row)
	expectedDel := `DELETE FROM users WHERE "id" = 10;`
	if delSql != expectedDel {
		t.Errorf("GenerateDeleteSql mismatch:\ngot:  %s\nwant: %s", delSql, expectedDel)
	}
}
