package postgres

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
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
		t.Fatalf("Failed to create adapter: %v", err)
	}
	table, err := adapter.ExtractTable("air_inst_document")
	if err != nil {
		t.Fatalf("Failed to extract table detail: %v", err)
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
		t.Fatalf("Failed to create adapter: %v", err)
	}
	view, err := adapter.ExtractView("air_inst_document_tab_whereused")
	if err != nil {
		t.Fatalf("Failed to extract table detail: %v", err)
	}
	dbDialect := NewPostgreDialect()
	ddl := dbDialect.GenerateViewDDL(view)
	t.Logf("View DDL: %s", ddl)
}
