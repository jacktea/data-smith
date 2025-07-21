package postgres

import (
	"encoding/json"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
)

func TestExtractTableDetail(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Port:        5432,
		User:        "postgres",
		Password:    "air20220401",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := NewPostgresAdapter(&cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	table, err := adapter.ExtractTable("air_inst_document")
	if err != nil {
		t.Fatalf("Failed to extract table detail: %v", err)
	}
	json, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal table: %v", err)
	}
	t.Logf("Table: %s", string(json))

}

func TestExtractViewDetail(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Port:        5432,
		User:        "postgres",
		Password:    "air20220401",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := NewPostgresAdapter(&cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	view, err := adapter.ExtractView("air_inst_document_tab_whereused")
	if err != nil {
		t.Fatalf("Failed to extract table detail: %v", err)
	}
	json, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal table: %v", err)
	}
	t.Logf("View: %s", string(json))
}
