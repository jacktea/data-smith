package local

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeMigration(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestScanMigrationsIncludesUpDownPairs(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1.0.0__create_users.up.sql")
	writeMigration(t, dir, "v1.0.0__create_users.down.sql")
	writeMigration(t, dir, "v1.1.0__drop_users.down.sql")
	writeMigration(t, dir, "v1.2.0__add_index.sql")
	writeMigration(t, dir, "README.txt")

	files, err := ScanMigrations(dir)
	if err != nil {
		t.Fatal(err)
	}
	SortMigrations(files)
	if len(files) != 4 {
		t.Fatalf("got %d scripts, want 4 (up+down pairs and directionless)", len(files))
	}
	downCount := 0
	for _, file := range files {
		if file.Ext != "sql" {
			t.Fatalf("unexpected candidate: %#v", file)
		}
		if file.Direction == "down" {
			downCount++
		}
	}
	if downCount != 2 {
		t.Fatalf("got %d down scripts, want 2", downCount)
	}
	if files[0].Version != "v1.0.0" || files[1].Version != "v1.0.0" {
		t.Fatalf("same-version pair must sort adjacently: %#v", files)
	}
	if files[3].Version != "v1.2.0" {
		t.Fatalf("directionless SQL was not retained: %#v", files)
	}
}

func TestScanMigrationsRejectsJSON(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1__users.up.sql")
	writeMigration(t, dir, "v2__users.up.json")

	_, err := ScanMigrations(dir)
	if err == nil || !strings.Contains(err.Error(), "JSON migration is not supported") {
		t.Fatalf("expected actionable JSON error, got %v", err)
	}
}

func TestScanMigrationsRejectsDuplicateVersions(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1__first.up.sql")
	writeMigration(t, dir, "1__second.sql")

	_, err := ScanMigrations(dir)
	if err == nil || !strings.Contains(err.Error(), "duplicate migration script") {
		t.Fatalf("expected actionable duplicate version error, got %v", err)
	}
}
