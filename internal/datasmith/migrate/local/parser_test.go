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

func TestScanMigrationsUpgradeOnly(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1.0.0__create_users.up.sql")
	writeMigration(t, dir, "v1.1.0__drop_users.down.sql")
	writeMigration(t, dir, "v1.2.0__add_index.sql")
	writeMigration(t, dir, "README.txt")

	files, err := ScanMigrations(dir)
	if err != nil {
		t.Fatal(err)
	}
	SortMigrations(files)
	if len(files) != 2 {
		t.Fatalf("got %d upgrade files, want 2", len(files))
	}
	for _, file := range files {
		if file.Direction != "up" || file.Ext != "sql" {
			t.Fatalf("unexpected upgrade candidate: %#v", file)
		}
	}
	if files[1].Version != "v1.2.0" {
		t.Fatalf("directionless SQL was not retained as an upgrade: %#v", files)
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
	if err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
		t.Fatalf("expected actionable duplicate version error, got %v", err)
	}
}
