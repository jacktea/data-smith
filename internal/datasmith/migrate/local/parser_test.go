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

	files, skipped, err := ScanMigrations(dir)
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
	if len(skipped) != 1 || skipped[0] != "README.txt" {
		t.Fatalf("skipped = %v, want [README.txt]", skipped)
	}
}

func TestScanMigrationsReportsNonCompliantFileNames(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1.0.0__create_users.up.sql")
	// 单下划线：Flyway 风格命名，不满足 V<版本>__<标题> 规范，曾被静默跳过。
	writeMigration(t, dir, "V1.0.1_update.up.sql")
	nested := filepath.Join(dir, "notes")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	writeMigration(t, nested, " drafts~1.sql")

	files, skipped, err := ScanMigrations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Version != "v1.0.0" {
		t.Fatalf("files = %#v, want only v1.0.0", files)
	}
	if len(skipped) != 2 {
		t.Fatalf("skipped = %v, want the two non-compliant files", skipped)
	}
	if skipped[0] != "V1.0.1_update.up.sql" || skipped[1] != filepath.Join("notes", " drafts~1.sql") {
		t.Fatalf("skipped = %v, want sorted relative paths", skipped)
	}
}

func TestScanMigrationsRejectsJSON(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1__users.up.sql")
	writeMigration(t, dir, "v2__users.up.json")

	_, _, err := ScanMigrations(dir)
	if err == nil || !strings.Contains(err.Error(), "JSON migration is not supported") {
		t.Fatalf("expected actionable JSON error, got %v", err)
	}
}

func TestScanMigrationsRejectsDuplicateVersions(t *testing.T) {
	dir := t.TempDir()
	writeMigration(t, dir, "v1__first.up.sql")
	writeMigration(t, dir, "1__second.sql")

	_, _, err := ScanMigrations(dir)
	if err == nil || !strings.Contains(err.Error(), "duplicate migration script") {
		t.Fatalf("expected actionable duplicate version error, got %v", err)
	}
}
