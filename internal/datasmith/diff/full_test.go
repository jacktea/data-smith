package diff

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
)

const header = "-- DATASMITH EXECUTE-ON: source"

func TestAssembleVersionScriptsFixedDomainOrder(t *testing.T) {
	up, down := AssembleVersionScripts(true, true,
		header+"\nALTER TABLE a ADD COLUMN c int;\n",
		header+"\nALTER TABLE a DROP COLUMN c;\n",
		header+"\nINSERT INTO a VALUES (1);\n",
		header+"\nDELETE FROM a WHERE id = 1;\n",
	)

	if idx := strings.Index(up, "-- ==== schema forward ===="); idx < 0 {
		t.Fatalf("up must label schema forward section: %q", up)
	}
	if strings.Index(up, "schema forward") > strings.Index(up, "data forward") {
		t.Fatalf("up must order schema forward before data forward: %q", up)
	}
	if strings.Index(down, "data rollback") > strings.Index(down, "schema rollback") {
		t.Fatalf("down must order data rollback before schema rollback: %q", down)
	}
	// 重复头必须剥掉,仅保留首个 EXECUTE-ON 头
	if got := strings.Count(up, "EXECUTE-ON"); got != 1 {
		t.Fatalf("up must carry exactly one EXECUTE-ON header, got %d: %q", got, up)
	}
	if got := strings.Count(down, "EXECUTE-ON"); got != 1 {
		t.Fatalf("down must carry exactly one EXECUTE-ON header, got %d: %q", got, down)
	}
	if !strings.HasPrefix(up, header) || !strings.HasPrefix(down, header) {
		t.Fatalf("both scripts must start with the EXECUTE-ON header:\nup=%q\ndown=%q", up, down)
	}
}

func TestAssembleVersionScriptsIncludesOnlySelectedParts(t *testing.T) {
	up, down := AssembleVersionScripts(false, true,
		"", "", header+"\nINSERT INTO a VALUES (1);\n", header+"\nDELETE FROM a WHERE id = 1;\n")
	if strings.Contains(up, "schema") || strings.Contains(down, "schema") {
		t.Fatalf("schema sections must be absent when includeSchema=false:\nup=%q\ndown=%q", up, down)
	}
	if !strings.Contains(up, "data forward") || !strings.Contains(down, "data rollback") {
		t.Fatalf("data sections must be present:\nup=%q\ndown=%q", up, down)
	}
}

func TestAssembleVersionScriptsDropsBlankSections(t *testing.T) {
	// 结构比对无差异(空内容)时,up/down 只应包含数据段
	up, down := AssembleVersionScripts(true, true,
		header+"\n", header+"\n", header+"\nINSERT INTO a VALUES (1);\n", header+"\nDELETE FROM a WHERE id = 1;\n")
	if strings.Contains(up, "schema forward") || strings.Contains(down, "schema rollback") {
		t.Fatalf("blank schema sections must be dropped:\nup=%q\ndown=%q", up, down)
	}
}

func TestFilterSingleSidedRules(t *testing.T) {
	rules := []pkgconfig.Rule{
		{Table: "users"},
		{Table: "orders"},
		{Table: "new_table"},
		{Table: "old_table"},
	}
	kept, skipped := filterSingleSidedRules(rules, []string{"new_table"}, []string{"old_table"})
	if len(kept) != 2 || kept[0].Table != "users" || kept[1].Table != "orders" {
		t.Fatalf("expected both-sided rules kept, got %v", kept)
	}
	if len(skipped) != 2 || skipped[0] != "new_table" || skipped[1] != "old_table" {
		t.Fatalf("expected single-sided rules skipped in rule order, got %v", skipped)
	}
}

func TestFilterSingleSidedRulesKeepsAllWhenNoSchemaDiff(t *testing.T) {
	rules := []pkgconfig.Rule{{Table: "users"}, {Table: "orders"}}
	kept, skipped := filterSingleSidedRules(rules, nil, nil)
	if len(kept) != 2 || len(skipped) != 0 {
		t.Fatalf("expected no filtering without schema diff, got kept=%v skipped=%v", kept, skipped)
	}
}

func writeArtifact(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestImportMigrationVersionAssemblesUpDownPair(t *testing.T) {
	diffDir := t.TempDir()
	writeArtifact(t, diffDir, SchemaDiffForwardFile, header+"\nCREATE TABLE a(id int);\n")
	writeArtifact(t, diffDir, SchemaDiffRollbackFile, header+"\nDROP TABLE a;\n")
	writeArtifact(t, diffDir, DataDiffForwardFile, header+"\nINSERT INTO a VALUES (1);\n")
	writeArtifact(t, diffDir, DataDiffRollbackFile, header+"\nDELETE FROM a WHERE id = 1;\n")

	migrateDir := filepath.Join(t.TempDir(), "migrations")
	upFile, downFile, err := importMigrationVersion(diffDir, migrateDir, "1.0", "full_sync")
	if err != nil {
		t.Fatalf("import should succeed: %v", err)
	}
	if upFile != "V1.0__full_sync.up.sql" || downFile != "V1.0__full_sync.down.sql" {
		t.Fatalf("unexpected file names: %s, %s", upFile, downFile)
	}
	upRaw, err := os.ReadFile(filepath.Join(migrateDir, upFile))
	if err != nil {
		t.Fatal(err)
	}
	downRaw, err := os.ReadFile(filepath.Join(migrateDir, downFile))
	if err != nil {
		t.Fatal(err)
	}
	up := string(upRaw)
	down := string(downRaw)
	if strings.Index(up, "schema forward") > strings.Index(up, "data forward") {
		t.Fatalf("up must order schema before data: %q", up)
	}
	if strings.Index(down, "data rollback") > strings.Index(down, "schema rollback") {
		t.Fatalf("down must order data rollback before schema rollback: %q", down)
	}
}

func TestImportMigrationVersionRejectsDuplicate(t *testing.T) {
	diffDir := t.TempDir()
	writeArtifact(t, diffDir, SchemaDiffForwardFile, header)
	writeArtifact(t, diffDir, SchemaDiffRollbackFile, header)
	writeArtifact(t, diffDir, DataDiffForwardFile, header)
	writeArtifact(t, diffDir, DataDiffRollbackFile, header)

	migrateDir := t.TempDir()
	writeArtifact(t, migrateDir, "V1.0__existing.up.sql", "-- existing")
	writeArtifact(t, migrateDir, "V1.0__existing.down.sql", "-- existing")

	if _, _, err := importMigrationVersion(diffDir, migrateDir, "v1.0", "another"); err == nil {
		t.Fatal("duplicate version (normalized) must be rejected")
	}
}

func TestImportMigrationVersionRejectsMissingArtifact(t *testing.T) {
	diffDir := t.TempDir()
	writeArtifact(t, diffDir, SchemaDiffForwardFile, header)
	// 缺其余三个产物
	migrateDir := t.TempDir()
	if _, _, err := importMigrationVersion(diffDir, migrateDir, "1.0", "x"); err == nil {
		t.Fatal("missing artifacts must be rejected")
	}
}

func TestValidateMigrationImport(t *testing.T) {
	if err := validateMigrationImport("", "", ""); err != nil {
		t.Fatalf("no import must validate: %v", err)
	}
	if err := validateMigrationImport("", "1.0", "x"); err == nil {
		t.Fatal("version/title without --migrate-dir must be rejected")
	}
	if err := validateMigrationImport("dir", "abc", "x"); err == nil {
		t.Fatal("non-dotted version must be rejected")
	}
	if err := validateMigrationImport("dir", "1.0", "a.b"); err == nil {
		t.Fatal("title with dot must be rejected")
	}
	if err := validateMigrationImport("dir", "v1.0.2", "full_sync"); err != nil {
		t.Fatalf("v-prefixed dotted version must validate: %v", err)
	}
}
