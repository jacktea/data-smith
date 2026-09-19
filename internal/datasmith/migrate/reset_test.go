package migrate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeResetConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestResetDBRequiresYesBeforeReadingConfig(t *testing.T) {
	cmd := newResetDBCommand()
	cmd.SetArgs([]string{"--config", "/definitely/missing/config.yaml"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "requires --yes") {
		t.Fatalf("got %v, want confirmation error", err)
	}
}

func TestResetDBDryRunPrintsSQLWithoutConnecting(t *testing.T) {
	path := writeResetConfig(t, `targetDb:
  type: mysql
  host: unreachable.invalid
  port: 3306
  user: obvious-placeholder
  password: obvious-placeholder
  dbname: orders_archive
`)
	cmd := newResetDBCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--config", path, "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if got := output.String(); !strings.Contains(got, "DROP DATABASE IF EXISTS `orders_archive`") {
		t.Fatalf("unexpected dry-run output: %q", got)
	}
}

func TestResetDBRejectsDangerousTargetBeforeDBConnection(t *testing.T) {
	path := writeResetConfig(t, `targetDb:
  type: postgres
  host: unreachable.invalid
  port: 5432
  user: obvious-placeholder
  password: obvious-placeholder
  dbname: application
  tableSchema: pg_catalog
`)
	cmd := newResetDBCommand()
	cmd.SetArgs([]string{"--config", path, "--yes"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "system schema") {
		t.Fatalf("got %v, want pre-connection system target error", err)
	}
}
