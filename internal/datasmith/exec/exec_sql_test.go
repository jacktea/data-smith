package exec

import (
	"os"
	"path/filepath"
	"testing"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/spf13/cobra"
)

func TestResolveDBConfig(t *testing.T) {
	cfg := &pkgconfig.Config{
		SourceDB: pkgconfig.ConnConfig{
			Type:   consts.DBTypePostgres,
			Host:   "src-host",
			Port:   5432,
			DBName: "src-db",
		},
		TargetDB: pkgconfig.ConnConfig{
			Type:   consts.DBTypeMySQL,
			Host:   "tgt-host",
			Port:   3306,
			DBName: "tgt-db",
		},
	}

	tests := []struct {
		name        string
		choice      string
		expectedDB  string
		expectedLbl string
		expectErr   bool
	}{
		{
			name:        "source lowercase",
			choice:      "source",
			expectedDB:  "src-db",
			expectedLbl: "source",
			expectErr:   false,
		},
		{
			name:        "source uppercase",
			choice:      "SOURCE",
			expectedDB:  "src-db",
			expectedLbl: "source",
			expectErr:   false,
		},
		{
			name:        "src abbreviation",
			choice:      "src",
			expectedDB:  "src-db",
			expectedLbl: "source",
			expectErr:   false,
		},
		{
			name:        "target lowercase",
			choice:      "target",
			expectedDB:  "tgt-db",
			expectedLbl: "target",
			expectErr:   false,
		},
		{
			name:        "target uppercase",
			choice:      "TARGET",
			expectedDB:  "tgt-db",
			expectedLbl: "target",
			expectErr:   false,
		},
		{
			name:        "tgt abbreviation",
			choice:      "tgt",
			expectedDB:  "tgt-db",
			expectedLbl: "target",
			expectErr:   false,
		},
		{
			name:      "invalid choice",
			choice:    "unknown_db",
			expectErr: true,
		},
		{
			name:      "empty choice",
			choice:    "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connCfg, label, err := resolveDBConfig(cfg, tt.choice)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error for choice %q, got nil", tt.choice)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if connCfg.DBName != tt.expectedDB {
				t.Errorf("expected DBName %q, got %q", tt.expectedDB, connCfg.DBName)
			}
			if label != tt.expectedLbl {
				t.Errorf("expected label %q, got %q", tt.expectedLbl, label)
			}
		})
	}
}

func TestRunExecSQL_Validation(t *testing.T) {
	tempDir := t.TempDir()

	// 准备合法的配置文件
	configContent := `
sourceDb:
  type: postgres
  host: localhost
  port: 5432
  user: user
  password: pwd
  dbname: test_src

targetDb:
  type: postgres
  host: localhost
  port: 5432
  user: user
  password: pwd
  dbname: test_tgt
`
	configPath := filepath.Join(tempDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	t.Run("missing sql file", func(t *testing.T) {
		cmd := &cobra.Command{}
		cmd.Flags().String("config", configPath, "")
		cmd.Flags().String("file", "", "")
		cmd.Flags().String("db", "target", "")
		cmd.Flags().Bool("source", false, "")
		cmd.Flags().Bool("target", false, "")
		cmd.Flags().Bool("dry-run", false, "")
		cmd.Flags().Bool("tx", false, "")

		err := runExecSQL(cmd, []string{})
		if err == nil || err.Error() != "请指定 SQL 文件路径 (通过 -f/--file 参数或位置参数)" {
			t.Fatalf("expected missing file error, got: %v", err)
		}
	})

	t.Run("non-existent sql file", func(t *testing.T) {
		cmd := &cobra.Command{}
		cmd.Flags().String("config", configPath, "")
		cmd.Flags().String("file", filepath.Join(tempDir, "non_existent.sql"), "")
		cmd.Flags().String("db", "target", "")
		cmd.Flags().Bool("source", false, "")
		cmd.Flags().Bool("target", false, "")
		cmd.Flags().Bool("dry-run", false, "")
		cmd.Flags().Bool("tx", false, "")

		err := runExecSQL(cmd, []string{})
		if err == nil {
			t.Fatal("expected error for non-existent file, got nil")
		}
	})

	t.Run("empty sql file should return nil without error", func(t *testing.T) {
		emptySQLFile := filepath.Join(tempDir, "empty.sql")
		if err := os.WriteFile(emptySQLFile, []byte("   \n\t  "), 0644); err != nil {
			t.Fatalf("failed to write empty sql: %v", err)
		}

		cmd := &cobra.Command{}
		cmd.Flags().String("config", configPath, "")
		cmd.Flags().String("file", emptySQLFile, "")
		cmd.Flags().String("db", "target", "")
		cmd.Flags().Bool("source", false, "")
		cmd.Flags().Bool("target", false, "")
		cmd.Flags().Bool("dry-run", false, "")
		cmd.Flags().Bool("tx", false, "")

		err := runExecSQL(cmd, []string{})
		if err != nil {
			t.Fatalf("expected nil error for empty sql file, got: %v", err)
		}
	})

	t.Run("invalid db choice", func(t *testing.T) {
		sqlFile := filepath.Join(tempDir, "test.sql")
		_ = os.WriteFile(sqlFile, []byte("SELECT 1;"), 0644)

		cmd := &cobra.Command{}
		cmd.Flags().String("config", configPath, "")
		cmd.Flags().String("file", sqlFile, "")
		cmd.Flags().String("db", "invalid_choice", "")
		cmd.Flags().Bool("source", false, "")
		cmd.Flags().Bool("target", false, "")
		cmd.Flags().Bool("dry-run", false, "")
		cmd.Flags().Bool("tx", false, "")

		err := runExecSQL(cmd, []string{})
		if err == nil {
			t.Fatal("expected error for invalid db choice, got nil")
		}
	})

	t.Run("positional argument for sql file", func(t *testing.T) {
		emptySQLFile := filepath.Join(tempDir, "positional_empty.sql")
		if err := os.WriteFile(emptySQLFile, []byte(""), 0644); err != nil {
			t.Fatalf("failed to write empty sql: %v", err)
		}

		cmd := &cobra.Command{}
		cmd.Flags().String("config", configPath, "")
		cmd.Flags().String("file", "", "")
		cmd.Flags().String("db", "target", "")
		cmd.Flags().Bool("source", false, "")
		cmd.Flags().Bool("target", false, "")
		cmd.Flags().Bool("dry-run", false, "")
		cmd.Flags().Bool("tx", false, "")

		// 传位置参数
		err := runExecSQL(cmd, []string{emptySQLFile})
		if err != nil {
			t.Fatalf("expected nil for valid positional arg, got: %v", err)
		}
	})
}
