package diff

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/conn"

	"github.com/spf13/cobra"
)

var diffSchemaCmd = &cobra.Command{
	Use:   "diff-schema",
	Short: "Compare database schemas and generate forward diff and rollback SQL scripts",
	RunE:  runDiffSchema,
}

func runDiffSchema(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	diffDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current working directory: %w", err)
	}

	summary, err := RunSchemaDiff(cmd.Context(), SchemaDiffParams{
		Source:        &cfg.SourceDB,
		Target:        &cfg.TargetDB,
		IncludeTables: cfg.IncludeTables,
		ExcludeTables: cfg.ExcludeTables,
	}, diffDir, func(message string) {
		log.Printf("%s\n", message)
	})
	if err != nil {
		return err
	}

	printDiffSummary(summary)
	log.Printf("Forward diff SQL generated: %s (to be executed on SOURCE)\n", filepath.Join(diffDir, schemaDiffForwardFile))
	log.Printf("Rollback SQL generated: %s (to restore SOURCE back to original state)\n", filepath.Join(diffDir, schemaDiffRollbackFile))
	return nil
}

func filterTables(tables map[string]*conn.Table, includes, excludes []string) map[string]*conn.Table {
	incSet := make(map[string]bool)
	for _, t := range includes {
		incSet[t] = true
	}
	excSet := make(map[string]bool)
	for _, t := range excludes {
		excSet[t] = true
	}

	result := make(map[string]*conn.Table)
	for name, tbl := range tables {
		if len(incSet) > 0 && !incSet[name] {
			continue
		}
		if excSet[name] {
			continue
		}
		result[name] = tbl
	}
	return result
}

func printDiffSummary(s SchemaDiffSummary) {
	fmt.Println("================== Schema Diff Summary ==================")
	fmt.Printf("Tables Added   : %d\n", len(s.TablesAdded))
	for _, name := range s.TablesAdded {
		fmt.Printf("  + [ADD TABLE] %s\n", name)
	}

	fmt.Printf("Tables Dropped : %d\n", len(s.TablesDropped))
	for _, name := range s.TablesDropped {
		fmt.Printf("  - [DROP TABLE] %s\n", name)
	}

	fmt.Printf("Tables Modified: %d\n", len(s.TablesModified))
	dropColCount := 0
	for _, t := range s.TablesModified {
		dropColCount += len(t.ColumnsDropped)
		fmt.Printf("  * [ALTER TABLE] %s\n", t.Table)
		for _, col := range t.ColumnsAdded {
			fmt.Printf("      + Column Added   : %s\n", col)
		}
		for _, col := range t.ColumnsDropped {
			fmt.Printf("      - Column Dropped : %s\n", col)
		}
		for _, cmod := range t.ColumnsModified {
			fmt.Printf("      ~ Column Modified: %s (%s)\n", cmod.Name, cmod.Detail)
		}
		for _, idx := range t.IndexesAdded {
			fmt.Printf("      + Index Added    : %s\n", idx)
		}
		for _, idx := range t.IndexesDropped {
			fmt.Printf("      - Index Dropped  : %s\n", idx)
		}
		if t.PrimaryKeyChanged {
			fmt.Printf("      ~ Primary Key Modified\n")
		}
		for _, fk := range t.ForeignKeysAdded {
			fmt.Printf("      + Foreign Key Added  : %s\n", fk)
		}
		for _, fk := range t.ForeignKeysDropped {
			fmt.Printf("      - Foreign Key Dropped: %s\n", fk)
		}
		if t.CommentChanged {
			fmt.Printf("      ~ Table Comment Modified\n")
		}
	}
	fmt.Println("=========================================================")

	// 破坏性变更安全提示
	if s.Destructive {
		fmt.Println("⚠️  WARNING: Destructive changes detected!")
		if len(s.TablesDropped) > 0 {
			fmt.Printf("    - %d table(s) will be DROPPED.\n", len(s.TablesDropped))
		}
		if dropColCount > 0 {
			fmt.Printf("    - %d column(s) will be DROPPED.\n", dropColCount)
		}
		fmt.Println("    Please review 'schema_diff.sql' carefully before applying to production!")
		fmt.Println("=========================================================")
	}
}

func writeSQLStatements(writer io.Writer, sqls []string) error {
	for _, s := range sqls {
		if s != "" {
			if _, err := fmt.Fprintln(writer, s); err != nil {
				return err
			}
		}
	}
	return nil
}

func init() {
	diffSchemaCmd.Flags().StringP("config", "c", "", "Path to config file")
	diffSchemaCmd.MarkFlagRequired("config")
}
