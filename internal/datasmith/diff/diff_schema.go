package diff

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/diff"
	"github.com/jacktea/data-smith/pkg/sql"

	"github.com/spf13/cobra"
)

var diffSchemaCmd = &cobra.Command{
	Use:   "diff-schema",
	Short: "Compare database schemas and generate forward diff and rollback SQL scripts",
	Run: func(cmd *cobra.Command, args []string) {
		configPath, _ := cmd.Flags().GetString("config")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Println("Error loading config:", err)
			os.Exit(1)
		}

		srcDB, err := db.NewDBAdapter(&cfg.SourceDB)
		if err != nil {
			log.Println("Error connecting to source DB:", err)
			os.Exit(1)
		}
		defer srcDB.Close()
		tgtDB, err := db.NewDBAdapter(&cfg.TargetDB)
		if err != nil {
			log.Println("Error connecting to target DB:", err)
			os.Exit(1)
		}
		defer tgtDB.Close()

		start := time.Now()
		log.Printf("Start reading schemas from source (%s) and target (%s)...\n", cfg.SourceDB.Type, cfg.TargetDB.Type)

		srcSchema, err := srcDB.ReadSchema()
		if err != nil {
			log.Println("Error reading source schema:", err)
			os.Exit(1)
		}
		tgtSchema, err := tgtDB.ReadSchema()
		if err != nil {
			log.Println("Error reading target schema:", err)
			os.Exit(1)
		}

		// 表过滤支持
		if len(cfg.IncludeTables) > 0 || len(cfg.ExcludeTables) > 0 {
			srcSchema.Tables = filterTables(srcSchema.Tables, cfg.IncludeTables, cfg.ExcludeTables)
			tgtSchema.Tables = filterTables(tgtSchema.Tables, cfg.IncludeTables, cfg.ExcludeTables)
		}

		// 1. 正向比较 (Source -> Target): 目标是将 Source 升级为 Target
		forwardDiff := diff.CompareSchemas(srcSchema, tgtSchema)

		// 2. 逆向比较 (Target -> Source): 目标是将已升级的 Source 回滚还原
		rollbackDiff := diff.CompareSchemas(tgtSchema, srcSchema)

		log.Printf("Schemas compared successfully, time taken: %v\n", time.Since(start))

		// 打印结构化差异概览
		printDiffSummary(forwardDiff)

		// 生成 SQL 脚本 (使用 Source 端方言，在 Source 端执行)
		forwardSQLs := sql.GenerateSchemaSQL(forwardDiff, cfg.SourceDB.Type)
		rollbackSQLs := sql.GenerateSchemaSQL(rollbackDiff, cfg.SourceDB.Type)

		diffDir, err := os.Getwd()
		if err != nil {
			log.Println("Error getting current working directory:", err)
			os.Exit(1)
		}

		diffFile := fmt.Sprintf("%s/schema_diff.sql", diffDir)
		rollbackFile := fmt.Sprintf("%s/schema_diff_rollback.sql", diffDir)

		if err := writeSqlFile(diffFile, forwardSQLs); err != nil {
			log.Println("Error writing diff SQL file:", err)
			os.Exit(1)
		}
		log.Printf("Forward diff SQL generated: %s (to be executed on SOURCE)\n", diffFile)

		if err := writeSqlFile(rollbackFile, rollbackSQLs); err != nil {
			log.Println("Error writing rollback SQL file:", err)
			os.Exit(1)
		}
		log.Printf("Rollback SQL generated: %s (to restore SOURCE back to original state)\n", rollbackFile)
	},
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

func printDiffSummary(d *diff.SchemaDiff) {
	fmt.Println("================== Schema Diff Summary ==================")
	fmt.Printf("Tables Added   : %d\n", len(d.TablesAdded))
	for _, t := range d.TablesAdded {
		fmt.Printf("  + [ADD TABLE] %s (Type: %s)\n", t.Name, t.Type)
	}

	fmt.Printf("Tables Dropped : %d\n", len(d.TablesDropped))
	for _, t := range d.TablesDropped {
		fmt.Printf("  - [DROP TABLE] %s (Type: %s)\n", t.Name, t.Type)
	}

	fmt.Printf("Tables Modified: %d\n", len(d.TablesModified))
	dropColCount := 0
	for _, t := range d.TablesModified {
		tblName := t.Table.Name
		dropColCount += len(t.ColumnsDropped)
		fmt.Printf("  * [ALTER TABLE] %s\n", tblName)
		for _, col := range t.ColumnsAdded {
			fmt.Printf("      + Column Added   : %s (%s)\n", col.Name, col.DataType)
		}
		for _, col := range t.ColumnsDropped {
			fmt.Printf("      - Column Dropped : %s (%s)\n", col.Name, col.DataType)
		}
		for _, cmod := range t.ColumnsModified {
			var reasons []string
			if cmod.Old.DataType != cmod.New.DataType {
				reasons = append(reasons, fmt.Sprintf("Type: %s -> %s", cmod.Old.DataType, cmod.New.DataType))
			}
			if cmod.Old.Nullable != cmod.New.Nullable {
				reasons = append(reasons, fmt.Sprintf("Nullable: %v -> %v", cmod.Old.Nullable, cmod.New.Nullable))
			}
			if cmod.Old.Comment != cmod.New.Comment {
				reasons = append(reasons, "Comment changed")
			}
			if len(reasons) == 0 {
				reasons = append(reasons, fmt.Sprintf("%s -> %s", cmod.Old.DataType, cmod.New.DataType))
			}
			fmt.Printf("      ~ Column Modified: %s (%s)\n", cmod.New.Name, strings.Join(reasons, ", "))
		}
		for _, idx := range t.IndexesAdded {
			fmt.Printf("      + Index Added    : %s\n", idx.Name)
		}
		for _, idx := range t.IndexesDropped {
			fmt.Printf("      - Index Dropped  : %s\n", idx.Name)
		}
		if t.PrimaryKeyChange != nil {
			fmt.Printf("      ~ Primary Key Modified\n")
		}
		for _, fk := range t.ForeignKeysAdded {
			fmt.Printf("      + Foreign Key Added  : %s\n", fk.Name)
		}
		for _, fk := range t.ForeignKeysDropped {
			fmt.Printf("      - Foreign Key Dropped: %s\n", fk.Name)
		}
		if t.CommentChange != nil {
			fmt.Printf("      ~ Table Comment Modified\n")
		}
	}
	fmt.Println("=========================================================")

	// 破坏性变更安全提示
	if len(d.TablesDropped) > 0 || dropColCount > 0 {
		fmt.Println("⚠️  WARNING: Destructive changes detected!")
		if len(d.TablesDropped) > 0 {
			fmt.Printf("    - %d table(s) will be DROPPED.\n", len(d.TablesDropped))
		}
		if dropColCount > 0 {
			fmt.Printf("    - %d column(s) will be DROPPED.\n", dropColCount)
		}
		fmt.Println("    Please review 'schema_diff.sql' carefully before applying to production!")
		fmt.Println("=========================================================")
	}
}

func writeSqlFile(filepath string, sqls []string) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, s := range sqls {
		if s != "" {
			if _, err := f.WriteString(s + "\n"); err != nil {
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
