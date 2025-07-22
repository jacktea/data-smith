package diff

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/diff"
	"github.com/jacktea/data-smith/pkg/sql"

	"github.com/spf13/cobra"
)

var diffSchemaCmd = &cobra.Command{
	Use:   "diff-schema",
	Short: "Compare database schemas and generate SQL diff",
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
		log.Printf("Start comparing schemas\n")
		diff, err := diff.CompareSchemasWithAdapter(srcDB, tgtDB)
		if err != nil {
			log.Println("Error comparing schemas:", err)
			os.Exit(1)
		}
		log.Printf("Schemas compared successfully, time taken: %v\n", time.Since(start))
		sqls := sql.GenerateSchemaSQL(diff, cfg.TargetDB.Type)
		diffDir, err := os.Getwd()
		if err != nil {
			log.Println("Error getting current working directory:", err)
			os.Exit(1)
		}
		diffFile := fmt.Sprintf("%s/schema_diff.sql", diffDir)
		log.Printf("Diff file: %s\n", diffFile)
		sqlFile, err := os.Create(diffFile)
		if err != nil {
			log.Println("Error creating sql file:", err)
			os.Exit(1)
		}
		defer sqlFile.Close()
		for _, s := range sqls {
			if s != "" {
				sqlFile.WriteString(s + "\n")
			}
		}
	},
}

func init() {
	diffSchemaCmd.Flags().StringP("config", "c", "", "Path to config file")
	diffSchemaCmd.MarkFlagRequired("config")
}
