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

var diffDataCmd = &cobra.Command{
	Use:   "diff-data",
	Short: "Compare table data and generate SQL diff",
	Run: func(cmd *cobra.Command, args []string) {
		configPath, _ := cmd.Flags().GetString("config")
		rulesPath, _ := cmd.Flags().GetString("rules")
		batchSize, _ := cmd.Flags().GetInt("batch-size")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Println("Error loading config:", err)
			os.Exit(1)
		}
		rules, err := config.LoadRules(rulesPath)
		if err != nil {
			log.Println("Error loading rules:", err)
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

		dbDialect := sql.NewDialect(cfg.TargetDB.Type)

		// diff dir 设定为当前程序的执行目录
		diffDir, err := os.Getwd()
		if err != nil {
			log.Println("Error getting current working directory:", err)
			os.Exit(1)
		}
		diffFile := fmt.Sprintf("%s/data_diff.sql", diffDir)
		log.Printf("Diff file: %s\n", diffFile)
		sqlFile, err := os.Create(diffFile)
		if err != nil {
			log.Println("Error creating sql file:", err)
			os.Exit(1)
		}
		defer sqlFile.Close()

		for _, rule := range rules.Rules {
			tgtTable, err := tgtDB.ExtractTable(rule.Table)
			if err != nil || tgtTable == nil {
				log.Printf("Error extracting table %s: %v\n", rule.Table, err)
				continue
			}
			log.Printf("Start comparing data for table %s\n", rule.Table)
			sqlFile.WriteString(fmt.Sprintf("--- diff %s \n", rule.Table))
			start := time.Now()
			diff, err := diff.StreamCompareDataToDiff(
				srcDB,
				tgtDB,
				diff.CreateCompareRule(tgtTable, rule.ComparisonKey),
				batchSize,
			)
			if err != nil {
				log.Printf("Error comparing data for table %s: %v\n", rule.Table, err)
				continue
			}
			log.Printf("Time taken: %v\n", time.Since(start))
			for _, row := range diff.Dropped {
				sqlFile.WriteString(dbDialect.GenerateDeleteSql(tgtTable, row) + "\n")
			}
			for _, row := range diff.Added {
				sqlFile.WriteString(dbDialect.GenerateInsertSql(tgtTable, row) + "\n")
			}
			for _, row := range diff.Modified {
				sqlFile.WriteString(dbDialect.GenerateUpdateSql(tgtTable, row.New, rule.ComparisonKey) + "\n")
			}

			// err = diff.StreamCompareData(
			// 	srcDB,
			// 	tgtDB,
			// 	diff.CreateCompareRule(tgtTable, rule.ComparisonKey),
			// 	batchSize,
			// 	func(diffType diff.DiffType, srcRow, tgtRow conn.Record) {
			// 		var str string
			// 		switch diffType {
			// 		case diff.DiffTypeAdd:
			// 			str = dbDialect.GenerateInsertSql(tgtTable, srcRow)
			// 			sqlFile.WriteString(str + "\n")
			// 		case diff.DiffTypeDrop:
			// 			str = dbDialect.GenerateDeleteSql(tgtTable, tgtRow)
			// 			sqlFile.WriteString(str + "\n")
			// 		case diff.DiffTypeModify:
			// 			str = dbDialect.GenerateUpdateSql(tgtTable, srcRow, rule.ComparisonKey)
			// 			sqlFile.WriteString(str + "\n")
			// 		}
			// 	})
			// log.Printf("Time taken: %v\n", time.Since(start))
			// if err != nil {
			// 	log.Printf("Error comparing data for table %s: %v\n", rule.Table, err)
			// 	continue
			// }
		}
	},
}

func init() {
	diffDataCmd.Flags().StringP("config", "c", "", "Path to config file")
	diffDataCmd.Flags().StringP("rules", "r", "", "Path to rules file")
	diffDataCmd.Flags().Int("batch-size", 1000, "Batch size for data diff and SQL output")
	diffDataCmd.MarkFlagRequired("config")
	diffDataCmd.MarkFlagRequired("rules")
}
