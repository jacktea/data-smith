package diff

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/diff"
	"github.com/jacktea/data-smith/pkg/sql"

	"github.com/spf13/cobra"
)

var diffDataCmd = &cobra.Command{
	Use:   "diff-data",
	Short: "Compare table data and generate SQL diff",
	RunE:  runDiffData,
}

func runDiffData(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	rulesPath, _ := cmd.Flags().GetString("rules")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	enableChunkHash, _ := cmd.Flags().GetBool("chunk-hash")
	chunkSize, _ := cmd.Flags().GetInt("chunk-size")
	if err := validateDiffDataInputs(configPath, rulesPath, batchSize, chunkSize, enableChunkHash); err != nil {
		return err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	rules, err := config.LoadRules(rulesPath)
	if err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	if err := validateRules(rules); err != nil {
		return err
	}

	srcDB, err := db.NewDBAdapter(&cfg.SourceDB)
	if err != nil {
		return fmt.Errorf("connect to source DB: %w", err)
	}
	defer srcDB.Close()
	tgtDB, err := db.NewDBAdapter(&cfg.TargetDB)
	if err != nil {
		return fmt.Errorf("connect to target DB: %w", err)
	}
	defer tgtDB.Close()

	dbDialect := sql.NewDialect(cfg.TargetDB.Type)

	diffFile, _ := cmd.Flags().GetString("output")
	rollbackFile, _ := cmd.Flags().GetString("rollback-output")

	diffDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current working directory: %w", err)
	}
	if diffFile == "" {
		diffFile = fmt.Sprintf("%s/data_diff.sql", diffDir)
	}
	if rollbackFile == "" {
		rollbackFile = fmt.Sprintf("%s/data_diff_rollback.sql", diffDir)
	}
	log.Printf("Forward Diff file: %s\n", diffFile)
	log.Printf("Rollback Diff file: %s\n", rollbackFile)

	sqlFile, err := os.Create(diffFile)
	if err != nil {
		return fmt.Errorf("create sql file: %w", err)
	}
	defer sqlFile.Close()

	rollbackSqlFile, err := os.Create(rollbackFile)
	if err != nil {
		return fmt.Errorf("create rollback sql file: %w", err)
	}
	defer rollbackSqlFile.Close()

	type tableRollbackSQL struct {
		tableName string
		sqls      []string
	}
	var rollbackList []tableRollbackSQL

	for _, rule := range rules.Rules {
		tgtTable, err := tgtDB.ExtractTable(rule.Table)
		if err != nil || tgtTable == nil {
			log.Printf("Error extracting table %s: %v\n", rule.Table, err)
			continue
		}
		srcTable, _ := srcDB.ExtractTable(rule.Table)

		start := time.Now()
		var diffResult *diff.DataDiff
		compareRule := diff.CreateCompareRule(tgtTable, rule.ComparisonKey, rule.IgnoreColumns)
		var effectiveCols []string
		if allRule, ok := compareRule.(*diff.AllFieldsEqualRule); ok {
			effectiveCols = allRule.Columns
		} else {
			effectiveCols = rule.ComparisonKey
		}

		if enableChunkHash {
			diffResult, err = diff.StreamCompareDataToDiffWithChunkFilter(
				srcDB,
				tgtDB,
				compareRule,
				batchSize,
				chunkSize,
			)
		} else {
			diffResult, err = diff.StreamCompareDataToDiff(
				srcDB,
				tgtDB,
				compareRule,
				batchSize,
			)
		}
		if err != nil {
			return fmt.Errorf("compare data for table %s: %w", rule.Table, err)
		}
		log.Printf("Time taken: %v\n", time.Since(start))

		// 1. 写入正向升级 SQL
		sqlFile.WriteString(fmt.Sprintf("--- diff %s \n", rule.Table))
		for _, row := range diffResult.Dropped {
			sqlFile.WriteString(dbDialect.GenerateDeleteSql(tgtTable, row) + "\n")
		}
		for _, row := range diffResult.Added {
			sqlFile.WriteString(dbDialect.GenerateInsertSql(tgtTable, row) + "\n")
		}
		for _, row := range diffResult.Modified {
			colsToUpdate := row.ModifiedCols
			if len(colsToUpdate) == 0 {
				colsToUpdate = effectiveCols
			}
			sqlStr := dbDialect.GenerateUpdateSql(tgtTable, row.New, colsToUpdate)
			if sqlStr != "" {
				sqlFile.WriteString(sqlStr + "\n")
			}
		}

		// 2. 收集当前表反向还原 SQL
		var tblRollback []string
		// 反向步骤 1: 将修改的字段还原为 Old 值
		for _, row := range diffResult.Modified {
			colsToUpdate := row.ModifiedCols
			if len(colsToUpdate) == 0 {
				colsToUpdate = effectiveCols
			}
			sqlStr := dbDialect.GenerateUpdateSql(tgtTable, row.Old, colsToUpdate)
			if sqlStr != "" {
				tblRollback = append(tblRollback, sqlStr)
			}
		}
		// 反向步骤 2: 将正向新增的数据 DELETE 掉（按 tgtRow 主键）
		for _, row := range diffResult.Added {
			tblRollback = append(tblRollback, dbDialect.GenerateDeleteSql(tgtTable, row))
		}
		// 反向步骤 3: 将正向删除的数据 INSERT 插回（按 srcRow 完整数据）
		for _, row := range diffResult.Dropped {
			tblForInsert := tgtTable
			if srcTable != nil {
				tblForInsert = srcTable
			}
			tblRollback = append(tblRollback, dbDialect.GenerateInsertSql(tblForInsert, row))
		}

		if len(tblRollback) > 0 {
			rollbackList = append(rollbackList, tableRollbackSQL{
				tableName: rule.Table,
				sqls:      tblRollback,
			})
		}
	}

	// 3. 逆序写入回滚 SQL（表级倒序回滚，规避外键依赖冲突）
	for i := len(rollbackList) - 1; i >= 0; i-- {
		tb := rollbackList[i]
		rollbackSqlFile.WriteString(fmt.Sprintf("--- rollback %s \n", tb.tableName))
		for _, s := range tb.sqls {
			rollbackSqlFile.WriteString(s + "\n")
		}
	}
	return nil
}

func validateDiffDataInputs(configPath, rulesPath string, batchSize, chunkSize int, enableChunkHash bool) error {
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if enableChunkHash && chunkSize <= 0 {
		return fmt.Errorf("chunk size must be greater than zero when chunk hashing is enabled")
	}
	if strings.TrimSpace(configPath) == "" {
		return fmt.Errorf("config path is required")
	}
	if strings.TrimSpace(rulesPath) == "" {
		return fmt.Errorf("rules path is required")
	}
	return nil
}

func validateRules(rules *pkgconfig.RuleSet) error {
	if rules == nil {
		return fmt.Errorf("rules are required")
	}
	for i, rule := range rules.Rules {
		if strings.TrimSpace(rule.Table) == "" {
			return fmt.Errorf("rule %d table name is required", i)
		}
		for _, key := range rule.ComparisonKey {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("rule %d comparison keys must not be empty", i)
			}
		}
		for _, column := range rule.IgnoreColumns {
			if strings.TrimSpace(column) == "" {
				return fmt.Errorf("rule %d ignored columns must not be empty", i)
			}
		}
	}
	return nil
}

func init() {
	diffDataCmd.Flags().StringP("config", "c", "", "Path to config file")
	diffDataCmd.Flags().StringP("rules", "r", "", "Path to rules file")
	diffDataCmd.Flags().StringP("output", "o", "", "Path to output forward diff SQL file")
	diffDataCmd.Flags().String("rollback-output", "", "Path to output rollback SQL file")
	diffDataCmd.Flags().Int("batch-size", 1000, "Batch size for data diff and SQL output")
	diffDataCmd.Flags().Bool("chunk-hash", false, "Enable probabilistic chunk fingerprints after exact count/min/max checks (opt-in)")
	diffDataCmd.Flags().Int("chunk-size", 10000, "Chunk size for hash pre-filtering")
	diffDataCmd.MarkFlagRequired("config")
	diffDataCmd.MarkFlagRequired("rules")
}
