package diff

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
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

type tableDiffFailure struct {
	table string
	err   error
}

type tableDiffResult struct {
	target        *conn.Table
	source        *conn.Table
	diff          *diff.DataDiff
	effectiveCols []string
}

type tableDiffFunc func(pkgconfig.Rule) (*tableDiffResult, error)

func runDiffData(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	rulesPath, _ := cmd.Flags().GetString("rules")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	enableChunkHash, _ := cmd.Flags().GetBool("chunk-hash")
	chunkSize, _ := cmd.Flags().GetInt("chunk-size")
	dmlBatchSize, _ := cmd.Flags().GetInt("dml-batch-size")
	bestEffort, _ := cmd.Flags().GetBool("best-effort")
	if err := validateDiffDataInputs(configPath, rulesPath, batchSize, chunkSize, enableChunkHash); err != nil {
		return err
	}
	if err := validateDMLBatchSize(dmlBatchSize); err != nil {
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

	srcDB, err := db.NewDBAdapterContext(cmd.Context(), &cfg.SourceDB)
	if err != nil {
		return fmt.Errorf("connect to source DB: %w", err)
	}
	defer srcDB.Close()
	tgtDB, err := db.NewDBAdapterContext(cmd.Context(), &cfg.TargetDB)
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

	sourceModels := newTableModelCache(srcDB)
	targetModels := newTableModelCache(tgtDB)
	prepareTable := func(rule pkgconfig.Rule) (*tableModels, error) {
		tgtTable, err := targetModels.get(rule.Table)
		if err != nil {
			return nil, fmt.Errorf("extract target table: %w", err)
		}
		if tgtTable == nil {
			return nil, fmt.Errorf("extract target table: table not found")
		}
		srcTable, err := sourceModels.get(rule.Table)
		if err != nil {
			return nil, fmt.Errorf("extract source table: %w", err)
		}
		compareRule := diff.CreateCompareRule(tgtTable, rule.ComparisonKey, rule.IgnoreColumns)
		var effectiveCols []string
		if allRule, ok := compareRule.(*diff.AllFieldsEqualRule); ok {
			effectiveCols = allRule.Columns
		} else {
			effectiveCols = rule.ComparisonKey
		}
		return &tableModels{target: tgtTable, source: srcTable, effectiveCols: effectiveCols}, nil
	}
	compareTable := func(rule pkgconfig.Rule, models *tableModels, handle diff.DetailedDiffErrorHandler) error {
		start := time.Now()
		compareRule := diff.CreateCompareRule(models.target, rule.ComparisonKey, rule.IgnoreColumns)
		var err error
		if enableChunkHash {
			err = diff.StreamCompareDataWithChunkFilterAndTableContext(
				cmd.Context(),
				srcDB,
				tgtDB,
				compareRule,
				models.target,
				batchSize,
				chunkSize,
				handle,
			)
		} else {
			err = diff.StreamCompareDataDetailedWithTableContext(
				cmd.Context(),
				srcDB,
				tgtDB,
				compareRule,
				models.target,
				batchSize,
				handle,
			)
		}
		if err != nil {
			return fmt.Errorf("compare data: %w", err)
		}
		log.Printf("Table %s compared in %v\n", rule.Table, time.Since(start))
		return nil
	}

	var failures []tableDiffFailure
	err = writeAtomicPair(diffFile, rollbackFile, func(forward, rollback io.Writer) error {
		var generateErr error
		failures, generateErr = generateStreamingDataDiffOutputs(
			forward,
			rollback,
			filepath.Dir(rollbackFile),
			rules.Rules,
			dbDialect,
			dmlBatchSize,
			bestEffort,
			prepareTable,
			compareTable,
		)
		return generateErr
	})
	if err != nil {
		return err
	}
	if len(failures) > 0 {
		log.Printf("WARNING: data diff is INCOMPLETE (--best-effort); %d table(s) failed:", len(failures))
		for _, failure := range failures {
			log.Printf("  - %s: %v", failure.table, failure.err)
		}
	}
	return nil
}

type tableRollbackSQL struct {
	tableName string
	sqls      []string
}

func generateDataDiffOutputs(
	forward io.Writer,
	rollback io.Writer,
	rules []pkgconfig.Rule,
	dialect sql.IDialect,
	bestEffort bool,
	compareTable tableDiffFunc,
) ([]tableDiffFailure, error) {
	if _, err := fmt.Fprintln(forward, executeOnSourceHeader); err != nil {
		return nil, fmt.Errorf("write forward execution target: %w", err)
	}
	if _, err := fmt.Fprintln(rollback, executeOnSourceHeader); err != nil {
		return nil, fmt.Errorf("write rollback execution target: %w", err)
	}
	var failures []tableDiffFailure
	var rollbackList []tableRollbackSQL

	for _, rule := range rules {
		result, err := compareTable(rule)
		if err != nil {
			failure := tableDiffFailure{table: rule.Table, err: err}
			failures = append(failures, failure)
			if !bestEffort {
				return failures, fmt.Errorf("diff table %s: %w", rule.Table, err)
			}
			continue
		}

		if err := writeDataDiffTable(forward, rule.Table, result, dialect); err != nil {
			return failures, fmt.Errorf("write forward diff for table %s: %w", rule.Table, err)
		}
		rollbackList = append(rollbackList, buildTableRollback(rule.Table, result, dialect))
	}

	for i := len(rollbackList) - 1; i >= 0; i-- {
		table := rollbackList[i]
		if len(table.sqls) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(rollback, "--- rollback %s \n", table.tableName); err != nil {
			return failures, err
		}
		for _, statement := range table.sqls {
			if _, err := fmt.Fprintln(rollback, statement); err != nil {
				return failures, err
			}
		}
	}

	if err := writeCompletionReport(forward, bestEffort, failures); err != nil {
		return failures, fmt.Errorf("write forward completion report: %w", err)
	}
	if err := writeCompletionReport(rollback, bestEffort, failures); err != nil {
		return failures, fmt.Errorf("write rollback completion report: %w", err)
	}
	return failures, nil
}

func writeDataDiffTable(writer io.Writer, table string, result *tableDiffResult, dialect sql.IDialect) error {
	if _, err := fmt.Fprintf(writer, "--- diff %s \n", table); err != nil {
		return err
	}
	for _, row := range result.diff.Dropped {
		if _, err := fmt.Fprintln(writer, dialect.GenerateDeleteSql(result.target, row)); err != nil {
			return err
		}
	}
	for _, row := range result.diff.Added {
		if _, err := fmt.Fprintln(writer, dialect.GenerateInsertSql(result.target, row)); err != nil {
			return err
		}
	}
	for _, row := range result.diff.Modified {
		colsToUpdate := row.ModifiedCols
		if len(colsToUpdate) == 0 {
			colsToUpdate = result.effectiveCols
		}
		if statement := dialect.GenerateUpdateSql(result.target, row.New, colsToUpdate); statement != "" {
			if _, err := fmt.Fprintln(writer, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildTableRollback(table string, result *tableDiffResult, dialect sql.IDialect) tableRollbackSQL {
	rollback := tableRollbackSQL{tableName: table}
	for _, row := range result.diff.Modified {
		colsToUpdate := row.ModifiedCols
		if len(colsToUpdate) == 0 {
			colsToUpdate = result.effectiveCols
		}
		if statement := dialect.GenerateUpdateSql(result.target, row.Old, colsToUpdate); statement != "" {
			rollback.sqls = append(rollback.sqls, statement)
		}
	}
	for _, row := range result.diff.Added {
		rollback.sqls = append(rollback.sqls, dialect.GenerateDeleteSql(result.target, row))
	}
	for _, row := range result.diff.Dropped {
		tableForInsert := result.target
		if result.source != nil {
			tableForInsert = result.source
		}
		rollback.sqls = append(rollback.sqls, dialect.GenerateInsertSql(tableForInsert, row))
	}
	return rollback
}

func writeCompletionReport(writer io.Writer, bestEffort bool, failures []tableDiffFailure) error {
	if len(failures) == 0 {
		_, err := fmt.Fprintln(writer, "-- DATASMITH RESULT: COMPLETE")
		return err
	}
	if !bestEffort {
		return fmt.Errorf("internal error: failures cannot be published without --best-effort")
	}
	if _, err := fmt.Fprintf(writer, "-- DATASMITH RESULT: INCOMPLETE (--best-effort); %d TABLE(S) FAILED\n", len(failures)); err != nil {
		return err
	}
	for _, failure := range failures {
		if _, err := fmt.Fprintf(writer, "-- FAILED TABLE %s: %v\n", failure.table, failure.err); err != nil {
			return err
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
	diffDataCmd.Flags().Int("dml-batch-size", 1000, "Maximum rows per generated multi-row INSERT or DELETE (hard limit 10000)")
	diffDataCmd.Flags().Bool("best-effort", false, "Continue after table errors and emit an explicitly incomplete report")
	diffDataCmd.MarkFlagRequired("config")
	diffDataCmd.MarkFlagRequired("rules")
}
