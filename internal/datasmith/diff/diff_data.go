package diff

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jacktea/data-smith/internal/config"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
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
	excludeFlag, _ := cmd.Flags().GetString("exclude-tables")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	enableChunkHash, _ := cmd.Flags().GetBool("chunk-hash")
	chunkSize, _ := cmd.Flags().GetInt("chunk-size")
	dmlBatchSize, _ := cmd.Flags().GetInt("dml-batch-size")
	bestEffort, _ := cmd.Flags().GetBool("best-effort")
	skipMissingTables, _ := cmd.Flags().GetBool("skip-missing-tables")
	if err := validateDiffDataInputs(configPath, batchSize, chunkSize, enableChunkHash); err != nil {
		return err
	}
	if err := validateDMLBatchSize(dmlBatchSize); err != nil {
		return err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	var rules *pkgconfig.RuleSet
	if strings.TrimSpace(rulesPath) == "" {
		// 规则省略：整库模式，展开为全部有行身份的表（排除默认账本表）。
		rules = &pkgconfig.RuleSet{}
	} else {
		rules, err = config.LoadRules(rulesPath)
		if err != nil {
			return fmt.Errorf("load rules: %w", err)
		}
	}
	if err := validateRules(rules); err != nil {
		return err
	}

	diffDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current working directory: %w", err)
	}
	diffFile, _ := cmd.Flags().GetString("output")
	rollbackFile, _ := cmd.Flags().GetString("rollback-output")
	if diffFile == "" {
		diffFile = filepath.Join(diffDir, DataDiffForwardFile)
	}
	if rollbackFile == "" {
		rollbackFile = filepath.Join(diffDir, DataDiffRollbackFile)
	}
	log.Printf("Forward Diff file: %s\n", diffFile)
	log.Printf("Rollback Diff file: %s\n", rollbackFile)

	result, err := RunDataDiff(cmd.Context(), DataDiffParams{
		Source:            &cfg.SourceDB,
		Target:            &cfg.TargetDB,
		Rules:             rules.Rules,
		ExcludeTables:     append(cfg.ExcludeTables, splitCSVExcludeTables(excludeFlag)...),
		BatchSize:         batchSize,
		ChunkSize:         chunkSize,
		DMLBatchSize:      dmlBatchSize,
		ChunkHash:         enableChunkHash,
		BestEffort:        bestEffort,
		SkipMissingTables: skipMissingTables,
		ForwardPath:       diffFile,
		RollbackPath:      rollbackFile,
	}, diffDir, func(event TableProgress) {
		if event.Phase == "log" {
			log.Printf("%s\n", event.Error)
		}
		if event.Phase == "skipped" {
			log.Printf("WARNING: %s\n", event.Error)
		}
	})
	if err != nil {
		return err
	}
	failed := 0
	for _, table := range result.Tables {
		if table.Status != "failed" {
			continue
		}
		failed++
		log.Printf("  - %s: %s", table.Table, table.Error)
	}
	if failed > 0 {
		log.Printf("WARNING: data diff is INCOMPLETE (--best-effort); %d table(s) failed:", failed)
	}
	for _, name := range result.SkippedTables {
		log.Printf("WARNING: 表 %s 不存在, 已按 --skip-missing-tables 跳过; 该表将由后续版本的迁移轮次同步\n", name)
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
		if _, err := fmt.Fprintf(rollback, "-- rollback %s \n", table.tableName); err != nil {
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
	if _, err := fmt.Fprintf(writer, "-- diff %s \n", table); err != nil {
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

func validateDiffDataInputs(configPath string, batchSize, chunkSize int, enableChunkHash bool) error {
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if enableChunkHash && chunkSize <= 0 {
		return fmt.Errorf("chunk size must be greater than zero when chunk hashing is enabled")
	}
	if strings.TrimSpace(configPath) == "" {
		return fmt.Errorf("config path is required")
	}
	return nil
}

// splitCSVExcludeTables 解析逗号分隔的 --exclude-tables 取值，忽略空白项。
func splitCSVExcludeTables(raw string) []string {
	items := []string{}
	for _, item := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func validateRules(rules *pkgconfig.RuleSet) error {
	if rules == nil {
		return fmt.Errorf("rules are required")
	}
	for i, rule := range rules.Rules {
		if isWildcardPattern(rule.Table) {
			if err := validateWildcardRules([]pkgconfig.Rule{rule}); err != nil {
				return fmt.Errorf("rule %d: %w", i, err)
			}
			continue
		}
		if strings.TrimSpace(rule.Table) == "" {
			return fmt.Errorf("rule %d table name is required", i)
		}
		for _, key := range rule.ComparisonKey {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("rule %d comparison keys must not be empty", i)
			}
		}
		for _, column := range rule.Columns {
			if strings.TrimSpace(column) == "" {
				return fmt.Errorf("rule %d compare columns must not be empty", i)
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
	diffDataCmd.Flags().StringP("rules", "r", "", "Path to rules file; omit for whole-database mode (all tables with row identity, ledger tables excluded)")
	diffDataCmd.Flags().String("exclude-tables", "", "Comma-separated tables to exclude from data comparison (ledger tables are always excluded)")
	diffDataCmd.Flags().StringP("output", "o", "", "Path to output forward diff SQL file")
	diffDataCmd.Flags().String("rollback-output", "", "Path to output rollback SQL file")
	diffDataCmd.Flags().Int("batch-size", 1000, "Batch size for data diff and SQL output")
	diffDataCmd.Flags().Bool("chunk-hash", false, "Enable probabilistic chunk fingerprints after exact count/min/max checks (opt-in)")
	diffDataCmd.Flags().Int("chunk-size", 10000, "Chunk size for hash pre-filtering")
	diffDataCmd.Flags().Int("dml-batch-size", 1000, "Maximum rows per generated multi-row INSERT or DELETE (hard limit 10000)")
	diffDataCmd.Flags().Bool("best-effort", false, "Continue after table errors and emit an explicitly incomplete report")
	diffDataCmd.Flags().Bool("skip-missing-tables", false, "Skip rules whose table does not exist on either side and log a warning list instead of failing")
	diffDataCmd.MarkFlagRequired("config")
}
