package diff

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jacktea/data-smith/internal/config"
	local "github.com/jacktea/data-smith/internal/datasmith/migrate/local"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"

	"github.com/spf13/cobra"
)

var diffFullCmd = &cobra.Command{
	Use:   "diff-full",
	Short: "Compare schema and data in one pass and generate forward/rollback SQL for both",
	RunE:  runDiffFull,
}

// versionOnlyPattern constrains --version to dotted numbers (optionally
// v-prefixed), matching the migration filename grammar.
var versionOnlyPattern = regexp.MustCompile(`^[vV]?(\d+(?:\.\d+)*)$`)

func runDiffFull(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	rulesPath, _ := cmd.Flags().GetString("rules")
	excludeFlag, _ := cmd.Flags().GetString("exclude-tables")
	dataDiffMode, _ := cmd.Flags().GetString("data-diff-mode")
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
	if err := ValidateDataDiffMode(dataDiffMode); err != nil {
		return err
	}
	migrateDir, _ := cmd.Flags().GetString("migrate-dir")
	version, _ := cmd.Flags().GetString("version")
	title, _ := cmd.Flags().GetString("title")
	if err := validateMigrationImport(migrateDir, version, title); err != nil {
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

	diffDir, err := resolveDiffFullOutputDir(cmd)
	if err != nil {
		return err
	}
	log.Printf("Output directory: %s\n", diffDir)

	result, err := RunFullDiff(cmd.Context(), FullDiffParams{
		Source:            &cfg.SourceDB,
		Target:            &cfg.TargetDB,
		IncludeTables:     cfg.IncludeTables,
		ExcludeTables:     append(cfg.ExcludeTables, splitCSVExcludeTables(excludeFlag)...),
		Rules:             rules.Rules,
		DataDiffMode:      dataDiffMode,
		BatchSize:         batchSize,
		ChunkSize:         chunkSize,
		DMLBatchSize:      dmlBatchSize,
		ChunkHash:         enableChunkHash,
		BestEffort:        bestEffort,
		SkipMissingTables: skipMissingTables,
	}, diffDir, func(message string) { log.Printf("%s\n", message) }, nil)
	if err != nil {
		return err
	}

	log.Printf("结构比对完成: 新增 %d 张表, 删除 %d 张表, 修改 %d 张表",
		len(result.Schema.TablesAdded), len(result.Schema.TablesDropped), len(result.Schema.TablesModified))
	if result.Schema.Destructive {
		log.Printf("WARNING: schema diff contains destructive changes (dropped tables/columns)\n")
	}
	failed := 0
	for _, table := range result.Data.Tables {
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
		log.Printf("跳过数据比对表 %s: 仅单侧存在(结构差异已由结构比对覆盖)\n", name)
	}
	log.Printf("Forward diff SQL: %s\n", filepath.Join(diffDir, SchemaDiffForwardFile))
	log.Printf("Schema rollback SQL: %s\n", filepath.Join(diffDir, SchemaDiffRollbackFile))
	log.Printf("Data diff SQL: %s\n", filepath.Join(diffDir, DataDiffForwardFile))
	log.Printf("Data rollback SQL: %s\n", filepath.Join(diffDir, DataDiffRollbackFile))

	if migrateDir != "" {
		upFile, downFile, err := importMigrationVersion(diffDir, migrateDir, version, title)
		if err != nil {
			return err
		}
		log.Printf("Migration version scripts generated (execute via migrate-script): %s, %s\n", upFile, downFile)
	}
	return nil
}

// resolveDiffFullOutputDir returns the directory that receives the four diff
// artifacts: --output when given, the current working directory otherwise.
func resolveDiffFullOutputDir(cmd *cobra.Command) (string, error) {
	output, _ := cmd.Flags().GetString("output")
	if output == "" {
		return os.Getwd()
	}
	abs, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}
	return abs, nil
}

// validateMigrationImport enforces that migration import flags are given as a
// complete set and carry a grammar-valid version and title.
func validateMigrationImport(migrateDir, version, title string) error {
	if migrateDir == "" {
		if version != "" || title != "" {
			return fmt.Errorf("--version and --title require --migrate-dir")
		}
		return nil
	}
	if !versionOnlyPattern.MatchString(strings.TrimSpace(version)) {
		return fmt.Errorf("--version must be a dotted number (e.g. 1.0) when --migrate-dir is set")
	}
	if title == "" || strings.ContainsAny(title, ". \t/") {
		return fmt.Errorf("--title must be non-empty and must not contain dots, whitespace or path separators")
	}
	return nil
}

// importMigrationVersion assembles the four diff artifacts in diffDir into a
// V{version}__{title} up/down pair inside migrateDir. It refuses duplicate
// versions and writes nothing unless both scripts can be produced.
func importMigrationVersion(diffDir, migrateDir, version, title string) (string, string, error) {
	if err := os.MkdirAll(migrateDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create migration directory: %w", err)
	}
	existing, _, err := local.ScanMigrations(migrateDir)
	if err != nil {
		return "", "", fmt.Errorf("scan migration directory: %w", err)
	}
	normalized := strings.TrimPrefix(strings.ToLower(version), "v")
	for _, file := range existing {
		if file.Direction == "down" {
			continue
		}
		if strings.TrimPrefix(strings.ToLower(file.Version), "v") == normalized {
			return "", "", fmt.Errorf("migration version %s already exists in %s (%s)", version, migrateDir, filepath.Base(file.Path))
		}
	}

	readArtifact := func(name string) (string, error) {
		raw, err := os.ReadFile(filepath.Join(diffDir, name))
		if err != nil {
			return "", fmt.Errorf("read diff artifact %s: %w", name, err)
		}
		return string(raw), nil
	}
	schemaForward, err := readArtifact(SchemaDiffForwardFile)
	if err != nil {
		return "", "", err
	}
	schemaRollback, err := readArtifact(SchemaDiffRollbackFile)
	if err != nil {
		return "", "", err
	}
	dataForward, err := readArtifact(DataDiffForwardFile)
	if err != nil {
		return "", "", err
	}
	dataRollback, err := readArtifact(DataDiffRollbackFile)
	if err != nil {
		return "", "", err
	}

	upContent, downContent := AssembleVersionScripts(true, true, schemaForward, schemaRollback, dataForward, dataRollback)
	upFile := fmt.Sprintf("V%s__%s.up.sql", version, title)
	downFile := fmt.Sprintf("V%s__%s.down.sql", version, title)
	if err := os.WriteFile(filepath.Join(migrateDir, upFile), []byte(upContent), 0o644); err != nil {
		return "", "", fmt.Errorf("write up script: %w", err)
	}
	if err := os.WriteFile(filepath.Join(migrateDir, downFile), []byte(downContent), 0o644); err != nil {
		return "", "", fmt.Errorf("write down script: %w", err)
	}
	return upFile, downFile, nil
}

func init() {
	diffFullCmd.Flags().StringP("config", "c", "", "Path to config file")
	diffFullCmd.Flags().StringP("rules", "r", "", "Path to rules file; omit for whole-database mode (all tables with row identity, ledger tables excluded)")
	diffFullCmd.Flags().String("exclude-tables", "", "Comma-separated tables to exclude from schema and data comparison (ledger tables are always excluded)")
	diffFullCmd.Flags().String("data-diff-mode", DataDiffModeAuto, "Data comparison mode: auto (shadow transaction for PostgreSQL sources with schema changes), shadow, or direct")
	diffFullCmd.Flags().StringP("output", "o", "", "Directory for the four diff artifacts (default: current directory)")
	diffFullCmd.Flags().Int("batch-size", 1000, "Batch size for data diff and SQL output")
	diffFullCmd.Flags().Bool("chunk-hash", false, "Enable probabilistic chunk fingerprints after exact count/min/max checks (opt-in)")
	diffFullCmd.Flags().Int("chunk-size", 10000, "Chunk size for hash pre-filtering")
	diffFullCmd.Flags().Int("dml-batch-size", 1000, "Maximum rows per generated multi-row INSERT or DELETE (hard limit 10000)")
	diffFullCmd.Flags().Bool("best-effort", false, "Continue after table errors and emit an explicitly incomplete report")
	diffFullCmd.Flags().Bool("skip-missing-tables", false, "Skip data rules whose table does not exist on either side and log a warning list instead of failing")
	diffFullCmd.Flags().String("migrate-dir", "", "Migration script directory; when set, also assemble a V{version}__{title} up/down pair into it")
	diffFullCmd.Flags().String("version", "", "Migration version for --migrate-dir (dotted number, e.g. 1.0)")
	diffFullCmd.Flags().String("title", "", "Migration title for --migrate-dir (no dots or whitespace)")
	diffFullCmd.MarkFlagRequired("config")
}
