package exec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/logger"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

var execSQLCmd = &cobra.Command{
	Use:     "exec-sql [flags] [sql-file]",
	Aliases: []string{"exec", "execute-sql"},
	Short:   "Execute SQL file on source or target database",
	Long: `Execute a SQL file against either the source or target database defined in the configuration file.

Examples:
  # diff-data / diff-schema 生成的正向和回滚 SQL 都在源数据库执行
  datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source

  # 执行回滚 SQL 到源数据库
  datasmith exec-sql -c configs/config.yaml -f data_diff_rollback.sql -d source

  # 执行自定义 SQL 到目标数据库
  datasmith exec-sql -c configs/config.yaml -f custom.sql -d target

  # 模拟执行 (事务中执行后自动回滚)
  datasmith exec-sql -c configs/config.yaml -f schema_diff.sql -d source -n

  # 启用显式事务执行 (成功后提交，失败回滚)
  datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d source --tx`,
	RunE: runExecSQL,
}

func init() {
	execSQLCmd.Flags().StringP("config", "c", "", "Path to config file")
	execSQLCmd.Flags().StringP("file", "f", "", "Path to SQL file")
	execSQLCmd.Flags().StringP("db", "d", "", "Database to execute against (required): 'source' or 'target'")
	execSQLCmd.Flags().Bool("source", false, "Execute against source database (shortcut for -d source)")
	execSQLCmd.Flags().Bool("target", false, "Execute against target database (shortcut for -d target)")
	execSQLCmd.Flags().BoolP("dry-run", "n", false, "Dry run mode (execute inside a transaction and rollback)")
	execSQLCmd.Flags().Bool("tx", false, "Execute inside a transaction (commit on success, rollback on error)")

	_ = execSQLCmd.MarkFlagRequired("config")
}

func runExecSQL(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	filePath, _ := cmd.Flags().GetString("file")
	dbChoice, _ := cmd.Flags().GetString("db")
	sourceFlag, _ := cmd.Flags().GetBool("source")
	targetFlag, _ := cmd.Flags().GetBool("target")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	useTx, _ := cmd.Flags().GetBool("tx")

	// 支持位置参数作为 SQL 文件路径
	if filePath == "" && len(args) > 0 {
		filePath = args[0]
	}
	if filePath == "" {
		return errors.New("请指定 SQL 文件路径 (通过 -f/--file 参数或位置参数)")
	}

	// 数据库是破坏性操作的关键目标，禁止静默使用默认值。
	dbChoice, err := selectDBChoice(dbChoice, sourceFlag, targetFlag)
	if err != nil {
		return err
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("加载配置文件失败 (%s): %w", configPath, err)
	}

	targetConnCfg, dbLabel, err := resolveDBConfig(cfg, dbChoice)
	if err != nil {
		return err
	}

	sqlBytes, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取 SQL 文件失败 (%s): %w", filePath, err)
	}

	sqlContent := string(sqlBytes)
	if strings.TrimSpace(sqlContent) == "" {
		logger.Infof("SQL 文件为空 (%s)，无需执行", filePath)
		return nil
	}
	if err := validateDeclaredDatabase(sqlContent, dbLabel); err != nil {
		return fmt.Errorf("拒绝执行 SQL 文件 %s: %w", filePath, err)
	}

	logger.Infof("准备执行 SQL 文件: %s (大小: %d 字节)", filePath, len(sqlBytes))
	logger.Infof("目标数据库 [%s]: 类型=%s, 地址=%s:%d, 库名=%s, Schema=%s",
		dbLabel, targetConnCfg.Type, targetConnCfg.Host, targetConnCfg.Port, targetConnCfg.DBName, targetConnCfg.TableSchema)

	adapter, err := db.NewDBAdapterContext(cmd.Context(), targetConnCfg)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	defer adapter.Close()

	return ExecuteSQLContext(cmd.Context(), adapter, sqlContent, dryRun, useTx)
}

const executeOnMarker = "-- DATASMITH EXECUTE-ON:"

func selectDBChoice(dbChoice string, sourceFlag, targetFlag bool) (string, error) {
	choice := strings.ToLower(strings.TrimSpace(dbChoice))
	if sourceFlag && targetFlag {
		return "", errors.New("--source 与 --target 不能同时使用")
	}
	if sourceFlag {
		if choice != "" && choice != "source" && choice != "src" {
			return "", fmt.Errorf("数据库参数冲突: -d %s 与 --source", dbChoice)
		}
		return "source", nil
	}
	if targetFlag {
		if choice != "" && choice != "target" && choice != "tgt" {
			return "", fmt.Errorf("数据库参数冲突: -d %s 与 --target", dbChoice)
		}
		return "target", nil
	}
	if choice == "" {
		return "", errors.New("必须通过 -d source、-d target、--source 或 --target 明确指定执行数据库")
	}
	return choice, nil
}

func validateDeclaredDatabase(sqlContent, dbLabel string) error {
	declared := declaredDatabase(sqlContent)
	if declared == "" {
		return nil
	}
	if declared != "source" && declared != "target" {
		return fmt.Errorf("无效的 %s 标记值 %q", executeOnMarker, declared)
	}
	if declared != strings.ToLower(strings.TrimSpace(dbLabel)) {
		return fmt.Errorf("脚本声明只能在 %s 数据库执行，当前选择为 %s", declared, dbLabel)
	}
	return nil
}

func declaredDatabase(sqlContent string) string {
	const maxHeaderLines = 20
	for index, line := range strings.Split(sqlContent, "\n") {
		if index >= maxHeaderLines {
			break
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if len(trimmed) >= len(executeOnMarker) && strings.EqualFold(trimmed[:len(executeOnMarker)], executeOnMarker) {
			return strings.ToLower(strings.TrimSpace(trimmed[len(executeOnMarker):]))
		}
		if !strings.HasPrefix(trimmed, "--") {
			break
		}
	}
	return ""
}

// resolveDBConfig 解析待操作的数据库配置
func resolveDBConfig(cfg *pkgconfig.Config, dbChoice string) (*pkgconfig.ConnConfig, string, error) {
	switch strings.ToLower(strings.TrimSpace(dbChoice)) {
	case "source", "src":
		return &cfg.SourceDB, "source", nil
	case "target", "tgt":
		return &cfg.TargetDB, "target", nil
	default:
		return nil, "", fmt.Errorf("无效的数据库参数: '%s'，仅支持 'source' 或 'target'", dbChoice)
	}
}

// ExecuteSQL 执行指定的 SQL 语句内容
func ExecuteSQL(adapter conn.DBAdapter, sqlContent string, dryRun bool, useTx bool) error {
	return ExecuteSQLContext(context.Background(), adapter, sqlContent, dryRun, useTx)
}

func ExecuteSQLContext(ctx context.Context, adapter conn.DBAdapter, sqlContent string, dryRun bool, useTx bool) error {
	if ctx == nil {
		return errors.New("SQL execution context is required")
	}
	cfg := adapter.GetConfig()
	options := sqlScannerOptions{nestedBlockComments: true, dollarQuotes: true}
	if cfg != nil && cfg.Type == consts.DBTypeMySQL {
		options.hashLineComments = true
		options.dashDashRequiresSpace = true
		options.nestedBlockComments = false
		options.backslashQuoteEscapes = true
		options.dollarQuotes = false
		options.modeDependentEscapes = true
	}
	scan, err := scanSQLWithOptions(sqlContent, options)
	if err != nil {
		if dryRun || useTx {
			return fmt.Errorf("SQL 扫描失败: %w", err)
		}
		scan = sqlScan{diagnosticError: err}
	}
	if dryRun || useTx {
		if scan.hasModeDependentBackslashQuote {
			return errors.New("事务模式拒绝依赖数据库反斜杠转义模式的引号；请使用成对引号（例如 ''）或 PostgreSQL E'...' 语法")
		}
		if cfg != nil && cfg.Type == consts.DBTypeMySQL && scan.hasMySQLExecutableComment {
			return errors.New("MySQL 事务模式拒绝可执行注释 (/*! ... */ 或 /*M! ... */)，因为无法安全验证其事务行为")
		}
		if err := validateNoTransactionControl(scan); err != nil {
			return fmt.Errorf("事务模式拒绝显式事务控制且不会改写 SQL: %w", err)
		}
	}
	if dryRun {
		if cfg == nil {
			return errors.New("数据库配置不可用")
		}
		switch cfg.Type {
		case consts.DBTypeMySQL:
			if err := validateMySQLDryRun(scan); err != nil {
				return err
			}
		case consts.DBTypePostgres:
		default:
			return fmt.Errorf("不支持数据库类型 %q 的 dry-run", cfg.Type)
		}
	}

	connDB := adapter.GetConn()
	if connDB == nil {
		return errors.New("数据库连接不可用")
	}

	start := time.Now()

	if dryRun {
		logger.Info("开始模拟执行 SQL (Dry-run 模式，在事务中执行后自动回滚)...")
		tx, err := connDB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("开启模拟执行事务失败: %w", err)
		}
		defer func() {
			_ = tx.Rollback()
		}()

		if _, err := tx.ExecContext(ctx, sqlContent); err != nil {
			_ = tx.Rollback()
			return executionError("模拟执行 SQL 失败，已回滚", err, sqlContent, scan)
		}
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("模拟执行 SQL 成功但回滚失败: %w", err)
		}

		logger.Infof("模拟执行成功，所有操作已回滚，耗时: %v", time.Since(start))
		return nil
	}

	if useTx {
		logger.Info("开始在事务中执行 SQL...")
		tx, err := connDB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("开启事务失败: %w", err)
		}

		if _, err := tx.ExecContext(ctx, sqlContent); err != nil {
			_ = tx.Rollback()
			return executionError("事务执行 SQL 失败，已回滚", err, sqlContent, scan)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("提交事务失败: %w", err)
		}

		logger.Infof("SQL 事务执行成功并已提交，耗时: %v", time.Since(start))
		return nil
	}

	logger.Info("开始执行 SQL...")
	if _, err := connDB.ExecContext(ctx, sqlContent); err != nil {
		return executionError("执行 SQL 失败", err, sqlContent, scan)
	}

	logger.Infof("SQL 执行成功，耗时: %v", time.Since(start))
	return nil
}

func executionError(prefix string, driverErr error, sqlText string, scan sqlScan) error {
	if position := postgresErrorPosition(driverErr); position > 0 {
		if offset, ok := characterPositionToByteOffset(sqlText, position); ok {
			line, column := sqlLineColumn(sqlText, offset)
			for i, statement := range scan.statements {
				if offset >= statement.start && offset < statement.end {
					return fmt.Errorf("%s (第 %d 条语句，脚本第 %d 行第 %d 列): %w", prefix, i+1, line, column, driverErr)
				}
			}
			return fmt.Errorf("%s (驱动位置：脚本第 %d 行第 %d 列): %w", prefix, line, column, driverErr)
		}
	}
	if scan.diagnosticError != nil {
		return fmt.Errorf("%s (scanner 无法安全推导语句位置: %v): %w", prefix, scan.diagnosticError, driverErr)
	}

	switch len(scan.statements) {
	case 0:
		return fmt.Errorf("%s (未扫描到可执行语句): %w", prefix, driverErr)
	case 1:
		statement := scan.statements[0]
		return fmt.Errorf("%s (第 1 条语句，起始于脚本第 %d 行第 %d 列): %w", prefix, statement.startLine, statement.startColumn, driverErr)
	default:
		first := scan.statements[0]
		last := scan.statements[len(scan.statements)-1]
		return fmt.Errorf("%s (语句范围 1-%d，起始行 %d-%d；驱动未提供精确位置): %w", prefix, len(scan.statements), first.startLine, last.startLine, driverErr)
	}
}

func characterPositionToByteOffset(sqlText string, oneBasedPosition int) (int, bool) {
	if oneBasedPosition <= 0 {
		return 0, false
	}
	wanted := oneBasedPosition - 1
	character := 0
	for offset := range sqlText {
		if character == wanted {
			return offset, true
		}
		character++
	}
	if character == wanted {
		return len(sqlText), true
	}
	return 0, false
}

func postgresErrorPosition(err error) int {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return 0
	}
	position, parseErr := strconv.Atoi(pqErr.Position)
	if parseErr != nil || position <= 0 {
		return 0
	}
	return position
}

func sqlLineColumn(sqlText string, offset int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if offset > len(sqlText) {
		offset = len(sqlText)
	}
	line, column := 1, 1
	for _, character := range sqlText[:offset] {
		if character == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return line, column
}
