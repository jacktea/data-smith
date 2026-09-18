package exec

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jacktea/data-smith/internal/config"
	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/logger"
	"github.com/jacktea/data-smith/pkg/utils"
	"github.com/spf13/cobra"
)

var execSQLCmd = &cobra.Command{
	Use:     "exec-sql [flags] [sql-file]",
	Aliases: []string{"exec", "execute-sql"},
	Short:   "Execute SQL file on source or target database",
	Long: `Execute a SQL file against either the source or target database defined in the configuration file.

Examples:
  # 执行 SQL 文件到目标数据库 (默认)
  datasmith exec-sql -c configs/config.yaml -f data_diff.sql -d target

  # 执行 SQL 文件到源数据库
  datasmith exec-sql -c configs/config.yaml -f data_diff_rollback.sql -d source

  # 通过位置参数传入 SQL 文件
  datasmith exec-sql -c configs/config.yaml data_diff.sql

  # 模拟执行 (事务中执行后自动回滚)
  datasmith exec-sql -c configs/config.yaml -f schema_diff.sql -n

  # 启用显式事务执行 (成功后提交，失败回滚)
  datasmith exec-sql -c configs/config.yaml -f data_diff.sql --tx`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runExecSQL(cmd, args); err != nil {
			logger.Errorf("执行失败: %v", err)
			os.Exit(1)
		}
	},
}

func init() {
	execSQLCmd.Flags().StringP("config", "c", "", "Path to config file")
	execSQLCmd.Flags().StringP("file", "f", "", "Path to SQL file")
	execSQLCmd.Flags().StringP("db", "d", "target", "Database to execute against: 'source' or 'target'")
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

	// 判定选择的数据库
	if sourceFlag {
		dbChoice = "source"
	} else if targetFlag {
		dbChoice = "target"
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

	sqlContent := strings.TrimSpace(string(sqlBytes))
	if sqlContent == "" {
		logger.Infof("SQL 文件为空 (%s)，无需执行", filePath)
		return nil
	}

	logger.Infof("准备执行 SQL 文件: %s (大小: %d 字节)", filePath, len(sqlBytes))
	logger.Infof("目标数据库 [%s]: 类型=%s, 地址=%s:%d, 库名=%s, Schema=%s",
		dbLabel, targetConnCfg.Type, targetConnCfg.Host, targetConnCfg.Port, targetConnCfg.DBName, targetConnCfg.TableSchema)

	adapter, err := db.NewDBAdapter(targetConnCfg)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	defer adapter.Close()

	return ExecuteSQL(adapter, sqlContent, dryRun, useTx)
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
	connDB := adapter.GetConn()
	if connDB == nil {
		return errors.New("数据库连接不可用")
	}

	start := time.Now()

	if dryRun {
		logger.Info("开始模拟执行 SQL (Dry-run 模式，在事务中执行后自动回滚)...")
		tx, err := connDB.Begin()
		if err != nil {
			return fmt.Errorf("开启模拟执行事务失败: %w", err)
		}
		defer func() {
			_ = tx.Rollback()
		}()

		cleanedSQL := utils.CleanTransaction(sqlContent)
		if _, err := tx.Exec(cleanedSQL); err != nil {
			return fmt.Errorf("模拟执行 SQL 失败: %w", err)
		}

		logger.Infof("模拟执行成功，所有操作已回滚，耗时: %v", time.Since(start))
		return nil
	}

	if useTx {
		logger.Info("开始在事务中执行 SQL...")
		tx, err := connDB.Begin()
		if err != nil {
			return fmt.Errorf("开启事务失败: %w", err)
		}

		cleanedSQL := utils.CleanTransaction(sqlContent)
		if _, err := tx.Exec(cleanedSQL); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("事务执行 SQL 失败，已回滚: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("提交事务失败: %w", err)
		}

		logger.Infof("SQL 事务执行成功并已提交，耗时: %v", time.Since(start))
		return nil
	}

	logger.Info("开始执行 SQL...")
	if _, err := connDB.Exec(sqlContent); err != nil {
		// 尝试定位出错的语句片段
		stmts := splitStatements(sqlContent)
		for idx, stmt := range stmts {
			// 用只包含单语句的测试尝试复现并定位具体失败语句
			if dryTx, dryErr := connDB.Begin(); dryErr == nil {
				if _, sErr := dryTx.Exec(stmt); sErr != nil {
					_ = dryTx.Rollback()
					logger.Errorf("第 %d 条 SQL 语句执行失败:\n>>> %s\n", idx+1, stmt)
					return fmt.Errorf("执行 SQL 失败 (第 %d 条语句): %w", idx+1, sErr)
				}
				_ = dryTx.Rollback()
			}
		}
		return fmt.Errorf("执行 SQL 失败: %w", err)
	}

	logger.Infof("SQL 执行成功，耗时: %v", time.Since(start))
	return nil
}

func splitStatements(sqlText string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	var quoteChar rune

	runes := []rune(sqlText)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if inQuote {
			current.WriteRune(r)
			if r == quoteChar {
				// 处理转义单引号 ''
				if r == '\'' && i+1 < len(runes) && runes[i+1] == '\'' {
					current.WriteRune('\'')
					i++
				} else {
					inQuote = false
				}
			}
		} else {
			if r == '\'' || r == '"' {
				inQuote = true
				quoteChar = r
				current.WriteRune(r)
			} else if r == ';' {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					result = append(result, stmt+";")
				}
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		}
	}
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			result = append(result, stmt)
		}
	}
	return result
}
