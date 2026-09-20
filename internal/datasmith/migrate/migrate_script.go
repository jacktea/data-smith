package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/internal/datasmith/migrate/local"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/logger"
	"github.com/jacktea/data-smith/pkg/migrate"
	"github.com/spf13/cobra"
)

var migrateScript = &cobra.Command{
	Use:   "migrate-script",
	Short: "Migration script to target database",
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		tgtDB, err := db.NewDBAdapterContext(cmd.Context(), &cfg.TargetDB)
		if err != nil {
			return fmt.Errorf("connect to target DB: %w", err)
		}
		defer tgtDB.Close()

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		dir, _ := cmd.Flags().GetString("dir")
		targetVersion, _ := cmd.Flags().GetString("version")

		if err := RunMigrations(cmd.Context(), tgtDB, dir, dryRun, targetVersion, nil); err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
		return nil
	},
}

func init() {
	migrateScript.Flags().StringP("config", "c", "", "Path to config file")
	migrateScript.Flags().StringP("dir", "d", "", "Path to migration script directory")
	migrateScript.Flags().StringP("version", "v", "", "Target version")
	migrateScript.Flags().BoolP("dry-run", "n", false, "Dry run")
	migrateScript.MarkFlagRequired("config")
	migrateScript.MarkFlagRequired("dir")
}

func progressLogger(progress func(string)) func(string) {
	return func(message string) {
		logger.Info(message)
		if progress != nil {
			progress(message)
		}
	}
}

// warnSkippedMigrations 把扫描跳过的不合规迁移文件名输出为 warning：CLI 侧
// 走 logger.Warn，Web 侧经 progress 回调透出到任务日志。这些文件不参与
// 迁移链——若是命名笔误（如 V1.0.1_update.up.sql 单下划线），缺失的版本
// 会直接表现为迁移缺轮，必须在推进前可见。
func warnSkippedMigrations(progress func(string), skipped []string) {
	for _, name := range skipped {
		logger.Warnf("跳过不合规迁移文件名(不参与迁移链): %s", name)
		if progress != nil {
			progress(fmt.Sprintf("WARNING: 跳过不合规迁移文件名(不参与迁移链): %s", name))
		}
	}
}

// RunMigrations applies forward migrations from dir up to targetVersion (the
// latest version when empty). Down scripts in dir are validated but never
// applied here; they only run through RollbackLatest.
func RunMigrations(ctx context.Context, db conn.DBAdapter, dir string, dryRun bool, targetVersion string, progress func(string)) error {
	report := progressLogger(progress)
	report(fmt.Sprintf("开始执行迁移, 脚本目录: %s", dir))
	files, skipped, err := local.ScanMigrations(dir)
	if err != nil {
		return err
	}
	warnSkippedMigrations(progress, skipped)
	local.SortMigrations(files)
	// Read and validate every selected migration before creating or changing the
	// ledger. This guarantees JSON/unreadable/duplicate inputs fail pre-mutation.
	if err := migrate.PrepareMigrationFiles(files); err != nil {
		return err
	}

	var upFiles []*migrate.MigrationFile
	for _, f := range files {
		if f.Direction == "up" {
			upFiles = append(upFiles, f)
		}
	}

	report("创建或更新配置表")
	err = migrate.EnsureVersionTable(db)
	if err != nil {
		return err
	}
	applied, err := migrate.SuccessfulMigrations(db)
	if err != nil {
		return err
	}
	if targetVersion == "" && len(upFiles) > 0 {
		targetVersion = upFiles[len(upFiles)-1].Version
	}
	var pendingFiles []*migrate.MigrationFile
	for _, f := range upFiles {
		if targetVersion != "" && local.CompareVersion(f.Version, targetVersion) > 0 {
			continue
		}
		if checksum, ok := applied[f.Version]; ok {
			if checksum != "" && checksum != f.Checksum {
				return fmt.Errorf("migration version %q checksum drift: database=%s file=%s (%s)", f.Version, checksum, f.Checksum, f.Path)
			}
			continue
		}
		pendingFiles = append(pendingFiles, f)
	}
	report(fmt.Sprintf("获取待执行的迁移文件: %d", len(pendingFiles)))
	if dryRun {
		report("开始执行迁移(预览模式)")
		err = migrate.DryRunMigrations(db, pendingFiles)
	} else {
		report("开始执行迁移(执行模式)")
		err = migrate.ApplyMigrations(db, pendingFiles)
	}
	report("迁移完成")
	return err
}

// RollbackLatest executes the down script of the latest successfully applied
// version and marks it rolled_back in the ledger. It returns the rolled-back
// version.
func RollbackLatest(ctx context.Context, db conn.DBAdapter, dir string, progress func(string)) (string, error) {
	report := progressLogger(progress)
	report(fmt.Sprintf("开始回退最新版本, 脚本目录: %s", dir))
	files, skipped, err := local.ScanMigrations(dir)
	if err != nil {
		return "", err
	}
	warnSkippedMigrations(progress, skipped)
	local.SortMigrations(files)
	rolled, err := migrate.RollbackLatestMigration(db, files)
	if err != nil {
		return "", err
	}
	report(fmt.Sprintf("版本 %s 回退完成", rolled))
	return rolled, nil
}

// RollbackTo rolls back applied versions one by one, newest first, until
// targetVersion is the latest applied version. An empty targetVersion rolls
// back exactly the latest version. It returns the rolled-back versions in
// execution order.
func RollbackTo(ctx context.Context, db conn.DBAdapter, dir string, targetVersion string, progress func(string)) ([]string, error) {
	report := progressLogger(progress)
	if targetVersion == "" {
		report(fmt.Sprintf("开始回退最新版本, 脚本目录: %s", dir))
	} else {
		report(fmt.Sprintf("开始回退到版本 %s, 脚本目录: %s", targetVersion, dir))
	}
	files, skipped, err := local.ScanMigrations(dir)
	if err != nil {
		return nil, err
	}
	warnSkippedMigrations(progress, skipped)
	local.SortMigrations(files)
	rolled, err := migrate.RollbackToMigration(db, files, targetVersion)
	if err != nil {
		return nil, err
	}
	if len(rolled) == 0 {
		report(fmt.Sprintf("当前已处于版本 %s, 无需回退", targetVersion))
		return rolled, nil
	}
	report(fmt.Sprintf("回退完成, 共回退 %d 个版本: %s", len(rolled), strings.Join(rolled, " -> ")))
	return rolled, nil
}

// PlanRollback computes which versions would be rolled back to reach
// targetVersion (the latest single step when empty) without touching the
// database. It returns the planned versions, newest first.
func PlanRollback(ctx context.Context, db conn.DBAdapter, dir string, targetVersion string, progress func(string)) ([]string, error) {
	report := progressLogger(progress)
	report(fmt.Sprintf("生成回退计划, 脚本目录: %s", dir))
	files, skipped, err := local.ScanMigrations(dir)
	if err != nil {
		return nil, err
	}
	warnSkippedMigrations(progress, skipped)
	local.SortMigrations(files)
	planned, err := migrate.PlanRollbackTo(db, files, targetVersion)
	if err != nil {
		return nil, err
	}
	report(fmt.Sprintf("将依次回退 %d 个版本: %s", len(planned), strings.Join(planned, " -> ")))
	return planned, nil
}
