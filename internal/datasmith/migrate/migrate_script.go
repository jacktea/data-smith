package migrate

import (
	"context"
	"fmt"

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

// RunMigrations applies forward migrations from dir up to targetVersion (the
// latest version when empty). Down scripts in dir are validated but never
// applied here; they only run through RollbackLatest.
func RunMigrations(ctx context.Context, db conn.DBAdapter, dir string, dryRun bool, targetVersion string, progress func(string)) error {
	report := progressLogger(progress)
	report(fmt.Sprintf("开始执行迁移, 脚本目录: %s", dir))
	files, err := local.ScanMigrations(dir)
	if err != nil {
		return err
	}
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
	files, err := local.ScanMigrations(dir)
	if err != nil {
		return "", err
	}
	local.SortMigrations(files)
	rolled, err := migrate.RollbackLatestMigration(db, files)
	if err != nil {
		return "", err
	}
	report(fmt.Sprintf("版本 %s 回退完成", rolled))
	return rolled, nil
}
