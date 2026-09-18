package migrate

import (
	"fmt"
	"os"

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
	Run: func(cmd *cobra.Command, args []string) {
		configPath, _ := cmd.Flags().GetString("config")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			logger.Errorf("Error loading config: %v", err)
			os.Exit(1)
		}

		tgtDB, err := db.NewDBAdapter(&cfg.TargetDB)
		if err != nil {
			logger.Errorf("Error connecting to target DB: %v", err)
			os.Exit(1)
		}
		defer tgtDB.Close()

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		dir, _ := cmd.Flags().GetString("dir")
		targetVersion, _ := cmd.Flags().GetString("version")

		err = runMigrations(tgtDB, dir, dryRun, targetVersion)
		if err != nil {
			logger.Errorf("Error running migrations: %v", err)
			os.Exit(1)
		}

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

func runMigrations(db conn.DBAdapter, dir string, dryRun bool, targetVersion string) error {
	logger.Infof("开始执行迁移, 脚本目录: %s", dir)
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

	logger.Info("创建或更新配置表")
	err = migrate.EnsureVersionTable(db)
	if err != nil {
		return err
	}
	applied, err := migrate.SuccessfulMigrations(db)
	if err != nil {
		return err
	}
	if targetVersion == "" && len(files) > 0 {
		targetVersion = files[len(files)-1].Version
	}
	var pendingFiles []*migrate.MigrationFile
	for _, f := range files {
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
	logger.Infof("获取待执行的迁移文件: %d", len(pendingFiles))
	if dryRun {
		logger.Info("开始执行迁移(预览模式)")
		err = migrate.DryRunMigrations(db, pendingFiles)
	} else {
		logger.Info("开始执行迁移(执行模式)")
		err = migrate.ApplyMigrations(db, pendingFiles)
	}
	logger.Info("迁移完成")
	return err
}
