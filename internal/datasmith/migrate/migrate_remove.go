package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/internal/datasmith/migrate/local"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
	pkgmigrate "github.com/jacktea/data-smith/pkg/migrate"
	"github.com/spf13/cobra"
)

var migrateRemove = newMigrateRemoveCommand()

// newMigrateRemoveCommand builds the migrate-remove command: it deletes one
// schema_migrations ledger record so a revised script can re-migrate under the
// same version. Like migrate-rollback it refuses destructive execution without
// --yes; --dry-run previews the record and its warnings.
func newMigrateRemoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-remove",
		Short: "Delete a failed or rolled-back migration ledger record",
		RunE: func(cmd *cobra.Command, args []string) error {
			configPath, _ := cmd.Flags().GetString("config")
			version, _ := cmd.Flags().GetString("version")
			dir, _ := cmd.Flags().GetString("dir")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			if !dryRun && !yes {
				return fmt.Errorf("migrate-remove requires --yes for destructive execution")
			}
			if strings.TrimSpace(version) == "" {
				return fmt.Errorf("migrate-remove requires --version")
			}

			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			tgtDB, err := db.NewDBAdapterContext(cmd.Context(), &cfg.TargetDB)
			if err != nil {
				return fmt.Errorf("connect to target DB: %w", err)
			}
			defer tgtDB.Close()

			if dryRun {
				record, warnings, err := planRemove(cmd.Context(), tgtDB, dir, version)
				if err != nil {
					return fmt.Errorf("plan remove: %w", err)
				}
				out := cmd.OutOrStdout()
				if _, err := fmt.Fprintf(out, "将删除账本记录: 版本 %s (状态 %s)\n", record.Version, record.Status); err != nil {
					return err
				}
				for _, warning := range warnings {
					if _, err := fmt.Fprintf(out, "WARNING: %s\n", warning); err != nil {
						return err
					}
				}
				_, err = fmt.Fprintln(out, "预览模式,未执行删除;确认请加 --yes")
				return err
			}

			deleted, _, err := RemoveMigrationRecord(cmd.Context(), tgtDB, dir, version, nil)
			if err != nil {
				return fmt.Errorf("remove migration record: %w", err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "已删除版本 %s 的账本记录, 修正脚本后可重新迁移\n", deleted)
			return err
		},
	}
	cmd.Flags().StringP("config", "c", "", "Path to config file")
	cmd.Flags().StringP("dir", "d", "", "Path to migration script directory")
	cmd.Flags().StringP("version", "v", "", "Ledger version to delete (V-prefix and case tolerant)")
	cmd.Flags().BoolP("dry-run", "n", false, "Preview the record and warnings without deleting")
	cmd.Flags().Bool("yes", false, "Confirm destructive ledger deletion")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagRequired("dir")
	_ = cmd.MarkFlagRequired("version")
	return cmd
}

// RemoveMigrationRecord deletes the ledger record of version (only failed or
// rolled_back), emitting advisory warnings before the deletion:
//   - no up script for the version in dir: re-running the version is impossible
//   - newer successful versions exist: a re-run lands on top of the rollback stack
//   - MySQL failed records: DDL may have partially auto-committed
//
// It returns the exact deleted ledger version and the emitted warnings.
func RemoveMigrationRecord(ctx context.Context, db conn.DBAdapter, dir, version string, progress func(string)) (string, []string, error) {
	report := progressLogger(progress)
	files, skipped, err := local.ScanMigrations(dir)
	if err != nil {
		return "", nil, err
	}
	warnSkippedMigrations(progress, skipped)

	record, warnings, err := planRemove(ctx, db, dir, version, files...)
	if err != nil {
		return "", nil, err
	}
	report(fmt.Sprintf("准备删除迁移账本记录: 版本 %s (状态 %s)", record.Version, record.Status))
	for _, warning := range warnings {
		report(fmt.Sprintf("WARNING: %s", warning))
	}
	deleted, err := pkgmigrate.DeleteMigrationRecord(db, record.Version)
	if err != nil {
		return "", warnings, err
	}
	report(fmt.Sprintf("版本 %s 账本记录已删除, 修正脚本后可重新迁移", deleted))
	return deleted, warnings, nil
}

// planRemove resolves the ledger record and collects the advisory warnings
// without touching the database, so both --dry-run preview and the actual
// deletion share one resolution path.
func planRemove(ctx context.Context, db conn.DBAdapter, dir, version string, files ...*pkgmigrate.MigrationFile) (pkgmigrate.MigrationRecord, []string, error) {
	record, found, err := pkgmigrate.FindMigrationRecord(ctx, db, version)
	if err != nil {
		return pkgmigrate.MigrationRecord{}, nil, err
	}
	if !found {
		return pkgmigrate.MigrationRecord{}, nil, fmt.Errorf("迁移账本中不存在版本 %s 的记录", version)
	}
	// 提前拦截不可删除状态,避免为一条必然被拒绝的记录继续查询甚至取锁;
	// DeleteMigrationRecord 内部的白名单复核仍是最终防线。
	if record.Status != "failed" && record.Status != "rolled_back" {
		return pkgmigrate.MigrationRecord{}, nil, fmt.Errorf("版本 %s 状态为 %s,仅 failed/rolled_back 记录允许删除", record.Version, record.Status)
	}

	var warnings []string
	if len(files) == 0 {
		scanned, skipped, scanErr := local.ScanMigrations(dir)
		if scanErr != nil {
			return pkgmigrate.MigrationRecord{}, nil, scanErr
		}
		warnSkippedMigrations(nil, skipped)
		files = scanned
	}
	hasUp := false
	for _, file := range files {
		if file.Direction == "up" && local.CompareVersion(file.Version, record.Version) == 0 {
			hasUp = true
			break
		}
	}
	if !hasUp {
		warnings = append(warnings, fmt.Sprintf("脚本库中不存在版本 %s 的 up 脚本, 删除记录后无法通过迁移重新执行该版本", record.Version))
	}

	successSet, err := pkgmigrate.SuccessfulMigrations(db)
	if err != nil {
		return pkgmigrate.MigrationRecord{}, nil, err
	}
	for successVersion := range successSet {
		if local.CompareVersion(successVersion, record.Version) > 0 {
			warnings = append(warnings, fmt.Sprintf("账本中存在比版本 %s 更新的已成功版本, 重新迁移后该版本将排到回退栈顶, 后续回滚顺序与版本顺序不一致", record.Version))
			break
		}
	}

	if db.GetConfig().Type == consts.DBTypeMySQL && record.Status == "failed" {
		warnings = append(warnings, "MySQL DDL 可能已部分自动提交, 删除记录不会清理残留对象, 修正后的脚本应写成幂等(IF NOT EXISTS 等)")
	}
	return record, warnings, nil
}
