package migrate

import (
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/spf13/cobra"
)

var migrateRollback = newMigrateRollbackCommand()

// newMigrateRollbackCommand builds the migrate-rollback command: it rolls back
// applied versions one by one, newest first, until the target version is the
// latest applied one (a single step when --target is empty). Like reset-db it
// refuses destructive execution without --yes; --dry-run only previews the
// plan.
func newMigrateRollbackCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-rollback",
		Short: "Roll back applied migrations to the latest or a target version",
		RunE: func(cmd *cobra.Command, args []string) error {
			configPath, _ := cmd.Flags().GetString("config")
			dir, _ := cmd.Flags().GetString("dir")
			target, _ := cmd.Flags().GetString("target")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			if !dryRun && !yes {
				return fmt.Errorf("migrate-rollback requires --yes for destructive execution")
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
				planned, err := PlanRollback(cmd.Context(), tgtDB, dir, target, nil)
				if err != nil {
					return fmt.Errorf("plan rollback: %w", err)
				}
				if len(planned) == 0 {
					_, err := fmt.Fprintf(cmd.OutOrStdout(), "已处于目标版本 %s, 无需回退\n", target)
					return err
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "将依次回退 %d 个版本:\n  %s\n", len(planned), strings.Join(planned, "\n  "))
				return err
			}

			rolled, err := RollbackTo(cmd.Context(), tgtDB, dir, target, nil)
			if err != nil {
				return fmt.Errorf("rollback migrations: %w", err)
			}
			if len(rolled) == 0 {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "已处于目标版本 %s, 无需回退\n", target)
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "已依次回退 %d 个版本: %s\n", len(rolled), strings.Join(rolled, " -> "))
			return err
		},
	}
	cmd.Flags().StringP("config", "c", "", "Path to config file")
	cmd.Flags().StringP("dir", "d", "", "Path to migration script directory")
	cmd.Flags().String("target", "", "Roll back until this version is the latest applied one (default: roll back the latest version)")
	cmd.Flags().BoolP("dry-run", "n", false, "Preview the versions that would be rolled back without executing")
	cmd.Flags().Bool("yes", false, "Confirm destructive rollback execution")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagRequired("dir")
	return cmd
}
