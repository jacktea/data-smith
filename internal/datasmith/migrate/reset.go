package migrate

import (
	"fmt"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/migrate"
	"github.com/spf13/cobra"
)

var resetDBCmd = newResetDBCommand()

func newResetDBCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset-db",
		Short: "Reset database to initial state",
		RunE: func(cmd *cobra.Command, args []string) error {
			configPath, _ := cmd.Flags().GetString("config")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			yes, _ := cmd.Flags().GetBool("yes")
			if !dryRun && !yes {
				return fmt.Errorf("reset-db requires --yes for destructive execution")
			}

			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			resetSQL, err := migrate.BuildResetSQL(&cfg.TargetDB)
			if err != nil {
				return fmt.Errorf("validate reset target: %w", err)
			}
			if dryRun {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), resetSQL)
				return err
			}

			tgtDB, err := db.NewDBAdapterContext(cmd.Context(), &cfg.TargetDB)
			if err != nil {
				return fmt.Errorf("connect to target DB: %w", err)
			}
			defer tgtDB.Close()

			if err := migrate.ResetDatabaseContext(cmd.Context(), tgtDB); err != nil {
				return fmt.Errorf("reset database: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringP("config", "c", "", "Path to config file")
	cmd.Flags().BoolP("dry-run", "n", false, "Print the validated reset SQL without connecting or executing")
	cmd.Flags().Bool("yes", false, "Confirm destructive reset execution")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
