package migrate

import (
	"fmt"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/migrate"
	"github.com/spf13/cobra"
)

var resetDBCmd = &cobra.Command{
	Use:   "reset-db",
	Short: "Reset database to initial state",
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		tgtDB, err := db.NewDBAdapter(&cfg.TargetDB)
		if err != nil {
			return fmt.Errorf("connect to target DB: %w", err)
		}
		defer tgtDB.Close()

		if err := migrate.ResetDatabase(tgtDB); err != nil {
			return fmt.Errorf("reset database: %w", err)
		}
		return nil
	},
}

func init() {
	resetDBCmd.Flags().StringP("config", "c", "", "Path to config file")
	resetDBCmd.MarkFlagRequired("config")
}
