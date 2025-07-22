package migrate

import (
	"log"
	"os"

	"github.com/jacktea/data-smith/internal/config"
	"github.com/jacktea/data-smith/pkg/db"
	"github.com/jacktea/data-smith/pkg/migrate"
	"github.com/spf13/cobra"
)

var resetDBCmd = &cobra.Command{
	Use:   "reset-db",
	Short: "Reset database to initial state",
	Run: func(cmd *cobra.Command, args []string) {
		configPath, _ := cmd.Flags().GetString("config")

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Println("Error loading config:", err)
			os.Exit(1)
		}

		tgtDB, err := db.NewDBAdapter(&cfg.TargetDB)
		if err != nil {
			log.Println("Error connecting to target DB:", err)
			os.Exit(1)
		}
		defer tgtDB.Close()

		err = migrate.ResetDatabase(tgtDB)
		if err != nil {
			log.Println("Error resetting database:", err)
			os.Exit(1)
		}
	},
}

func init() {
	resetDBCmd.Flags().StringP("config", "c", "", "Path to config file")
	resetDBCmd.MarkFlagRequired("config")
}
