package datasmith

import (
	"log"
	"os"

	"github.com/jacktea/data-smith/internal/datasmith/diff"
	"github.com/jacktea/data-smith/internal/datasmith/migrate"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "datasmith",
	Short: "A CLI tool for database toolkit",
}

func init() {
	diff.Install(rootCmd)
	migrate.Install(rootCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
