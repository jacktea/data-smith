package migrate

import "github.com/spf13/cobra"

func Install(root *cobra.Command) {
	root.AddCommand(resetDBCmd)
	root.AddCommand(migrateScript)
}
