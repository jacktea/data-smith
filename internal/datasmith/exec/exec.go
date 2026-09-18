package exec

import "github.com/spf13/cobra"

func Install(root *cobra.Command) {
	root.AddCommand(execSQLCmd)
}
