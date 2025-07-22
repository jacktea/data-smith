package diff

import "github.com/spf13/cobra"

func Install(root *cobra.Command) {
	// 对比数据库结构
	root.AddCommand(diffSchemaCmd)
	// 对比数据库数据
	root.AddCommand(diffDataCmd)
}
