// Package web installs the `datasmith web` cobra subcommand that serves the
// web console backend.
package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jacktea/data-smith/internal/server"
	"github.com/spf13/cobra"
)

func newWebCommand() *cobra.Command {
	var (
		addr    string
		dataDir string
	)
	cmd := &cobra.Command{
		Use:   "web",
		Short: "启动 Web 控制台",
		Long:  `启动 DataSmith Web 控制台,提供连接管理、结构/数据比对、SQL 执行、重置与版本迁移的图形界面。`,
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := server.New(dataDir)
			if err != nil {
				return fmt.Errorf("初始化 Web 控制台: %w", err)
			}
			defer srv.Close()

			httpSrv := &http.Server{
				Addr:              addr,
				Handler:           srv.Handler(),
				ReadHeaderTimeout: 10 * time.Second,
			}
			serveErr := make(chan error, 1)
			go func() {
				fmt.Printf("DataSmith Web 控制台已启动: http://%s (数据目录: %s)\n", addr, dataDir)
				serveErr <- httpSrv.ListenAndServe()
			}()

			stop := make(chan os.Signal, 1)
			signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
			select {
			case err := <-serveErr:
				if errors.Is(err, http.ErrServerClosed) {
					return nil
				}
				return err
			case sig := <-stop:
				fmt.Printf("收到信号 %v,正在停止 Web 控制台...\n", sig)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return httpSrv.Shutdown(ctx)
			}
		},
	}
	cmd.Flags().StringVar(&addr, "addr", ":8080", "HTTP 监听地址")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./datasmith-web-data", "数据目录(存储连接、方案、脚本库与任务产物)")
	return cmd
}

// Install registers the web subcommand on the root command.
func Install(root *cobra.Command) {
	root.AddCommand(newWebCommand())
}
