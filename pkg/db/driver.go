package db

import (
	"context"
	"fmt"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db/mysql"
	"github.com/jacktea/data-smith/pkg/db/postgres"
)

// NewDBAdapter 由外部注入实现，避免 import cycle
func NewDBAdapter(cfg *config.ConnConfig) (conn.DBAdapter, error) {
	return NewDBAdapterContext(context.Background(), cfg)
}

func NewDBAdapterContext(ctx context.Context, cfg *config.ConnConfig) (conn.DBAdapter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("connection configuration is required")
	}
	switch cfg.Type {
	case consts.DBTypeMySQL:
		return mysql.NewMySQLAdapterContext(ctx, cfg)
	case consts.DBTypePostgres:
		return postgres.NewPostgresAdapterContext(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
}
