package db

import (
	"fmt"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db/mysql"
	"github.com/jacktea/data-smith/pkg/db/postgres"
)

// NewDBAdapter 由外部注入实现，避免 import cycle
func NewDBAdapter(cfg *config.ConnConfig) (conn.DBAdapter, error) {
	switch cfg.Type {
	case consts.DBTypeMySQL:
		return mysql.NewMySQLAdapter(cfg)
	case consts.DBTypePostgres:
		return postgres.NewPostgresAdapter(cfg)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
}
