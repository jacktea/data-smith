package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/logger"
)

// AppliedSuccessVersions returns the successfully applied versions in
// application order, newest first (ledger id DESC). The order is the rollback
// stack: rolling back means popping entries from the front until the target
// version is on top.
func AppliedSuccessVersions(db conn.DBAdapter) ([]string, error) {
	rows, err := db.GetConn().Query(`SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}

// PlanRollbackTo computes which applied versions would be rolled back to reach
// targetVersion: every success-stack entry above it, newest first. An empty
// targetVersion means a single step (equivalent to RollbackLatestMigration).
// It validates the file set and every required down script but never touches
// the database, so callers can preview the plan with --dry-run.
func PlanRollbackTo(db conn.DBAdapter, files []*MigrationFile, targetVersion string) ([]string, error) {
	if err := PrepareMigrationFiles(files); err != nil {
		return nil, err
	}
	stack, err := AppliedSuccessVersions(db)
	if err != nil {
		return nil, fmt.Errorf("read applied migration versions: %w", err)
	}
	if len(stack) == 0 {
		return nil, errors.New("no applied migration to roll back")
	}

	var planned []string
	if targetVersion == "" {
		planned = stack[:1]
	} else {
		index := -1
		for i, version := range stack {
			if normalizeVersion(version) == normalizeVersion(targetVersion) {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("target version %q is not applied or has been rolled back", targetVersion)
		}
		planned = stack[:index]
	}

	downs := downScriptsByVersion(files)
	for _, version := range planned {
		if _, ok := downs[normalizeVersion(version)]; !ok {
			return nil, fmt.Errorf("migration version %q has no down script and cannot be rolled back", version)
		}
	}
	return planned, nil
}

// RollbackToMigration rolls back applied versions one by one, newest first,
// until targetVersion is the latest applied version. An empty targetVersion
// rolls back exactly the latest version. The whole sequence runs under a
// single migration lock; each step marks its ledger row rolled_back, so an
// interrupted run (MySQL DDL auto-commits) can resume from the new current
// version. It returns the rolled-back versions in execution order.
func RollbackToMigration(db conn.DBAdapter, files []*MigrationFile, targetVersion string) ([]string, error) {
	planned, err := PlanRollbackTo(db, files, targetVersion)
	if err != nil {
		return nil, err
	}
	if len(planned) == 0 {
		return []string{}, nil
	}
	downs := downScriptsByVersion(files)

	rolled := make([]string, 0, len(planned))
	if err := withMigrationLock(db, func(ctx context.Context, connection *sql.Conn) error {
		for _, version := range planned {
			down := downs[normalizeVersion(version)]
			switch db.GetConfig().Type {
			case consts.DBTypePostgres:
				err = rollbackPostgresMigration(ctx, connection, down, version)
			case consts.DBTypeMySQL:
				err = rollbackMySQLMigration(ctx, connection, down, version)
			default:
				err = fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
			}
			if err != nil {
				return err
			}
			rolled = append(rolled, version)
			logger.Infof("版本 %s 已回退", version)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return rolled, nil
}

func downScriptsByVersion(files []*MigrationFile) map[string]*MigrationFile {
	downs := make(map[string]*MigrationFile, len(files))
	for _, file := range files {
		if file.Direction == "down" {
			downs[normalizeVersion(file.Version)] = file
		}
	}
	return downs
}
