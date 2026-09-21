package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/logger"
)

// 可删除的账本状态白名单。failed 未完整生效、rolled_back 已被 down 撤销，
// 两类记录都不代表任何生效的 schema 效果，删除后同版本修正脚本可以重新
// 迁移。success 与真实库结构绑定（删除即脱钩），running/rolling_back 可能有
// 活跃迁移，一律拒绝。
const (
	statusFailed     = "failed"
	statusRolledBack = "rolled_back"
)

// MigrationRecord is one schema_migrations ledger row as seen by the
// find/delete flows.
type MigrationRecord struct {
	Version  string
	Status   string
	Checksum string
}

// FindMigrationRecord resolves a version (V-prefix and case tolerant, the same
// normalization as the rollback stack) to its exact ledger row. Read-only and
// lock-free; callers use it for previews while DeleteMigrationRecord re-checks
// under the migration lock.
func FindMigrationRecord(ctx context.Context, db conn.DBAdapter, version string) (MigrationRecord, bool, error) {
	return findMigrationRecord(ctx, db.GetConn().QueryContext, version)
}

// DeleteMigrationRecord removes the ledger record of version so a revised
// script with the same version number can migrate again. Only failed and
// rolled_back records are deletable; the DELETE itself re-checks the status
// whitelist so a record flipped by a concurrent runner between read and delete
// is refused, never destroyed. It returns the exact ledger version deleted.
func DeleteMigrationRecord(db conn.DBAdapter, version string) (string, error) {
	deleted := ""
	err := withMigrationLock(db, func(ctx context.Context, connection *sql.Conn) error {
		record, found, err := findMigrationRecord(ctx, connection.QueryContext, version)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("迁移账本中不存在版本 %s 的记录", version)
		}
		if record.Status != statusFailed && record.Status != statusRolledBack {
			return fmt.Errorf("版本 %s 状态为 %s,仅 failed/rolled_back 记录允许删除", record.Version, record.Status)
		}
		query := `DELETE FROM schema_migrations WHERE version = ` + placeholder(db.GetConfig().Type, 1) +
			` AND status IN ('failed','rolled_back')`
		result, err := connection.ExecContext(ctx, query, record.Version)
		if err != nil {
			return fmt.Errorf("删除迁移账本记录 %s: %w", record.Version, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("确认迁移账本记录 %s 删除结果: %w", record.Version, err)
		}
		if affected == 0 {
			return fmt.Errorf("版本 %s 状态在删除前已变化(可能被并发迁移更新),已拒绝删除,请重试", record.Version)
		}
		deleted = record.Version
		logger.Infof("已删除迁移账本记录: 版本 %s (状态 %s), 该版本可修正脚本后重新迁移", record.Version, record.Status)
		return nil
	})
	return deleted, err
}

// findMigrationRecord scans the (small) ledger for rows whose normalized
// version matches. Multiple matches (e.g. both "V1" and "1" stored) are an
// ambiguity error rather than a silent pick.
func findMigrationRecord(ctx context.Context, query func(context.Context, string, ...any) (*sql.Rows, error), version string) (MigrationRecord, bool, error) {
	key := normalizeVersion(version)
	if key == "" {
		return MigrationRecord{}, false, errors.New("版本号不能为空")
	}
	rows, err := query(ctx, `SELECT version, status, checksum FROM schema_migrations`)
	if err != nil {
		return MigrationRecord{}, false, fmt.Errorf("读取迁移账本失败: %w", err)
	}
	defer rows.Close()

	var matches []MigrationRecord
	for rows.Next() {
		var record MigrationRecord
		var checksum sql.NullString
		if err := rows.Scan(&record.Version, &record.Status, &checksum); err != nil {
			return MigrationRecord{}, false, err
		}
		record.Checksum = checksum.String
		if normalizeVersion(record.Version) == key {
			matches = append(matches, record)
		}
	}
	if err := rows.Err(); err != nil {
		return MigrationRecord{}, false, err
	}
	switch len(matches) {
	case 0:
		return MigrationRecord{}, false, nil
	case 1:
		return matches[0], true, nil
	default:
		versions := make([]string, 0, len(matches))
		for _, match := range matches {
			versions = append(versions, match.Version)
		}
		return MigrationRecord{}, false, fmt.Errorf("版本 %s 在账本中存在多条记录(%s),请先用精确版本号处理歧义",
			version, strings.Join(versions, ", "))
	}
}
