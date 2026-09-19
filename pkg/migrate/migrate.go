package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/logger"
	"github.com/jacktea/data-smith/pkg/sql/ident"
	"github.com/jacktea/data-smith/pkg/utils"
)

const migrationLockName = "data-smith:schema-migrations"

type MigrationState struct {
	Checksum string
	Status   string
}

func CurrentVersion(db conn.DBAdapter) (string, error) {
	row := db.GetConn().QueryRow("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC LIMIT 1")
	var version string
	err := row.Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return version, err
}

// PrepareMigrationFiles validates every migration and loads its content before
// callers make any database change. It also calculates the ledger checksum.
func PrepareMigrationFiles(files []*MigrationFile) error {
	versions := make(map[string]map[string]string, len(files))
	for _, file := range files {
		if file == nil {
			return errors.New("migration list contains a nil file")
		}
		if file.Ext == "" {
			file.Ext = strings.TrimPrefix(strings.ToLower(filepath.Ext(file.Path)), ".")
			if file.Ext == "" {
				file.Ext = "sql"
			}
		}
		if !strings.EqualFold(file.Ext, "sql") {
			return fmt.Errorf("migration %q has unsupported .%s format; only SQL scripts are allowed", file.Path, file.Ext)
		}
		if file.Direction == "" {
			file.Direction = "up"
		}
		if file.Direction != "up" && file.Direction != "down" {
			return fmt.Errorf("migration %q has direction %q; only .up.sql, .down.sql or directionless .sql files are allowed", file.Path, file.Direction)
		}

		versionKey := normalizeVersion(file.Version)
		if versionKey == "" {
			return fmt.Errorf("migration %q has an empty version", file.Path)
		}
		directions := versions[versionKey]
		if directions == nil {
			directions = make(map[string]string, 2)
			versions[versionKey] = directions
		}
		if previous, exists := directions[file.Direction]; exists {
			return fmt.Errorf("duplicate migration version %q (%s) in %s and %s", file.Version, file.Direction, previous, file.Path)
		}
		directions[file.Direction] = file.Path

		content, err := file.ReadContent()
		if err != nil {
			return fmt.Errorf("read migration %q: %w", file.Path, err)
		}
		file.Content = content
		sum := sha256.Sum256([]byte(content))
		file.Checksum = hex.EncodeToString(sum[:])
	}
	return nil
}

func DryRunMigrations(db conn.DBAdapter, files []*MigrationFile) error {
	if err := prepareForwardFiles(files); err != nil {
		return err
	}
	if db.GetConfig().Type == consts.DBTypeMySQL {
		return errors.New("MySQL migration dry-run is unsupported because DDL may auto-commit and cannot be rolled back reliably")
	}
	if db.GetConfig().Type != consts.DBTypePostgres {
		return fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
	}

	logger.Info("开始模拟数据迁移")
	return withMigrationLock(db, func(ctx context.Context, connection *sql.Conn) error {
		tx, err := connection.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("开始事务失败: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		for _, file := range files {
			logger.Infof("模拟执行脚本 %s__%s", file.Version, file.Title)
			if _, err := tx.ExecContext(ctx, utils.CleanTransaction(file.Content)); err != nil {
				return fmt.Errorf("模拟执行脚本 %s__%s 失败: %w", file.Version, file.Title, err)
			}
		}
		logger.Info("模拟数据迁移成功")
		return nil
	})
}

func ApplyMigrations(db conn.DBAdapter, files []*MigrationFile) error {
	if err := prepareForwardFiles(files); err != nil {
		return err
	}
	logger.Info("开始数据迁移")
	return withMigrationLock(db, func(ctx context.Context, connection *sql.Conn) error {
		for _, file := range files {
			state, exists, err := migrationState(ctx, connection, db.GetConfig().Type, file.Version)
			if err != nil {
				return err
			}
			if exists && state.Checksum != "" && state.Checksum != file.Checksum {
				return fmt.Errorf("migration version %q checksum drift: database=%s file=%s (%s)", file.Version, state.Checksum, file.Checksum, file.Path)
			}
			if exists && state.Status == "success" {
				logger.Infof("跳过已成功迁移: %s__%s", file.Version, file.Title)
				continue
			}

			logger.Infof("操作脚本 %s__%s", file.Version, file.Title)
			switch db.GetConfig().Type {
			case consts.DBTypePostgres:
				err = applyPostgresMigration(ctx, connection, file)
			case consts.DBTypeMySQL:
				err = applyMySQLMigration(ctx, connection, file)
			default:
				err = fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
			}
			if err != nil {
				return fmt.Errorf("migration %s__%s failed: %w", file.Version, file.Title, err)
			}
		}
		logger.Info("数据迁移成功")
		return nil
	})
}

// prepareForwardFiles validates the full set and rejects down scripts, which
// only ever run through RollbackLatestMigration.
func prepareForwardFiles(files []*MigrationFile) error {
	if err := PrepareMigrationFiles(files); err != nil {
		return err
	}
	for _, file := range files {
		if file.Direction == "down" {
			return fmt.Errorf("migration %q is a down script; it cannot run in a forward migration", file.Path)
		}
	}
	return nil
}

// RollbackLatestMigration executes the down script of the latest successfully
// applied version and marks it rolled_back in the ledger. PostgreSQL runs the
// down script in a single transaction; MySQL DDL may auto-commit, in which
// case a failed rollback leaves the ledger untouched for retry.
func RollbackLatestMigration(db conn.DBAdapter, files []*MigrationFile) (rolledVersion string, err error) {
	if err := PrepareMigrationFiles(files); err != nil {
		return "", err
	}
	current, err := CurrentVersion(db)
	if err != nil {
		return "", fmt.Errorf("read current migration version: %w", err)
	}
	if current == "" {
		return "", errors.New("no applied migration to roll back")
	}
	var down *MigrationFile
	for _, file := range files {
		if file.Direction == "down" && normalizeVersion(file.Version) == normalizeVersion(current) {
			down = file
			break
		}
	}
	if down == nil {
		return "", fmt.Errorf("migration version %q has no down script and cannot be rolled back", current)
	}

	if err := withMigrationLock(db, func(ctx context.Context, connection *sql.Conn) error {
		switch db.GetConfig().Type {
		case consts.DBTypePostgres:
			return rollbackPostgresMigration(ctx, connection, down, current)
		case consts.DBTypeMySQL:
			return rollbackMySQLMigration(ctx, connection, down, current)
		default:
			return fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
		}
	}); err != nil {
		return "", err
	}
	logger.Infof("版本 %s 已回退", current)
	return current, nil
}

func rollbackPostgresMigration(ctx context.Context, connection *sql.Conn, down *MigrationFile, ledgerVersion string) error {
	tx, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	start := time.Now()
	if _, err = tx.ExecContext(ctx, `UPDATE schema_migrations SET status = 'rolling_back', error_summary = NULL WHERE version = $1`, ledgerVersion); err == nil {
		_, err = tx.ExecContext(ctx, down.Content)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE schema_migrations SET status = 'rolled_back', execution_time = $1,
			error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = $2`, time.Since(start).Milliseconds(), ledgerVersion)
	}
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("rollback migration %s__%s: %w", down.Version, down.Title, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rollback transaction (outcome unknown): %w", err)
	}
	return nil
}

func rollbackMySQLMigration(ctx context.Context, connection *sql.Conn, down *MigrationFile, ledgerVersion string) error {
	start := time.Now()
	if _, err := connection.ExecContext(ctx, down.Content); err != nil {
		if _, ledgerErr := connection.ExecContext(ctx, `UPDATE schema_migrations SET error_summary = ?,
			applied_at = CURRENT_TIMESTAMP WHERE version = ?`, errorSummary(fmt.Errorf("回退失败: %w", err)), ledgerVersion); ledgerErr != nil {
			return fmt.Errorf("%v (also failed to record rollback failure: %w)", err, ledgerErr)
		}
		return fmt.Errorf("rollback migration %s__%s: %w", down.Version, down.Title, err)
	}
	executionTime := time.Since(start).Milliseconds()
	if _, err := connection.ExecContext(ctx, `UPDATE schema_migrations SET status = 'rolled_back', execution_time = ?,
		error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = ?`, executionTime, ledgerVersion); err != nil {
		return fmt.Errorf("rollback SQL completed but ledger update failed; MySQL DDL may already be committed: %w", err)
	}
	return nil
}

func ResetDatabase(db conn.DBAdapter) error {
	return ResetDatabaseContext(context.Background(), db)
}

func ResetDatabaseContext(ctx context.Context, db conn.DBAdapter) error {
	if db == nil {
		return errors.New("database adapter is required")
	}
	query, err := BuildResetSQL(db.GetConfig())
	if err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("reset context is required")
	}
	logger.Info("开始重置数据库")
	connection := db.GetConn()
	if connection == nil {
		return errors.New("database connection is required")
	}
	if _, err := connection.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("重置数据库失败: %w", err)
	}
	logger.Info("数据库重置成功")
	return nil
}

func BuildResetSQL(cfg *config.ConnConfig) (string, error) {
	if err := ValidateResetTarget(cfg); err != nil {
		return "", err
	}
	switch cfg.Type {
	case consts.DBTypeMySQL:
		name := ident.Quote(ident.Backtick, cfg.DBName)
		return fmt.Sprintf("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;", name, name), nil
	case consts.DBTypePostgres:
		name := ident.Quote(ident.DoubleQuote, resetSchema(cfg))
		return fmt.Sprintf("DROP SCHEMA %s CASCADE; CREATE SCHEMA %s;", name, name), nil
	default:
		return "", fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
}

// resetSchema returns the PostgreSQL schema a reset targets: the configured
// table schema, defaulting to "public" exactly like the PostgreSQL adapter
// does for every other code path (diff, query, execution).
func resetSchema(cfg *config.ConnConfig) string {
	if schema := strings.TrimSpace(cfg.TableSchema); schema != "" {
		return schema
	}
	return "public"
}

func ValidateResetTarget(cfg *config.ConnConfig) error {
	if cfg == nil {
		return errors.New("connection configuration is required")
	}
	database := strings.TrimSpace(cfg.DBName)
	if database == "" {
		return errors.New("refusing to reset an empty database name")
	}
	switch cfg.Type {
	case consts.DBTypeMySQL:
		system := map[string]bool{"mysql": true, "information_schema": true, "performance_schema": true, "sys": true}
		if system[strings.ToLower(database)] {
			return fmt.Errorf("refusing to reset MySQL system database %q", database)
		}
	case consts.DBTypePostgres:
		if system := map[string]bool{"postgres": true, "template0": true, "template1": true}; system[strings.ToLower(database)] {
			return fmt.Errorf("refusing to reset PostgreSQL system database %q", database)
		}
		schema := resetSchema(cfg)
		lowerSchema := strings.ToLower(schema)
		if lowerSchema == "information_schema" || strings.HasPrefix(lowerSchema, "pg_") {
			return fmt.Errorf("refusing to reset PostgreSQL system schema %q", schema)
		}
	default:
		return fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
	return nil
}

func EnsureVersionTable(db conn.DBAdapter) error {
	connection := db.GetConn()
	switch db.GetConfig().Type {
	case consts.DBTypeMySQL:
		if _, err := connection.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			version VARCHAR(255) NOT NULL,
			title VARCHAR(255),
			checksum VARCHAR(64),
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			execution_time BIGINT NOT NULL DEFAULT 0,
			status VARCHAR(50) NOT NULL DEFAULT 'success',
			error_summary TEXT,
			UNIQUE KEY schema_migrations_version_uq (version)
		)`); err != nil {
			return fmt.Errorf("create migration ledger: %w", err)
		}
		return ensureMySQLLedgerColumns(connection)
	case consts.DBTypePostgres:
		queries := []string{
			`CREATE TABLE IF NOT EXISTS schema_migrations (
				id BIGSERIAL PRIMARY KEY,
				version VARCHAR(255) NOT NULL,
				title VARCHAR(255),
				checksum VARCHAR(64),
				applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				execution_time BIGINT NOT NULL DEFAULT 0,
				status VARCHAR(50) NOT NULL DEFAULT 'success',
				error_summary TEXT
			)`,
			`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum VARCHAR(64)`,
			`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS execution_time BIGINT NOT NULL DEFAULT 0`,
			`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS status VARCHAR(50) NOT NULL DEFAULT 'success'`,
			`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS error_summary TEXT`,
			`CREATE UNIQUE INDEX IF NOT EXISTS schema_migrations_version_uq ON schema_migrations (version)`,
		}
		for _, query := range queries {
			if _, err := connection.Exec(query); err != nil {
				return fmt.Errorf("upgrade migration ledger: %w", err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
	}
}

func ensureMySQLLedgerColumns(connection *sql.DB) error {
	columns := []struct {
		name       string
		definition string
	}{
		{"checksum", "VARCHAR(64)"},
		{"execution_time", "BIGINT NOT NULL DEFAULT 0"},
		{"status", "VARCHAR(50) NOT NULL DEFAULT 'success'"},
		{"error_summary", "TEXT"},
	}
	for _, column := range columns {
		var count int
		err := connection.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schema_migrations' AND COLUMN_NAME = ?`, column.name).Scan(&count)
		if err != nil {
			return fmt.Errorf("inspect migration ledger column %s: %w", column.name, err)
		}
		if count == 0 {
			query := fmt.Sprintf("ALTER TABLE schema_migrations ADD COLUMN %s %s", column.name, column.definition)
			if _, err := connection.Exec(query); err != nil {
				return fmt.Errorf("add migration ledger column %s: %w", column.name, err)
			}
		}
	}

	var count int
	err := connection.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schema_migrations' AND INDEX_NAME = 'schema_migrations_version_uq'`).Scan(&count)
	if err != nil {
		return fmt.Errorf("inspect migration ledger unique index: %w", err)
	}
	if count == 0 {
		if _, err := connection.Exec(`CREATE UNIQUE INDEX schema_migrations_version_uq ON schema_migrations (version)`); err != nil {
			return fmt.Errorf("create unique migration version index (remove duplicate versions first): %w", err)
		}
	}
	return nil
}

// SuccessfulMigrations returns the applied-success set keyed by version.
func SuccessfulMigrations(db conn.DBAdapter) (map[string]string, error) {
	rows, err := db.GetConn().Query(`SELECT version, checksum FROM schema_migrations WHERE status = 'success'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var version string
		var checksum sql.NullString
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, err
		}
		result[version] = checksum.String
	}
	return result, rows.Err()
}

func migrationState(ctx context.Context, connection *sql.Conn, dbType consts.DBType, version string) (MigrationState, bool, error) {
	query := `SELECT checksum, status FROM schema_migrations WHERE version = ` + placeholder(dbType, 1)
	var checksum sql.NullString
	var status string
	err := connection.QueryRowContext(ctx, query, version).Scan(&checksum, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return MigrationState{}, false, nil
	}
	if err != nil {
		return MigrationState{}, false, fmt.Errorf("read migration ledger for version %q: %w", version, err)
	}
	return MigrationState{Checksum: checksum.String, Status: status}, true, nil
}

func applyPostgresMigration(ctx context.Context, connection *sql.Conn, file *MigrationFile) error {
	tx, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	start := time.Now()
	_, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)
		VALUES ($1, $2, $3, 'running', 0, NULL)
		ON CONFLICT (version) DO UPDATE SET title = EXCLUDED.title, checksum = EXCLUDED.checksum,
		status = 'running', execution_time = 0, error_summary = NULL, applied_at = CURRENT_TIMESTAMP`, file.Version, file.Title, file.Checksum)
	if err == nil {
		_, err = tx.ExecContext(ctx, file.Content)
	}
	executionTime := time.Since(start).Milliseconds()
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE schema_migrations SET status = 'success', execution_time = $1,
			error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = $2`, executionTime, file.Version)
	}
	if err != nil {
		_ = tx.Rollback()
		if ledgerErr := recordPostgresFailure(ctx, connection, file, executionTime, err); ledgerErr != nil {
			return fmt.Errorf("%v (also failed to record failure: %w)", err, ledgerErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration transaction (outcome unknown): %w", err)
	}
	return nil
}

func recordPostgresFailure(ctx context.Context, connection *sql.Conn, file *MigrationFile, executionTime int64, migrationErr error) error {
	_, err := connection.ExecContext(ctx, `INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)
		VALUES ($1, $2, $3, 'failed', $4, $5)
		ON CONFLICT (version) DO UPDATE SET title = EXCLUDED.title, checksum = EXCLUDED.checksum,
		status = 'failed', execution_time = EXCLUDED.execution_time, error_summary = EXCLUDED.error_summary,
		applied_at = CURRENT_TIMESTAMP`, file.Version, file.Title, file.Checksum, executionTime, errorSummary(migrationErr))
	return err
}

func applyMySQLMigration(ctx context.Context, connection *sql.Conn, file *MigrationFile) error {
	start := time.Now()
	_, err := connection.ExecContext(ctx, `INSERT INTO schema_migrations (version, title, checksum, status, execution_time, error_summary)
		VALUES (?, ?, ?, 'running', 0, NULL)
		ON DUPLICATE KEY UPDATE title = VALUES(title), checksum = VALUES(checksum), status = 'running',
		execution_time = 0, error_summary = NULL, applied_at = CURRENT_TIMESTAMP`, file.Version, file.Title, file.Checksum)
	if err != nil {
		return fmt.Errorf("record running migration: %w", err)
	}

	_, migrationErr := connection.ExecContext(ctx, file.Content)
	executionTime := time.Since(start).Milliseconds()
	if migrationErr != nil {
		_, ledgerErr := connection.ExecContext(ctx, `UPDATE schema_migrations SET status = 'failed', execution_time = ?,
			error_summary = ?, applied_at = CURRENT_TIMESTAMP WHERE version = ?`, executionTime, errorSummary(migrationErr), file.Version)
		if ledgerErr != nil {
			return fmt.Errorf("%v (also failed to record failure: %w)", migrationErr, ledgerErr)
		}
		return migrationErr
	}
	_, err = connection.ExecContext(ctx, `UPDATE schema_migrations SET status = 'success', execution_time = ?,
		error_summary = NULL, applied_at = CURRENT_TIMESTAMP WHERE version = ?`, executionTime, file.Version)
	if err != nil {
		return fmt.Errorf("migration SQL completed but success ledger update failed; MySQL DDL may already be committed: %w", err)
	}
	return nil
}

func withMigrationLock(db conn.DBAdapter, fn func(context.Context, *sql.Conn) error) (resultErr error) {
	ctx := context.Background()
	connection, err := db.GetConn().Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve migration connection: %w", err)
	}
	defer connection.Close()

	locked := false
	switch db.GetConfig().Type {
	case consts.DBTypePostgres:
		err = connection.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, migrationLockName).Scan(&locked)
	case consts.DBTypeMySQL:
		var mysqlLock sql.NullInt64
		err = connection.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, migrationLockName).Scan(&mysqlLock)
		locked = mysqlLock.Valid && mysqlLock.Int64 == 1
	default:
		return fmt.Errorf("unsupported database type: %s", db.GetConfig().Type)
	}
	if err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !locked {
		return errors.New("another migration runner is active; could not acquire migration lock")
	}
	defer func() {
		var releaseErr error
		switch db.GetConfig().Type {
		case consts.DBTypePostgres:
			var released bool
			releaseErr = connection.QueryRowContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, migrationLockName).Scan(&released)
		case consts.DBTypeMySQL:
			var released sql.NullInt64
			releaseErr = connection.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, migrationLockName).Scan(&released)
		}
		if resultErr == nil && releaseErr != nil {
			resultErr = fmt.Errorf("release migration lock: %w", releaseErr)
		}
	}()

	return fn(ctx, connection)
}

func placeholder(dbType consts.DBType, position int) string {
	if dbType == consts.DBTypeMySQL {
		return "?"
	}
	return fmt.Sprintf("$%d", position)
}

func normalizeVersion(version string) string {
	return strings.TrimPrefix(strings.ToLower(version), "v")
}

func errorSummary(err error) string {
	if err == nil {
		return ""
	}
	runes := []rune(err.Error())
	if len(runes) > 2000 {
		runes = runes[:2000]
	}
	return string(runes)
}
