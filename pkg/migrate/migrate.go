package migrate

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/logger"
	"github.com/jacktea/data-smith/pkg/utils"
)

func CurrentVersion(db conn.DBAdapter) (string, error) {
	row := db.GetConn().QueryRow("SELECT version FROM schema_migrations WHERE status = 'success' ORDER BY id DESC LIMIT 1")
	var version string
	err := row.Scan(&version)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return version, err
}

func DryRunMigrations(db conn.DBAdapter, files []*MigrationFile) error {
	logger.Info("开始模拟数据迁移")
	conn := db.GetConn()
	// 开始事务
	tx, err := conn.Begin()
	if err != nil {
		msg := fmt.Sprintf("开始事务失败: %s", err.Error())
		logger.Info(msg)
		return errors.New(msg)
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				msg := fmt.Sprintf("回滚事务失败: %s", err.Error())
				logger.Info(msg)
			}
		}
	}()

	// 模拟执行每个文件
	for _, f := range files {
		content := f.GetContent()
		msg := fmt.Sprintf("模拟执行脚本 %s__%s", f.Version, f.Title)
		logger.Info(msg)
		// 执行 SQL
		_, err = tx.Exec(utils.CleanTransaction(content))
		if err != nil {
			msg := fmt.Sprintf("模拟执行脚本 %s__%s 失败: %s", f.Version, f.Title, err.Error())
			logger.Info(msg)
			return errors.New(msg)
		}

		// 模拟记录版本
		_, err = tx.Exec(`INSERT INTO schema_migrations (version, title) VALUES ($1, $2)`, f.Version, f.Title)
		if err != nil {
			msg := fmt.Sprintf("记录版本失败: %s", err.Error())
			logger.Info(msg)
			return errors.New(msg)
		}
	}
	logger.Info("模拟数据迁移成功")
	return nil
}

func ApplyMigrations(db conn.DBAdapter, files []*MigrationFile) error {
	logger.Info("开始数据迁移")
	for _, f := range files {
		msg := fmt.Sprintf("操作脚本 %s__%s", f.Version, f.Title)
		logger.Info(msg)
		if err := applyMigration(db, f); err != nil {
			msg := fmt.Sprintf("操作脚本 %s__%s 失败: %s", f.Version, f.Title, err.Error())
			logger.Info(msg)
			return err
		}
	}
	logger.Info("数据迁移成功")
	return nil
}

func ResetDatabase(db conn.DBAdapter) error {
	logger.Info("开始重置数据库")
	conn := db.GetConn()
	cfg := db.GetConfig()
	query := ""
	switch cfg.Type {
	case consts.DBTypeMySQL:
		query = fmt.Sprintf("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;", cfg.DBName, cfg.DBName)
	case consts.DBTypePostgres:
		query = fmt.Sprintf("DROP SCHEMA %s CASCADE; CREATE SCHEMA %s;", cfg.TableSchema, cfg.TableSchema)
	default:
		logger.Error("不支持的数据库类型")
		return errors.New("不支持的数据库类型")
	}
	if query != "" {
		_, err := conn.Exec(query)
		if err != nil {
			logger.Errorf("重置数据库失败: %s\n", err.Error())
			return err
		}
	}
	return nil
}

func EnsureVersionTable(db conn.DBAdapter) error {
	conn := db.GetConn()
	cfg := db.GetConfig()
	query := ""
	switch cfg.Type {
	case consts.DBTypeMySQL:
		query = `CREATE TABLE IF NOT EXISTS schema_migrations (
			id INT AUTO_INCREMENT PRIMARY KEY,
			version VARCHAR(255) NOT NULL,
			title VARCHAR(255),
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			execution_time INT,
			status VARCHAR(50) DEFAULT 'success'
		)`
	case consts.DBTypePostgres:
		query = `CREATE TABLE IF NOT EXISTS schema_migrations (
			id SERIAL PRIMARY KEY,
			version VARCHAR(255) NOT NULL,
			title VARCHAR(255),
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			execution_time INTEGER,
			status VARCHAR(50) DEFAULT 'success'
		)`
	default:
		return errors.New("unsupported database type")
	}
	if query != "" {
		_, err := conn.Exec(query)
		return err
	}
	return nil
}

func applyMigration(db conn.DBAdapter, f *MigrationFile) error {
	content := f.GetContent()
	start := time.Now()
	conn := db.GetConn()
	_, err := conn.Exec(content)
	execTime := int(time.Since(start).Milliseconds())
	if err != nil {
		_, _ = conn.Exec(`INSERT INTO schema_migrations (version, title, execution_time, status) VALUES ($1, $2, $3, $4)`, f.Version, f.Title, execTime, "failed")
		return err
	}
	_, err = conn.Exec(`INSERT INTO schema_migrations (version, title, execution_time) VALUES ($1, $2, $3)`, f.Version, f.Title, execTime)
	return err
}
