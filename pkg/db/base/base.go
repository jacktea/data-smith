package base

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/proxy"
)

type BaseAdapter struct {
	ConnId string
	Conn   *sql.DB
	tunnel *proxy.SSHTunnel
	Cfg    *config.ConnConfig
	// session 非空时，读取通道整体切换到该会话（影子事务），nil 恢复连接池。
	session Querier
}

// Querier 是读取通道的最小查询接口：*sql.DB（连接池）与 *sql.Tx（事务会话）均满足。
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SessionBinder 由支持会话路由的适配器实现；影子数据比对（diff-full 两阶段）
// 借助它把未提交的结构 DDL 与行读取收敛到同一个会话。
type SessionBinder interface {
	BindSession(session Querier)
}

// BindSession 把适配器全部读取查询切换到给定会话；传 nil 恢复连接池。
// 影子事务期间未提交的 DDL 仅对同一会话可见，行读取必须同会话路由才一致。
func (p *BaseAdapter) BindSession(session Querier) {
	p.session = session
}

func (p *BaseAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if p.session != nil {
		return p.session.QueryContext(ctx, query, args...)
	}
	return p.Conn.QueryContext(ctx, query, args...)
}

func (p *BaseAdapter) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if p.session != nil {
		return p.session.QueryRowContext(ctx, query, args...)
	}
	return p.Conn.QueryRowContext(ctx, query, args...)
}

const (
	defaultConnectTimeout  = 10 * time.Second
	defaultMaxOpenConns    = 20
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// NormalizeConnectionSettings applies validated defaults to a cloned config.
func NormalizeConnectionSettings(cfg *config.ConnConfig) error {
	if cfg == nil {
		return fmt.Errorf("connection configuration is required")
	}
	if cfg.ConnectTimeout < 0 || cfg.ConnMaxLifetime < 0 || cfg.ConnMaxIdleTime < 0 {
		return fmt.Errorf("connection timeouts and lifetimes must not be negative")
	}
	if cfg.MaxOpenConns < 0 || cfg.MaxIdleConns < 0 {
		return fmt.Errorf("database pool limits must not be negative")
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = defaultMaxOpenConns
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = min(defaultMaxIdleConns, cfg.MaxOpenConns)
	}
	if cfg.ConnMaxLifetime == 0 {
		cfg.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if cfg.ConnMaxIdleTime == 0 {
		cfg.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return fmt.Errorf("maxIdleConns (%d) must not exceed maxOpenConns (%d)", cfg.MaxIdleConns, cfg.MaxOpenConns)
	}
	return nil
}

func ApplyConnectionSettings(db *sql.DB, cfg *config.ConnConfig) {
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
}

func PingWithRetry(ctx context.Context, db *sql.DB, cfg *config.ConnConfig) error {
	for attempt := 0; attempt < 3; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
		err := db.PingContext(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 2 {
			return RedactError(err, cfg)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func RedactError(err error, cfg *config.ConnConfig) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if cfg == nil {
		return errors.New(message)
	}
	for _, secret := range []string{cfg.Password, cfg.User} {
		if secret != "" {
			userInfoEscaped := url.User(secret).String()
			passwordEscaped := strings.TrimPrefix(url.UserPassword("", secret).String(), ":")
			for _, encoded := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret), userInfoEscaped, passwordEscaped} {
				message = strings.ReplaceAll(message, encoded, "[REDACTED]")
			}
		}
	}
	return errors.New(message)
}

func (p *BaseAdapter) Close() error {
	var result error
	if p.tunnel != nil {
		result = p.tunnel.Stop()
	}
	if p.Conn != nil {
		result = errors.Join(result, p.Conn.Close())
	}
	return result
}

func (p *BaseAdapter) Init(cfg *config.ConnConfig) error {
	if cfg == nil {
		return fmt.Errorf("connection configuration is required")
	}
	p.Cfg = cfg.Clone()
	proxyCfg, err := p.Cfg.SSHProxyConfig()
	if err != nil {
		return fmt.Errorf("invalid SSH proxy configuration: %w", err)
	}
	if proxyCfg == nil {
		return nil
	}
	tunnel, local, err := proxy.CreateSSHTunnel(proxyCfg, &proxy.Endpoint{Host: p.Cfg.Host, Port: p.Cfg.Port})
	if err != nil {
		return err
	}
	if err := tunnel.Start(); err != nil {
		return err
	}
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), tunnel.Config.Timeout)
	err = tunnel.Verify(verifyCtx)
	cancelVerify()
	if err != nil {
		_ = tunnel.Stop()
		return err
	}
	p.Cfg.Host = local.Host
	p.Cfg.Port = local.Port
	p.tunnel = tunnel
	return nil
}
