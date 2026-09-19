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
	p.Cfg.Host = local.Host
	p.Cfg.Port = local.Port
	p.tunnel = tunnel
	return nil
}
