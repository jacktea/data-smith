package base

import (
	"database/sql"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/proxy"
)

type BaseAdapter struct {
	ConnId string
	Conn   *sql.DB
	tunnel *proxy.SSHTunnel
	Cfg    *config.ConnConfig
}

func (p *BaseAdapter) Close() error {
	defer func() {
		if p.tunnel != nil {
			p.tunnel.Stop()
		}
	}()
	if p.Conn != nil {
		return p.Conn.Close()
	}
	return nil
}

func (p *BaseAdapter) Init(cfg *config.ConnConfig) error {
	p.Cfg = cfg
	if cfg.Proxy == nil {
		return nil
	}
	proxyCfg, ok := cfg.Proxy.(*config.SSHProxy)
	if ok && proxyCfg != nil {
		tunnel, local, err := proxy.CreateSSHTunnel(proxyCfg, &proxy.Endpoint{Host: cfg.Host, Port: cfg.Port})
		if err != nil {
			return err
		}
		cfg.Host = local.Host
		cfg.Port = local.Port
		err = tunnel.Start()
		if err != nil {
			return err
		}
		p.tunnel = tunnel
	}
	return nil
}
