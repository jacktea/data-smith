package config

import (
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/pkg/consts"
)

type DBParams map[string]any

// ConnConfig defines the database connection configuration.
type ConnConfig struct {
	Type        consts.DBType `yaml:"type"`
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	User        string        `yaml:"user"`
	Password    string        `yaml:"password"`
	DBName      string        `yaml:"dbname"`
	TableSchema string        `yaml:"tableSchema"`
	SSL         bool          `yaml:"ssl"`
	Extra       DBParams      `yaml:"extra"`
	Proxy       any           `yaml:"proxy"`
}

func (c *ConnConfig) ExtraString() string {
	var sb strings.Builder
	for k, v := range c.Extra {
		sb.WriteString(fmt.Sprintf("%s=%v&", k, v))
	}
	if sb.Len() > 0 {
		return "?" + sb.String()[:sb.Len()-1]
	}
	return ""
}

func (c *ConnConfig) ContainsExtra(key string) bool {
	_, ok := c.Extra[key]
	return ok
}

func (c *ConnConfig) GetExtra(key string) any {
	return c.Extra[key]
}

func (c *ConnConfig) SetExtra(key string, value any) {
	if c.Extra == nil {
		c.Extra = make(DBParams)
	}
	c.Extra[key] = value
}

func (c *ConnConfig) RemoveExtra(key string) {
	delete(c.Extra, key)
}

// Config defines the main application configuration.
type Config struct {
	SourceDB ConnConfig `yaml:"sourceDb"`
	TargetDB ConnConfig `yaml:"targetDb"`
}

// Rule defines a single comparison rule.
type Rule struct {
	Table         string   `json:"table"`
	ComparisonKey []string `json:"comparisonKey"`
}

// RuleSet defines a set of comparison rules.
type RuleSet struct {
	Rules []Rule `json:"rules"`
}

type SSHProxy struct {
	Host           string `json:"host" dc:"主机"`
	Port           int    `json:"port" dc:"端口"`
	User           string `json:"user" dc:"用户"`
	Type           string `json:"type" dc:"验证类型: pass/rsa"`
	Pass           string `json:"pass" dc:"密码"`
	RsaKey         string `json:"rsaKey" dc:"RSA私钥内容"`
	RsaKeyPath     string `json:"rsaKeyPath" dc:"RSA私钥文件路径"`
	RsaKeyPassword string `json:"rsaKeyPassword" dc:"RSA私钥密码"`
	// KnownHosts     string `json:"known_hosts" dc:"已知主机文件内容"`
	// KnownHostsPath string `json:"known_hosts_path" dc:"已知主机文件路径"`
}
