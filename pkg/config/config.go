package config

import (
	"fmt"
	"net/url"
	"reflect"
	"time"

	"github.com/jacktea/data-smith/pkg/consts"
)

type DBParams map[string]any

// ConnConfig defines the database connection configuration.
type ConnConfig struct {
	Type            consts.DBType `yaml:"type"`
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	DBName          string        `yaml:"dbname"`
	TableSchema     string        `yaml:"tableSchema"`
	SSL             bool          `yaml:"ssl"`
	Extra           DBParams      `yaml:"extra"`
	Proxy           any           `yaml:"proxy"`
	ConnectTimeout  time.Duration `yaml:"connectTimeout"`
	MaxOpenConns    int           `yaml:"maxOpenConns"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
	ConnMaxLifetime time.Duration `yaml:"connMaxLifetime"`
	ConnMaxIdleTime time.Duration `yaml:"connMaxIdleTime"`
}

// Clone returns a connection configuration whose mutable fields are independent
// from the caller-owned value. Concrete proxy values are copied as well.
func (c *ConnConfig) Clone() *ConnConfig {
	if c == nil {
		return nil
	}
	clone := *c
	if c.Extra != nil {
		clone.Extra = make(DBParams, len(c.Extra))
		for key, value := range c.Extra {
			clone.Extra[key] = cloneConfigValue(value)
		}
	}
	switch proxy := c.Proxy.(type) {
	case *SSHProxy:
		if proxy != nil {
			proxyClone := *proxy
			clone.Proxy = &proxyClone
		}
	case SSHProxy:
		clone.Proxy = proxy
	case map[string]any:
		clone.Proxy = cloneStringMap(proxy)
	case map[any]any:
		clone.Proxy = cloneAnyMap(proxy)
	}
	return &clone
}

func cloneStringMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = cloneConfigValue(value)
	}
	return result
}

func cloneAnyMap(input map[any]any) map[any]any {
	result := make(map[any]any, len(input))
	for key, value := range input {
		result[key] = cloneConfigValue(value)
	}
	return result
}

func cloneConfigValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneStringMap(typed)
	case map[any]any:
		return cloneAnyMap(typed)
	case []any:
		clone := make([]any, len(typed))
		for i := range typed {
			clone[i] = cloneConfigValue(typed[i])
		}
		return clone
	default:
		return value
	}
}

// SSHProxyConfig normalizes supported concrete and YAML-decoded proxy shapes.
// A configured but malformed proxy is always returned as an actionable error.
func (c *ConnConfig) SSHProxyConfig() (*SSHProxy, error) {
	if c == nil || c.Proxy == nil {
		return nil, nil
	}
	value := reflect.ValueOf(c.Proxy)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return nil, fmt.Errorf("proxy configuration must not be a typed nil")
	}

	var proxy SSHProxy
	switch configured := c.Proxy.(type) {
	case *SSHProxy:
		proxy = *configured
	case SSHProxy:
		proxy = configured
	case map[string]any:
		if err := decodeSSHProxyMap(configured, &proxy); err != nil {
			return nil, err
		}
	case map[any]any:
		mapped := make(map[string]any, len(configured))
		for key, value := range configured {
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("proxy configuration key %v must be a string", key)
			}
			mapped[name] = value
		}
		if err := decodeSSHProxyMap(mapped, &proxy); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("proxy configuration has unsupported type %T; expected config.SSHProxy or a YAML map", c.Proxy)
	}
	return &proxy, nil
}

func decodeSSHProxyMap(values map[string]any, proxy *SSHProxy) error {
	allowed := map[string]bool{
		"host": true, "port": true, "user": true, "type": true, "pass": true,
		"rsaKey": true, "rsaKeyPath": true, "rsaKeyPassword": true,
		"knownHostsPath": true, "hostFingerprint": true,
	}
	for key := range values {
		if !allowed[key] {
			return fmt.Errorf("proxy configuration contains unknown field %q", key)
		}
	}
	readString := func(key string, target *string) error {
		value, ok := values[key]
		if !ok || value == nil {
			return nil
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("proxy field %q must be a string, got %T", key, value)
		}
		*target = text
		return nil
	}
	for _, item := range []struct {
		key    string
		target *string
	}{
		{"host", &proxy.Host}, {"user", &proxy.User}, {"type", &proxy.Type},
		{"pass", &proxy.Pass}, {"rsaKey", &proxy.RsaKey}, {"rsaKeyPath", &proxy.RsaKeyPath},
		{"rsaKeyPassword", &proxy.RsaKeyPassword}, {"knownHostsPath", &proxy.KnownHostsPath},
		{"hostFingerprint", &proxy.HostFingerprint},
	} {
		if err := readString(item.key, item.target); err != nil {
			return err
		}
	}
	if value, ok := values["port"]; ok && value != nil {
		switch port := value.(type) {
		case int:
			proxy.Port = port
		case int64:
			proxy.Port = int(port)
		case uint64:
			if port > uint64(^uint(0)>>1) {
				return fmt.Errorf("proxy field %q is out of range", "port")
			}
			proxy.Port = int(port)
		default:
			return fmt.Errorf("proxy field %q must be an integer, got %T", "port", value)
		}
	}
	return nil
}

func (c *ConnConfig) ExtraString() string {
	values := make(url.Values, len(c.Extra))
	for k, v := range c.Extra {
		values.Set(k, fmt.Sprint(v))
	}
	if len(values) > 0 {
		return "?" + values.Encode()
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
	SourceDB      ConnConfig `yaml:"sourceDb"`
	TargetDB      ConnConfig `yaml:"targetDb"`
	IncludeTables []string   `yaml:"includeTables"`
	ExcludeTables []string   `yaml:"excludeTables"`
}

// Rule defines a single comparison rule.
type Rule struct {
	Table         string   `json:"table"`
	Columns       []string `json:"columns"`
	ComparisonKey []string `json:"comparisonKey"`
	IgnoreColumns []string `json:"ignoreColumns"`
}

// RuleSet defines a set of comparison rules.
type RuleSet struct {
	Rules []Rule `json:"rules"`
}

type SSHProxy struct {
	Host            string `json:"host" yaml:"host" dc:"主机"`
	Port            int    `json:"port" yaml:"port" dc:"端口"`
	User            string `json:"user" yaml:"user" dc:"用户"`
	Type            string `json:"type" yaml:"type" dc:"验证类型: pass/rsa"`
	Pass            string `json:"pass" yaml:"pass" dc:"密码"`
	RsaKey          string `json:"rsaKey" yaml:"rsaKey" dc:"RSA私钥内容"`
	RsaKeyPath      string `json:"rsaKeyPath" yaml:"rsaKeyPath" dc:"RSA私钥文件路径"`
	RsaKeyPassword  string `json:"rsaKeyPassword" yaml:"rsaKeyPassword" dc:"RSA私钥密码"`
	KnownHostsPath  string `json:"knownHostsPath" yaml:"knownHostsPath" dc:"known_hosts文件路径"`
	HostFingerprint string `json:"hostFingerprint" yaml:"hostFingerprint" dc:"固定的SHA256主机指纹"`
}
