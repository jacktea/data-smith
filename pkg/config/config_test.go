package config

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestSSHProxyConfigNormalizesYAMLMap(t *testing.T) {
	input := `
sourceDb:
  type: mysql
  host: database.internal
  port: 3306
  proxy:
    host: bastion.internal
    port: 22
    user: deploy
    type: pass
    pass: obvious-test-placeholder
    hostFingerprint: SHA256:abcdefghijklmnopqrstuvwxyz0123456789
`
	var cfg Config
	if err := yaml.Unmarshal([]byte(input), &cfg); err != nil {
		t.Fatalf("unmarshal YAML: %v", err)
	}
	proxy, err := cfg.SourceDB.SSHProxyConfig()
	if err != nil {
		t.Fatalf("normalize YAML proxy: %v", err)
	}
	if proxy == nil || proxy.Host != "bastion.internal" || proxy.Port != 22 || proxy.User != "deploy" {
		t.Fatalf("unexpected normalized proxy: %#v", proxy)
	}
}

func TestSSHProxyConfigAcceptedConcreteAndMapShapes(t *testing.T) {
	want := SSHProxy{Host: "bastion", Port: 22, User: "user", Type: "pass", Pass: "placeholder", HostFingerprint: "SHA256:abcdefghijklmnopqrstuvwxyz0123456789"}
	tests := []any{
		want,
		&want,
		map[string]any{"host": want.Host, "port": want.Port, "user": want.User, "type": want.Type, "pass": want.Pass, "hostFingerprint": want.HostFingerprint},
		map[any]any{"host": want.Host, "port": want.Port, "user": want.User, "type": want.Type, "pass": want.Pass, "hostFingerprint": want.HostFingerprint},
	}
	for _, shape := range tests {
		got, err := (&ConnConfig{Proxy: shape}).SSHProxyConfig()
		if err != nil {
			t.Fatalf("normalize %T: %v", shape, err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("normalize %T: got %#v want %#v", shape, *got, want)
		}
	}
}

func TestSSHProxyConfigRejectsConfiguredInvalidShapes(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{value: "ssh://bastion", want: "unsupported type"},
		{value: map[string]any{"port": "22"}, want: `field "port" must be an integer`},
		{value: map[string]any{"unexpected": true}, want: `unknown field "unexpected"`},
		{value: map[any]any{1: "bad"}, want: "must be a string"},
		{value: map[string]any{"allowInsecureHostKey": "yes"}, want: `field "allowInsecureHostKey" must be a boolean`},
	}
	for _, test := range tests {
		_, err := (&ConnConfig{Proxy: test.value}).SSHProxyConfig()
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("value %#v: got %v, want error containing %q", test.value, err, test.want)
		}
	}
}

func TestSSHProxyConfigDecodesInsecureHostKeyFlag(t *testing.T) {
	got, err := (&ConnConfig{Proxy: map[string]any{
		"host": "bastion", "port": 22, "user": "u", "type": "pass", "pass": "p",
		"allowInsecureHostKey": true,
	}}).SSHProxyConfig()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.AllowInsecureHostKey {
		t.Fatal("allowInsecureHostKey was not decoded")
	}
}

func TestConnConfigCloneDoesNotShareMutableValues(t *testing.T) {
	proxy := &SSHProxy{Host: "bastion", Port: 22}
	original := &ConnConfig{Extra: DBParams{"search_path": "original"}, Proxy: proxy}
	before := original.Clone()
	clone := original.Clone()
	clone.SetExtra("search_path", "changed")
	clone.Proxy.(*SSHProxy).Host = "changed"
	if !reflect.DeepEqual(original, before) {
		t.Fatalf("caller config mutated: got %#v want %#v", original, before)
	}
}

func TestEffectiveExcludeTables(t *testing.T) {
	t.Run("nil or empty excludes returns default tables", func(t *testing.T) {
		got := EffectiveExcludeTables(nil)
		want := []string{"flyway_schema_history", "schema_migrations"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}

		gotEmpty := EffectiveExcludeTables([]string{})
		if !reflect.DeepEqual(gotEmpty, want) {
			t.Fatalf("got %v, want %v", gotEmpty, want)
		}
	})

	t.Run("merges user tables and deduplicates case-insensitively", func(t *testing.T) {
		userExcludes := []string{
			"custom_table",
			"FLYWAY_SCHEMA_HISTORY", // 应该去重
			"schema_migrations",     // 应该去重
			"another_table",
		}
		got := EffectiveExcludeTables(userExcludes)
		want := []string{"flyway_schema_history", "schema_migrations", "custom_table", "another_table"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestConfigGetEffectiveExcludeTables(t *testing.T) {
	var nilCfg *Config
	if got := nilCfg.GetEffectiveExcludeTables(); len(got) != 2 {
		t.Fatalf("nil config should return default excludes, got %v", got)
	}

	cfg := &Config{
		ExcludeTables: []string{"audit_log"},
	}
	got := cfg.GetEffectiveExcludeTables()
	want := []string{"flyway_schema_history", "schema_migrations", "audit_log"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
