package base

import (
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"golang.org/x/crypto/ssh"
)

func testFingerprint(t *testing.T) string {
	t.Helper()
	public, _, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	key, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatalf("SSH public key: %v", err)
	}
	return ssh.FingerprintSHA256(key)
}

func TestApplyConnectionSettingsConfiguresPoolLimit(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	cfg := &config.ConnConfig{MaxOpenConns: 7, MaxIdleConns: 3, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Second}
	ApplyConnectionSettings(db, cfg)
	if got := db.Stats().MaxOpenConnections; got != 7 {
		t.Fatalf("max open connections: got %d want 7", got)
	}
}

func TestInitClonesConfigBeforeTunnelEndpointMutation(t *testing.T) {
	original := &config.ConnConfig{
		Host: "database.internal", Port: 5432,
		Extra: config.DBParams{"search_path": "original"},
		Proxy: map[any]any{
			"host": "127.0.0.1", "port": 22, "user": "tester", "type": "pass",
			"pass": "obvious-placeholder", "hostFingerprint": testFingerprint(t),
		},
	}
	before := original.Clone()
	adapter := &BaseAdapter{}
	if err := adapter.Init(original); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	if !reflect.DeepEqual(original, before) {
		t.Fatalf("caller config changed on success: got %#v want %#v", original, before)
	}
	if adapter.Cfg == original || adapter.Cfg.Host != "127.0.0.1" || adapter.Cfg.Port == 0 {
		t.Fatalf("adapter did not retain independent tunnel endpoint: %#v", adapter.Cfg)
	}
}

func TestInitFailureLeavesCallerConfigUnchanged(t *testing.T) {
	original := &config.ConnConfig{
		Host: "database.internal", Port: 5432,
		Extra: config.DBParams{"search_path": "original"},
		Proxy: map[string]any{"host": "bastion", "port": "bad"},
	}
	before := original.Clone()
	err := (&BaseAdapter{}).Init(original)
	if err == nil || !strings.Contains(err.Error(), "invalid SSH proxy configuration") {
		t.Fatalf("got %v, want proxy validation error", err)
	}
	if !reflect.DeepEqual(original, before) {
		t.Fatalf("caller config changed on failure: got %#v want %#v", original, before)
	}
}

func TestNormalizeConnectionSettingsValidatesAndAppliesDefaults(t *testing.T) {
	cfg := &config.ConnConfig{}
	if err := NormalizeConnectionSettings(cfg); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if cfg.ConnectTimeout <= 0 || cfg.MaxOpenConns <= 0 || cfg.MaxIdleConns <= 0 || cfg.ConnMaxLifetime <= 0 || cfg.ConnMaxIdleTime <= 0 {
		t.Fatalf("defaults were not applied: %#v", cfg)
	}
	lowLimit := &config.ConnConfig{MaxOpenConns: 1}
	if err := NormalizeConnectionSettings(lowLimit); err != nil || lowLimit.MaxIdleConns != 1 {
		t.Fatalf("idle default did not respect max open limit: %#v, %v", lowLimit, err)
	}
	bad := &config.ConnConfig{MaxOpenConns: 1, MaxIdleConns: 2}
	if err := NormalizeConnectionSettings(bad); err == nil {
		t.Fatal("invalid pool limits were accepted")
	}
	negative := &config.ConnConfig{ConnectTimeout: -time.Second}
	if err := NormalizeConnectionSettings(negative); err == nil {
		t.Fatal("negative timeout was accepted")
	}
}

func TestRedactErrorDoesNotLeakCredentials(t *testing.T) {
	cfg := &config.ConnConfig{User: "sensitive user@example", Password: "sensitive:password/?#"}
	encodedUser := url.User(cfg.User).String()
	encodedPassword := strings.TrimPrefix(url.UserPassword("", cfg.Password).String(), ":")
	err := RedactError(errors.New("login "+cfg.User+" "+cfg.Password+" "+encodedUser+" "+encodedPassword+" failed"), cfg)
	if strings.Contains(err.Error(), cfg.User) || strings.Contains(err.Error(), cfg.Password) || strings.Contains(err.Error(), encodedUser) || strings.Contains(err.Error(), encodedPassword) {
		t.Fatalf("credential leaked: %v", err)
	}
}
