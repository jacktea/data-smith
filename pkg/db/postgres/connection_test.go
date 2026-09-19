package postgres

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db/base"
)

func TestBuildPostgresDSNRoundTripsSpecialCharacters(t *testing.T) {
	cfg := &config.ConnConfig{
		Host:        "2001:db8::1",
		Port:        5432,
		User:        "user:name@host",
		Password:    "obvious-test-placeholder:p@ss/?#&=% word",
		DBName:      "db/name?x",
		TableSchema: "odd,schema & two",
		Extra: config.DBParams{
			"application_name": "data smith=tests",
		},
	}
	dsn := buildPostgresDSN(cfg)
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse generated DSN: %v", err)
	}
	password, ok := parsed.User.Password()
	if parsed.User.Username() != cfg.User || !ok || password != cfg.Password {
		t.Fatalf("credentials did not round trip: %s", parsed.Redacted())
	}
	if parsed.Path != "/"+cfg.DBName {
		t.Fatalf("database did not round trip: got %q want %q", parsed.Path, "/"+cfg.DBName)
	}
	if got := parsed.Query().Get("search_path"); got != `"odd,schema & two"` {
		t.Fatalf("schema parameter: got %q", got)
	}
	if got := parsed.Query().Get("application_name"); got != "data smith=tests" {
		t.Fatalf("application_name: got %q", got)
	}
}

func TestBuildPostgresDSNQuotesSearchPathIdentifier(t *testing.T) {
	cfg := &config.ConnConfig{
		Host:        "127.0.0.1",
		Port:        5432,
		User:        "test",
		Password:    "obvious-placeholder",
		DBName:      "application",
		TableSchema: `Mixed,"Schema`,
		Extra:       config.DBParams{"sslmode": "disable"},
	}
	parsed, err := url.Parse(buildPostgresDSN(cfg))
	if err != nil {
		t.Fatalf("parse generated DSN: %v", err)
	}
	if got, want := parsed.Query().Get("search_path"), `"Mixed,""Schema"`; got != want {
		t.Fatalf("search_path = %q, want %q", got, want)
	}
}

func TestGetTableDataBatchContextHonorsCancellation(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	adapter := &PostgresAdapter{BaseAdapter: base.BaseAdapter{Conn: database, Cfg: &config.ConnConfig{TableSchema: "public"}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = adapter.GetTableDataBatchContext(ctx, "items", []string{"id"}, []string{"id"}, nil, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context canceled", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database work: %v", err)
	}
}

func TestNewPostgresAdapterConnectionFailureDoesNotMutateInput(t *testing.T) {
	cfg := &config.ConnConfig{Host: "127.0.0.1", Port: 1, User: "example", Password: "obvious-placeholder", DBName: "application", Extra: config.DBParams{"application_name": "test"}}
	before := cfg.Clone()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewPostgresAdapterContext(ctx, cfg); err == nil {
		t.Fatal("canceled connection unexpectedly succeeded")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatalf("input mutated on connection failure: got %#v want %#v", cfg, before)
	}
}
