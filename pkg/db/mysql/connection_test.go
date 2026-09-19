package mysql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db/base"
)

func TestBuildMySQLDSNRoundTripsSpecialCharacters(t *testing.T) {
	cfg := &config.ConnConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "user name%?&=host",
		Password: "obvious-test-placeholder:p@ss/?#&=% word",
		DBName:   "db/name?x",
		Extra: config.DBParams{
			"sql_mode": "ANSI_QUOTES,NO_ZERO_DATE",
			"custom":   "a&b=c space",
		},
	}
	dsn := buildMySQLDSN(cfg)
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse generated DSN: %v", err)
	}
	if parsed.User != cfg.User || parsed.Passwd != cfg.Password || parsed.DBName != cfg.DBName {
		t.Fatalf("credentials/database did not round trip: %#v", parsed)
	}
	if parsed.Params["custom"] != "a&b=c space" || parsed.Params["sql_mode"] != "ANSI_QUOTES,NO_ZERO_DATE" {
		t.Fatalf("query parameters did not round trip: %#v", parsed.Params)
	}
	if !parsed.ParseTime || !parsed.MultiStatements {
		t.Fatalf("required driver settings missing: %#v", parsed)
	}
}

func TestGetTableDataBatchContextHonorsCancellation(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	adapter := &MySQLAdapter{BaseAdapter: base.BaseAdapter{Conn: database, Cfg: &config.ConnConfig{TableSchema: "test"}}}
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

func TestNewMySQLAdapterValidationDoesNotMutateInput(t *testing.T) {
	cfg := &config.ConnConfig{MaxOpenConns: 1, MaxIdleConns: 2, Extra: config.DBParams{"custom": "value"}}
	before := cfg.Clone()
	if _, err := NewMySQLAdapterContext(context.Background(), cfg); err == nil {
		t.Fatal("invalid pool configuration was accepted")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatalf("input mutated on failure: got %#v want %#v", cfg, before)
	}
}

func TestNewMySQLAdapterConnectionFailureDoesNotMutateInput(t *testing.T) {
	cfg := &config.ConnConfig{Host: "127.0.0.1", Port: 1, User: "example", Password: "obvious-placeholder", DBName: "application", Extra: config.DBParams{"custom": "value"}}
	before := cfg.Clone()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMySQLAdapterContext(ctx, cfg); err == nil {
		t.Fatal("canceled connection unexpectedly succeeded")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatalf("input mutated on connection failure: got %#v want %#v", cfg, before)
	}
}
