package migrate

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/consts"
)

func TestBuildResetSQLQuotesIdentifiers(t *testing.T) {
	mysqlSQL, err := BuildResetSQL(&config.ConnConfig{Type: consts.DBTypeMySQL, DBName: "orders`archive"})
	if err != nil {
		t.Fatalf("MySQL SQL: %v", err)
	}
	if mysqlSQL != "DROP DATABASE IF EXISTS `orders``archive`; CREATE DATABASE `orders``archive`;" {
		t.Fatalf("unexpected MySQL SQL: %s", mysqlSQL)
	}
	postgresSQL, err := BuildResetSQL(&config.ConnConfig{Type: consts.DBTypePostgres, DBName: "application", TableSchema: `team"data`})
	if err != nil {
		t.Fatalf("PostgreSQL SQL: %v", err)
	}
	if postgresSQL != `DROP SCHEMA "team""data" CASCADE; CREATE SCHEMA "team""data";` {
		t.Fatalf("unexpected PostgreSQL SQL: %s", postgresSQL)
	}
}

// 连接档案允许 tableSchema 留空;与 PostgreSQL 适配器其余路径一致,
// 重置目标此时应默认为 public,而不是拒绝。
func TestBuildResetSQLDefaultsEmptySchemaToPublic(t *testing.T) {
	for _, schema := range []string{"", "   "} {
		sql, err := BuildResetSQL(&config.ConnConfig{Type: consts.DBTypePostgres, DBName: "application", TableSchema: schema})
		if err != nil {
			t.Fatalf("schema %q: %v", schema, err)
		}
		if want := `DROP SCHEMA "public" CASCADE; CREATE SCHEMA "public";`; sql != want {
			t.Fatalf("schema %q: got %s, want %s", schema, sql, want)
		}
	}
}

func TestResetDatabaseRejectsDangerousTargetBeforeConnectionAccess(t *testing.T) {
	adapter := &mockAdapter{cfg: &config.ConnConfig{Type: consts.DBTypePostgres, DBName: "application", TableSchema: "pg_catalog"}}
	err := ResetDatabase(adapter)
	if err == nil || !strings.Contains(err.Error(), "system schema") {
		t.Fatalf("got %v, want system schema rejection", err)
	}
}

func TestResetDatabaseContextHonorsDeadline(t *testing.T) {
	adapter, mock := newMockAdapter(t, consts.DBTypeMySQL)
	adapter.cfg.DBName = "application"
	query, err := BuildResetSQL(adapter.cfg)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(regexp.QuoteMeta(query)).WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = ResetDatabaseContext(ctx, adapter)
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "canceling query") {
		t.Fatalf("got %v, want context deadline", err)
	}
}

func TestValidateResetTargetRejectsSystemTargets(t *testing.T) {
	tests := []*config.ConnConfig{
		{Type: consts.DBTypeMySQL},
		{Type: consts.DBTypeMySQL, DBName: "mysql"},
		{Type: consts.DBTypePostgres, DBName: "application", TableSchema: "information_schema"},
		{Type: consts.DBTypePostgres, DBName: "application", TableSchema: "pg_catalog"},
		{Type: consts.DBTypePostgres, DBName: "postgres", TableSchema: "public"},
	}
	for _, cfg := range tests {
		if err := ValidateResetTarget(cfg); err == nil {
			t.Fatalf("dangerous target accepted: %#v", cfg)
		}
	}
	// 空 schema 默认 public,属于合法目标。
	if err := ValidateResetTarget(&config.ConnConfig{Type: consts.DBTypePostgres, DBName: "application"}); err != nil {
		t.Fatalf("empty schema should default to public: %v", err)
	}
}
