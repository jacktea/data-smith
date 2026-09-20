package base

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// recordingQuerier 记录收到的查询，用于断言读取通道确实切换到会话。
type recordingQuerier struct {
	queries []string
}

func (q *recordingQuerier) QueryContext(_ context.Context, query string, _ ...any) (*sql.Rows, error) {
	q.queries = append(q.queries, query)
	return nil, errors.New("fake querier: no rows")
}

func (q *recordingQuerier) QueryRowContext(_ context.Context, query string, _ ...any) *sql.Row {
	q.queries = append(q.queries, query)
	return nil
}

func TestBindSessionRoutesReadsToSession(t *testing.T) {
	pool, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer pool.Close()
	adapter := &BaseAdapter{Conn: pool}

	// 未绑定会话：读取必须落连接池（sqlmock 无期望 → 池侧报错，而非 fake 报错）。
	if _, err := adapter.QueryContext(context.Background(), "SELECT 1"); err == nil || strings.Contains(err.Error(), "fake querier") {
		t.Fatalf("unbound read must hit the pool, got %v", err)
	}

	fake := &recordingQuerier{}
	adapter.BindSession(fake)
	if _, err := adapter.QueryContext(context.Background(), "SELECT 1"); err == nil || !strings.Contains(err.Error(), "fake querier") {
		t.Fatalf("bound read must route to the session, got %v", err)
	}
	adapter.QueryRow(context.Background(), "SELECT 2")
	if len(fake.queries) != 2 || fake.queries[0] != "SELECT 1" || fake.queries[1] != "SELECT 2" {
		t.Fatalf("session queries = %v, want both reads routed", fake.queries)
	}

	// 解绑恢复连接池：影子事务结束后的后续读取不再进入已回滚的会话。
	adapter.BindSession(nil)
	if _, err := adapter.QueryContext(context.Background(), "SELECT 3"); err == nil || strings.Contains(err.Error(), "fake querier") {
		t.Fatalf("unbound read must fall back to the pool, got %v", err)
	}
	if len(fake.queries) != 2 {
		t.Fatalf("session must not receive reads after unbind, got %v", fake.queries)
	}
}
