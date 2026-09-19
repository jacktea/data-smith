package diff

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
)

type contextOnlyDB struct {
	cfg config.ConnConfig
}

func (d *contextOnlyDB) ReadSchema() (*conn.DatabaseSchema, error) { return nil, nil }
func (d *contextOnlyDB) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	panic("legacy row reader must not be used by context comparison")
}
func (d *contextOnlyDB) GetTableDataBatchContext(ctx context.Context, _ string, _, _ []string, _ []any, _ int) ([]conn.Record, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *contextOnlyDB) ExtractTable(string) (*conn.Table, error) { return nil, nil }
func (d *contextOnlyDB) ExtractView(string) (*conn.Table, error)  { return nil, nil }
func (d *contextOnlyDB) GetConn() *sql.DB                         { return nil }
func (d *contextOnlyDB) GetConfig() *config.ConnConfig            { return &d.cfg }
func (d *contextOnlyDB) Close() error                             { return nil }

func TestStreamCompareDataContextUsesCancellableAdapter(t *testing.T) {
	table := &conn.Table{
		Name: "items",
		Columns: map[string]*conn.Column{
			"id": {Name: "id", DataType: "bigint", Position: 1},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rule := CreateCompareRule(table, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := StreamCompareDataDetailedWithTableContext(ctx, &contextOnlyDB{}, &contextOnlyDB{}, rule, table, 10, func(DiffType, conn.Record, conn.Record, []string) error {
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline", err)
	}
}
