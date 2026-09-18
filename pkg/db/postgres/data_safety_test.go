package postgres

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db/base"
)

func newMockPostgresAdapter(t *testing.T) (*PostgresAdapter, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &PostgresAdapter{BaseAdapter: base.BaseAdapter{Conn: db, Cfg: &config.ConnConfig{TableSchema: "public"}}}, mock
}

func TestGetTableDataBatchPropagatesCursorError(t *testing.T) {
	adapter, mock := newMockPostgresAdapter(t)
	wantErr := errors.New("cursor interrupted")
	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)).RowError(1, wantErr)
	mock.ExpectQuery(`SELECT .* FROM "public"\."items" .*`).WithArgs(2).WillReturnRows(rows)

	_, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id"}, nil, 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected cursor error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetTableDataBatchRejectsInvalidInputsBeforeQuery(t *testing.T) {
	adapter, mock := newMockPostgresAdapter(t)
	if _, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id"}, nil, 0); err == nil {
		t.Fatal("expected invalid batch size error")
	}
	if _, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id", "part"}, []any{1}, 10); err == nil {
		t.Fatal("expected invalid composite key boundary error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChunkRangesStartUnboundedAndHashDistinguishesNull(t *testing.T) {
	adapter, mock := newMockPostgresAdapter(t)
	statsQuery := regexp.QuoteMeta(`SELECT COUNT(*), MIN("id"), MAX("id") FROM "public"."items"`)
	mock.ExpectQuery(statsQuery).WillReturnRows(sqlmock.NewRows([]string{"count", "min", "max"}).AddRow(int64(3), int64(1), int64(3)))
	mock.ExpectQuery(`(?s)SELECT pk FROM .*ROW_NUMBER\(\) OVER.*`).WithArgs(2).
		WillReturnRows(sqlmock.NewRows([]string{"pk"}).AddRow(int64(1)).AddRow(int64(3)))

	ranges, err := adapter.GetChunkRanges("items", "id", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 2 || ranges[0].MinPK != nil || ranges[0].MaxPK != int64(3) || ranges[0].IsLast || ranges[1].MinPK != int64(3) || ranges[1].MaxPK != nil || !ranges[1].IsLast {
		t.Fatalf("unexpected ranges: %#v", ranges)
	}

	mock.ExpectQuery(`(?s)CASE WHEN "value" IS NULL THEN 'N'.*WHERE "id" < \$1`).WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"hash"}).AddRow("abc"))
	hash, err := adapter.GetChunkHash("items", []string{"id", "value"}, "id", nil, int64(3), false)
	if err != nil {
		t.Fatal(err)
	}
	if hash != "abc" {
		t.Fatalf("unexpected hash %q", hash)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
