package mysql

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db/base"
)

func newMockMySQLAdapter(t *testing.T) (*MySQLAdapter, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &MySQLAdapter{BaseAdapter: base.BaseAdapter{Conn: db, Cfg: &config.ConnConfig{TableSchema: "test"}}}, mock
}

func TestChunkRangesUseBoundedKeysetProgression(t *testing.T) {
	adapter, mock := newMockMySQLAdapter(t)
	statsQuery := regexp.QuoteMeta("SELECT COUNT(*), MIN(`id`), MAX(`id`) FROM `test`.`items`")
	mock.ExpectQuery(statsQuery).WillReturnRows(sqlmock.NewRows([]string{"count", "min", "max"}).AddRow(int64(5), int64(1), int64(5)))
	mock.ExpectQuery("SELECT `id` FROM `test`\\.`items` ORDER BY `id` ASC LIMIT \\?").WithArgs(3).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)).AddRow(int64(3)))
	mock.ExpectQuery("SELECT `id` FROM `test`\\.`items` WHERE `id` >= \\? ORDER BY `id` ASC LIMIT \\?").WithArgs(int64(3), 3).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(3)).AddRow(int64(4)).AddRow(int64(5)))
	mock.ExpectQuery("SELECT `id` FROM `test`\\.`items` WHERE `id` >= \\? ORDER BY `id` ASC LIMIT \\?").WithArgs(int64(5), 3).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(5)))

	ranges, err := adapter.GetChunkRanges("items", "id", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 3 || ranges[0].MinPK != nil || ranges[0].MaxPK != int64(3) || ranges[1].MinPK != int64(3) || ranges[1].MaxPK != int64(5) || ranges[2].MinPK != int64(5) || ranges[2].MaxPK != nil || !ranges[2].IsLast {
		t.Fatalf("unexpected ranges: %#v", ranges)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetTableDataBatchPropagatesCursorError(t *testing.T) {
	adapter, mock := newMockMySQLAdapter(t)
	wantErr := errors.New("cursor interrupted")
	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)).RowError(1, wantErr)
	mock.ExpectQuery("SELECT .* FROM `test`\\.`items` .*").WithArgs(2).WillReturnRows(rows)

	_, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id"}, nil, 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected cursor error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetTableDataBatchRejectsInvalidInputsBeforeQuery(t *testing.T) {
	adapter, mock := newMockMySQLAdapter(t)
	if _, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id"}, nil, -1); err == nil {
		t.Fatal("expected invalid batch size error")
	}
	if _, err := adapter.GetTableDataBatch("items", []string{"id"}, []string{"id", "part"}, []any{1}, 10); err == nil {
		t.Fatal("expected invalid composite key boundary error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
