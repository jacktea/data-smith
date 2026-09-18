package mysql

import (
	"errors"
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

func TestGetTableDataBatchPropagatesCursorError(t *testing.T) {
	adapter, mock := newMockMySQLAdapter(t)
	wantErr := errors.New("cursor interrupted")
	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)).RowError(1, wantErr)
	mock.ExpectQuery("SELECT .* FROM `items` .*").WithArgs(2).WillReturnRows(rows)

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
