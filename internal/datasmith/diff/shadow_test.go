package diff

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestFinishShadowDataDiffFailsWhenRollbackFails(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New(): %v", err)
	}
	defer database.Close()

	mock.ExpectBegin()
	tx, err := database.Begin()
	if err != nil {
		t.Fatalf("begin shadow transaction: %v", err)
	}
	rollbackErr := errors.New("connection lost during rollback")
	mock.ExpectRollback().WillReturnError(rollbackErr)

	unbound := false
	var logs []string
	err = finishShadowDataDiff(
		&shadowTx{tx: tx},
		func() { unbound = true },
		func(message string) { logs = append(logs, message) },
	)
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("finishShadowDataDiff() error = %v, want rollback error", err)
	}
	if !unbound {
		t.Fatal("shadow session was not unbound before rollback")
	}
	if strings.Contains(strings.Join(logs, "\n"), "恢复原状") {
		t.Fatalf("logs claim recovery despite rollback failure: %v", logs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
