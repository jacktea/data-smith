package diff

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

type metadataCountingDB struct {
	table        *conn.Table
	extractCalls int
}

func (d *metadataCountingDB) ReadSchema() (*conn.DatabaseSchema, error) { return nil, nil }
func (d *metadataCountingDB) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}
func (d *metadataCountingDB) ExtractTable(string) (*conn.Table, error) {
	d.extractCalls++
	return d.table, nil
}
func (d *metadataCountingDB) ExtractView(string) (*conn.Table, error) { return nil, nil }
func (d *metadataCountingDB) GetConn() *sql.DB                        { return nil }
func (d *metadataCountingDB) GetConfig() *config.ConnConfig           { return nil }
func (d *metadataCountingDB) Close() error                            { return nil }

func streamingTestTable(name string) *conn.Table {
	return &conn.Table{
		Name: name,
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "int", Position: 1},
			"name": {Name: "name", DataType: "varchar", Position: 2},
			"note": {Name: "note", DataType: "varchar", Position: 3},
		},
		PrimaryKey:  &conn.PrimaryKey{Columns: []string{"id"}},
		ForeignKeys: map[string]*conn.ForeignKey{},
	}
}

func TestTableModelCacheReducesRepeatedCLIExtractionQueries(t *testing.T) {
	const repeatedRules = 5
	table := streamingTestTable("items")
	oldSource := &metadataCountingDB{table: table}
	oldTarget := &metadataCountingDB{table: table}
	for i := 0; i < repeatedRules; i++ {
		_, _ = oldSource.ExtractTable("items")
		_, _ = oldTarget.ExtractTable("items")
		_, _ = oldTarget.ExtractTable("items") // comparison fetched target metadata again
	}
	if got := oldSource.extractCalls + oldTarget.extractCalls; got != 15 {
		t.Fatalf("baseline extraction queries = %d, want 15", got)
	}

	newSource := &metadataCountingDB{table: table}
	newTarget := &metadataCountingDB{table: table}
	sourceCache := newTableModelCache(newSource)
	targetCache := newTableModelCache(newTarget)
	for i := 0; i < repeatedRules; i++ {
		if _, err := sourceCache.get("items"); err != nil {
			t.Fatal(err)
		}
		if _, err := targetCache.get("items"); err != nil {
			t.Fatal(err)
		}
	}
	if got := newSource.extractCalls + newTarget.extractCalls; got != 2 {
		t.Fatalf("cached extraction queries = %d, want 2", got)
	}
}

func TestDMLBatchSizeHasHardUpperBound(t *testing.T) {
	if err := validateDMLBatchSize(0); err == nil {
		t.Fatal("expected zero DML batch size to fail")
	}
	if err := validateDMLBatchSize(maxDMLBatchSize + 1); err == nil {
		t.Fatal("expected oversized DML batch to fail")
	}
	if err := validateDMLBatchSize(maxDMLBatchSize); err != nil {
		t.Fatalf("hard-limit DML batch rejected: %v", err)
	}
}

func TestStreamingBuffersStayBatchBoundedAsDifferencesGrow(t *testing.T) {
	table := streamingTestTable("items")
	const batchSize = 1000
	stream := newTableDMLStream(io.Discard, io.Discard, pkgsql.NewDialect(consts.DBTypeMySQL), &tableModels{target: table, source: table}, batchSize)
	peakBufferedRows := 0
	for i := 0; i < 100000; i++ {
		row := conn.Record{"id": i + 1, "name": "value", "note": "stable"}
		if err := stream.handle(pkgdiff.DiffTypeAdd, nil, row, nil); err != nil {
			t.Fatal(err)
		}
		buffered := len(stream.forward.rows) + len(stream.rollback.rows)
		if buffered > peakBufferedRows {
			peakBufferedRows = buffered
		}
	}
	if err := stream.flush(); err != nil {
		t.Fatal(err)
	}
	if peakBufferedRows > 2*batchSize {
		t.Fatalf("peak buffered rows = %d, bound = %d", peakBufferedRows, 2*batchSize)
	}
	if len(stream.forward.rows) != 0 || len(stream.rollback.rows) != 0 {
		t.Fatal("final flush retained pending rows")
	}
}

func TestGenerateStreamingDataDiffOutputsBatchesAndReversesRollbackTables(t *testing.T) {
	rules := []config.Rule{{Table: "parent"}, {Table: "child"}}
	prepare := func(rule config.Rule) (*tableModels, error) {
		table := streamingTestTable(rule.Table)
		return &tableModels{target: table, source: table, effectiveCols: []string{"name", "note"}}, nil
	}
	compare := func(rule config.Rule, _ *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		if rule.Table == "parent" {
			for i := 1; i <= 3; i++ {
				if err := handle(pkgdiff.DiffTypeAdd, nil, conn.Record{"id": i, "name": "new", "note": "same"}, nil); err != nil {
					return err
				}
			}
			return nil
		}
		return handle(pkgdiff.DiffTypeModify,
			conn.Record{"id": 10, "name": "old", "note": "same"},
			conn.Record{"id": 10, "name": "new", "note": "same"},
			[]string{"name"},
		)
	}
	var forward, rollback bytes.Buffer
	spoolParent := t.TempDir()
	failures, err := generateStreamingDataDiffOutputs(&forward, &rollback, spoolParent, rules, pkgsql.NewDialect(consts.DBTypeMySQL), 2, false, prepare, compare)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 0 {
		t.Fatalf("unexpected failures: %#v", failures)
	}
	if !strings.HasPrefix(forward.String(), executeOnSourceHeader+"\n") {
		t.Fatalf("forward execution target header missing:\n%s", forward.String())
	}
	if !strings.HasPrefix(rollback.String(), executeOnSourceHeader+"\n") {
		t.Fatalf("rollback execution target header missing:\n%s", rollback.String())
	}
	if got := strings.Count(forward.String(), "INSERT INTO `parent`"); got != 2 {
		t.Fatalf("bounded batch statement count = %d, want 2\n%s", got, forward.String())
	}
	if strings.Contains(forward.String(), "`note` =") {
		t.Fatalf("UPDATE wrote unchanged column:\n%s", forward.String())
	}
	child := strings.Index(rollback.String(), "--- rollback child")
	parent := strings.Index(rollback.String(), "--- rollback parent")
	if child < 0 || parent < 0 || child > parent {
		t.Fatalf("rollback tables are not reverse-rule ordered:\n%s", rollback.String())
	}
	entries, err := os.ReadDir(spoolParent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool artifacts leaked: %#v", entries)
	}
}

func TestGenerateStreamingDataDiffOutputsBestEffortDiscardsFailedTablePartialSQL(t *testing.T) {
	rules := []config.Rule{{Table: "bad_a"}, {Table: "good"}, {Table: "bad_b"}}
	prepare := func(rule config.Rule) (*tableModels, error) {
		table := streamingTestTable(rule.Table)
		return &tableModels{target: table, source: table}, nil
	}
	compare := func(rule config.Rule, _ *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		if err := handle(pkgdiff.DiffTypeAdd, nil, conn.Record{"id": 1, "name": rule.Table}, nil); err != nil {
			return err
		}
		if strings.HasPrefix(rule.Table, "bad_") {
			return errors.New("injected scan failure")
		}
		return nil
	}
	var forward, rollback bytes.Buffer
	spoolParent := t.TempDir()
	failures, err := generateStreamingDataDiffOutputs(&forward, &rollback, spoolParent, rules, pkgsql.NewDialect(consts.DBTypeMySQL), 10, true, prepare, compare)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 2 || failures[0].table != "bad_a" || failures[1].table != "bad_b" {
		t.Fatalf("failures = %#v", failures)
	}
	if strings.Contains(forward.String(), "--- diff bad_") || strings.Contains(forward.String(), "'bad_") {
		t.Fatalf("failed table partial SQL was published:\n%s", forward.String())
	}
	if !strings.Contains(forward.String(), "--- diff good") || !strings.Contains(forward.String(), "DATASMITH RESULT: INCOMPLETE (--best-effort); 2 TABLE(S) FAILED") {
		t.Fatalf("successful table or incomplete report missing:\n%s", forward.String())
	}
	matches, err := filepath.Glob(filepath.Join(spoolParent, ".datasmith-diff-spool-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("spool directories leaked: %v", matches)
	}
}

func TestStreamingFailFastPreservesAtomicOutputPair(t *testing.T) {
	dir := t.TempDir()
	forwardPath := filepath.Join(dir, "forward.sql")
	rollbackPath := filepath.Join(dir, "rollback.sql")
	if err := os.WriteFile(forwardPath, []byte("old forward\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rollbackPath, []byte("old rollback\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	table := streamingTestTable("items")
	prepare := func(config.Rule) (*tableModels, error) {
		return &tableModels{target: table, source: table}, nil
	}
	compare := func(_ config.Rule, _ *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		if err := handle(pkgdiff.DiffTypeAdd, nil, conn.Record{"id": 1, "name": "partial"}, nil); err != nil {
			return err
		}
		return errors.New("injected scan failure")
	}
	err := writeAtomicPair(forwardPath, rollbackPath, func(forward, rollback io.Writer) error {
		_, generateErr := generateStreamingDataDiffOutputs(forward, rollback, dir, []config.Rule{{Table: "items"}}, pkgsql.NewDialect(consts.DBTypeMySQL), 1000, false, prepare, compare)
		return generateErr
	})
	if err == nil || !strings.Contains(err.Error(), "injected scan failure") {
		t.Fatalf("expected fail-fast comparison error, got %v", err)
	}
	for path, want := range map[string]string{forwardPath: "old forward\n", rollbackPath: "old rollback\n"} {
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(got) != want {
			t.Fatalf("atomic output %s changed: %q", path, got)
		}
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".*tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary outputs leaked: %v", matches)
	}
}
