package diff

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

func TestValidateDiffDataInputsRejectsSizesBeforePaths(t *testing.T) {
	err := validateDiffDataInputs("missing-config.yml", "missing-rules.json", 0, 100, false)
	if err == nil || !strings.Contains(err.Error(), "batch size") {
		t.Fatalf("expected batch-size preflight error, got %v", err)
	}
	err = validateDiffDataInputs("missing-config.yml", "missing-rules.json", 1, 0, true)
	if err == nil || !strings.Contains(err.Error(), "chunk size") {
		t.Fatalf("expected chunk-size preflight error, got %v", err)
	}
}

func TestValidateRulesRejectsEmptyKeys(t *testing.T) {
	rules := &config.RuleSet{Rules: []config.Rule{{Table: "items", ComparisonKey: []string{"id", ""}}}}
	if err := validateRules(rules); err == nil {
		t.Fatal("expected empty comparison key error")
	}
}

func TestGenerateDataDiffOutputsFailsFastByDefault(t *testing.T) {
	rules := []config.Rule{{Table: "first"}, {Table: "second"}}
	called := 0
	compare := func(rule config.Rule) (*tableDiffResult, error) {
		called++
		return nil, errors.New("injected table failure")
	}

	var forward, rollback bytes.Buffer
	failures, err := generateDataDiffOutputs(
		&forward,
		&rollback,
		rules,
		pkgsql.NewDialect(consts.DBTypeMySQL),
		false,
		compare,
	)
	if err == nil || !strings.Contains(err.Error(), "diff table first") {
		t.Fatalf("expected first table error, got %v", err)
	}
	if called != 1 {
		t.Fatalf("default mode compared %d tables, want fail-fast after 1", called)
	}
	if len(failures) != 1 || failures[0].table != "first" {
		t.Fatalf("failures = %#v, want first table only", failures)
	}
	if strings.Contains(forward.String(), "DATASMITH RESULT") || strings.Contains(rollback.String(), "DATASMITH RESULT") {
		t.Fatal("failed default generation must not emit a completion report")
	}
}

func TestGenerateDataDiffOutputsBestEffortListsEveryFailedTable(t *testing.T) {
	rules := []config.Rule{{Table: "bad_a"}, {Table: "good"}, {Table: "bad_b"}}
	called := 0
	compare := func(rule config.Rule) (*tableDiffResult, error) {
		called++
		if strings.HasPrefix(rule.Table, "bad_") {
			return nil, errors.New("injected table failure")
		}
		table := &conn.Table{Name: rule.Table, Columns: map[string]*conn.Column{}}
		return &tableDiffResult{
			target: table,
			source: table,
			diff:   &pkgdiff.DataDiff{},
		}, nil
	}

	var forward, rollback bytes.Buffer
	failures, err := generateDataDiffOutputs(
		&forward,
		&rollback,
		rules,
		pkgsql.NewDialect(consts.DBTypeMySQL),
		true,
		compare,
	)
	if err != nil {
		t.Fatalf("best-effort generation failed: %v", err)
	}
	if called != len(rules) {
		t.Fatalf("best-effort compared %d tables, want %d", called, len(rules))
	}
	if len(failures) != 2 {
		t.Fatalf("failures = %#v, want two failures", failures)
	}
	for name, output := range map[string]string{"forward": forward.String(), "rollback": rollback.String()} {
		if !strings.Contains(output, "DATASMITH RESULT: INCOMPLETE (--best-effort); 2 TABLE(S) FAILED") {
			t.Fatalf("%s output lacks unambiguous incomplete marker: %q", name, output)
		}
		for _, table := range []string{"bad_a", "bad_b"} {
			if !strings.Contains(output, "FAILED TABLE "+table+": injected table failure") {
				t.Fatalf("%s output does not list %s: %q", name, table, output)
			}
		}
	}
}
