package diff

import (
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
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
