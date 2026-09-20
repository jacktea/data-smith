package diff

import (
	"context"
	"errors"
	"fmt"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
)

// FullDiffParams carries the inputs of a full diff run: a schema comparison
// plus a data comparison against the same connection pair.
type FullDiffParams struct {
	Source        *pkgconfig.ConnConfig
	Target        *pkgconfig.ConnConfig
	IncludeTables []string
	ExcludeTables []string
	Rules         []pkgconfig.Rule
	BatchSize     int
	ChunkSize     int
	DMLBatchSize  int
	ChunkHash     bool
	BestEffort    bool
}

// FullDiffResult merges the schema and data projections of a full diff run.
// SkippedTables lists data rules skipped because the table exists on only one
// side; those changes are already covered by the schema phase.
type FullDiffResult struct {
	Schema        SchemaDiffSummary `json:"schema"`
	Data          DataDiffResult    `json:"data"`
	SkippedTables []string          `json:"skippedTables"`
}

// FullDiffFileNames lists the artifact names produced by RunFullDiff.
func FullDiffFileNames() []string {
	return append(SchemaDiffFileNames(), DataDiffFileNames()...)
}

// filterSingleSidedRules drops data rules whose table exists on only one side
// (per the schema diff: TablesAdded = tables only in target, TablesDropped =
// tables only in source). It returns the kept rules and the skipped table
// names in rule order.
func filterSingleSidedRules(rules []pkgconfig.Rule, tablesAdded, tablesDropped []string) ([]pkgconfig.Rule, []string) {
	singleSided := make(map[string]bool, len(tablesAdded)+len(tablesDropped))
	for _, name := range tablesAdded {
		singleSided[name] = true
	}
	for _, name := range tablesDropped {
		singleSided[name] = true
	}
	kept := make([]pkgconfig.Rule, 0, len(rules))
	skipped := []string{}
	for _, rule := range rules {
		if singleSided[rule.Table] {
			skipped = append(skipped, rule.Table)
			continue
		}
		kept = append(kept, rule)
	}
	return kept, skipped
}

// RunFullDiff compares schema and data in one pass and writes all four
// artifacts (schema/data × forward/rollback) into dir. Data rules referencing
// tables that exist on only one side are skipped and reported instead of
// failing the run — creating or dropping those tables is the schema phase's
// job.
func RunFullDiff(ctx context.Context, params FullDiffParams, dir string, progress func(string), tableProgress func(TableProgress)) (FullDiffResult, error) {
	if len(params.Rules) == 0 {
		return FullDiffResult{}, errors.New("at least one comparison rule is required")
	}
	report := func(message string) {
		if progress != nil {
			progress(message)
		}
	}
	result := FullDiffResult{SkippedTables: []string{}}

	report("=== 结构比对阶段 ===")
	schemaSummary, err := RunSchemaDiff(ctx, SchemaDiffParams{
		Source:        params.Source,
		Target:        params.Target,
		IncludeTables: params.IncludeTables,
		ExcludeTables: params.ExcludeTables,
	}, dir, report)
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("schema diff: %w", err)
	}
	result.Schema = schemaSummary

	// 数据比对只覆盖两侧都存在的表;单侧表属于结构差异,已由结构比对覆盖。
	rules, skipped := filterSingleSidedRules(params.Rules, schemaSummary.TablesAdded, schemaSummary.TablesDropped)
	result.SkippedTables = skipped
	for _, name := range skipped {
		report(fmt.Sprintf("跳过表 %s: 仅单侧存在(结构差异已由结构比对覆盖)", name))
	}
	if len(rules) == 0 {
		result.Data = DataDiffResult{Complete: true, Tables: []TableDiffSummary{}}
		report("所有数据表均为单侧表, 数据比对无可比对内容")
		return result, nil
	}

	report("=== 数据比对阶段 ===")
	dataResult, err := RunDataDiff(ctx, DataDiffParams{
		Source:       params.Source,
		Target:       params.Target,
		Rules:        rules,
		BatchSize:    params.BatchSize,
		ChunkSize:    params.ChunkSize,
		DMLBatchSize: params.DMLBatchSize,
		ChunkHash:    params.ChunkHash,
		BestEffort:   params.BestEffort,
	}, dir, tableProgress)
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("data diff: %w", err)
	}
	result.Data = dataResult
	return result, nil
}
