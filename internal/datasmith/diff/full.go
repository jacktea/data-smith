package diff

import (
	"context"
	"errors"
	"fmt"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/db"
)

// FullDiffParams carries the inputs of a full diff run: a schema comparison
// plus a data comparison against the same connection pair. Rules 为空或含通配
// 条目时按整库语义展开；DataDiffMode 控制数据比对是否经影子事务两阶段
// （默认 auto：PostgreSQL source 且存在结构差异时启用）。
type FullDiffParams struct {
	Source        *pkgconfig.ConnConfig
	Target        *pkgconfig.ConnConfig
	IncludeTables []string
	ExcludeTables []string
	Rules         []pkgconfig.Rule
	DataDiffMode  string
	BatchSize     int
	ChunkSize     int
	DMLBatchSize  int
	ChunkHash     bool
	BestEffort    bool
	// SkipMissingTables 透传给数据比对阶段：rules 引用的表在任一侧不存在时
	// 跳过并告警，而不是失败（结构阶段的单侧表过滤仍先行生效）。
	SkipMissingTables bool
}

// FullDiffResult merges the schema and data projections of a full diff run.
// SkippedTables lists explicit source-only data rules skipped because schema
// DROP fully covers them. Target-only rules always participate in data diff.
type FullDiffResult struct {
	Schema        SchemaDiffSummary `json:"schema"`
	Data          DataDiffResult    `json:"data"`
	SkippedTables []string          `json:"skippedTables"`
}

// FullDiffFileNames lists the artifact names produced by RunFullDiff.
func FullDiffFileNames() []string {
	return append(SchemaDiffFileNames(), DataDiffFileNames()...)
}

// filterSourceOnlyRules drops source-only data rules. Target-only tables stay
// in the data phase: direct mode treats source as an empty set and emits every
// target row as INSERT; shadow mode has already created the empty source table.
func filterSourceOnlyRules(rules []pkgconfig.Rule, tablesDropped []string) ([]pkgconfig.Rule, []string) {
	sourceOnly := make(map[string]bool, len(tablesDropped))
	for _, name := range tablesDropped {
		sourceOnly[name] = true
	}
	kept := make([]pkgconfig.Rule, 0, len(rules))
	skipped := []string{}
	for _, rule := range rules {
		if sourceOnly[rule.Table] {
			skipped = append(skipped, rule.Table)
			continue
		}
		kept = append(kept, rule)
	}
	return kept, skipped
}

// RunFullDiff compares schema and data in one pass and writes all four
// artifacts (schema/data × forward/rollback) into dir.
//
// 数据比对默认两阶段（C5）：先结构比对，再把结构 forward 语句应用在 source
// 连接内的影子事务（PostgreSQL 事务性 DDL），数据比对经同会话读取对齐后的
// 影子结构，最后 ROLLBACK——不落库、单连接，生成的 up 一次执行即可对齐
// 结构与数据，无需外部预对齐编排。MySQL source 无事务性 DDL，自动回退
// 直接比对（公共列/主键列集的漂移容错）并明确告警。
func RunFullDiff(ctx context.Context, params FullDiffParams, dir string, progress func(string), tableProgress func(TableProgress)) (FullDiffResult, error) {
	if params.Source == nil || params.Target == nil {
		return FullDiffResult{}, errors.New("source and target connection configs are required")
	}
	if dir == "" {
		return FullDiffResult{}, errors.New("output directory is required")
	}
	if err := ValidateDataDiffMode(params.DataDiffMode); err != nil {
		return FullDiffResult{}, err
	}
	if err := validateWildcardRules(params.Rules); err != nil {
		return FullDiffResult{}, err
	}
	report := func(message string) {
		if progress != nil {
			progress(message)
		}
	}
	result := FullDiffResult{SkippedTables: []string{}}

	srcDB, err := db.NewDBAdapterContext(ctx, params.Source)
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("connect to source DB: %w", err)
	}
	defer srcDB.Close()
	tgtDB, err := db.NewDBAdapterContext(ctx, params.Target)
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("connect to target DB: %w", err)
	}
	defer tgtDB.Close()
	outputs, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		return FullDiffResult{}, err
	}
	defer func() { _ = outputs.abort() }()
	stagingDir := outputs.stagingDir

	report("=== 结构比对阶段 ===")
	phase, err := runSchemaDiffOnAdapters(ctx, srcDB, tgtDB, SchemaDiffParams{
		Source:        params.Source,
		Target:        params.Target,
		IncludeTables: params.IncludeTables,
		ExcludeTables: params.ExcludeTables,
	}, stagingDir, report)
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("schema diff: %w", err)
	}
	result.Schema = phase.summary

	shadow, err := decideShadowDataDiff(params.DataDiffMode, params.Source.Type, phase.forward)
	if err != nil {
		return FullDiffResult{}, err
	}
	if len(phase.forward) > 0 && !shadow {
		report("数据比对模式: direct — 数据差异跑在未对齐结构上(仅公共列/主键列集容错); 影子两阶段仅支持 PostgreSQL source")
	}

	// target-only 规则保留：影子模式在事务内创建空表，direct 模式把
	// source 视为空集。source-only 表在两种模式都由 schema DROP 覆盖，
	// 不能进入数据阶段（影子对齐后两侧都已不存在）。
	rules, skipped := filterSourceOnlyRules(params.Rules, phase.summary.TablesDropped)
	result.SkippedTables = skipped
	for _, name := range skipped {
		report(fmt.Sprintf("跳过表 %s: 仅 source 侧存在(由结构 DROP 覆盖)", name))
	}

	opts := dataPhaseOptions{schemaSnapshot: &phase.tables, shadow: shadow}
	if !shadow {
		opts.targetOnly = make(map[string]bool, len(phase.summary.TablesAdded))
		for _, name := range phase.summary.TablesAdded {
			opts.targetOnly[name] = true
		}
	}
	var (
		shadowTransaction *shadowTx
		unbindShadow      func()
		shadowPending     bool
	)
	if shadow {
		report(fmt.Sprintf("=== 影子结构对齐(事务内, 不落库): 应用 %d 条结构 DDL ===", len(phase.forward)))
		txs, err := beginShadowTx(ctx, srcDB)
		if err != nil {
			return FullDiffResult{}, err
		}
		if err := txs.apply(ctx, phase.forward); err != nil {
			// 影子对齐失败即失败：这是 forward 产物在真实结构上不可执行的
			// 信号，不允许静默降级掩盖。
			return FullDiffResult{}, errors.Join(fmt.Errorf("shadow schema alignment: %w", err), txs.rollback())
		}
		unbind, err := bindSession(srcDB, txs.tx)
		if err != nil {
			return FullDiffResult{}, errors.Join(err, txs.rollback())
		}
		shadowTransaction = txs
		unbindShadow = unbind
		shadowPending = true
		defer func() {
			if shadowPending {
				unbindShadow()
				_ = shadowTransaction.rollback()
			}
		}()
	}

	report("=== 数据比对阶段 ===")
	dataResult, err := runDataDiffOnAdapters(ctx, srcDB, tgtDB, DataDiffParams{
		Source:            params.Source,
		Target:            params.Target,
		Rules:             rules,
		ExcludeTables:     params.ExcludeTables,
		BatchSize:         params.BatchSize,
		ChunkSize:         params.ChunkSize,
		DMLBatchSize:      params.DMLBatchSize,
		ChunkHash:         params.ChunkHash,
		BestEffort:        params.BestEffort,
		SkipMissingTables: params.SkipMissingTables,
	}, opts, stagingDir, dataPhaseProgress(report, tableProgress))
	if shadowPending {
		shadowPending = false
		rollbackErr := finishShadowDataDiff(shadowTransaction, unbindShadow, report)
		if err != nil {
			return FullDiffResult{}, errors.Join(fmt.Errorf("data diff: %w", err), rollbackErr)
		}
		if rollbackErr != nil {
			return FullDiffResult{}, rollbackErr
		}
	}
	if err != nil {
		return FullDiffResult{}, fmt.Errorf("data diff: %w", err)
	}
	result.Data = dataResult
	if err := outputs.commit(); err != nil {
		return FullDiffResult{}, fmt.Errorf("publish full diff outputs: %w", err)
	}
	return result, nil
}

// dataPhaseProgress 把数据阶段的日志/跳过事件汇入文本进度，同时保留原有
// 的逐表事件回调（可为 nil）。
func dataPhaseProgress(report func(string), tableProgress func(TableProgress)) func(TableProgress) {
	return func(event TableProgress) {
		switch event.Phase {
		case "log", "skipped":
			report(event.Error)
		}
		if tableProgress != nil {
			tableProgress(event)
		}
	}
}
