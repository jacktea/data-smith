package diff

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
	pkgsql "github.com/jacktea/data-smith/pkg/sql"
)

// ColumnModSummary names a modified column and carries the human-readable
// change reasons (type/nullability/comment).
type ColumnModSummary struct {
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
}

// TableModSummary describes the changes of a single modified table.
type TableModSummary struct {
	Table              string             `json:"table"`
	ColumnsAdded       []string           `json:"columnsAdded"`
	ColumnsDropped     []string           `json:"columnsDropped"`
	ColumnsModified    []ColumnModSummary `json:"columnsModified"`
	IndexesAdded       []string           `json:"indexesAdded"`
	IndexesDropped     []string           `json:"indexesDropped"`
	ForeignKeysAdded   []string           `json:"foreignKeysAdded"`
	ForeignKeysDropped []string           `json:"foreignKeysDropped"`
	ChecksAdded        []string           `json:"checksAdded"`
	ChecksDropped      []string           `json:"checksDropped"`
	PrimaryKeyChanged  bool               `json:"primaryKeyChanged"`
	CommentChanged     bool               `json:"commentChanged"`
}

// SchemaDiffSummary is the user-facing projection of a schema comparison.
type SchemaDiffSummary struct {
	TablesAdded    []string          `json:"tablesAdded"`
	TablesDropped  []string          `json:"tablesDropped"`
	TablesModified []TableModSummary `json:"tablesModified"`
	Destructive    bool              `json:"destructive"`
}

// SchemaDiffParams carries the inputs of a schema diff run without a config file.
type SchemaDiffParams struct {
	Source        *pkgconfig.ConnConfig
	Target        *pkgconfig.ConnConfig
	IncludeTables []string
	ExcludeTables []string
}

// TableProgress reports the lifecycle of one table inside a data diff run.
type TableProgress struct {
	Table string `json:"table"`
	Phase string `json:"phase"` // "start" | "done"
	Error string `json:"error,omitempty"`
}

// TableDiffSummary counts the row-level differences of one table.
type TableDiffSummary struct {
	Table    string `json:"table"`
	Added    int    `json:"added"`
	Modified int    `json:"modified"`
	Dropped  int    `json:"dropped"`
	Status   string `json:"status"` // "ok" | "failed"
	Error    string `json:"error,omitempty"`
}

// DataDiffParams carries the inputs of a data diff run without a rules file.
// Rules 为空或含通配条目时按整库语义展开（见 rules.go）；排除清单（含默认
// 账本表）对显式与展开规则一律生效。
type DataDiffParams struct {
	Source        *pkgconfig.ConnConfig
	Target        *pkgconfig.ConnConfig
	Rules         []pkgconfig.Rule
	ExcludeTables []string
	BatchSize     int
	ChunkSize     int
	DMLBatchSize  int
	ChunkHash     bool
	BestEffort    bool
	// SkipMissingTables：rules 引用的表在任一侧不存在时跳过并输出 warning
	// 清单，而不是报错中断。关闭（默认）时显式报 "table not found: X"。
	SkipMissingTables bool
	// ForwardPath and RollbackPath optionally override the default output file
	// names inside dir.
	ForwardPath  string
	RollbackPath string
}

// DataDiffResult is the user-facing projection of a data diff run.
type DataDiffResult struct {
	Complete bool               `json:"complete"`
	Tables   []TableDiffSummary `json:"tables"`
	// SkippedTables 列出因 --skip-missing-tables 被跳过的不存在表。
	SkippedTables []string `json:"skippedTables,omitempty"`
	// ExcludedTables 列出因排除配置（含默认账本表 schema_migrations 等）
	// 被过滤的数据规则表。
	ExcludedTables []string `json:"excludedTables,omitempty"`
}

const SchemaDiffForwardFile = "schema_diff.sql"
const SchemaDiffRollbackFile = "schema_diff_rollback.sql"
const DataDiffForwardFile = "data_diff.sql"
const DataDiffRollbackFile = "data_diff_rollback.sql"

// SchemaDiffFileNames lists the artifact names produced by RunSchemaDiff.
func SchemaDiffFileNames() []string {
	return []string{SchemaDiffForwardFile, SchemaDiffRollbackFile}
}

// DataDiffFileNames lists the artifact names produced by RunDataDiff.
func DataDiffFileNames() []string {
	return []string{DataDiffForwardFile, DataDiffRollbackFile}
}

// schemaPair 缓存结构阶段读取（并经 include/exclude 过滤）后的两侧基础表，
// 供数据阶段复用：通配展开的候选清单与 target 模型预热。
type schemaPair struct {
	src map[string]*conn.Table
	tgt map[string]*conn.Table
}

// baseTables 只保留基础表（视图/物化视图不参与数据比对）。
func baseTables(tables map[string]*conn.Table) map[string]*conn.Table {
	result := make(map[string]*conn.Table, len(tables))
	for name, tbl := range tables {
		if tbl != nil && tbl.Type == conn.TableTypeTable {
			result[name] = tbl
		}
	}
	return result
}

// schemaPhase 汇总结构阶段产物：摘要、forward 语句序列（影子事务的应用序
// 列）与两侧过滤后的表集合。
type schemaPhase struct {
	summary SchemaDiffSummary
	forward []string
	tables  schemaPair
}

// RunSchemaDiff reads both schemas, compares them, and atomically writes the
// forward and rollback SQL pair into dir. The forward SQL upgrades the source
// database towards the target database and must be executed on the source.
func RunSchemaDiff(ctx context.Context, params SchemaDiffParams, dir string, progress func(string)) (SchemaDiffSummary, error) {
	if params.Source == nil || params.Target == nil {
		return SchemaDiffSummary{}, errors.New("source and target connection configs are required")
	}
	if dir == "" {
		return SchemaDiffSummary{}, errors.New("output directory is required")
	}
	srcDB, err := db.NewDBAdapterContext(ctx, params.Source)
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("connect to source DB: %w", err)
	}
	defer srcDB.Close()
	tgtDB, err := db.NewDBAdapterContext(ctx, params.Target)
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("connect to target DB: %w", err)
	}
	defer tgtDB.Close()
	phase, err := runSchemaDiffOnAdapters(ctx, srcDB, tgtDB, params, dir, progress)
	return phase.summary, err
}

// runSchemaDiffOnAdapters 在已打开的连接对上执行结构比对并写出产物，返回
// 供 diff-full 两阶段复用的完整阶段产物（forward 语句序列与表集合）。
func runSchemaDiffOnAdapters(ctx context.Context, srcDB, tgtDB conn.DBAdapter, params SchemaDiffParams, dir string, progress func(string)) (schemaPhase, error) {
	report := func(message string) {
		if progress != nil {
			progress(message)
		}
	}

	report("读取两侧数据库结构")
	start := time.Now()
	srcSchema, err := srcDB.ReadSchema()
	if err != nil {
		return schemaPhase{}, fmt.Errorf("read source schema: %w", err)
	}
	tgtSchema, err := tgtDB.ReadSchema()
	if err != nil {
		return schemaPhase{}, fmt.Errorf("read target schema: %w", err)
	}

	effectiveExcludes := pkgconfig.EffectiveExcludeTables(params.ExcludeTables)
	if len(params.IncludeTables) > 0 || len(effectiveExcludes) > 0 {
		srcSchema.Tables = filterTables(srcSchema.Tables, params.IncludeTables, effectiveExcludes)
		tgtSchema.Tables = filterTables(tgtSchema.Tables, params.IncludeTables, effectiveExcludes)
		// 被排除表的 SERIAL 隐式序列随表一起排除（如迁移账本
		// schema_migrations 的 id_seq），否则序列单独成为差异对象，产物会
		// 对账本的从属结构产生反向污染。
		srcSchema.Sequences = filterSequencesByOwnedTable(srcSchema.Sequences, effectiveExcludes)
		tgtSchema.Sequences = filterSequencesByOwnedTable(tgtSchema.Sequences, effectiveExcludes)
	}
	phase := schemaPhase{tables: schemaPair{src: baseTables(srcSchema.Tables), tgt: baseTables(tgtSchema.Tables)}}

	report("比对结构差异")
	forwardDiff := pkgdiff.CompareSchemas(srcSchema, tgtSchema)
	rollbackDiff := pkgdiff.CompareSchemas(tgtSchema, srcSchema)
	phase.summary = projectSchemaDiff(forwardDiff)

	report("生成正向与回滚 SQL")
	forwardSQLs, err := pkgsql.GenerateSchemaSQLSafe(forwardDiff, params.Source.Type)
	if err != nil {
		return schemaPhase{}, fmt.Errorf("generate forward schema SQL: %w", err)
	}
	rollbackSQLs, err := pkgsql.GenerateSchemaSQLSafe(rollbackDiff, params.Source.Type)
	if err != nil {
		return schemaPhase{}, fmt.Errorf("generate rollback schema SQL: %w", err)
	}
	phase.forward = forwardSQLs

	forwardPath := filepath.Join(dir, SchemaDiffForwardFile)
	rollbackPath := filepath.Join(dir, SchemaDiffRollbackFile)
	err = writeAtomicPair(forwardPath, rollbackPath, func(forward, rollback io.Writer) error {
		if _, err := fmt.Fprintln(forward, executeOnSourceHeader); err != nil {
			return fmt.Errorf("write forward execution target: %w", err)
		}
		if _, err := fmt.Fprintln(rollback, executeOnSourceHeader); err != nil {
			return fmt.Errorf("write rollback execution target: %w", err)
		}
		if err := writeSQLStatements(forward, forwardSQLs); err != nil {
			return fmt.Errorf("write forward schema SQL: %w", err)
		}
		if err := writeSQLStatements(rollback, rollbackSQLs); err != nil {
			return fmt.Errorf("write rollback schema SQL: %w", err)
		}
		return nil
	})
	if err != nil {
		return schemaPhase{}, err
	}
	report(fmt.Sprintf("结构比对完成, 耗时 %v", time.Since(start)))
	return phase, nil
}

func projectSchemaDiff(d *pkgdiff.SchemaDiff) SchemaDiffSummary {
	// 切片字段一律保持非 nil:JSON 序列化为 [] 而非 null,
	// 否则前端对 null 取 length/join 会导致详情渲染崩溃。
	summary := SchemaDiffSummary{
		TablesAdded:    tableNames(d.TablesAdded),
		TablesDropped:  tableNames(d.TablesDropped),
		TablesModified: []TableModSummary{},
	}
	dropColCount := 0
	for _, t := range d.TablesModified {
		mod := TableModSummary{
			Table:              t.Table.Name,
			ColumnsAdded:       []string{},
			ColumnsDropped:     []string{},
			ColumnsModified:    []ColumnModSummary{},
			IndexesAdded:       []string{},
			IndexesDropped:     []string{},
			ForeignKeysAdded:   []string{},
			ForeignKeysDropped: []string{},
			ChecksAdded:        []string{},
			ChecksDropped:      []string{},
			PrimaryKeyChanged:  t.PrimaryKeyChange != nil,
			CommentChanged:     t.CommentChange != nil,
		}
		for _, col := range t.ColumnsAdded {
			mod.ColumnsAdded = append(mod.ColumnsAdded, col.Name)
		}
		for _, col := range t.ColumnsDropped {
			mod.ColumnsDropped = append(mod.ColumnsDropped, col.Name)
			dropColCount++
		}
		for _, cmod := range t.ColumnsModified {
			mod.ColumnsModified = append(mod.ColumnsModified, ColumnModSummary{
				Name:   cmod.New.Name,
				Detail: columnModReasons(cmod.Old, cmod.New),
			})
		}
		for _, idx := range t.IndexesAdded {
			mod.IndexesAdded = append(mod.IndexesAdded, idx.Name)
		}
		for _, idx := range t.IndexesDropped {
			mod.IndexesDropped = append(mod.IndexesDropped, idx.Name)
		}
		for _, fk := range t.ForeignKeysAdded {
			mod.ForeignKeysAdded = append(mod.ForeignKeysAdded, fk.Name)
		}
		for _, fk := range t.ForeignKeysDropped {
			mod.ForeignKeysDropped = append(mod.ForeignKeysDropped, fk.Name)
		}
		for _, chk := range t.ChecksAdded {
			mod.ChecksAdded = append(mod.ChecksAdded, chk.Name)
		}
		for _, chk := range t.ChecksDropped {
			mod.ChecksDropped = append(mod.ChecksDropped, chk.Name)
		}
		sort.Strings(mod.ColumnsAdded)
		sort.Strings(mod.ColumnsDropped)
		sort.Slice(mod.ColumnsModified, func(i, j int) bool {
			return mod.ColumnsModified[i].Name < mod.ColumnsModified[j].Name
		})
		sort.Strings(mod.IndexesAdded)
		sort.Strings(mod.IndexesDropped)
		sort.Strings(mod.ForeignKeysAdded)
		sort.Strings(mod.ForeignKeysDropped)
		sort.Strings(mod.ChecksAdded)
		sort.Strings(mod.ChecksDropped)
		summary.TablesModified = append(summary.TablesModified, mod)
	}
	sort.Slice(summary.TablesModified, func(i, j int) bool {
		return summary.TablesModified[i].Table < summary.TablesModified[j].Table
	})
	summary.Destructive = len(summary.TablesDropped) > 0 || dropColCount > 0
	return summary
}

func tableNames(tables []*conn.Table) []string {
	names := make([]string, 0, len(tables))
	for _, tbl := range tables {
		names = append(names, tbl.Name)
	}
	sort.Strings(names)
	return names
}

// slimTable copies tbl with only the kept columns so downstream SQL generation
// never references columns outside the fetched compare set.
func slimTable(tbl *conn.Table, keep map[string]bool) *conn.Table {
	if tbl == nil {
		return nil
	}
	slim := *tbl
	slim.Columns = make(map[string]*conn.Column, len(keep))
	for name, col := range tbl.Columns {
		if keep[name] {
			slim.Columns[name] = col
		}
	}
	return &slim
}

// dataDiffKeepColumns 计算数据比对的行读取与 SQL 生成列集:
// 比对列 ∪ 业务键 ∪ 主键 ∪ 双侧均存在的忽略列。忽略字段不参与比对,但
// 生成的 INSERT 必须携带完整行数据(NOT NULL 忽略字段缺列会执行失败);
// 业务键（rules.comparisonKey）是无键表的行身份/行定位键,必须随行读取;
// 仅单侧存在的忽略列不进入 SQL,维持对两侧结构差异的容错。
func dataDiffKeepColumns(tgtTable, srcTable *conn.Table, effectiveCols, businessKey, ignoreColumns []string) map[string]bool {
	keep := make(map[string]bool, len(effectiveCols)+len(businessKey)+len(ignoreColumns)+2)
	for _, name := range effectiveCols {
		keep[name] = true
	}
	for _, name := range businessKey {
		keep[name] = true
	}
	if tgtTable == nil {
		return keep
	}
	for _, pk := range tgtTable.GetPrimaryKeyColumns() {
		keep[pk] = true
	}
	for _, name := range ignoreColumns {
		if _, inTarget := tgtTable.Columns[name]; !inTarget {
			continue
		}
		if srcTable == nil {
			continue
		}
		if _, inSource := srcTable.Columns[name]; inSource {
			keep[name] = true
		}
	}
	return keep
}

func intersectNames(keep map[string]bool, tbl *conn.Table) map[string]bool {
	if tbl == nil {
		return map[string]bool{}
	}
	result := make(map[string]bool, len(keep))
	for name := range keep {
		if _, ok := tbl.Columns[name]; ok {
			result[name] = true
		}
	}
	return result
}

func columnModReasons(oldCol, newCol *conn.Column) string {
	var reasons []string
	if oldCol.DataType != newCol.DataType {
		reasons = append(reasons, fmt.Sprintf("Type: %s -> %s", oldCol.DataType, newCol.DataType))
	}
	if oldCol.Nullable != newCol.Nullable {
		reasons = append(reasons, fmt.Sprintf("Nullable: %v -> %v", oldCol.Nullable, newCol.Nullable))
	}
	if oldCol.Comment != newCol.Comment {
		reasons = append(reasons, "Comment changed")
	}
	if len(reasons) == 0 {
		reasons = append(reasons, fmt.Sprintf("%s -> %s", oldCol.DataType, newCol.DataType))
	}
	return strings.Join(reasons, ", ")
}

// buildPrepareTableFunc 构造流式数据比对的表模型准备闭包：取目标侧与源侧
// 表模型并按 keep 列集裁剪。开启 --skip-missing-tables 时，任一侧缺表
// （ErrTableNotFound）登记跳过并返回 errTableSkipped；关闭时显式报错。
func buildPrepareTableFunc(params DataDiffParams, sourceModels, targetModels *tableModelCache, targetOnly map[string]bool, markSkipped func(pkgconfig.Rule, string)) prepareTableModelsFunc {
	return func(rule pkgconfig.Rule) (*tableModels, error) {
		tgtTable, err := targetModels.get(rule.Table)
		if err != nil {
			if params.SkipMissingTables && errors.Is(err, conn.ErrTableNotFound) {
				markSkipped(rule, "目标")
				return nil, errTableSkipped
			}
			return nil, fmt.Errorf("extract target table: %w", err)
		}
		if tgtTable == nil {
			return nil, fmt.Errorf("extract target table: table not found: %s", rule.Table)
		}
		var srcTable *conn.Table
		if !targetOnly[rule.Table] {
			srcTable, err = sourceModels.get(rule.Table)
			if err != nil {
				if params.SkipMissingTables && errors.Is(err, conn.ErrTableNotFound) {
					markSkipped(rule, "源")
					return nil, errTableSkipped
				}
				return nil, fmt.Errorf("extract source table: %w", err)
			}
		}
		compareRule := pkgdiff.CreateCompareRuleColumns(tgtTable, rule.Columns, rule.ComparisonKey, rule.IgnoreColumns)
		var effectiveCols []string
		if allRule, ok := compareRule.(*pkgdiff.AllFieldsEqualRule); ok {
			effectiveCols = allRule.Columns
		} else {
			effectiveCols = rule.ComparisonKey
		}
		keep := dataDiffKeepColumns(tgtTable, srcTable, effectiveCols, rule.ComparisonKey, rule.IgnoreColumns)
		if targetOnly[rule.Table] {
			// source 为空集时每行都是 INSERT；读取完整 target 行，避免遗漏
			// ignored/非比较列导致 NOT NULL 或默认值语义丢失。
			for name := range tgtTable.Columns {
				keep[name] = true
			}
		}
		slimTarget := slimTable(tgtTable, keep)
		// C4a：无键表按显式业务键比对时，把业务键注入裁剪后的 target 模型
		// 作为行定位键，UPDATE/DELETE 以业务键定位行；否则生成层回退全列
		// 定位，UPDATE 会退化为空串（变更行被静默丢弃）。
		applyBusinessKeyIdentity(slimTarget, rule)
		slimSource := slimTable(srcTable, intersectNames(keep, srcTable))
		return &tableModels{target: slimTarget, source: slimSource, effectiveCols: effectiveCols}, nil
	}
}

// applyBusinessKeyIdentity 在表无任何物理行身份且规则显式配置业务键
// （rules.comparisonKey）时，把业务键设为裁剪副本的行定位主键。业务键列
// 缺失或可空时不注入——由比对层的行身份校验显式报错。只操作 slimTable
// 产生的副本，不影响结构阶段共享的表模型。
func applyBusinessKeyIdentity(tbl *conn.Table, rule pkgconfig.Rule) {
	if tbl == nil || tbl.PrimaryKey != nil || len(rule.ComparisonKey) == 0 {
		return
	}
	for _, key := range rule.ComparisonKey {
		col := tbl.Columns[key]
		if col == nil || col.Nullable {
			return
		}
	}
	tbl.PrimaryKey = &conn.PrimaryKey{
		Name:    "datasmith_business_key",
		Columns: append([]string(nil), rule.ComparisonKey...),
	}
}

// RunDataDiff streams row-level comparisons rule by rule and atomically writes
// the forward and rollback SQL pair into dir. The forward SQL aligns the source
// database to the target database and must be executed on the source.
func RunDataDiff(ctx context.Context, params DataDiffParams, dir string, progress func(TableProgress)) (DataDiffResult, error) {
	if params.Source == nil || params.Target == nil {
		return DataDiffResult{}, errors.New("source and target connection configs are required")
	}
	if dir == "" {
		return DataDiffResult{}, errors.New("output directory is required")
	}
	srcDB, err := db.NewDBAdapterContext(ctx, params.Source)
	if err != nil {
		return DataDiffResult{}, fmt.Errorf("connect to source DB: %w", err)
	}
	defer srcDB.Close()
	tgtDB, err := db.NewDBAdapterContext(ctx, params.Target)
	if err != nil {
		return DataDiffResult{}, fmt.Errorf("connect to target DB: %w", err)
	}
	defer tgtDB.Close()
	return runDataDiffOnAdapters(ctx, srcDB, tgtDB, params, dataPhaseOptions{}, dir, progress)
}

// dataPhaseOptions 携带 diff-full 编排注入的上下文：结构阶段的表集合快照
// （通配展开与 target 模型预热复用）与影子模式标记（source 结构已在事务内
// 对齐，展开候选取 target 全集）。
type dataPhaseOptions struct {
	schemaSnapshot *schemaPair
	shadow         bool
	targetOnly     map[string]bool
}

// emptyTableDataAdapter exposes an empty row set while retaining the source
// adapter's connection/configuration behavior. It is used only for target-only
// tables discovered by diff-full's schema phase.
type emptyTableDataAdapter struct {
	conn.DBAdapter
}

func (emptyTableDataAdapter) GetTableDataBatch(string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}

func (emptyTableDataAdapter) GetTableDataBatchContext(context.Context, string, []string, []string, []any, int) ([]conn.Record, error) {
	return nil, nil
}

// runDataDiffOnAdapters 在已打开的连接对上执行数据比对。规则先展开（省略/
// 通配 → 整库有行身份的表）再排除（默认账本表 + 配置排除），随后逐表流式
// 比对并原子写出 forward/rollback 对。
func runDataDiffOnAdapters(ctx context.Context, srcDB, tgtDB conn.DBAdapter, params DataDiffParams, opts dataPhaseOptions, dir string, progress func(TableProgress)) (DataDiffResult, error) {
	if params.BatchSize <= 0 {
		return DataDiffResult{}, errors.New("batch size must be greater than zero")
	}
	if params.ChunkHash && params.ChunkSize <= 0 {
		return DataDiffResult{}, errors.New("chunk size must be greater than zero when chunk hashing is enabled")
	}
	if err := validateDMLBatchSize(params.DMLBatchSize); err != nil {
		return DataDiffResult{}, err
	}
	if err := validateWildcardRules(params.Rules); err != nil {
		return DataDiffResult{}, err
	}
	report := func(event TableProgress) {
		if progress != nil {
			progress(event)
		}
	}

	rules, excludedTables, err := resolveDataRules(srcDB, tgtDB, params, opts, func(message string) {
		report(TableProgress{Phase: "log", Error: message})
	})
	if err != nil {
		return DataDiffResult{}, err
	}
	if err := validateRules(&pkgconfig.RuleSet{Rules: rules}); err != nil {
		return DataDiffResult{}, err
	}

	dbDialect := pkgsql.NewDialect(params.Target.Type)
	sourceModels := newTableModelCache(srcDB)
	targetModels := newTableModelCache(tgtDB)
	// target 模型复用结构阶段读取结果；source 模型始终现取——影子模式下
	// source 结构已被事务内 DDL 改变，预读模型不再成立。
	if opts.schemaSnapshot != nil {
		for name, tbl := range opts.schemaSnapshot.tgt {
			targetModels.models[name] = tbl
		}
	}

	// skippedByFlag 记录因 --skip-missing-tables 被跳过的表；onTableDone
	// 回调据此把对应条目标记为 skipped 而非 failed。
	skippedByFlag := make(map[string]bool)
	markSkipped := func(rule pkgconfig.Rule, side string) {
		if !skippedByFlag[rule.Table] {
			skippedByFlag[rule.Table] = true
			report(TableProgress{Table: rule.Table, Phase: "skipped", Error: fmt.Sprintf("表 %s 在%s侧不存在, 已按 --skip-missing-tables 跳过", rule.Table, side)})
		}
	}
	prepareTable := buildPrepareTableFunc(params, sourceModels, targetModels, opts.targetOnly, markSkipped)
	compareTable := func(rule pkgconfig.Rule, models *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		start := time.Now()
		compareRule := pkgdiff.CreateCompareRuleColumns(models.target, rule.Columns, rule.ComparisonKey, rule.IgnoreColumns)
		compareSource := srcDB
		if opts.targetOnly[rule.Table] {
			compareSource = emptyTableDataAdapter{DBAdapter: srcDB}
		}
		var compareErr error
		if params.ChunkHash {
			compareErr = pkgdiff.StreamCompareDataWithChunkFilterAndTableContext(
				ctx,
				compareSource,
				tgtDB,
				compareRule,
				models.target,
				params.BatchSize,
				params.ChunkSize,
				handle,
			)
		} else {
			compareErr = pkgdiff.StreamCompareDataDetailedWithTableContext(
				ctx,
				compareSource,
				tgtDB,
				compareRule,
				models.target,
				params.BatchSize,
				handle,
			)
		}
		if compareErr != nil {
			return fmt.Errorf("compare data: %w", compareErr)
		}
		report(TableProgress{Table: rule.Table, Phase: "log", Error: fmt.Sprintf("表 %s 比对完成, 耗时 %v", rule.Table, time.Since(start))})
		return nil
	}

	result := DataDiffResult{Complete: true, Tables: make([]TableDiffSummary, 0, len(rules)), ExcludedTables: excludedTables}
	tablesByRule := make(map[string]int, len(rules))
	for i, rule := range rules {
		tablesByRule[rule.Table] = i
		result.Tables = append(result.Tables, TableDiffSummary{Table: rule.Table, Status: "ok"})
	}

	forwardPath := params.ForwardPath
	if forwardPath == "" {
		forwardPath = filepath.Join(dir, DataDiffForwardFile)
	}
	rollbackPath := params.RollbackPath
	if rollbackPath == "" {
		rollbackPath = filepath.Join(dir, DataDiffRollbackFile)
	}

	var failures []tableDiffFailure
	err = writeAtomicPair(
		forwardPath,
		rollbackPath,
		func(forward, rollback io.Writer) error {
			var generateErr error
			failures, generateErr = generateStreamingDataDiffOutputs(
				forward,
				rollback,
				filepath.Dir(rollbackPath),
				rules,
				dbDialect,
				params.DMLBatchSize,
				params.BestEffort,
				prepareTable,
				compareTable,
				func(rule pkgconfig.Rule, stats tableDiffStats, tableErr error) {
					if tableErr == nil {
						report(TableProgress{Table: rule.Table, Phase: "done"})
					} else {
						report(TableProgress{Table: rule.Table, Phase: "done", Error: tableErr.Error()})
					}
					idx, ok := tablesByRule[rule.Table]
					if !ok {
						return
					}
					entry := &result.Tables[idx]
					entry.Added = stats.added
					entry.Modified = stats.modified
					entry.Dropped = stats.dropped
					if tableErr != nil {
						entry.Status = "failed"
						entry.Error = tableErr.Error()
					} else if skippedByFlag[rule.Table] {
						entry.Status = "skipped"
						entry.Error = "table not found (skipped by --skip-missing-tables)"
					}
				},
			)
			return generateErr
		},
	)
	if err != nil {
		return DataDiffResult{}, err
	}
	if len(failures) > 0 {
		result.Complete = false
	}
	for name := range skippedByFlag {
		result.SkippedTables = append(result.SkippedTables, name)
	}
	sort.Strings(result.SkippedTables)
	return result, nil
}
