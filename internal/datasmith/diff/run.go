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
type DataDiffParams struct {
	Source       *pkgconfig.ConnConfig
	Target       *pkgconfig.ConnConfig
	Rules        []pkgconfig.Rule
	BatchSize    int
	ChunkSize    int
	DMLBatchSize int
	ChunkHash    bool
	BestEffort   bool
	// ForwardPath and RollbackPath optionally override the default output file
	// names inside dir.
	ForwardPath  string
	RollbackPath string
}

// DataDiffResult is the user-facing projection of a data diff run.
type DataDiffResult struct {
	Complete bool               `json:"complete"`
	Tables   []TableDiffSummary `json:"tables"`
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
	report := func(message string) {
		if progress != nil {
			progress(message)
		}
	}

	report("连接源库与目标库")
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

	report("读取两侧数据库结构")
	start := time.Now()
	srcSchema, err := srcDB.ReadSchema()
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("read source schema: %w", err)
	}
	tgtSchema, err := tgtDB.ReadSchema()
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("read target schema: %w", err)
	}

	if len(params.IncludeTables) > 0 || len(params.ExcludeTables) > 0 {
		srcSchema.Tables = filterTables(srcSchema.Tables, params.IncludeTables, params.ExcludeTables)
		tgtSchema.Tables = filterTables(tgtSchema.Tables, params.IncludeTables, params.ExcludeTables)
	}

	report("比对结构差异")
	forwardDiff := pkgdiff.CompareSchemas(srcSchema, tgtSchema)
	rollbackDiff := pkgdiff.CompareSchemas(tgtSchema, srcSchema)
	summary := projectSchemaDiff(forwardDiff)

	report("生成正向与回滚 SQL")
	forwardSQLs, err := pkgsql.GenerateSchemaSQLSafe(forwardDiff, params.Source.Type)
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("generate forward schema SQL: %w", err)
	}
	rollbackSQLs, err := pkgsql.GenerateSchemaSQLSafe(rollbackDiff, params.Source.Type)
	if err != nil {
		return SchemaDiffSummary{}, fmt.Errorf("generate rollback schema SQL: %w", err)
	}

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
		return SchemaDiffSummary{}, err
	}
	report(fmt.Sprintf("结构比对完成, 耗时 %v", time.Since(start)))
	return summary, nil
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
		sort.Strings(mod.ColumnsAdded)
		sort.Strings(mod.ColumnsDropped)
		sort.Slice(mod.ColumnsModified, func(i, j int) bool {
			return mod.ColumnsModified[i].Name < mod.ColumnsModified[j].Name
		})
		sort.Strings(mod.IndexesAdded)
		sort.Strings(mod.IndexesDropped)
		sort.Strings(mod.ForeignKeysAdded)
		sort.Strings(mod.ForeignKeysDropped)
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
// 比对列 ∪ 主键 ∪ 双侧均存在的忽略列。忽略字段不参与比对,但生成的
// INSERT 必须携带完整行数据(NOT NULL 忽略字段缺列会执行失败);
// 仅单侧存在的忽略列不进入 SQL,维持对两侧结构差异的容错。
func dataDiffKeepColumns(tgtTable, srcTable *conn.Table, effectiveCols, ignoreColumns []string) map[string]bool {
	keep := make(map[string]bool, len(effectiveCols)+len(ignoreColumns)+2)
	for _, name := range effectiveCols {
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
	if len(params.Rules) == 0 {
		return DataDiffResult{}, errors.New("at least one comparison rule is required")
	}
	if params.BatchSize <= 0 {
		return DataDiffResult{}, errors.New("batch size must be greater than zero")
	}
	if params.ChunkHash && params.ChunkSize <= 0 {
		return DataDiffResult{}, errors.New("chunk size must be greater than zero when chunk hashing is enabled")
	}
	if err := validateDMLBatchSize(params.DMLBatchSize); err != nil {
		return DataDiffResult{}, err
	}
	ruleSet := &pkgconfig.RuleSet{Rules: params.Rules}
	if err := validateRules(ruleSet); err != nil {
		return DataDiffResult{}, err
	}
	report := func(event TableProgress) {
		if progress != nil {
			progress(event)
		}
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

	dbDialect := pkgsql.NewDialect(params.Target.Type)
	sourceModels := newTableModelCache(srcDB)
	targetModels := newTableModelCache(tgtDB)

	prepareTable := func(rule pkgconfig.Rule) (*tableModels, error) {
		tgtTable, err := targetModels.get(rule.Table)
		if err != nil {
			return nil, fmt.Errorf("extract target table: %w", err)
		}
		if tgtTable == nil {
			return nil, fmt.Errorf("extract target table: table not found")
		}
		srcTable, err := sourceModels.get(rule.Table)
		if err != nil {
			return nil, fmt.Errorf("extract source table: %w", err)
		}
		compareRule := pkgdiff.CreateCompareRuleColumns(tgtTable, rule.Columns, rule.ComparisonKey, rule.IgnoreColumns)
		var effectiveCols []string
		if allRule, ok := compareRule.(*pkgdiff.AllFieldsEqualRule); ok {
			effectiveCols = allRule.Columns
		} else {
			effectiveCols = rule.ComparisonKey
		}
		keep := dataDiffKeepColumns(tgtTable, srcTable, effectiveCols, rule.IgnoreColumns)
		slimTarget := slimTable(tgtTable, keep)
		slimSource := slimTable(srcTable, intersectNames(keep, srcTable))
		return &tableModels{target: slimTarget, source: slimSource, effectiveCols: effectiveCols}, nil
	}
	compareTable := func(rule pkgconfig.Rule, models *tableModels, handle pkgdiff.DetailedDiffErrorHandler) error {
		start := time.Now()
		compareRule := pkgdiff.CreateCompareRuleColumns(models.target, rule.Columns, rule.ComparisonKey, rule.IgnoreColumns)
		var compareErr error
		if params.ChunkHash {
			compareErr = pkgdiff.StreamCompareDataWithChunkFilterAndTableContext(
				ctx,
				srcDB,
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
				srcDB,
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

	result := DataDiffResult{Complete: true, Tables: make([]TableDiffSummary, 0, len(params.Rules))}
	tablesByRule := make(map[string]int, len(params.Rules))
	for i, rule := range params.Rules {
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
				params.Rules,
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
	return result, nil
}
