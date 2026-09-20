package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

// GenerateSchemaSQL preserves the original API. Callers that need cycle or
// configuration errors should use GenerateSchemaSQLSafe.
func GenerateSchemaSQL(schemaDiff *diff.SchemaDiff, dialect consts.DBType) []string {
	statements, _ := GenerateSchemaSQLSafe(schemaDiff, dialect)
	return statements
}

// GenerateSchemaSQLSafe generates deterministic SQL in dependency-safe phases.
// Tables are created without foreign keys; every foreign key is added only
// after all table creates have completed. View dependency cycles are rejected
// because no executable CREATE VIEW ordering exists for them.
func GenerateSchemaSQLSafe(schemaDiff *diff.SchemaDiff, dialect consts.DBType) ([]string, error) {
	if schemaDiff == nil {
		return nil, fmt.Errorf("schema diff is required")
	}
	dbDialect := NewDialect(dialect)
	if dbDialect == nil {
		return nil, fmt.Errorf("unsupported SQL dialect %q", dialect)
	}

	added := sortedTables(schemaDiff.TablesAdded)
	dropped := sortedTables(schemaDiff.TablesDropped)
	modified := sortedTableDiffs(schemaDiff.TablesModified)

	var dropViews, dropDependencies, dropTables []string
	var sequencesDropped, routinesDropped, sequencesCreated, sequencesAltered, routinesCreated []string
	var alterColumns, createTables, buildKeys, addForeignKeys, createViews, comments []string
	add := func(destination *[]string, statement string) {
		if statement = strings.TrimSpace(statement); statement != "" {
			*destination = append(*destination, statement)
		}
	}

	// 序列与例程的增删改由实现 INonTableObjectDialect 的方言生成；其余方言
	// （如 MySQL）其驱动本就不会提取这些对象，直接跳过。
	if objectDialect, ok := dbDialect.(INonTableObjectDialect); ok {
		for _, sequence := range sortedSequences(schemaDiff.SequencesDropped) {
			add(&sequencesDropped, objectDialect.GenerateDropSequenceSql(sequence))
		}
		for _, routine := range sortedRoutines(schemaDiff.RoutinesDropped) {
			add(&routinesDropped, objectDialect.GenerateDropRoutineSql(routine))
		}
		for _, sequence := range sortedSequences(schemaDiff.SequencesAdded) {
			add(&sequencesCreated, objectDialect.GenerateCreateSequenceSql(sequence))
		}
		for _, change := range sortedSequenceDiffs(schemaDiff.SequencesModified) {
			for _, statement := range objectDialect.GenerateAlterSequenceSql(change.Old, change.New) {
				add(&sequencesAltered, statement)
			}
		}
		for _, routine := range sortedRoutines(schemaDiff.RoutinesAdded) {
			add(&routinesCreated, objectDialect.GenerateCreateRoutineSql(routine))
		}
		for _, change := range sortedRoutineDiffs(schemaDiff.RoutinesModified) {
			add(&routinesCreated, objectDialect.GenerateCreateRoutineSql(change.New))
		}
	}

	viewsToDrop := make([]*conn.Table, 0)
	for _, table := range dropped {
		if table.Type == conn.TableTypeView {
			viewsToDrop = append(viewsToDrop, table)
		}
	}
	for _, tableDiff := range modified {
		if tableDiff.ViewDefinitionChange != nil {
			view := tableDiff.SourceTable
			if view == nil {
				view = tableDiff.Table
			}
			viewsToDrop = append(viewsToDrop, view)
		}
	}
	// 定义未变但依赖被变更对象的视图（依赖闭包）同样要先 DROP 再重建，
	// 否则列类型/列删除类 DDL 会被视图依赖拒绝。
	viewsToDrop = append(viewsToDrop, schemaDiff.ViewsAffected...)
	orderedDropViews, err := orderViews(viewsToDrop, true)
	if err != nil {
		return nil, fmt.Errorf("order dropped views: %w", err)
	}
	for _, view := range orderedDropViews {
		add(&dropViews, dbDialect.GenerateDropViewSql(view))
	}

	// Remove constraints owned by tables that will disappear. This makes
	// mutually-referencing table drops executable before the tables are dropped.
	for _, table := range dropped {
		if table.Type != conn.TableTypeTable {
			continue
		}
		for _, foreignKey := range sortedForeignKeyMap(table.ForeignKeys) {
			add(&dropDependencies, dbDialect.GenerateDropForeignKeySql(table, foreignKey))
		}
	}
	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		for _, foreignKey := range sortedForeignKeys(tableDiff.ForeignKeysDropped) {
			add(&dropDependencies, dbDialect.GenerateDropForeignKeySql(table, foreignKey))
		}
		for _, change := range sortedForeignKeyDiffs(tableDiff.ForeignKeysModified) {
			if change.Old != nil {
				add(&dropDependencies, dbDialect.GenerateDropForeignKeySql(table, change.Old))
			}
		}
		for _, index := range sortedIndexes(tableDiff.IndexesDropped) {
			// 主键背书索引只能随约束删除 (DROP INDEX 会被 PG 拒绝),
			// 其生命周期跟随下方 PrimaryKeyChange 的 DROP CONSTRAINT。
			if index.Primary {
				continue
			}
			add(&dropDependencies, dbDialect.GenerateDropIndexSql(table, index))
		}
		for _, change := range sortedIndexDiffs(tableDiff.IndexesModified) {
			if change.Old != nil && !change.Old.Primary {
				add(&dropDependencies, dbDialect.GenerateDropIndexSql(table, change.Old))
			}
		}
		if tableDiff.PrimaryKeyChange != nil && tableDiff.PrimaryKeyChange.Old != nil {
			add(&dropDependencies, dbDialect.GenerateDropPrimaryKeySql(table, tableDiff.PrimaryKeyChange.Old))
		}
		for _, column := range sortedColumns(tableDiff.ColumnsDropped) {
			add(&dropDependencies, dbDialect.GenerateDropColumnSql(table, column))
		}
	}

	for _, table := range orderTablesForDrop(dropped) {
		add(&dropTables, dbDialect.GenerateDropTableSql(table))
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		for _, change := range sortedColumnDiffs(tableDiff.ColumnsModified) {
			add(&alterColumns, dbDialect.GenerateAlterColumnSql(table, change.Old, change.New))
		}
		for _, column := range sortedColumns(tableDiff.ColumnsAdded) {
			add(&alterColumns, dbDialect.GenerateAddColumnSql(table, column))
		}
	}

	for _, table := range added {
		if table.Type == conn.TableTypeTable {
			add(&createTables, dbDialect.GenerateTableDDL(tableWithoutForeignKeys(table)))
		}
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		if tableDiff.PrimaryKeyChange != nil && tableDiff.PrimaryKeyChange.New != nil {
			add(&buildKeys, dbDialect.GenerateAddPrimaryKeySql(table, tableDiff.PrimaryKeyChange.New))
		}
		for _, change := range sortedIndexDiffs(tableDiff.IndexesModified) {
			if change.New != nil {
				add(&buildKeys, dbDialect.GenerateCreateIndexSql(table, change.New))
			}
		}
		for _, index := range sortedIndexes(tableDiff.IndexesAdded) {
			add(&buildKeys, dbDialect.GenerateCreateIndexSql(table, index))
		}
	}

	for _, operation := range orderForeignKeyOperations(collectForeignKeyOperations(added, modified)) {
		add(&addForeignKeys, dbDialect.GenerateAddForeignKeySql(operation.table, operation.foreignKey))
	}

	viewsToCreate := make([]*conn.Table, 0)
	for _, table := range added {
		if table.Type == conn.TableTypeView {
			viewsToCreate = append(viewsToCreate, table)
		}
	}
	for _, tableDiff := range modified {
		if tableDiff.ViewDefinitionChange == nil || tableDiff.ViewDefinitionChange.New == nil {
			continue
		}
		table := targetTable(tableDiff)
		view := *table
		view.Type = conn.TableTypeView
		view.ViewDefinition = tableDiff.ViewDefinitionChange.New
		viewsToCreate = append(viewsToCreate, &view)
	}
	// 受影响视图按新态定义参与拓扑排序重建。
	viewsToCreate = append(viewsToCreate, schemaDiff.ViewsAffected...)
	orderedCreateViews, err := orderViews(viewsToCreate, false)
	if err != nil {
		return nil, fmt.Errorf("order created views: %w", err)
	}
	for _, view := range orderedCreateViews {
		add(&createViews, dbDialect.GenerateViewDDL(view))
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table != nil && table.Type == conn.TableTypeTable && tableDiff.CommentChange != nil {
			add(&comments, dbDialect.GenerateAlterTableCommentSql(table, tableDiff.CommentChange.New))
		}
	}

	result := make([]string, 0, len(dropViews)+len(dropDependencies)+len(dropTables)+
		len(sequencesDropped)+len(routinesDropped)+len(sequencesCreated)+len(sequencesAltered)+len(routinesCreated)+
		len(alterColumns)+len(createTables)+len(buildKeys)+len(addForeignKeys)+len(createViews)+len(comments))
	result = append(result, dropViews...)
	result = append(result, dropDependencies...)
	result = append(result, dropTables...)
	// 序列/例程删除放在表删除之后（被删表的 SERIAL 隐式序列已随表消失，
	// IF EXISTS 兜底）；创建放在表 DDL 之前（列默认值 nextval / 函数依赖）。
	result = append(result, sequencesDropped...)
	result = append(result, routinesDropped...)
	result = append(result, sequencesCreated...)
	result = append(result, sequencesAltered...)
	result = append(result, routinesCreated...)
	result = append(result, alterColumns...)
	result = append(result, createTables...)
	result = append(result, buildKeys...)
	result = append(result, addForeignKeys...)
	result = append(result, createViews...)
	result = append(result, comments...)
	return result, nil
}

type foreignKeyOperation struct {
	table      *conn.Table
	foreignKey *conn.ForeignKey
}

func collectForeignKeyOperations(added []*conn.Table, modified []*diff.TableDiff) []foreignKeyOperation {
	var operations []foreignKeyOperation
	for _, table := range added {
		if table.Type != conn.TableTypeTable {
			continue
		}
		for _, foreignKey := range sortedForeignKeyMap(table.ForeignKeys) {
			operations = append(operations, foreignKeyOperation{table: table, foreignKey: foreignKey})
		}
	}
	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		for _, change := range sortedForeignKeyDiffs(tableDiff.ForeignKeysModified) {
			if change.New != nil {
				operations = append(operations, foreignKeyOperation{table: table, foreignKey: change.New})
			}
		}
		for _, foreignKey := range sortedForeignKeys(tableDiff.ForeignKeysAdded) {
			operations = append(operations, foreignKeyOperation{table: table, foreignKey: foreignKey})
		}
	}
	return operations
}

func orderForeignKeyOperations(operations []foreignKeyOperation) []foreignKeyOperation {
	dependencies := make(map[string]map[string]struct{})
	for _, operation := range operations {
		key := objectKey(operation.table.Schema, operation.table.Name)
		if dependencies[key] == nil {
			dependencies[key] = make(map[string]struct{})
		}
		refSchema := operation.foreignKey.ReferencedSchema
		if refSchema == "" {
			refSchema = operation.table.Schema
		}
		refKey := objectKey(refSchema, operation.foreignKey.ReferencedTable)
		if refKey != key {
			dependencies[key][refKey] = struct{}{}
		}
	}
	order, _ := stableDependencyOrder(dependencies)
	rank := make(map[string]int, len(order))
	for index, key := range order {
		rank[key] = index
	}
	sort.SliceStable(operations, func(i, j int) bool {
		leftKey := objectKey(operations[i].table.Schema, operations[i].table.Name)
		rightKey := objectKey(operations[j].table.Schema, operations[j].table.Name)
		if rank[leftKey] != rank[rightKey] {
			return rank[leftKey] < rank[rightKey]
		}
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return operations[i].foreignKey.Name < operations[j].foreignKey.Name
	})
	return operations
}

func orderTablesForDrop(tables []*conn.Table) []*conn.Table {
	candidates := make(map[string]*conn.Table)
	dependencies := make(map[string]map[string]struct{})
	for _, table := range tables {
		if table.Type != conn.TableTypeTable {
			continue
		}
		key := objectKey(table.Schema, table.Name)
		candidates[key] = table
		dependencies[key] = make(map[string]struct{})
	}
	for key, table := range candidates {
		for _, foreignKey := range table.ForeignKeys {
			refSchema := foreignKey.ReferencedSchema
			if refSchema == "" {
				refSchema = table.Schema
			}
			refKey := objectKey(refSchema, foreignKey.ReferencedTable)
			if _, ok := candidates[refKey]; ok && refKey != key {
				dependencies[key][refKey] = struct{}{}
			}
		}
	}
	creationOrder, _ := stableDependencyOrder(dependencies)
	result := make([]*conn.Table, 0, len(creationOrder))
	for index := len(creationOrder) - 1; index >= 0; index-- {
		result = append(result, candidates[creationOrder[index]])
	}
	return result
}

func orderViews(views []*conn.Table, reverse bool) ([]*conn.Table, error) {
	candidates := make(map[string]*conn.Table)
	aliases := make(map[string][]string)
	for _, view := range views {
		if view == nil {
			continue
		}
		key := objectKey(view.Schema, view.Name)
		candidates[key] = view
		aliases[view.Name] = append(aliases[view.Name], key)
		alias := qualifiedAlias(view.Schema, view.Name)
		aliases[alias] = append(aliases[alias], key)
	}
	dependencies := make(map[string]map[string]struct{}, len(candidates))
	for key, view := range candidates {
		dependencies[key] = make(map[string]struct{})
		if view.ViewDefinition == nil {
			continue
		}
		for _, dependency := range view.ViewDefinition.Dependencies {
			resolved := resolveDependency(dependency, view.Schema, candidates, aliases)
			if resolved != "" {
				dependencies[key][resolved] = struct{}{}
			}
		}
	}
	order, cyclic := stableDependencyOrder(dependencies)
	if len(cyclic) > 0 {
		display := make([]string, len(cyclic))
		for index, key := range cyclic {
			display[index] = strings.ReplaceAll(key, "\x00", ".")
		}
		return nil, fmt.Errorf("view dependency cycle: %s", strings.Join(display, ", "))
	}
	result := make([]*conn.Table, 0, len(order))
	if reverse {
		for index := len(order) - 1; index >= 0; index-- {
			result = append(result, candidates[order[index]])
		}
		return result, nil
	}
	for _, key := range order {
		result = append(result, candidates[key])
	}
	return result, nil
}

// stableDependencyOrder returns dependencies before dependents. Cyclic nodes
// are appended in stable order so table/FK cycles can be handled by the
// surrounding two-phase algorithm; view callers reject the returned cycle.
func stableDependencyOrder(dependencies map[string]map[string]struct{}) ([]string, []string) {
	remaining := make(map[string]map[string]struct{}, len(dependencies))
	for key, values := range dependencies {
		remaining[key] = make(map[string]struct{})
		for dependency := range values {
			if _, ok := dependencies[dependency]; ok {
				remaining[key][dependency] = struct{}{}
			}
		}
	}
	var result []string
	for len(remaining) > 0 {
		ready := make([]string, 0)
		for key, values := range remaining {
			if len(values) == 0 {
				ready = append(ready, key)
			}
		}
		if len(ready) == 0 {
			cyclic := make([]string, 0, len(remaining))
			for key := range remaining {
				cyclic = append(cyclic, key)
			}
			sort.Strings(cyclic)
			result = append(result, cyclic...)
			return result, cyclic
		}
		sort.Strings(ready)
		for _, key := range ready {
			delete(remaining, key)
			result = append(result, key)
		}
		for _, values := range remaining {
			for _, key := range ready {
				delete(values, key)
			}
		}
	}
	return result, nil
}

func resolveDependency(raw, schema string, candidates map[string]*conn.Table, aliases map[string][]string) string {
	dependency := strings.TrimSpace(raw)
	if keys := aliases[dependency]; len(keys) == 1 {
		return keys[0]
	}
	if !strings.Contains(dependency, ".") {
		key := objectKey(schema, dependency)
		if _, ok := candidates[key]; ok {
			return key
		}
	}
	return ""
}

func tableWithoutForeignKeys(table *conn.Table) *conn.Table {
	copy := *table
	copy.ForeignKeys = nil
	return &copy
}

func targetTable(tableDiff *diff.TableDiff) *conn.Table {
	if tableDiff.TargetTable != nil {
		return tableDiff.TargetTable
	}
	return tableDiff.Table
}

func objectKey(schema, name string) string { return schema + "\x00" + name }
func qualifiedAlias(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

func sortedTables(tables []*conn.Table) []*conn.Table {
	result := append([]*conn.Table(nil), tables...)
	sort.SliceStable(result, func(i, j int) bool {
		return objectKey(result[i].Schema, result[i].Name) < objectKey(result[j].Schema, result[j].Name)
	})
	return result
}

func sortedSequences(sequences []*conn.Sequence) []*conn.Sequence {
	result := append([]*conn.Sequence(nil), sequences...)
	sort.SliceStable(result, func(i, j int) bool {
		return objectKey(result[i].Schema, result[i].Name) < objectKey(result[j].Schema, result[j].Name)
	})
	return result
}

func sortedSequenceDiffs(changes []*diff.SequenceDiff) []*diff.SequenceDiff {
	result := append([]*diff.SequenceDiff(nil), changes...)
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].New, result[j].New
		if left == nil {
			left = result[i].Old
		}
		if right == nil {
			right = result[j].Old
		}
		return objectKey(left.Schema, left.Name) < objectKey(right.Schema, right.Name)
	})
	return result
}

func sortedRoutines(routines []*conn.Routine) []*conn.Routine {
	result := append([]*conn.Routine(nil), routines...)
	sort.SliceStable(result, func(i, j int) bool {
		return objectKey(result[i].Schema, result[i].Identity()) < objectKey(result[j].Schema, result[j].Identity())
	})
	return result
}

func sortedRoutineDiffs(changes []*diff.RoutineDiff) []*diff.RoutineDiff {
	result := append([]*diff.RoutineDiff(nil), changes...)
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].New, result[j].New
		if left == nil {
			left = result[i].Old
		}
		if right == nil {
			right = result[j].Old
		}
		return objectKey(left.Schema, left.Identity()) < objectKey(right.Schema, right.Identity())
	})
	return result
}

func sortedTableDiffs(tableDiffs []*diff.TableDiff) []*diff.TableDiff {
	result := append([]*diff.TableDiff(nil), tableDiffs...)
	sort.SliceStable(result, func(i, j int) bool {
		left, right := targetTable(result[i]), targetTable(result[j])
		return objectKey(left.Schema, left.Name) < objectKey(right.Schema, right.Name)
	})
	return result
}

func sortedColumns(columns []*conn.Column) []*conn.Column {
	result := append([]*conn.Column(nil), columns...)
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Position != result[j].Position {
			return result[i].Position < result[j].Position
		}
		return result[i].Name < result[j].Name
	})
	return result
}

func sortedColumnDiffs(changes []*diff.ColumnDiff) []*diff.ColumnDiff {
	result := append([]*diff.ColumnDiff(nil), changes...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].New.Name < result[j].New.Name })
	return result
}

func sortedIndexes(indexes []*conn.Index) []*conn.Index {
	result := append([]*conn.Index(nil), indexes...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func sortedIndexDiffs(changes []*diff.IndexDiff) []*diff.IndexDiff {
	result := append([]*diff.IndexDiff(nil), changes...)
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].New, result[j].New
		if left == nil {
			left = result[i].Old
		}
		if right == nil {
			right = result[j].Old
		}
		return left.Name < right.Name
	})
	return result
}

func sortedForeignKeys(foreignKeys []*conn.ForeignKey) []*conn.ForeignKey {
	result := append([]*conn.ForeignKey(nil), foreignKeys...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func sortedForeignKeyMap(foreignKeys map[string]*conn.ForeignKey) []*conn.ForeignKey {
	result := make([]*conn.ForeignKey, 0, len(foreignKeys))
	for _, foreignKey := range foreignKeys {
		result = append(result, foreignKey)
	}
	return sortedForeignKeys(result)
}

func sortedForeignKeyDiffs(changes []*diff.ForeignKeyDiff) []*diff.ForeignKeyDiff {
	result := append([]*diff.ForeignKeyDiff(nil), changes...)
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].New, result[j].New
		if left == nil {
			left = result[i].Old
		}
		if right == nil {
			right = result[j].Old
		}
		return left.Name < right.Name
	})
	return result
}
