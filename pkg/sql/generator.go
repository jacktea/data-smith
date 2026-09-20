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

// GenerateSchemaSQLSafe generates deterministic SQL through a unified schema
// object dependency DAG. Tables are created without foreign keys; foreign keys
// and sequence ownership are separate operations whose prerequisites are
// explicit graph edges. Unreliable dependencies and cycles are rejected.
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

	dropPlan := newSchemaObjectPlan()
	createPlan := newSchemaObjectPlan()
	addPlanned := func(plan *schemaObjectPlan, id schemaOperationID, statement string) {
		plan.add(id, statement)
	}

	// 序列与例程的增删改由实现 INonTableObjectDialect 的方言生成；其余方言
	// （如 MySQL）其驱动本就不会提取这些对象，直接跳过。
	if objectDialect, ok := dbDialect.(INonTableObjectDialect); ok {
		for _, sequence := range sortedSequences(schemaDiff.SequencesDropped) {
			statement := objectDialect.GenerateDropSequenceSql(sequence)
			dropPlan.add(operationID(objectID(conn.SchemaObjectSequence, sequence.Schema, sequence.Name), "base"), statement)
		}
		for _, routine := range sortedRoutines(schemaDiff.RoutinesDropped) {
			statement := objectDialect.GenerateDropRoutineSql(routine)
			dropPlan.add(operationID(objectID(conn.SchemaObjectRoutine, routine.Schema, routine.Identity()), "base"), statement)
		}
		for _, sequence := range sortedSequences(schemaDiff.SequencesAdded) {
			object := objectID(conn.SchemaObjectSequence, sequence.Schema, sequence.Name)
			statement := objectDialect.GenerateCreateSequenceSql(sequence)
			createPlan.add(operationID(object, "base"), statement)
			if sequence.OwnedBy != "" {
				withoutOwner := *sequence
				withoutOwner.OwnedBy = ""
				for _, ownership := range objectDialect.GenerateAlterSequenceSql(&withoutOwner, sequence) {
					createPlan.add(operationID(object, "ownership"), ownership)
				}
			}
		}
		for _, change := range sortedSequenceDiffs(schemaDiff.SequencesModified) {
			for _, statement := range objectDialect.GenerateAlterSequenceSql(change.Old, change.New) {
				step := "base"
				if change.Old.OwnedBy != change.New.OwnedBy && change.New.OwnedBy != "" {
					step = "ownership"
				}
				createPlan.add(operationID(objectID(conn.SchemaObjectSequence, change.New.Schema, change.New.Name), step), statement)
			}
		}
		for _, routine := range sortedRoutines(schemaDiff.RoutinesAdded) {
			statement := objectDialect.GenerateCreateRoutineSql(routine)
			createPlan.add(operationID(objectID(conn.SchemaObjectRoutine, routine.Schema, routine.Identity()), "base"), statement)
		}
		for _, change := range sortedRoutineDiffs(schemaDiff.RoutinesModified) {
			statement := objectDialect.GenerateCreateRoutineSql(change.New)
			createPlan.add(operationID(objectID(conn.SchemaObjectRoutine, change.New.Schema, change.New.Identity()), "base"), statement)
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
	for _, view := range sortedTables(viewsToDrop) {
		statement := dbDialect.GenerateDropViewSql(view)
		dropPlan.add(operationID(objectID(conn.SchemaObjectView, view.Schema, view.Name), "base"), statement)
	}

	// Remove constraints owned by tables that will disappear. This makes
	// mutually-referencing table drops executable before the tables are dropped.
	for _, table := range dropped {
		if table.Type != conn.TableTypeTable {
			continue
		}
		tableDrop := operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "foreign-keys")
		for _, foreignKey := range sortedForeignKeyMap(table.ForeignKeys) {
			addPlanned(dropPlan, tableDrop, dbDialect.GenerateDropForeignKeySql(table, foreignKey))
		}
	}
	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		tableChange := operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "base")
		for _, foreignKey := range sortedForeignKeys(tableDiff.ForeignKeysDropped) {
			addPlanned(createPlan, tableChange, dbDialect.GenerateDropForeignKeySql(table, foreignKey))
		}
		for _, change := range sortedForeignKeyDiffs(tableDiff.ForeignKeysModified) {
			if change.Old != nil {
				addPlanned(createPlan, tableChange, dbDialect.GenerateDropForeignKeySql(table, change.Old))
			}
		}
		for _, index := range sortedIndexes(tableDiff.IndexesDropped) {
			// 主键背书索引只能随约束删除 (DROP INDEX 会被 PG 拒绝),
			// 其生命周期跟随下方 PrimaryKeyChange 的 DROP CONSTRAINT。
			if index.Primary {
				continue
			}
			addPlanned(createPlan, tableChange, dbDialect.GenerateDropIndexSql(table, index))
		}
		for _, change := range sortedIndexDiffs(tableDiff.IndexesModified) {
			if change.Old != nil && !change.Old.Primary {
				addPlanned(createPlan, tableChange, dbDialect.GenerateDropIndexSql(table, change.Old))
			}
		}
		// CHECK 约束删除（C11）：先于列删除执行——被删列上的 CHECK 若不先删
		// 会令 DROP COLUMN 被拒绝； modified 的旧约束同样在此删除。
		if checkDialect, ok := dbDialect.(ICheckConstraintDialect); ok {
			for _, check := range sortedChecks(tableDiff.ChecksDropped) {
				addPlanned(createPlan, tableChange, checkDialect.GenerateDropCheckConstraintSql(table, check))
			}
			for _, change := range sortedCheckDiffs(tableDiff.ChecksModified) {
				if change.Old != nil {
					addPlanned(createPlan, tableChange, checkDialect.GenerateDropCheckConstraintSql(table, change.Old))
				}
			}
		}
		if tableDiff.PrimaryKeyChange != nil && tableDiff.PrimaryKeyChange.Old != nil {
			addPlanned(createPlan, tableChange, dbDialect.GenerateDropPrimaryKeySql(table, tableDiff.PrimaryKeyChange.Old))
		}
		for _, column := range sortedColumns(tableDiff.ColumnsDropped) {
			addPlanned(createPlan, tableChange, dbDialect.GenerateDropColumnSql(table, column))
		}
	}

	for _, table := range dropped {
		if table.Type != conn.TableTypeTable {
			continue
		}
		statement := dbDialect.GenerateDropTableSql(table)
		dropPlan.add(operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "base"), statement)
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		tableChange := operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "base")
		for _, change := range sortedColumnDiffs(tableDiff.ColumnsModified) {
			addPlanned(createPlan, tableChange, dbDialect.GenerateAlterColumnSql(table, change.Old, change.New))
		}
		for _, column := range sortedColumns(tableDiff.ColumnsAdded) {
			addPlanned(createPlan, tableChange, dbDialect.GenerateAddColumnSql(table, column))
		}
	}

	for _, table := range added {
		if table.Type == conn.TableTypeTable {
			statement := dbDialect.GenerateTableDDL(tableWithoutForeignKeys(table))
			createPlan.add(operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "base"), statement)
		}
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil || table.Type == conn.TableTypeView {
			continue
		}
		tableChange := operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "base")
		if tableDiff.PrimaryKeyChange != nil && tableDiff.PrimaryKeyChange.New != nil {
			addPlanned(createPlan, tableChange, dbDialect.GenerateAddPrimaryKeySql(table, tableDiff.PrimaryKeyChange.New))
		}
		for _, change := range sortedIndexDiffs(tableDiff.IndexesModified) {
			if change.New != nil {
				addPlanned(createPlan, tableChange, dbDialect.GenerateCreateIndexSql(table, change.New))
			}
		}
		for _, index := range sortedIndexes(tableDiff.IndexesAdded) {
			addPlanned(createPlan, tableChange, dbDialect.GenerateCreateIndexSql(table, index))
		}
		// CHECK 约束新增/变更（C11）：在列 DDL 之后添加，引用新增列的约束
		// 可直接执行；与既有索引/主键同属建表后的键与约束阶段。
		if checkDialect, ok := dbDialect.(ICheckConstraintDialect); ok {
			for _, check := range sortedChecks(tableDiff.ChecksAdded) {
				addPlanned(createPlan, tableChange, checkDialect.GenerateAddCheckConstraintSql(table, check))
			}
			for _, change := range sortedCheckDiffs(tableDiff.ChecksModified) {
				if change.New != nil {
					addPlanned(createPlan, tableChange, checkDialect.GenerateAddCheckConstraintSql(table, change.New))
				}
			}
		}
	}

	for _, operation := range orderForeignKeyOperations(collectForeignKeyOperations(added, modified)) {
		foreignKeys := operationID(objectID(conn.SchemaObjectTable, operation.table.Schema, operation.table.Name), "foreign-keys")
		addPlanned(createPlan, foreignKeys, dbDialect.GenerateAddForeignKeySql(operation.table, operation.foreignKey))
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
	for _, view := range sortedTables(viewsToCreate) {
		statement := dbDialect.GenerateViewDDL(view)
		createPlan.add(operationID(objectID(conn.SchemaObjectView, view.Schema, view.Name), "base"), statement)
	}

	for _, tableDiff := range modified {
		table := targetTable(tableDiff)
		if table == nil {
			continue
		}
		if tableDiff.CommentChange == nil {
			continue
		}
		// 视图注释（C12）：经 IViewCommentDialect 生成 COMMENT ON VIEW；
		// 不支持视图注释的方言（MySQL）跳过，不产生删建。
		if table.Type == conn.TableTypeView {
			if viewCommentDialect, ok := dbDialect.(IViewCommentDialect); ok {
				addPlanned(createPlan,
					operationID(objectID(conn.SchemaObjectView, table.Schema, table.Name), "comment"),
					viewCommentDialect.GenerateAlterViewCommentSql(table, tableDiff.CommentChange.New))
			}
			continue
		}
		if table.Type == conn.TableTypeTable {
			addPlanned(createPlan,
				operationID(objectID(conn.SchemaObjectTable, table.Schema, table.Name), "comment"),
				dbDialect.GenerateAlterTableCommentSql(table, tableDiff.CommentChange.New))
		}
	}

	if err := wireSchemaObjectPlans(dropPlan, createPlan, schemaDiff); err != nil {
		return nil, err
	}
	dropStatements, err := dropPlan.ordered(true)
	if err != nil {
		return nil, fmt.Errorf("order dropped schema objects: %w", err)
	}
	createStatements, err := createPlan.ordered(false)
	if err != nil {
		return nil, fmt.Errorf("order created schema objects: %w", err)
	}
	return append(dropStatements, createStatements...), nil
}

func wireSchemaObjectPlans(dropPlan, createPlan *schemaObjectPlan, schemaDiff *diff.SchemaDiff) error {
	wire := func(plan *schemaObjectPlan, dependent schemaOperationID, dependencies []conn.SchemaObjectRef) error {
		for _, dependency := range dependencies {
			prerequisite, err := plan.baseFor(dependency)
			if err != nil {
				return fmt.Errorf("resolve dependency for %s: %w", dependent.display(), err)
			}
			if prerequisite != (schemaOperationID{}) {
				plan.require(dependent, prerequisite)
			}
		}
		return nil
	}
	wireTable := func(plan *schemaObjectPlan, table *conn.Table) error {
		if table == nil {
			return nil
		}
		kind := conn.SchemaObjectTable
		if table.Type == conn.TableTypeView {
			kind = conn.SchemaObjectView
		}
		base := operationID(objectID(kind, table.Schema, table.Name), "base")
		if !plan.has(base) {
			return nil
		}
		dependencies := append([]conn.SchemaObjectRef(nil), table.Dependencies...)
		inferred, err := inferTableDependencies(table, plan)
		if err != nil {
			return err
		}
		dependencies = append(dependencies, inferred...)
		if table.Type == conn.TableTypeView && table.ViewDefinition != nil {
			for _, dependency := range table.ViewDefinition.Dependencies {
				schema, name := splitObjectName(dependency, table.Schema)
				dependencyKind := conn.SchemaObjectTable
				viewRef := conn.SchemaObjectRef{Kind: conn.SchemaObjectView, Schema: schema, Name: name}
				if candidate, _ := plan.baseFor(viewRef); candidate != (schemaOperationID{}) {
					dependencyKind = conn.SchemaObjectView
				}
				dependencies = append(dependencies, conn.SchemaObjectRef{Kind: dependencyKind, Schema: schema, Name: name})
			}
		}
		if err := wire(plan, base, dependencies); err != nil {
			return err
		}
		comment := operationID(base.object, "comment")
		plan.require(comment, base)
		foreignKeys := operationID(base.object, "foreign-keys")
		plan.require(foreignKeys, base)
		for _, foreignKey := range table.ForeignKeys {
			schema := foreignKey.ReferencedSchema
			if schema == "" {
				schema = table.Schema
			}
			prerequisite, err := plan.baseFor(conn.SchemaObjectRef{Kind: conn.SchemaObjectTable, Schema: schema, Name: foreignKey.ReferencedTable})
			if err != nil {
				return err
			}
			if prerequisite != (schemaOperationID{}) {
				plan.require(foreignKeys, prerequisite)
			}
		}
		return nil
	}

	for _, table := range schemaDiff.TablesDropped {
		if err := wireTable(dropPlan, table); err != nil {
			return err
		}
	}
	for _, table := range schemaDiff.TablesAdded {
		if err := wireTable(createPlan, table); err != nil {
			return err
		}
	}
	for _, change := range schemaDiff.TablesModified {
		if err := wireTable(dropPlan, change.SourceTable); err != nil {
			return err
		}
		if err := wireTable(createPlan, targetTable(change)); err != nil {
			return err
		}
	}
	for _, view := range schemaDiff.ViewsAffected {
		if err := wireTable(dropPlan, view); err != nil {
			return err
		}
		if err := wireTable(createPlan, view); err != nil {
			return err
		}
	}

	wireRoutine := func(plan *schemaObjectPlan, routine *conn.Routine) error {
		if routine == nil {
			return nil
		}
		base := operationID(objectID(conn.SchemaObjectRoutine, routine.Schema, routine.Identity()), "base")
		if !plan.has(base) {
			return nil
		}
		inferred, err := inferRoutineDependencies(routine, plan)
		if err != nil {
			return err
		}
		dependencies := append(append([]conn.SchemaObjectRef(nil), routine.Dependencies...), inferred...)
		return wire(plan, base, dependencies)
	}
	for _, routine := range schemaDiff.RoutinesDropped {
		if err := wireRoutine(dropPlan, routine); err != nil {
			return err
		}
	}
	for _, routine := range schemaDiff.RoutinesAdded {
		if err := wireRoutine(createPlan, routine); err != nil {
			return err
		}
	}
	for _, change := range schemaDiff.RoutinesModified {
		if err := wireRoutine(createPlan, change.New); err != nil {
			return err
		}
	}

	wireSequenceOwnership := func(sequence *conn.Sequence) error {
		if sequence == nil || sequence.OwnedBy == "" {
			return nil
		}
		table, column, ok := strings.Cut(sequence.OwnedBy, ".")
		if !ok || table == "" || column == "" || strings.Contains(column, ".") {
			return fmt.Errorf("sequence:%s.%s has unresolvable ownership %q", sequence.Schema, sequence.Name, sequence.OwnedBy)
		}
		ownership := operationID(objectID(conn.SchemaObjectSequence, sequence.Schema, sequence.Name), "ownership")
		base := operationID(ownership.object, "base")
		createPlan.require(ownership, base)
		owner, err := createPlan.baseFor(conn.SchemaObjectRef{Kind: conn.SchemaObjectTable, Schema: sequence.Schema, Name: table})
		if err != nil {
			return err
		}
		if owner != (schemaOperationID{}) {
			createPlan.require(ownership, owner)
		}
		return nil
	}
	for _, sequence := range schemaDiff.SequencesAdded {
		if err := wireSequenceOwnership(sequence); err != nil {
			return err
		}
	}
	for _, change := range schemaDiff.SequencesModified {
		if err := wireSequenceOwnership(change.New); err != nil {
			return err
		}
	}
	return nil
}

func splitObjectName(value, defaultSchema string) (string, string) {
	value = strings.TrimSpace(value)
	if schema, name, ok := strings.Cut(value, "."); ok {
		return schema, name
	}
	return defaultSchema, value
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

// stableDependencyOrder returns dependencies before dependents. Cyclic nodes
// are appended in stable order so callers can either handle or reject them.
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

func sortedChecks(checks []*conn.CheckConstraint) []*conn.CheckConstraint {
	result := append([]*conn.CheckConstraint(nil), checks...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func sortedCheckDiffs(changes []*diff.CheckConstraintDiff) []*diff.CheckConstraintDiff {
	result := append([]*diff.CheckConstraintDiff(nil), changes...)
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
