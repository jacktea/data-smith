package diff

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

// baseCheckedTable 构造带主键的基础表，供约束/索引差异用例复用。
func baseCheckedTable(name string) *conn.Table {
	return &conn.Table{
		Name: name,
		Type: conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint", Position: 1, Nullable: false},
			"flag": {Name: "flag", DataType: "integer", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Name: name + "_pkey", Columns: []string{"id"}},
	}
}

// C11：CHECK 约束参与结构比对——增/删/改三路检出，且空白规范化后
// 等价定义不产生伪差异。
func TestCompareSchemasCheckConstraintLifecycle(t *testing.T) {
	src := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"air_inst": baseCheckedTable("air_inst"),
		},
	}
	src.Tables["air_inst"].Checks = map[string]*conn.CheckConstraint{
		"air_inst_delete_flag_check": {Name: "air_inst_delete_flag_check", Definition: "CHECK ((delete_flag IN (0, 1)))"},
		"air_inst_old_check":         {Name: "air_inst_old_check", Definition: "CHECK ((flag >= 0))"},
		"air_inst_same_check":        {Name: "air_inst_same_check", Definition: "CHECK ((flag <  100))"},
	}

	tgt := &conn.DatabaseSchema{
		Tables: map[string]*conn.Table{
			"air_inst": baseCheckedTable("air_inst"),
		},
	}
	tgt.Tables["air_inst"].Checks = map[string]*conn.CheckConstraint{
		"air_inst_delete_flag_check": {Name: "air_inst_delete_flag_check", Definition: "CHECK ((delete_flag IN (0, 1)))"},
		"air_inst_new_check":         {Name: "air_inst_new_check", Definition: "CHECK ((flag <> NULL))"},
		"air_inst_same_check":        {Name: "air_inst_same_check", Definition: "CHECK ((flag < 100))"},
	}
	// 同名不同义：触发 Modified
	tgt.Tables["air_inst"].Checks["air_inst_old_check"] = &conn.CheckConstraint{
		Name: "air_inst_old_check", Definition: "CHECK ((flag >= -1))",
	}

	diff := CompareSchemas(src, tgt)
	if len(diff.TablesModified) != 1 {
		t.Fatalf("expected 1 modified table, got %d", len(diff.TablesModified))
	}
	mod := diff.TablesModified[0]
	if len(mod.ChecksAdded) != 1 || mod.ChecksAdded[0].Name != "air_inst_new_check" {
		t.Errorf("expected air_inst_new_check added, got %v", mod.ChecksAdded)
	}
	if len(mod.ChecksDropped) != 0 {
		t.Errorf("expected no dropped checks (same-name redefinition counts as modified), got %v", mod.ChecksDropped)
	}
	if len(mod.ChecksModified) != 1 || mod.ChecksModified[0].Old.Name != "air_inst_old_check" {
		t.Errorf("expected air_inst_old_check modified, got %v", mod.ChecksModified)
	}
}

// C11 回滚对称：反向比较产出镜像的增/删集合。
func TestCompareSchemasCheckConstraintRollbackMirrorsForward(t *testing.T) {
	src := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"t": baseCheckedTable("t")}}
	tgt := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"t": baseCheckedTable("t")}}
	tgt.Tables["t"].Checks = map[string]*conn.CheckConstraint{
		"t_flag_check": {Name: "t_flag_check", Definition: "CHECK ((flag > 0))"},
	}

	forward := CompareSchemas(src, tgt)
	rollback := CompareSchemas(tgt, src)

	if len(forward.TablesModified) != 1 || len(forward.TablesModified[0].ChecksAdded) != 1 {
		t.Fatalf("expected forward to add t_flag_check, got %+v", forward.TablesModified)
	}
	if len(rollback.TablesModified) != 1 || len(rollback.TablesModified[0].ChecksDropped) != 1 {
		t.Fatalf("expected rollback to drop t_flag_check, got %+v", rollback.TablesModified)
	}
}

// C12：视图定义相等而注释不同 → 仅 CommentChange，不整组删建；注释相同
// → 无差异。
func TestCompareSchemasViewCommentOnlyChange(t *testing.T) {
	viewWith := func(comment string) *conn.Table {
		return &conn.Table{
			Name: "v_report",
			Type: conn.TableTypeView,
			Columns: map[string]*conn.Column{
				"id": {Name: "id", DataType: "integer", Position: 1},
			},
			Comment: comment,
			ViewDefinition: &conn.ViewDefinition{
				SelectStatement: `SELECT "id" FROM "base"`,
				Dependencies:    []string{"public.base"},
			},
		}
	}

	src := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"v_report": viewWith("")}}
	tgt := &conn.DatabaseSchema{Tables: map[string]*conn.Table{"v_report": viewWith("报表视图")}}

	diff := CompareSchemas(src, tgt)
	if len(diff.TablesModified) != 1 {
		t.Fatalf("expected view comment diff to surface, got %d modified", len(diff.TablesModified))
	}
	mod := diff.TablesModified[0]
	if mod.CommentChange == nil || mod.CommentChange.New != "报表视图" {
		t.Fatalf("expected CommentChange with new comment, got %+v", mod.CommentChange)
	}
	if mod.ViewDefinitionChange != nil {
		t.Fatalf("comment-only diff must not rebuild the view, got %+v", mod.ViewDefinitionChange)
	}

	same := CompareSchemas(src, &conn.DatabaseSchema{Tables: map[string]*conn.Table{"v_report": viewWith("")}})
	if len(same.TablesModified) != 0 {
		t.Fatalf("identical views must produce no diff, got %+v", same.TablesModified)
	}
}

// C13：主键背书索引名不同不构成表差异（生命周期跟随主键约束，与生成层
// F1 语义对齐）；主键列集差异仍由 PrimaryKeyChange 检出；非背书索引的
// 名称判异不受影响。
func TestCompareSchemasIgnoresPrimaryKeyBackingIndexName(t *testing.T) {
	srcTable := baseCheckedTable("air_inst_checklist_item")
	srcTable.Indexes = map[string]*conn.Index{
		"air_inst_checklist_item_pkey": {Name: "air_inst_checklist_item_pkey", Primary: true, Unique: true, Columns: []string{"id"}},
		"flag_idx":                     {Name: "flag_idx", Columns: []string{"flag"}},
	}
	tgtTable := baseCheckedTable("air_inst_checklist_item")
	tgtTable.Indexes = map[string]*conn.Index{
		"air_inst_checklist_item_rev_copy1_pkey": {Name: "air_inst_checklist_item_rev_copy1_pkey", Primary: true, Unique: true, Columns: []string{"id"}},
		"flag_idx":                               {Name: "flag_idx", Columns: []string{"flag"}},
	}

	diff := CompareSchemas(
		&conn.DatabaseSchema{Tables: map[string]*conn.Table{"air_inst_checklist_item": srcTable}},
		&conn.DatabaseSchema{Tables: map[string]*conn.Table{"air_inst_checklist_item": tgtTable}},
	)
	if len(diff.TablesModified) != 0 {
		t.Fatalf("backing-index rename must not count as table modification, got %+v", diff.TablesModified)
	}

	// 非背书索引仍按名称判异：tgt 侧 flag_idx 改名 → 删除 + 新增差异。
	tgtTable.Indexes["flag_idx"] = &conn.Index{Name: "flag_idx_renamed", Columns: []string{"flag"}}
	diff = CompareSchemas(
		&conn.DatabaseSchema{Tables: map[string]*conn.Table{"air_inst_checklist_item": srcTable}},
		&conn.DatabaseSchema{Tables: map[string]*conn.Table{"air_inst_checklist_item": tgtTable}},
	)
	if len(diff.TablesModified) != 1 {
		t.Fatalf("expected non-primary index rename to count as modification, got %+v", diff.TablesModified)
	}
	mod := diff.TablesModified[0]
	if len(mod.IndexesModified) != 1 || mod.IndexesModified[0].Old.Name != "flag_idx" || mod.IndexesModified[0].New.Name != "flag_idx_renamed" {
		t.Fatalf("expected flag_idx renamed (modified), got %+v", mod.IndexesModified)
	}
}
