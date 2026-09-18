package sql

import (
	"strings"
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/diff"
)

func TestGenerateSchemaSQL_FourStageOrdering(t *testing.T) {
	// 构造一个包含所有阶段变更的 SchemaDiff：
	// 1. 删除旧表 old_table
	// 2. 新增新表 new_table
	// 3. 修改已有表 users：
	//    - 删除列 old_col
	//    - 删除索引 idx_old
	//    - 删除外键 fk_old
	//    - 修改列 email
	//    - 新增列 age
	//    - 新增索引 idx_age
	//    - 新增外键 fk_new
	//    - 更新表注释
	tblUsers := &conn.Table{
		Name:   "users",
		Schema: "public",
		Type:   conn.TableTypeTable,
	}

	tblNew := &conn.Table{
		Name:   "new_table",
		Schema: "public",
		Type:   conn.TableTypeTable,
		Columns: map[string]*conn.Column{
			"id": {Name: "id", DataType: "integer", Position: 1},
		},
	}

	tblOld := &conn.Table{
		Name:   "old_table",
		Schema: "public",
		Type:   conn.TableTypeTable,
	}

	schemaDiff := &diff.SchemaDiff{
		TablesAdded:   []*conn.Table{tblNew},
		TablesDropped: []*conn.Table{tblOld},
		TablesModified: []*diff.TableDiff{
			{
				Table: tblUsers,
				ColumnsAdded: []*conn.Column{
					{Name: "age", DataType: "integer"},
				},
				ColumnsDropped: []*conn.Column{
					{Name: "old_col", DataType: "varchar"},
				},
				ColumnsModified: []*diff.ColumnDiff{
					{
						Old: &conn.Column{Name: "email", DataType: "varchar"},
						New: &conn.Column{Name: "email", DataType: "text"},
					},
				},
				IndexesAdded: []*conn.Index{
					{Name: "idx_age", Columns: []string{"age"}},
				},
				IndexesDropped: []*conn.Index{
					{Name: "idx_old", Columns: []string{"old_col"}},
				},
				ForeignKeysAdded: []*conn.ForeignKey{
					{
						Name:              "fk_new",
						Columns:           []string{"group_id"},
						ReferencedTable:   "groups",
						ReferencedColumns: []string{"id"},
					},
				},
				ForeignKeysDropped: []*conn.ForeignKey{
					{Name: "fk_old"},
				},
				CommentChange: &diff.CommentDiff{
					Old: "旧注释",
					New: "新注释",
				},
			},
		},
	}

	sqls := GenerateSchemaSQL(schemaDiff, consts.DBTypePostgres)
	if len(sqls) == 0 {
		t.Fatalf("expected sqls to be generated, got 0")
	}

	fullScript := strings.Join(sqls, "\n--SEP--\n")

	// 验证阶段 1: 解除依赖必须排在结构变更之前
	// DROP CONSTRAINT fk_old 必须早于 DROP COLUMN old_col
	// DROP INDEX idx_old 必须早于 DROP COLUMN old_col
	posDropFk := strings.Index(fullScript, "DROP CONSTRAINT IF EXISTS \"fk_old\"")
	posDropIdx := strings.Index(fullScript, "DROP INDEX \"idx_old\"")
	posDropCol := strings.Index(fullScript, "DROP COLUMN \"old_col\"")
	posAlterCol := strings.Index(fullScript, "ALTER COLUMN \"email\"")
	posAddCol := strings.Index(fullScript, "ADD COLUMN \"age\"")
	posCreateTbl := strings.Index(fullScript, "CREATE TABLE \"new_table\"")
	posAddFk := strings.Index(fullScript, "ADD CONSTRAINT \"fk_new\"")
	posDropTbl := strings.Index(fullScript, "DROP TABLE \"old_table\"")
	posComment := strings.Index(fullScript, "COMMENT ON TABLE \"public\".\"users\"")

	if posDropFk == -1 || posDropIdx == -1 || posDropCol == -1 || posAlterCol == -1 ||
		posAddCol == -1 || posCreateTbl == -1 || posAddFk == -1 || posDropTbl == -1 || posComment == -1 {
		t.Fatalf("one or more expected DDL statements are missing in script:\n%s", fullScript)
	}

	// 阶段 1 在前
	if !(posDropFk < posDropCol && posDropIdx < posDropCol) {
		t.Errorf("expected drop fk and drop idx before drop col")
	}
	// 阶段 1 在 阶段 2 之前
	if !(posDropCol < posAlterCol && posAlterCol < posAddCol) {
		t.Errorf("expected drop col < alter col < add col")
	}
	// 阶段 3（建表）在 阶段 2 之后
	if !(posAddCol < posCreateTbl) {
		t.Errorf("expected add col < create table")
	}
	// 阶段 4（加外键、删旧表、加注释）在 阶段 3 之后
	if !(posCreateTbl < posAddFk && posCreateTbl < posDropTbl && posCreateTbl < posComment) {
		t.Errorf("expected create table < add fk / drop table / comment")
	}
}
