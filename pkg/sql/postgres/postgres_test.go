package postgres

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db"
)

func TestGenerateTableDDL(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Type:        consts.DBTypePostgres,
		Port:        5432,
		User:        "postgres",
		Password:    "obvious-test-placeholder",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Skipf("Skipping live database test: %v", err)
	}
	table, err := adapter.ExtractTable("air_inst_document")
	if err != nil {
		t.Skipf("Failed to extract table detail: %v", err)
	}

	dbDialect := NewPostgreDialect()

	ddl := dbDialect.GenerateTableDDL(table)
	t.Logf("Table DDL: %s", ddl)
}

func TestExtractViewDetail(t *testing.T) {
	cfg := config.ConnConfig{
		Host:        "localhost",
		Type:        consts.DBTypePostgres,
		Port:        5432,
		User:        "postgres",
		Password:    "obvious-test-placeholder",
		DBName:      "airedge2.1db",
		TableSchema: "public",
	}
	adapter, err := db.NewDBAdapter(&cfg)
	if err != nil {
		t.Skipf("Skipping live database test: %v", err)
	}
	view, err := adapter.ExtractView("air_inst_document_tab_whereused")
	if err != nil {
		t.Skipf("Failed to extract view detail: %v", err)
	}
	dbDialect := NewPostgreDialect()
	ddl := dbDialect.GenerateViewDDL(view)
	t.Logf("View DDL: %s", ddl)
}

func TestPostgreDialect_DataSqlGeneration(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name:   "users",
		Schema: "public",
		Columns: map[string]*conn.Column{
			"id":        {Name: "id", DataType: "int", Position: 1},
			"name":      {Name: "name", DataType: "varchar", Position: 2},
			"is_active": {Name: "is_active", DataType: "boolean", Position: 3},
			"avatar":    {Name: "avatar", DataType: "bytea", Position: 4},
			"profile":   {Name: "profile", DataType: "jsonb", Position: 5},
		},
		PrimaryKey: &conn.PrimaryKey{
			Name:    "users_pkey",
			Columns: []string{"id"},
		},
	}

	row := conn.Record{
		"id":        10,
		"name":      "O'Connor",
		"is_active": true,
		"avatar":    []byte{0xde, 0xad, 0xbe, 0xef},
		"profile":   `{"theme":"dark"}`,
	}

	// 1. 测试增量 UPDATE（只更新 name 和 is_active）
	updateSql := dialect.GenerateUpdateSql(table, row, []string{"name", "is_active"})
	expectedUpdate := `UPDATE "public"."users" SET "name" = 'O''Connor', "is_active" = TRUE WHERE "id" = 10;`
	if updateSql != expectedUpdate {
		t.Errorf("GenerateUpdateSql mismatch:\ngot:  %s\nwant: %s", updateSql, expectedUpdate)
	}

	// 2. 测试 Bytea 转义
	updateByteaSql := dialect.GenerateUpdateSql(table, row, []string{"avatar"})
	expectedByteaUpdate := `UPDATE "public"."users" SET "avatar" = E'\\xdeadbeef'::bytea WHERE "id" = 10;`
	if updateByteaSql != expectedByteaUpdate {
		t.Errorf("GenerateUpdateSql bytea mismatch:\ngot:  %s\nwant: %s", updateByteaSql, expectedByteaUpdate)
	}

	// 3. 测试 DELETE
	delSql := dialect.GenerateDeleteSql(table, row)
	expectedDel := `DELETE FROM "public"."users" WHERE "id" = 10;`
	if delSql != expectedDel {
		t.Errorf("GenerateDeleteSql mismatch:\ngot:  %s\nwant: %s", delSql, expectedDel)
	}
}

func TestPostgreDialect_BoundedBatchDataSQL(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name:   "items",
		Schema: "custom",
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint", Position: 1},
			"name": {Name: "name", DataType: "text", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rows := []conn.Record{{"id": 1, "name": "one"}, {"id": 2, "name": "two"}}
	if got, want := dialect.GenerateInsertBatchSql(table, rows), `INSERT INTO "custom"."items" ("id", "name") VALUES (1, 'one'), (2, 'two');`; got != want {
		t.Fatalf("insert batch\ngot:  %s\nwant: %s", got, want)
	}
	if got, want := dialect.GenerateDeleteBatchSql(table, rows), `DELETE FROM "custom"."items" WHERE ("id" = 1) OR ("id" = 2);`; got != want {
		t.Fatalf("delete batch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestPostgreDialect_ForeignKeyAndCommentSql(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name:   "orders",
		Schema: "public",
	}

	fk := &conn.ForeignKey{
		Name:              "fk_orders_user",
		Columns:           []string{"user_id"},
		ReferencedSchema:  "public",
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
		OnDelete:          "CASCADE",
		OnUpdate:          "RESTRICT",
	}

	// 1. 测试添加外键
	addFkSql := dialect.GenerateAddForeignKeySql(table, fk)
	expectedAddFk := `ALTER TABLE "public"."orders" ADD CONSTRAINT "fk_orders_user" FOREIGN KEY ("user_id") REFERENCES "public"."users" ("id") ON DELETE CASCADE ON UPDATE RESTRICT;`
	if addFkSql != expectedAddFk {
		t.Errorf("GenerateAddForeignKeySql mismatch:\ngot:  %s\nwant: %s", addFkSql, expectedAddFk)
	}

	// 2. 测试删除外键
	dropFkSql := dialect.GenerateDropForeignKeySql(table, fk)
	expectedDropFk := `ALTER TABLE "public"."orders" DROP CONSTRAINT IF EXISTS "fk_orders_user";`
	if dropFkSql != expectedDropFk {
		t.Errorf("GenerateDropForeignKeySql mismatch:\ngot:  %s\nwant: %s", dropFkSql, expectedDropFk)
	}

	// 3. 测试修改表注释
	commentSql := dialect.GenerateAlterTableCommentSql(table, "订单主表")
	expectedComment := `COMMENT ON TABLE "public"."orders" IS '订单主表';`
	if commentSql != expectedComment {
		t.Errorf("GenerateAlterTableCommentSql mismatch:\ngot:  %s\nwant: %s", commentSql, expectedComment)
	}

	// 4. 测试修改列（类型带有 USING）
	oldCol := &conn.Column{Name: "amount", DataType: "varchar"}
	newCol := &conn.Column{Name: "amount", DataType: "numeric"}
	alterColSql := dialect.GenerateAlterColumnSql(table, oldCol, newCol)
	expectedAlter := `ALTER TABLE "public"."orders" ALTER COLUMN "amount" TYPE numeric USING "amount"::numeric;`
	if alterColSql != expectedAlter {
		t.Errorf("GenerateAlterColumnSql mismatch:\ngot:  %s\nwant: %s", alterColSql, expectedAlter)
	}
}

func TestPostgreDialect_NoPrimaryKeyDataSQL(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name:   "items",
		Schema: "custom",
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint", Position: 1},
			"name": {Name: "name", DataType: "text", Position: 2},
		},
	}
	rows := []conn.Record{{"id": 1, "name": "one"}, {"id": 2, "name": "two"}}
	if got, want := dialect.GenerateDeleteBatchSql(table, rows),
		`DELETE FROM "custom"."items" WHERE ("id" = 1 AND "name" = 'one') OR ("id" = 2 AND "name" = 'two');`; got != want {
		t.Fatalf("delete batch without primary key\ngot:  %s\nwant: %s", got, want)
	}
	if got := dialect.GenerateUpdateSql(table, rows[0], []string{"name"}); got != "" {
		t.Fatalf("update without primary key should be a no-op, got: %s", got)
	}
}

func TestPostgreDialect_UniqueRowIdentityDataSQL(t *testing.T) {
	dialect := NewPostgreDialect()
	table := &conn.Table{
		Name: "items", Schema: "custom",
		Columns: map[string]*conn.Column{
			"tenant": {Name: "tenant", DataType: "text", Nullable: false},
			"code":   {Name: "code", DataType: "text", Nullable: false},
			"note":   {Name: "note", DataType: "text", Nullable: true},
		},
		Indexes: map[string]*conn.Index{
			"uq_items": {Name: "uq_items", Unique: true, Columns: []string{"tenant", "code"}},
		},
	}
	row := conn.Record{"tenant": "acme", "code": "A", "note": "new"}
	if got, want := dialect.GenerateDeleteSql(table, row), `DELETE FROM "custom"."items" WHERE "tenant" = 'acme' AND "code" = 'A';`; got != want {
		t.Fatalf("delete by unique identity\ngot:  %s\nwant: %s", got, want)
	}
	if got, want := dialect.GenerateUpdateSql(table, row, []string{"note"}), `UPDATE "custom"."items" SET "note" = 'new' WHERE "tenant" = 'acme' AND "code" = 'A';`; got != want {
		t.Fatalf("update by unique identity\ngot:  %s\nwant: %s", got, want)
	}
}
