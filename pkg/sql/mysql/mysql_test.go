package mysql

import (
	"testing"

	"github.com/jacktea/data-smith/pkg/conn"
)

func TestMySQLDialect_DataSqlGeneration(t *testing.T) {
	dialect := NewMySQLDialect()
	table := &conn.Table{
		Name: "users",
		Columns: map[string]*conn.Column{
			"id":        {Name: "id", DataType: "int", Position: 1},
			"name":      {Name: "name", DataType: "varchar", Position: 2},
			"is_active": {Name: "is_active", DataType: "tinyint(1)", Position: 3},
			"avatar":    {Name: "avatar", DataType: "blob", Position: 4},
			"profile":   {Name: "profile", DataType: "json", Position: 5},
		},
		PrimaryKey: &conn.PrimaryKey{
			Name:    "PRIMARY",
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
	expectedUpdate := "UPDATE `users` SET `name` = 'O''Connor', `is_active` = 1 WHERE `id` = 10;"
	if updateSql != expectedUpdate {
		t.Errorf("GenerateUpdateSql mismatch:\ngot:  %s\nwant: %s", updateSql, expectedUpdate)
	}

	// 2. 测试 Blob 16进制转义
	updateBlobSql := dialect.GenerateUpdateSql(table, row, []string{"avatar"})
	expectedBlobUpdate := "UPDATE `users` SET `avatar` = 0xdeadbeef WHERE `id` = 10;"
	if updateBlobSql != expectedBlobUpdate {
		t.Errorf("GenerateUpdateSql blob mismatch:\ngot:  %s\nwant: %s", updateBlobSql, expectedBlobUpdate)
	}

	// 3. 测试 DELETE
	delSql := dialect.GenerateDeleteSql(table, row)
	expectedDel := "DELETE FROM `users` WHERE `id` = 10;"
	if delSql != expectedDel {
		t.Errorf("GenerateDeleteSql mismatch:\ngot:  %s\nwant: %s", delSql, expectedDel)
	}
}

func TestMySQLDialect_BoundedBatchDataSQL(t *testing.T) {
	dialect := NewMySQLDialect()
	table := &conn.Table{
		Name: "items",
		Columns: map[string]*conn.Column{
			"id":   {Name: "id", DataType: "bigint", Position: 1},
			"name": {Name: "name", DataType: "varchar", Position: 2},
		},
		PrimaryKey: &conn.PrimaryKey{Columns: []string{"id"}},
	}
	rows := []conn.Record{{"id": 1, "name": "one"}, {"id": 2, "name": "two"}}
	if got, want := dialect.GenerateInsertBatchSql(table, rows), "INSERT INTO `items` (`id`, `name`) VALUES (1, 'one'), (2, 'two');"; got != want {
		t.Fatalf("insert batch\ngot:  %s\nwant: %s", got, want)
	}
	if got, want := dialect.GenerateDeleteBatchSql(table, rows), "DELETE FROM `items` WHERE (`id` = 1) OR (`id` = 2);"; got != want {
		t.Fatalf("delete batch\ngot:  %s\nwant: %s", got, want)
	}
}
