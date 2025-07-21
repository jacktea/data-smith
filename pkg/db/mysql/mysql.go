package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db/base"
	"github.com/jacktea/data-smith/pkg/utils"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLAdapter struct {
	base.BaseAdapter
}

func NewMySQLAdapter(cfg *config.ConnConfig) (*MySQLAdapter, error) {
	adapter := &MySQLAdapter{}
	if err := adapter.Init(cfg); err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		adapter.Close()
		return nil, err
	}
	var pingErr error
	for range 3 {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if pingErr != nil {
		adapter.Close()
		return nil, pingErr
	}
	adapter.Conn = db
	adapter.Cfg.TableSchema = cfg.DBName
	return adapter, nil
}

func (a *MySQLAdapter) ReadSchema() (*conn.DatabaseSchema, error) {
	dbSchema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{}}
	tables, err := a.queryTables()
	if err != nil {
		return nil, err
	}
	dbSchema.Tables = tables
	return dbSchema, nil
}

func (a *MySQLAdapter) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	if len(pk) == 0 {
		return nil, fmt.Errorf("primary key required for batch scan")
	}
	// 构造 SELECT ... FROM table WHERE (pk) > (lastPK) ORDER BY pk LIMIT ?
	colList := utils.JoinWrap(cols, "`", ", ")
	pkList := utils.JoinWrap(pk, "`", ", ")
	orderBy := pkList
	where := ""
	var args []any
	if len(lastPK) > 0 {
		where = "WHERE ("
		where += pkList
		where += ") > ("
		for i := range pk {
			if i > 0 {
				where += ", "
			}
			where += "?"
			args = append(args, lastPK[i])
		}
		where += ")"
	}
	query := fmt.Sprintf("SELECT %s FROM `%s` %s ORDER BY %s LIMIT ?", colList, table, where, orderBy)
	args = append(args, limit)
	rows, err := a.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []conn.Record
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rec := conn.Record{}
		for i, c := range cols {
			rec[c] = vals[i]
		}
		result = append(result, rec)
	}
	return result, nil
}

func (a *MySQLAdapter) ExtractTable(tableName string) (*conn.Table, error) {
	table := &conn.Table{
		Name:        tableName,
		Type:        conn.TableTypeTable,
		Schema:      a.Cfg.TableSchema,
		Columns:     map[string]*conn.Column{},
		Indexes:     map[string]*conn.Index{},
		ForeignKeys: map[string]*conn.ForeignKey{},
	}
	// 解析列
	err := a.extractColumns(table)
	if err != nil {
		return nil, err
	}
	// 解析主键
	err = a.extractPrimaryKey(table)
	if err != nil {
		return nil, err
	}

	// 解析索引
	err = a.extractIndexes(table)
	if err != nil {
		return nil, err
	}

	// 解析外键
	err = a.extractForeignKeys(table)
	if err != nil {
		return nil, err
	}

	table.Comment = a.getTableComment(a.Cfg.TableSchema, tableName)
	return table, nil
}

func (a *MySQLAdapter) ExtractView(viewName string) (*conn.Table, error) {
	view := &conn.Table{
		Name:    viewName,
		Type:    conn.TableTypeView,
		Schema:  a.Cfg.TableSchema,
		Columns: map[string]*conn.Column{},
	}
	// 解析列
	err := a.extractColumns(view)
	if err != nil {
		return nil, err
	}

	err = a.extractViewDefinition(view)
	if err != nil {
		return nil, err
	}

	view.Comment = a.getTableComment(a.Cfg.TableSchema, viewName)
	return view, nil
}

func (a *MySQLAdapter) GetConn() *sql.DB {
	return a.Conn
}

func (a *MySQLAdapter) GetConfig() *config.ConnConfig {
	return a.Cfg
}

func (a *MySQLAdapter) queryTables() (map[string]*conn.Table, error) {
	rows, err := a.Conn.Query(`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = ?`, a.Cfg.TableSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := make(map[string]*conn.Table)
	for rows.Next() {
		var name, t string
		if err := rows.Scan(&name, &t); err != nil {
			return nil, err
		}
		switch conn.ParseTableType(t) {
		case conn.TableTypeTable:
			table, err := a.ExtractTable(name)
			if err != nil {
				return nil, err
			}
			tables[name] = table
		case conn.TableTypeView:
			table, err := a.ExtractView(name)
			if err != nil {
				return nil, err
			}
			tables[name] = table
		default:
			continue
		}
	}
	return tables, nil
}

func (a *MySQLAdapter) extractColumns(table *conn.Table) error {
	colRows, err := a.Conn.Query(`SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			column_comment,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			ordinal_position,
			extra
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer colRows.Close()
	columns := make(map[string]*conn.Column)
	for colRows.Next() {
		var col conn.Column
		var nullable string
		var charMaxLen, numericPrec, numericScale sql.NullInt64
		var comment sql.NullString
		if err := colRows.Scan(
			&col.Name,
			&col.DataType,
			&nullable,
			&col.Default,
			&comment,
			&charMaxLen,
			&numericPrec,
			&numericScale,
			&col.Position,
			&col.Extra,
		); err != nil {
			return err
		}
		if charMaxLen.Valid {
			maxLen := int(charMaxLen.Int64)
			col.CharMaxLen = &maxLen
		}
		if numericPrec.Valid {
			prec := int(numericPrec.Int64)
			col.NumericPrec = &prec
		}
		if numericScale.Valid {
			scale := int(numericScale.Int64)
			col.NumericScale = &scale
		}
		if comment.Valid {
			col.Comment = &comment.String
		}
		col.Nullable = nullable == "YES"
		columns[col.Name] = &col
	}
	table.Columns = columns
	return nil
}

func (a *MySQLAdapter) extractPrimaryKey(table *conn.Table) error {
	query := `
		SELECT 
			tc.constraint_name,
			GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) as columns
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_schema = ? 
		  AND tc.table_name = ? 
		  AND tc.constraint_type = 'PRIMARY KEY'
		GROUP BY tc.constraint_name
	`
	var constraintName, columns string
	err := a.Conn.QueryRow(query, table.Schema, table.Name).Scan(&constraintName, &columns)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	table.PrimaryKey = &conn.PrimaryKey{
		Name:    constraintName,
		Columns: strings.Split(columns, ","),
	}
	return nil
}

func (a *MySQLAdapter) extractIndexes(table *conn.Table) error {
	query := `
		SELECT 
			index_name,
			non_unique = 0 as is_unique,
			index_type as method,
			GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name != 'PRIMARY'
		GROUP BY index_name, non_unique, index_type
	`
	idxRows, err := a.Conn.Query(query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var idx conn.Index
		var columns string
		if err := idxRows.Scan(
			&idx.Name,
			&idx.Unique,
			&idx.Method,
			&columns,
		); err != nil {
			return err
		}
		idx.Columns = strings.Split(columns, ",")
		table.Indexes[idx.Name] = &idx
	}
	return nil
}

func (a *MySQLAdapter) extractForeignKeys(table *conn.Table) error {
	query := `
		SELECT 
			tc.constraint_name,
			GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
			ccu.table_schema as referenced_schema,
			ccu.table_name as referenced_table,
			GROUP_CONCAT(ccu.column_name ORDER BY kcu.ordinal_position) as referenced_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
		JOIN information_schema.referential_constraints rc ON rc.constraint_name = tc.constraint_name
		WHERE tc.table_schema = ? 
		  AND tc.table_name = ? 
		  AND tc.constraint_type = 'FOREIGN KEY'
		GROUP BY tc.constraint_name, ccu.table_schema, ccu.table_name, rc.delete_rule, rc.update_rule
	`
	fkRows, err := a.Conn.Query(query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer fkRows.Close()
	for fkRows.Next() {
		var fk conn.ForeignKey
		var columns, referencedColumns string
		if err := fkRows.Scan(
			&fk.Name,
			&columns,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&referencedColumns,
			&fk.OnDelete,
			&fk.OnUpdate,
		); err != nil {
			return err
		}
		fk.Columns = strings.Split(columns, ",")
		fk.ReferencedColumns = strings.Split(referencedColumns, ",")

		table.ForeignKeys[fk.Name] = &fk
	}
	return nil
}

func (a *MySQLAdapter) extractViewDefinition(table *conn.Table) error {
	query := `
		SELECT 
			view_definition,
			is_updatable,
			check_option
		FROM information_schema.views
		WHERE table_schema = ? AND table_name = ?
	`

	var viewDef conn.ViewDefinition
	var isUpdatable, checkOption sql.NullString

	err := a.Conn.QueryRow(query, table.Schema, table.Name).Scan(
		&viewDef.SelectStatement,
		&isUpdatable,
		&checkOption,
	)
	if err != nil {
		return err
	}

	viewDef.IsUpdatable = isUpdatable.String == "YES"
	if checkOption.Valid {
		viewDef.CheckOption = checkOption.String
	}

	table.ViewDefinition = &viewDef

	return nil
}

func (a *MySQLAdapter) getTableComment(schemaName, tableName string) string {
	query := `
		SELECT table_comment
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?
	`

	var comment sql.NullString
	err := a.Conn.QueryRow(query, schemaName, tableName).Scan(&comment)
	if err != nil {
		return ""
	}
	if comment.Valid {
		return comment.String
	}
	return ""
}
