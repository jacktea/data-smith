package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db/base"
	"github.com/jacktea/data-smith/pkg/utils"

	"github.com/lib/pq"
)

type PostgresAdapter struct {
	base.BaseAdapter
}

func NewPostgresAdapter(cfg *config.ConnConfig) (*PostgresAdapter, error) {
	adapter := &PostgresAdapter{}
	if err := adapter.Init(cfg); err != nil {
		return nil, err
	}
	if !cfg.SSL {
		cfg.SetExtra("sslmode", "disable")
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName, cfg.ExtraString())

	db, err := sql.Open("postgres", connStr)
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
	tableSchema := cfg.TableSchema
	if tableSchema == "" {
		tableSchema = "public"
	}
	adapter.Conn = db
	adapter.Cfg.TableSchema = tableSchema
	return adapter, nil
}

func (a *PostgresAdapter) ReadSchema() (*conn.DatabaseSchema, error) {
	dbSchema := &conn.DatabaseSchema{Tables: map[string]*conn.Table{}}
	tables, err := a.queryTables()
	if err != nil {
		return nil, err
	}
	dbSchema.Tables = tables
	return dbSchema, nil
}

func (a *PostgresAdapter) GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	if len(pk) == 0 {
		return nil, fmt.Errorf("primary key required for batch scan")
	}
	// 构造 SELECT ... FROM table WHERE (pk) > (lastPK) ORDER BY pk LIMIT $N
	colList := utils.JoinWrap(cols, "\"", ", ")
	pkList := utils.JoinWrap(pk, "\"", ", ")
	orderBy := pkList
	where := ""
	var args []any
	argIdx := 1
	if len(lastPK) > 0 {
		where = "WHERE ("
		where += pkList
		where += ") > ("
		for i := range pk {
			if i > 0 {
				where += ", "
			}
			where += fmt.Sprintf("$%d", argIdx)
			args = append(args, lastPK[i])
			argIdx++
		}
		where += ")"
	}
	query := fmt.Sprintf("SELECT %s FROM \"%s\" %s ORDER BY %s LIMIT $%d", colList, table, where, orderBy, argIdx)
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

func (a *PostgresAdapter) ExtractTable(tableName string) (*conn.Table, error) {
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

func (a *PostgresAdapter) ExtractView(viewName string) (*conn.Table, error) {
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

func (a *PostgresAdapter) GetConn() *sql.DB {
	return a.Conn
}

func (a *PostgresAdapter) GetConfig() *config.ConnConfig {
	return a.Cfg
}

func (a *PostgresAdapter) queryTables() (map[string]*conn.Table, error) {
	rows, err := a.Conn.Query(`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = $1`, a.Cfg.TableSchema)
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

func (a *PostgresAdapter) extractColumns(table *conn.Table) error {
	colRows, err := a.Conn.Query(`SELECT
			c.column_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			pgd.description,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			ordinal_position
		FROM
			information_schema.columns c
			LEFT JOIN pg_catalog.pg_statio_all_tables as st ON c.table_name = st.relname
			LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid=st.relid AND pgd.objsubid=c.ordinal_position
		WHERE
			c.table_name = $1 AND c.table_schema = $2
		ORDER BY c.ordinal_position`, table.Name, table.Schema)
	if err != nil {
		return err
	}
	defer colRows.Close()
	columns := make(map[string]*conn.Column)
	for colRows.Next() {
		var col conn.Column
		var nullable string
		var charMaxLen, numericPrec, numericScale sql.NullInt64
		if err := colRows.Scan(
			&col.Name,
			&col.DataType,
			&nullable,
			&col.Default,
			&col.Comment,
			&charMaxLen,
			&numericPrec,
			&numericScale,
			&col.Position,
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
		col.Nullable = nullable == "YES"
		columns[col.Name] = &col
	}
	table.Columns = columns
	return nil
}

func (a *PostgresAdapter) extractPrimaryKey(table *conn.Table) error {
	query := `
		SELECT 
			tc.constraint_name,
			array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_schema = $1 
		  AND tc.table_name = $2 
		  AND tc.constraint_type = 'PRIMARY KEY'
		GROUP BY tc.constraint_name
	`
	var constraintName string
	var columns pq.StringArray
	err := a.Conn.QueryRow(query, table.Schema, table.Name).Scan(&constraintName, &columns)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	table.PrimaryKey = &conn.PrimaryKey{
		Name:    constraintName,
		Columns: columns,
	}
	return nil
}

func (a *PostgresAdapter) extractIndexes(table *conn.Table) error {
	// Indexes
	idxRows, err := a.Conn.Query(`
	SELECT 
			i.relname as index_name,
			ix.indisunique,
			ix.indisprimary,
			am.amname as method,
			pg_get_expr(ix.indpred, ix.indrelid) as where_clause,
			array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = $1 AND t.relname = $2
		GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname, ix.indpred, ix.indrelid
	`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var idx conn.Index
		var whereClause sql.NullString
		var columns pq.StringArray
		if err := idxRows.Scan(
			&idx.Name,
			&idx.Unique,
			&idx.Primary,
			&idx.Method,
			&whereClause,
			&columns,
		); err != nil {
			return err
		}
		idx.Columns = columns
		if whereClause.Valid {
			idx.Where = &whereClause.String
		}
		table.Indexes[idx.Name] = &idx
	}
	return nil
}

func (a *PostgresAdapter) extractForeignKeys(table *conn.Table) error {
	query := `
		SELECT 
			tc.constraint_name,
			array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
			ccu.table_schema as referenced_schema,
			ccu.table_name as referenced_table,
			array_agg(ccu.column_name ORDER BY kcu.ordinal_position) as referenced_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
		JOIN information_schema.referential_constraints rc ON rc.constraint_name = tc.constraint_name
		WHERE tc.table_schema = $1 
		  AND tc.table_name = $2 
		  AND tc.constraint_type = 'FOREIGN KEY'
		GROUP BY tc.constraint_name, ccu.table_schema, ccu.table_name, rc.delete_rule, rc.update_rule
	`
	// Foreign Keys
	fkRows, err := a.Conn.Query(query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer fkRows.Close()
	for fkRows.Next() {
		var fk conn.ForeignKey
		var columns, referencedColumns pq.StringArray
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
		fk.Columns = columns
		fk.ReferencedColumns = referencedColumns

		table.ForeignKeys[fk.Name] = &fk
	}
	return nil
}

func (p *PostgresAdapter) extractViewDefinition(table *conn.Table) error {
	query := `
		SELECT 
			view_definition,
			is_updatable,
			check_option
		FROM information_schema.views
		WHERE table_schema = $1 AND table_name = $2
	`

	var viewDef conn.ViewDefinition
	var isUpdatable, checkOption sql.NullString

	err := p.Conn.QueryRow(query, table.Schema, table.Name).Scan(
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

func (p *PostgresAdapter) getTableComment(schemaName, tableName string) string {
	query := `
		SELECT obj_description(pgc.oid)
		FROM pg_class pgc
		JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
		WHERE pgn.nspname = $1 AND pgc.relname = $2
	`

	var comment sql.NullString
	err := p.Conn.QueryRow(query, schemaName, tableName).Scan(&comment)
	if err != nil {
		return ""
	}
	if comment.Valid {
		return comment.String
	}
	return ""
}
