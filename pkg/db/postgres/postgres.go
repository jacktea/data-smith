package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/jacktea/data-smith/pkg/chunk"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db/base"
	"github.com/jacktea/data-smith/pkg/sql/ident"

	"github.com/lib/pq"
)

type PostgresAdapter struct {
	base.BaseAdapter
}

func (a *PostgresAdapter) quotedTable(table string) string {
	schema := a.Cfg.TableSchema
	if schema == "" {
		schema = "public"
	}
	return ident.Qualified(ident.DoubleQuote, schema, table)
}

func NewPostgresAdapter(cfg *config.ConnConfig) (*PostgresAdapter, error) {
	return NewPostgresAdapterContext(context.Background(), cfg)
}

func NewPostgresAdapterContext(ctx context.Context, cfg *config.ConnConfig) (*PostgresAdapter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("connection context is required")
	}
	working := cfg.Clone()
	if err := base.NormalizeConnectionSettings(working); err != nil {
		return nil, err
	}
	adapter := &PostgresAdapter{}
	if err := adapter.Init(working); err != nil {
		return nil, err
	}
	if !adapter.Cfg.SSL && !adapter.Cfg.ContainsExtra("sslmode") {
		adapter.Cfg.SetExtra("sslmode", "disable")
	}
	if adapter.Cfg.TableSchema == "" {
		adapter.Cfg.TableSchema = "public"
	}
	connStr := buildPostgresDSN(adapter.Cfg)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		adapter.Close()
		return nil, fmt.Errorf("open PostgreSQL database: %w", base.RedactError(err, adapter.Cfg))
	}
	base.ApplyConnectionSettings(db, adapter.Cfg)
	if err := base.PingWithRetry(ctx, db, adapter.Cfg); err != nil {
		_ = db.Close()
		adapter.Close()
		return nil, fmt.Errorf("connect to PostgreSQL database: %w", err)
	}
	adapter.Conn = db
	return adapter, nil
}

func buildPostgresDSN(cfg *config.ConnConfig) string {
	query := make(url.Values, len(cfg.Extra))
	for key, value := range cfg.Extra {
		query.Set(key, fmt.Sprint(value))
	}
	if cfg.TableSchema != "" && query.Get("search_path") == "" {
		// search_path is a PostgreSQL identifier list, not a plain string. Quote
		// the configured schema so mixed case, commas, spaces, and embedded quotes
		// retain their literal identifier meaning.
		query.Set("search_path", pq.QuoteIdentifier(cfg.TableSchema))
	}
	escapedDatabase := url.PathEscape(cfg.DBName)
	connectionURL := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port)),
		Path:     "/" + cfg.DBName,
		RawPath:  "/" + escapedDatabase,
		RawQuery: query.Encode(),
	}
	return connectionURL.String()
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
	return a.GetTableDataBatchContext(context.Background(), table, cols, pk, lastPK, limit)
}

func (a *PostgresAdapter) GetTableDataBatchContext(ctx context.Context, table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	if ctx == nil {
		return nil, fmt.Errorf("query context is required")
	}
	if err := validateBatchScanInputs(table, cols, pk, lastPK, limit); err != nil {
		return nil, err
	}
	// 构造 SELECT ... FROM table WHERE (pk) > (lastPK) ORDER BY pk LIMIT $N
	colList := ident.List(ident.DoubleQuote, cols, ", ")
	pkList := ident.List(ident.DoubleQuote, pk, ", ")
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
	query := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT $%d", colList, a.quotedTable(table), where, orderBy, argIdx)
	args = append(args, limit)
	rows, err := a.Conn.QueryContext(ctx, query, args...)
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
	return result, rows.Err()
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (a *PostgresAdapter) extractColumns(table *conn.Table) error {
	colRows, err := a.Conn.Query(`SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			c.is_nullable,
			c.column_default,
			pgd.description,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.ordinal_position
		FROM
			information_schema.columns c
			LEFT JOIN pg_catalog.pg_statio_all_tables as st ON c.table_name = st.relname AND c.table_schema = st.schemaname
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
		var dataType, udtName, nullable string
		var charMaxLen, numericPrec, numericScale sql.NullInt64
		if err := colRows.Scan(
			&col.Name,
			&dataType,
			&udtName,
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
		if dataType == "USER-DEFINED" && udtName != "" {
			col.DataType = udtName
		} else if dataType == "ARRAY" && udtName != "" {
			elemType := strings.TrimPrefix(udtName, "_")
			col.DataType = elemType + "[]"
		} else {
			col.DataType = dataType
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
	if err := colRows.Err(); err != nil {
		return err
	}
	table.Columns = columns
	return nil
}

func (a *PostgresAdapter) extractPrimaryKey(table *conn.Table) error {
	query := `
		SELECT 
			c.conname,
			array_agg(a.attname ORDER BY u.attpos) as columns
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, attpos) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
		WHERE n.nspname = $1 
		  AND t.relname = $2 
		  AND c.contype = 'p'
		GROUP BY c.conname
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
			pg_get_expr(ix.indexprs, ix.indrelid) as expr,
			pg_get_indexdef(ix.indexrelid) as index_def,
			COALESCE(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) FILTER (WHERE a.attname IS NOT NULL), '{}') as columns
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = $1 AND t.relname = $2
		GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname, ix.indpred, ix.indexprs, ix.indrelid, ix.indexrelid
	`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var idx conn.Index
		var whereClause, expr, indexDef sql.NullString
		var columns pq.StringArray
		if err := idxRows.Scan(
			&idx.Name,
			&idx.Unique,
			&idx.Primary,
			&idx.Method,
			&whereClause,
			&expr,
			&indexDef,
			&columns,
		); err != nil {
			return err
		}
		idx.Columns = columns
		if whereClause.Valid {
			idx.Where = &whereClause.String
		}
		if indexDef.Valid && indexDef.String != "" {
			idx.Expression = &indexDef.String
		} else if expr.Valid && expr.String != "" {
			idx.Expression = &expr.String
		}
		table.Indexes[idx.Name] = &idx
	}
	return idxRows.Err()
}

func (a *PostgresAdapter) extractForeignKeys(table *conn.Table) error {
	query := `
		SELECT 
			c.conname,
			array_agg(a.attname ORDER BY u.attpos) as columns,
			ref_ns.nspname as referenced_schema,
			ref_cls.relname as referenced_table,
			array_agg(ref_a.attname ORDER BY u.attpos) as referenced_columns,
			CASE c.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END as on_delete,
			CASE c.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END as on_update
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_class ref_cls ON ref_cls.oid = c.confrelid
		JOIN pg_namespace ref_ns ON ref_ns.oid = ref_cls.relnamespace
		JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS u(attnum, confattnum, attpos) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
		JOIN pg_attribute ref_a ON ref_a.attrelid = ref_cls.oid AND ref_a.attnum = u.confattnum
		WHERE n.nspname = $1 
		  AND t.relname = $2 
		  AND c.contype = 'f'
		GROUP BY c.conname, ref_ns.nspname, ref_cls.relname, c.confdeltype, c.confupdtype
	`
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
	return fkRows.Err()
}

func (p *PostgresAdapter) extractViewDefinition(table *conn.Table) error {
	query := `
		SELECT 
			pg_get_viewdef(c.oid, true),
			v.is_updatable,
			v.check_option
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN information_schema.views v ON v.table_schema = n.nspname AND v.table_name = c.relname
		WHERE n.nspname = $1 AND c.relname = $2
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
	dependencyRows, err := p.Conn.Query(`
		SELECT table_schema, table_name
		FROM information_schema.view_table_usage
		WHERE view_schema = $1 AND view_name = $2
		ORDER BY table_schema, table_name`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer dependencyRows.Close()
	for dependencyRows.Next() {
		var schema, name string
		if err := dependencyRows.Scan(&schema, &name); err != nil {
			return err
		}
		viewDef.Dependencies = append(viewDef.Dependencies, schema+"."+name)
	}
	if err := dependencyRows.Err(); err != nil {
		return err
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

func (p *PostgresAdapter) GetChunkRanges(table string, pk string, chunkSize int) ([]chunk.ChunkRange, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" {
		return nil, fmt.Errorf("table and primary key are required for chunk ranges")
	}
	stats, err := p.GetChunkStats(table, pk)
	if err != nil {
		return nil, err
	}
	return p.GetChunkRangesWithStats(table, pk, chunkSize, stats)
}

func (p *PostgresAdapter) GetChunkRangesWithStats(table string, pk string, chunkSize int, stats chunk.ChunkStats) ([]chunk.ChunkRange, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" {
		return nil, fmt.Errorf("table and primary key are required for chunk ranges")
	}
	if stats.Count == 0 {
		return nil, nil
	}
	if stats.Count <= int64(chunkSize) {
		return []chunk.ChunkRange{
			{
				ChunkIndex: 0,
				MinPK:      nil,
				MaxPK:      nil,
				IsLast:     true,
			},
		}, nil
	}

	quotedPK := ident.Quote(ident.DoubleQuote, pk)
	var ranges []chunk.ChunkRange
	var lower any
	for index := 0; ; index++ {
		query := fmt.Sprintf("SELECT %s FROM %s", quotedPK, p.quotedTable(table))
		args := make([]any, 0, 2)
		if lower != nil {
			query += fmt.Sprintf(" WHERE %s >= $1", quotedPK)
			args = append(args, lower)
			query += fmt.Sprintf(" ORDER BY %s ASC LIMIT $2", quotedPK)
		} else {
			query += fmt.Sprintf(" ORDER BY %s ASC LIMIT $1", quotedPK)
		}
		args = append(args, chunkSize+1)
		rows, err := p.Conn.Query(query, args...)
		if err != nil {
			return nil, err
		}
		var count int
		var boundary any
		for rows.Next() {
			var point any
			if err := rows.Scan(&point); err != nil {
				_ = rows.Close()
				return nil, err
			}
			count++
			boundary = point
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil {
			return nil, rowErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if count == 0 {
			return nil, fmt.Errorf("chunk boundary query returned no rows for non-empty table %s", table)
		}
		isLast := count <= chunkSize
		var upper any
		if !isLast {
			upper = boundary
		}
		ranges = append(ranges, chunk.ChunkRange{ChunkIndex: index, MinPK: lower, MaxPK: upper, IsLast: isLast})
		if isLast {
			break
		}
		lower = boundary
	}
	return ranges, nil
}

func (p *PostgresAdapter) GetChunkStats(table string, pk string) (chunk.ChunkStats, error) {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" {
		return chunk.ChunkStats{}, fmt.Errorf("table and primary key are required for chunk stats")
	}
	quotedPK := ident.Quote(ident.DoubleQuote, pk)
	query := fmt.Sprintf("SELECT COUNT(*), MIN(%s), MAX(%s) FROM %s", quotedPK, quotedPK, p.quotedTable(table))
	var stats chunk.ChunkStats
	if err := p.Conn.QueryRow(query).Scan(&stats.Count, &stats.MinPK, &stats.MaxPK); err != nil {
		return chunk.ChunkStats{}, err
	}
	return stats, nil
}

func (p *PostgresAdapter) GetChunkHash(table string, cols []string, pk string, minPK, maxPK any, isLast bool) (string, error) {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" || len(cols) == 0 {
		return "", fmt.Errorf("table, columns, and primary key are required for chunk hash")
	}
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		if strings.TrimSpace(c) == "" {
			return "", fmt.Errorf("chunk hash column names must not be empty")
		}
		quoted := ident.Quote(ident.DoubleQuote, c)
		quotedCols[i] = fmt.Sprintf("CASE WHEN %s IS NULL THEN 'N' ELSE 'V' || octet_length(%s::text)::text || ':' || %s::text END", quoted, quoted, quoted)
	}
	concatExpr := strings.Join(quotedCols, " || '|' || ")

	var whereClause string
	var args []any
	quotedPK := ident.Quote(ident.DoubleQuote, pk)
	switch {
	case minPK == nil && maxPK == nil:
		whereClause = "TRUE"
	case minPK == nil:
		whereClause = fmt.Sprintf("%s < $1", quotedPK)
		args = append(args, maxPK)
	case maxPK == nil:
		whereClause = fmt.Sprintf("%s >= $1", quotedPK)
		args = append(args, minPK)
	default:
		whereClause = fmt.Sprintf("%s >= $1 AND %s < $2", quotedPK, quotedPK)
		args = append(args, minPK, maxPK)
	}

	query := fmt.Sprintf(`
		SELECT COALESCE(md5(string_agg(md5(row_data), '' ORDER BY %s)), '')
		FROM (
			SELECT (%s) AS row_data, %s
			FROM %s
			WHERE %s
		) t
	`, quotedPK, concatExpr, quotedPK, p.quotedTable(table), whereClause)

	var hash sql.NullString
	err := p.Conn.QueryRow(query, args...).Scan(&hash)
	if err != nil {
		return "", err
	}
	return hash.String, nil
}

func validateBatchScanInputs(table string, cols, pk []string, lastPK []any, limit int) error {
	if limit <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("table name is required for batch scan")
	}
	if len(cols) == 0 {
		return fmt.Errorf("at least one column is required for batch scan")
	}
	if len(pk) == 0 {
		return fmt.Errorf("primary key required for batch scan")
	}
	if len(lastPK) != 0 && len(lastPK) != len(pk) {
		return fmt.Errorf("last primary key value count must match primary key column count")
	}
	for _, name := range append(append([]string(nil), cols...), pk...) {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("column and primary key names must not be empty")
		}
	}
	return nil
}
