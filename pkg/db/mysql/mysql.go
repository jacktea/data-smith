package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/chunk"
	"github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/db/base"
	"github.com/jacktea/data-smith/pkg/sql/ident"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

type MySQLAdapter struct {
	base.BaseAdapter
}

func (a *MySQLAdapter) quotedTable(table string) string {
	return ident.Qualified(ident.Backtick, a.Cfg.TableSchema, table)
}

func NewMySQLAdapter(cfg *config.ConnConfig) (*MySQLAdapter, error) {
	return NewMySQLAdapterContext(context.Background(), cfg)
}

func NewMySQLAdapterContext(ctx context.Context, cfg *config.ConnConfig) (*MySQLAdapter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("connection context is required")
	}
	working := cfg.Clone()
	if err := base.NormalizeConnectionSettings(working); err != nil {
		return nil, err
	}
	adapter := &MySQLAdapter{}
	if err := adapter.Init(working); err != nil {
		return nil, err
	}
	connStr := buildMySQLDSN(adapter.Cfg)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		adapter.Close()
		return nil, fmt.Errorf("open MySQL database: %w", base.RedactError(err, adapter.Cfg))
	}
	base.ApplyConnectionSettings(db, adapter.Cfg)
	if err := base.PingWithRetry(ctx, db, adapter.Cfg); err != nil {
		_ = db.Close()
		adapter.Close()
		return nil, fmt.Errorf("connect to MySQL database: %w", err)
	}
	adapter.Conn = db
	adapter.Cfg.TableSchema = adapter.Cfg.DBName
	return adapter, nil
}

func buildMySQLDSN(cfg *config.ConnConfig) string {
	params := make(map[string]string, len(cfg.Extra)+1)
	params["charset"] = "utf8mb4"
	for key, value := range cfg.Extra {
		params[key] = fmt.Sprint(value)
	}
	driverConfig := mysqlDriver.NewConfig()
	driverConfig.User = cfg.User
	driverConfig.Passwd = cfg.Password
	driverConfig.Net = "tcp"
	driverConfig.Addr = net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	driverConfig.DBName = cfg.DBName
	driverConfig.Params = params
	driverConfig.ParseTime = true
	driverConfig.Loc = time.Local
	driverConfig.MultiStatements = true
	return driverConfig.FormatDSN()
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
	return a.GetTableDataBatchContext(context.Background(), table, cols, pk, lastPK, limit)
}

func (a *MySQLAdapter) GetTableDataBatchContext(ctx context.Context, table string, cols, pk []string, lastPK []any, limit int) ([]conn.Record, error) {
	if ctx == nil {
		return nil, fmt.Errorf("query context is required")
	}
	if err := validateBatchScanInputs(table, cols, pk, lastPK, limit); err != nil {
		return nil, err
	}
	// 构造 SELECT ... FROM table WHERE (pk) > (lastPK) ORDER BY pk LIMIT ?
	colList := ident.List(ident.Backtick, cols, ", ")
	pkList := ident.List(ident.Backtick, pk, ", ")
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
	query := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT ?", colList, a.quotedTable(table), where, orderBy)
	args = append(args, limit)
	rows, err := a.QueryContext(ctx, query, args...)
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

func (a *MySQLAdapter) ExtractTable(tableName string) (*conn.Table, error) {
	exists, err := a.baseTableExists(tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s.%s", conn.ErrTableNotFound, a.Cfg.TableSchema, tableName)
	}
	table := &conn.Table{
		Name:        tableName,
		Type:        conn.TableTypeTable,
		Schema:      a.Cfg.TableSchema,
		Columns:     map[string]*conn.Column{},
		Indexes:     map[string]*conn.Index{},
		ForeignKeys: map[string]*conn.ForeignKey{},
	}
	// 解析列
	err = a.extractColumns(table)
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

	// 解析 CHECK 约束
	err = a.extractChecks(table)
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

	// MySQL 不支持视图注释：information_schema.tables 的 TABLE_COMMENT 对视图
	// 恒为常量 'VIEW'，不提取，避免进入注释比对产生伪差异（C12）。
	return view, nil
}

// baseTableExists 判定目标 schema 下是否存在同名基础表。缺失表必须显式报错，
// 而不是返回零列空模型让上层误报「缺少主键」。
func (a *MySQLAdapter) baseTableExists(tableName string) (bool, error) {
	var found bool
	err := a.QueryRow(context.Background(),
		`SELECT COUNT(*) > 0 FROM information_schema.tables
		 WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'`,
		a.Cfg.TableSchema, tableName).Scan(&found)
	if err != nil {
		return false, err
	}
	return found, nil
}

func (a *MySQLAdapter) GetConn() *sql.DB {
	return a.Conn
}

func (a *MySQLAdapter) GetConfig() *config.ConnConfig {
	return a.Cfg
}

func (a *MySQLAdapter) queryTables() (map[string]*conn.Table, error) {
	rows, err := a.QueryContext(context.Background(), `SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = ?`, a.Cfg.TableSchema)
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

func (a *MySQLAdapter) extractColumns(table *conn.Table) error {
	colRows, err := a.QueryContext(context.Background(), `SELECT
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
	if err := colRows.Err(); err != nil {
		return err
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
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_schema = kcu.constraint_schema
		 AND tc.table_schema = kcu.table_schema
		 AND tc.table_name = kcu.table_name
		 AND tc.constraint_name = kcu.constraint_name
		WHERE tc.table_schema = ? 
		  AND tc.table_name = ? 
		  AND tc.constraint_type = 'PRIMARY KEY'
		GROUP BY tc.constraint_schema, tc.table_schema, tc.table_name, tc.constraint_name
	`
	var constraintName, columns string
	err := a.QueryRow(context.Background(), query, table.Schema, table.Name).Scan(&constraintName, &columns)
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
	idxRows, err := a.QueryContext(context.Background(), query, table.Schema, table.Name)
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
	return idxRows.Err()
}

func (a *MySQLAdapter) extractForeignKeys(table *conn.Table) error {
	query := `
		SELECT 
			tc.constraint_name,
			GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
			kcu.referenced_table_schema as referenced_schema,
			kcu.referenced_table_name as referenced_table,
			GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position) as referenced_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_schema = kcu.constraint_schema
		 AND tc.table_schema = kcu.table_schema
		 AND tc.table_name = kcu.table_name
		 AND tc.constraint_name = kcu.constraint_name
		JOIN information_schema.referential_constraints rc
		  ON tc.constraint_schema = rc.constraint_schema
		 AND tc.table_name = rc.table_name
		 AND tc.constraint_name = rc.constraint_name
		WHERE tc.table_schema = ? 
		  AND tc.table_name = ? 
		  AND tc.constraint_type = 'FOREIGN KEY'
		GROUP BY tc.constraint_name, kcu.referenced_table_schema, kcu.referenced_table_name, rc.delete_rule, rc.update_rule
	`
	fkRows, err := a.QueryContext(context.Background(), query, table.Schema, table.Name)
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
	return fkRows.Err()
}

// extractChecks 提取表级 CHECK 约束（MySQL 8.0.16+ 落地执行，更早版本仅解析）。
// check_constraints.check_clause 是裸表达式（无 CHECK 包裹），生成层负责补
// "CHECK (...)"；约束从属于所在表，随表过滤一致生效。
func (a *MySQLAdapter) extractChecks(table *conn.Table) error {
	query := `
		SELECT
			tc.constraint_name,
			cc.check_clause
		FROM information_schema.table_constraints tc
		JOIN information_schema.check_constraints cc
		  ON tc.constraint_schema = cc.constraint_schema
		 AND tc.constraint_name = cc.constraint_name
		WHERE tc.table_schema = ?
		  AND tc.table_name = ?
		  AND tc.constraint_type = 'CHECK'
		ORDER BY tc.constraint_name
	`
	rows, err := a.QueryContext(context.Background(), query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()
	checks := make(map[string]*conn.CheckConstraint)
	for rows.Next() {
		var chk conn.CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		checks[chk.Name] = &chk
	}
	if err := rows.Err(); err != nil {
		return err
	}
	table.Checks = checks
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

	err := a.QueryRow(context.Background(), query, table.Schema, table.Name).Scan(
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
	dependencyRows, err := a.QueryContext(context.Background(), `
		SELECT table_schema, table_name
		FROM information_schema.view_table_usage
		WHERE view_schema = ? AND view_name = ?
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

func (a *MySQLAdapter) getTableComment(schemaName, tableName string) string {
	query := `
		SELECT table_comment
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?
	`

	var comment sql.NullString
	err := a.QueryRow(context.Background(), query, schemaName, tableName).Scan(&comment)
	if err != nil {
		return ""
	}
	if comment.Valid {
		return comment.String
	}
	return ""
}

func (a *MySQLAdapter) GetChunkRanges(table string, pk string, chunkSize int) ([]chunk.ChunkRange, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" {
		return nil, fmt.Errorf("table and primary key are required for chunk ranges")
	}
	stats, err := a.GetChunkStats(table, pk)
	if err != nil {
		return nil, err
	}
	return a.GetChunkRangesWithStats(table, pk, chunkSize, stats)
}

func (a *MySQLAdapter) GetChunkRangesWithStats(table string, pk string, chunkSize int, stats chunk.ChunkStats) ([]chunk.ChunkRange, error) {
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

	quotedPK := ident.Quote(ident.Backtick, pk)
	var ranges []chunk.ChunkRange
	var lower any
	for index := 0; ; index++ {
		query := fmt.Sprintf("SELECT %s FROM %s", quotedPK, a.quotedTable(table))
		args := make([]any, 0, 2)
		if lower != nil {
			query += fmt.Sprintf(" WHERE %s >= ?", quotedPK)
			args = append(args, lower)
		}
		query += fmt.Sprintf(" ORDER BY %s ASC LIMIT ?", quotedPK)
		args = append(args, chunkSize+1)
		rows, err := a.QueryContext(context.Background(), query, args...)
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

func (a *MySQLAdapter) GetChunkStats(table string, pk string) (chunk.ChunkStats, error) {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" {
		return chunk.ChunkStats{}, fmt.Errorf("table and primary key are required for chunk stats")
	}
	quotedPK := ident.Quote(ident.Backtick, pk)
	query := fmt.Sprintf("SELECT COUNT(*), MIN(%s), MAX(%s) FROM %s", quotedPK, quotedPK, a.quotedTable(table))
	var stats chunk.ChunkStats
	if err := a.QueryRow(context.Background(), query).Scan(&stats.Count, &stats.MinPK, &stats.MaxPK); err != nil {
		return chunk.ChunkStats{}, err
	}
	return stats, nil
}

func (a *MySQLAdapter) GetChunkHash(table string, cols []string, pk string, minPK, maxPK any, isLast bool) (string, error) {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(pk) == "" || len(cols) == 0 {
		return "", fmt.Errorf("table, columns, and primary key are required for chunk hash")
	}
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		if strings.TrimSpace(c) == "" {
			return "", fmt.Errorf("chunk hash column names must not be empty")
		}
		quoted := ident.Quote(ident.Backtick, c)
		quotedCols[i] = fmt.Sprintf("IF(%s IS NULL, 'N', CONCAT('V', LENGTH(CAST(%s AS CHAR)), ':', CAST(%s AS CHAR)))", quoted, quoted, quoted)
	}
	concatExpr := fmt.Sprintf("CONCAT(%s)", strings.Join(quotedCols, ", '|', "))

	var whereClause string
	var args []any
	quotedPK := ident.Quote(ident.Backtick, pk)
	switch {
	case minPK == nil && maxPK == nil:
		whereClause = "1 = 1"
	case minPK == nil:
		whereClause = fmt.Sprintf("%s < ?", quotedPK)
		args = append(args, maxPK)
	case maxPK == nil:
		whereClause = fmt.Sprintf("%s >= ?", quotedPK)
		args = append(args, minPK)
	default:
		whereClause = fmt.Sprintf("%s >= ? AND %s < ?", quotedPK, quotedPK)
		args = append(args, minPK, maxPK)
	}

	query := fmt.Sprintf("SELECT COALESCE(HEX(BIT_XOR(CAST(CRC32(%s) AS UNSIGNED))), '0') FROM %s WHERE %s", concatExpr, a.quotedTable(table), whereClause)

	var hash sql.NullString
	err := a.QueryRow(context.Background(), query, args...).Scan(&hash)
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
