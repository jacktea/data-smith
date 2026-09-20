package conn

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"

	"github.com/jacktea/data-smith/pkg/config"
)

const (
	DBTypeMySQL    = "mysql"
	DBTypePostgres = "postgres"
)

type TableType string

const (
	TableTypeTable   TableType = "TABLE"
	TableTypeView    TableType = "VIEW"
	TableTypeUnknown TableType = "UNKNOWN"
)

func ParseTableType(t string) TableType {
	switch t {
	case "BASE TABLE", "TABLE":
		return TableTypeTable
	case "VIEW", "MATERIALIZED VIEW":
		return TableTypeView
	default:
		return TableTypeUnknown
	}
}

// ErrTableNotFound is returned by DBAdapter.ExtractTable when the requested
// table does not exist in the configured schema. Callers use errors.Is to
// distinguish a missing table from extraction failures.
var ErrTableNotFound = errors.New("table not found")

type DatabaseSchema struct {
	Tables map[string]*Table
	// Routines 以身份签名 name(identity_args) 为键；Sequences 以名称为键。
	// 两者由支持例程/序列的驱动（当前仅 PostgreSQL）填充，其余驱动为空映射。
	Routines  map[string]*Routine
	Sequences map[string]*Sequence
}

// SchemaObjectKind identifies an object category that participates in schema
// dependency ordering. It intentionally covers only object kinds DataSmith can
// currently extract and reproduce.
type SchemaObjectKind string

const (
	SchemaObjectTable    SchemaObjectKind = "table"
	SchemaObjectView     SchemaObjectKind = "view"
	SchemaObjectRoutine  SchemaObjectKind = "routine"
	SchemaObjectSequence SchemaObjectKind = "sequence"
)

// SchemaObjectRef names one prerequisite of a schema object. Routine Name is
// its identity signature (name(identity_args)); all other names are bare names.
type SchemaObjectRef struct {
	Kind   SchemaObjectKind
	Schema string
	Name   string
}

func (s *DatabaseSchema) GetTable(name string) *Table {
	return s.Tables[name]
}

type Table struct {
	Name           string
	Type           TableType
	Schema         string
	Comment        string
	Columns        map[string]*Column
	Indexes        map[string]*Index
	PrimaryKey     *PrimaryKey
	ForeignKeys    map[string]*ForeignKey
	Checks         map[string]*CheckConstraint
	ViewDefinition *ViewDefinition `json:"view_definition,omitempty"`
	Dependencies   []SchemaObjectRef
}

func (t *Table) GetColumn(name string) *Column {
	return t.Columns[name]
}

func (t *Table) GetColumns() []string {
	cols := make([]string, 0, len(t.Columns))
	for col := range t.Columns {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

func (t *Table) GetColumnsByPosition() []*Column {
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].Position != cols[j].Position {
			return cols[i].Position < cols[j].Position
		}
		return cols[i].Name < cols[j].Name
	})
	return cols
}

func (t *Table) GetPrimaryKeyColumns() []string {
	if t.PrimaryKey == nil {
		return nil
	}
	return t.PrimaryKey.Columns
}

func (t *Table) GetIndex(name string) *Index {
	return t.Indexes[name]
}

type Column struct {
	Name         string
	DataType     string
	Nullable     bool
	Default      *string
	Extra        string // 如 auto_increment
	Comment      *string
	CharMaxLen   *int // 字符类型最大长度
	NumericPrec  *int // 数值精度
	NumericScale *int // 数值标度
	Position     int  // 列在表中的位置
}

type Index struct {
	Name       string
	Columns    []string
	Unique     bool
	Primary    bool
	Method     string  // btree, hash, gin, gist等
	Where      *string // 部分索引的WHERE条件
	Expression *string // 表达式索引
	Definition string  // 驱动提取的完整定义，仅用于无损重建
}

type PrimaryKey struct {
	Name    string
	Columns []string
}

type ForeignKey struct {
	Name              string
	Columns           []string
	ReferencedSchema  string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string // CASCADE, RESTRICT, SET NULL等
	OnUpdate          string
}

// CheckConstraint 是表级 CHECK 约束。Definition 保存方言原生的完整定义文本，
// 比对只做空白规范化、不做表达式语义改写：PostgreSQL 为 pg_get_constraintdef
// 输出（形如 "CHECK ((expr))"，未验证约束带 NOT VALID 后缀）；MySQL 为
// information_schema.check_constraints.check_clause 裸表达式（各驱动的生成器
// 按各自口径拼接）。约束从属于表，随表增删与 include/exclude 表过滤一起生效。
type CheckConstraint struct {
	Name       string
	Definition string
}

type ViewDefinition struct {
	// 视图的SQL查询语句
	SelectStatement string `json:"select_statement"`

	// 视图依赖的表或其他视图
	Dependencies []string `json:"dependencies,omitempty"`

	// 视图是否可更新
	IsUpdatable bool `json:"is_updatable,omitempty"`

	// 视图的安全模式 (DEFINER/INVOKER)
	SecurityType string `json:"security_type,omitempty"`

	// 视图的定义者
	Definer string `json:"definer,omitempty"`

	// 视图的检查选项 (NONE/LOCAL/CASCADED)
	CheckOption string `json:"check_option,omitempty"`

	// 视图注释
	Comment string `json:"comment,omitempty"`
}

type Record map[string]any

// RoutineKind 区分 PostgreSQL 例程类别：普通函数（prokind='f'）与存储过程
// （prokind='p'）。聚合函数（prokind='a'）与窗口函数（prokind='w'）首版不支持，
// 提取时被排除（pg_get_functiondef 无法还原聚合定义）。
type RoutineKind string

const (
	RoutineKindFunction  RoutineKind = "function"
	RoutineKindProcedure RoutineKind = "procedure"
)

// Routine 是一个函数或存储过程。Definition 是 pg_get_functiondef 返回的完整
// CREATE OR REPLACE 语句（不含结尾分号）；IdentityArgs 是
// pg_get_function_identity_arguments 的结果，与 Name 一起构成重载下的唯一身份。
type Routine struct {
	Name         string
	Schema       string
	Kind         RoutineKind
	IdentityArgs string
	Definition   string
	Dependencies []SchemaObjectRef
}

// Identity 返回 name(identity_args) 形式的身份签名。
func (r *Routine) Identity() string {
	args := strings.TrimSpace(r.IdentityArgs)
	if args == "" {
		return r.Name + "()"
	}
	return r.Name + "(" + args + ")"
}

// Sequence 是一个 PostgreSQL 序列。数值字段以规范化的字符串形式保存
// （来自 pg_sequences），便于直接参与 DDL 生成与相等比较；LastValue 是易变
// 状态，不参与结构比对。OwnedBy 形如 "table.column"（SERIAL 等列拥有的
// 隐式序列），独立序列为空。
type Sequence struct {
	Name        string
	Schema      string
	DataType    string
	StartValue  string
	IncrementBy string
	MinValue    string
	MaxValue    string
	Cycle       bool
	CacheSize   string
	OwnedBy     string
}

func (s *Sequence) Equal(other *Sequence) bool {
	if s == nil || other == nil {
		return s == other
	}
	return s.DataType == other.DataType &&
		s.StartValue == other.StartValue &&
		s.IncrementBy == other.IncrementBy &&
		s.MinValue == other.MinValue &&
		s.MaxValue == other.MaxValue &&
		s.Cycle == other.Cycle &&
		s.CacheSize == other.CacheSize &&
		s.OwnedBy == other.OwnedBy
}

type DBAdapter interface {
	ReadSchema() (*DatabaseSchema, error)
	GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]Record, error)
	ExtractTable(tableName string) (*Table, error)
	ExtractView(viewName string) (*Table, error)
	GetConn() *sql.DB
	GetConfig() *config.ConnConfig
	Close() error
}

// ContextDBAdapter is an additive capability for cancellable long-running row
// reads. DBAdapter remains unchanged so existing implementations stay source
// compatible.
type ContextDBAdapter interface {
	DBAdapter
	GetTableDataBatchContext(ctx context.Context, table string, cols, pk []string, lastPK []any, limit int) ([]Record, error)
}

func GetTableDataBatchContext(ctx context.Context, db DBAdapter, table string, cols, pk []string, lastPK []any, limit int) ([]Record, error) {
	if ctx == nil {
		return nil, errors.New("query context is required")
	}
	if contextual, ok := db.(ContextDBAdapter); ok {
		return contextual.GetTableDataBatchContext(ctx, table, cols, pk, lastPK, limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return db.GetTableDataBatch(table, cols, pk, lastPK, limit)
}
