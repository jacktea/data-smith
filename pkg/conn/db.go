package conn

import (
	"database/sql"
	"sort"

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

type DatabaseSchema struct {
	Tables map[string]*Table
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
	ViewDefinition *ViewDefinition `json:"view_definition,omitempty"`
}

func (t *Table) GetColumn(name string) *Column {
	return t.Columns[name]
}

func (t *Table) GetColumns() []string {
	cols := make([]string, 0, len(t.Columns))
	for col := range t.Columns {
		cols = append(cols, col)
	}
	return cols
}

func (t *Table) GetColumnsByPosition() []*Column {
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Position < cols[j].Position
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

type DBAdapter interface {
	ReadSchema() (*DatabaseSchema, error)
	GetTableDataBatch(table string, cols, pk []string, lastPK []any, limit int) ([]Record, error)
	ExtractTable(tableName string) (*Table, error)
	ExtractView(viewName string) (*Table, error)
	GetConn() *sql.DB
	GetConfig() *config.ConnConfig
	Close() error
}
