package postgres

import (
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
)

type TypeHandler func(col *conn.Column) string

type PostgreSQLTypeConverter struct {
	typeMap map[string]TypeHandler
}

// NewPostgreSQLTypeConverter 创建新的类型转换器
func NewPostgreSQLTypeConverter() *PostgreSQLTypeConverter {
	converter := &PostgreSQLTypeConverter{
		typeMap: make(map[string]TypeHandler),
	}
	converter.initTypeMap()
	return converter
}

// initTypeMap 初始化类型映射表
func (c *PostgreSQLTypeConverter) initTypeMap() {
	// 字符类型
	c.typeMap["character varying"] = c.handleVarchar
	c.typeMap["varchar"] = c.handleVarchar
	c.typeMap["character"] = c.handleChar
	c.typeMap["char"] = c.handleChar
	c.typeMap["text"] = c.handleText

	// 数值类型
	c.typeMap["integer"] = c.handleInteger
	c.typeMap["int"] = c.handleInteger
	c.typeMap["int4"] = c.handleInteger
	c.typeMap["bigint"] = c.handleBigint
	c.typeMap["int8"] = c.handleBigint
	c.typeMap["smallint"] = c.handleSmallint
	c.typeMap["int2"] = c.handleSmallint
	c.typeMap["numeric"] = c.handleNumeric
	c.typeMap["decimal"] = c.handleNumeric
	c.typeMap["real"] = c.handleReal
	c.typeMap["float4"] = c.handleReal
	c.typeMap["double precision"] = c.handleDoublePrecision
	c.typeMap["float8"] = c.handleDoublePrecision
	c.typeMap["money"] = c.handleMoney

	// 序列类型
	c.typeMap["serial"] = c.handleSerial
	c.typeMap["bigserial"] = c.handleBigserial
	c.typeMap["smallserial"] = c.handleSmallserial

	// 布尔类型
	c.typeMap["boolean"] = c.handleBoolean
	c.typeMap["bool"] = c.handleBoolean

	// 日期时间类型
	c.typeMap["timestamp"] = c.handleTimestamp
	c.typeMap["timestamp without time zone"] = c.handleTimestamp
	c.typeMap["timestamp with time zone"] = c.handleTimestampTz
	c.typeMap["timestamptz"] = c.handleTimestampTz
	c.typeMap["date"] = c.handleDate
	c.typeMap["time"] = c.handleTime
	c.typeMap["time without time zone"] = c.handleTime
	c.typeMap["time with time zone"] = c.handleTimeTz
	c.typeMap["timetz"] = c.handleTimeTz
	c.typeMap["interval"] = c.handleInterval

	// 二进制类型
	c.typeMap["bytea"] = c.handleBytea

	// UUID类型
	c.typeMap["uuid"] = c.handleUUID

	// JSON类型
	c.typeMap["json"] = c.handleJSON
	c.typeMap["jsonb"] = c.handleJSONB

	// 数组类型
	c.typeMap["ARRAY"] = c.handleArray

	// 网络地址类型
	c.typeMap["inet"] = c.handleInet
	c.typeMap["cidr"] = c.handleCidr
	c.typeMap["macaddr"] = c.handleMacaddr

	// 几何类型
	c.typeMap["point"] = c.handlePoint
	c.typeMap["line"] = c.handleLine
	c.typeMap["lseg"] = c.handleLseg
	c.typeMap["box"] = c.handleBox
	c.typeMap["path"] = c.handlePath
	c.typeMap["polygon"] = c.handlePolygon
	c.typeMap["circle"] = c.handleCircle
}

// 字符类型处理函数
func (c *PostgreSQLTypeConverter) handleVarchar(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("varchar(%d)", *col.CharMaxLen)
	}
	return "varchar"
}

func (c *PostgreSQLTypeConverter) handleChar(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("char(%d)", *col.CharMaxLen)
	}
	return "char(1)"
}

func (c *PostgreSQLTypeConverter) handleText(col *conn.Column) string {
	return "text"
}

// 数值类型处理函数
func (c *PostgreSQLTypeConverter) handleInteger(col *conn.Column) string {
	return "int4"
}

func (c *PostgreSQLTypeConverter) handleBigint(col *conn.Column) string {
	return "int8"
}

func (c *PostgreSQLTypeConverter) handleSmallint(col *conn.Column) string {
	return "int2"
}

func (c *PostgreSQLTypeConverter) handleNumeric(col *conn.Column) string {
	if col.NumericPrec != nil && col.NumericScale != nil {
		return fmt.Sprintf("numeric(%d,%d)", *col.NumericPrec, *col.NumericScale)
	} else if col.NumericPrec != nil {
		return fmt.Sprintf("numeric(%d)", *col.NumericPrec)
	}
	return "numeric"
}

func (c *PostgreSQLTypeConverter) handleReal(col *conn.Column) string {
	return "real"
}

func (c *PostgreSQLTypeConverter) handleDoublePrecision(col *conn.Column) string {
	return "double precision"
}

func (c *PostgreSQLTypeConverter) handleMoney(col *conn.Column) string {
	return "money"
}

// 序列类型处理函数
func (c *PostgreSQLTypeConverter) handleSerial(col *conn.Column) string {
	return "serial"
}

func (c *PostgreSQLTypeConverter) handleBigserial(col *conn.Column) string {
	return "bigserial"
}

func (c *PostgreSQLTypeConverter) handleSmallserial(col *conn.Column) string {
	return "smallserial"
}

// 布尔类型处理函数
func (c *PostgreSQLTypeConverter) handleBoolean(col *conn.Column) string {
	return "boolean"
}

// 日期时间类型处理函数
func (c *PostgreSQLTypeConverter) handleTimestamp(col *conn.Column) string {
	return "timestamp"
}

func (c *PostgreSQLTypeConverter) handleTimestampTz(col *conn.Column) string {
	return "timestamp with time zone"
}

func (c *PostgreSQLTypeConverter) handleDate(col *conn.Column) string {
	return "date"
}

func (c *PostgreSQLTypeConverter) handleTime(col *conn.Column) string {
	return "time"
}

func (c *PostgreSQLTypeConverter) handleTimeTz(col *conn.Column) string {
	return "time with time zone"
}

func (c *PostgreSQLTypeConverter) handleInterval(col *conn.Column) string {
	return "interval"
}

// 其他类型处理函数
func (c *PostgreSQLTypeConverter) handleBytea(col *conn.Column) string {
	return "bytea"
}

func (c *PostgreSQLTypeConverter) handleUUID(col *conn.Column) string {
	return "uuid"
}

func (c *PostgreSQLTypeConverter) handleJSON(col *conn.Column) string {
	return "json"
}

func (c *PostgreSQLTypeConverter) handleJSONB(col *conn.Column) string {
	return "jsonb"
}

func (c *PostgreSQLTypeConverter) handleArray(col *conn.Column) string {
	// 这里需要根据具体的数组类型来处理
	return "ARRAY"
}

func (c *PostgreSQLTypeConverter) handleInet(col *conn.Column) string {
	return "inet"
}

func (c *PostgreSQLTypeConverter) handleCidr(col *conn.Column) string {
	return "cidr"
}

func (c *PostgreSQLTypeConverter) handleMacaddr(col *conn.Column) string {
	return "macaddr"
}

// 几何类型处理函数
func (c *PostgreSQLTypeConverter) handlePoint(col *conn.Column) string {
	return "point"
}

func (c *PostgreSQLTypeConverter) handleLine(col *conn.Column) string {
	return "line"
}

func (c *PostgreSQLTypeConverter) handleLseg(col *conn.Column) string {
	return "lseg"
}

func (c *PostgreSQLTypeConverter) handleBox(col *conn.Column) string {
	return "box"
}

func (c *PostgreSQLTypeConverter) handlePath(col *conn.Column) string {
	return "path"
}

func (c *PostgreSQLTypeConverter) handlePolygon(col *conn.Column) string {
	return "polygon"
}

func (c *PostgreSQLTypeConverter) handleCircle(col *conn.Column) string {
	return "circle"
}

// ConvertType 转换数据类型
func (c *PostgreSQLTypeConverter) ConvertType(col *conn.Column) string {
	dataType := strings.ToLower(col.DataType)

	if handler, exists := c.typeMap[dataType]; exists {
		return handler(col)
	}

	// 如果找不到对应的处理器，返回原始类型
	return col.DataType
}

// GenerateColumnDDL 生成列的DDL语句
func (c *PostgreSQLTypeConverter) GenerateColumnDDL(col *conn.Column) string {
	var parts []string

	// 列名（加引号以处理特殊字符）
	parts = append(parts, fmt.Sprintf(`"%s"`, col.Name))

	// 数据类型
	dataType := c.ConvertType(col)
	parts = append(parts, dataType)

	// NULL约束
	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	}

	// 默认值
	if col.Default != nil && *col.Default != "" {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", *col.Default))
	}

	return strings.Join(parts, " ")
}
