package mysql

import (
	"fmt"
	"strings"

	"github.com/jacktea/data-smith/pkg/conn"
)

type TypeHandler func(col *conn.Column) string

type MySQLTypeConverter struct {
	typeMap map[string]TypeHandler
}

// NewMySQLTypeConverter 创建新的MySQL类型转换器
func NewMySQLTypeConverter() *MySQLTypeConverter {
	converter := &MySQLTypeConverter{
		typeMap: make(map[string]TypeHandler),
	}
	converter.initTypeMap()
	return converter
}

// initTypeMap 初始化MySQL类型映射表
func (c *MySQLTypeConverter) initTypeMap() {
	// 字符类型
	c.typeMap["varchar"] = c.handleVarchar
	c.typeMap["char"] = c.handleChar
	c.typeMap["text"] = c.handleText
	c.typeMap["tinytext"] = c.handleTinyText
	c.typeMap["mediumtext"] = c.handleMediumText
	c.typeMap["longtext"] = c.handleLongText

	// 数值类型
	c.typeMap["int"] = c.handleInt
	c.typeMap["integer"] = c.handleInt
	c.typeMap["bigint"] = c.handleBigint
	c.typeMap["smallint"] = c.handleSmallint
	c.typeMap["tinyint"] = c.handleTinyint
	c.typeMap["mediumint"] = c.handleMediumint
	c.typeMap["decimal"] = c.handleDecimal
	c.typeMap["numeric"] = c.handleDecimal
	c.typeMap["float"] = c.handleFloat
	c.typeMap["double"] = c.handleDouble
	c.typeMap["real"] = c.handleDouble

	// 序列类型
	c.typeMap["serial"] = c.handleSerial

	// 布尔类型
	c.typeMap["boolean"] = c.handleBoolean
	c.typeMap["bool"] = c.handleBoolean

	// 日期时间类型
	c.typeMap["datetime"] = c.handleDatetime
	c.typeMap["timestamp"] = c.handleTimestamp
	c.typeMap["date"] = c.handleDate
	c.typeMap["time"] = c.handleTime
	c.typeMap["year"] = c.handleYear

	// 二进制类型
	c.typeMap["blob"] = c.handleBlob
	c.typeMap["tinyblob"] = c.handleTinyBlob
	c.typeMap["mediumblob"] = c.handleMediumBlob
	c.typeMap["longblob"] = c.handleLongBlob
	c.typeMap["binary"] = c.handleBinary
	c.typeMap["varbinary"] = c.handleVarbinary

	// JSON类型
	c.typeMap["json"] = c.handleJSON

	// 枚举和集合类型
	c.typeMap["enum"] = c.handleEnum
	c.typeMap["set"] = c.handleSet

	// 空间数据类型
	c.typeMap["geometry"] = c.handleGeometry
	c.typeMap["point"] = c.handlePoint
	c.typeMap["linestring"] = c.handleLineString
	c.typeMap["polygon"] = c.handlePolygon
	c.typeMap["multipoint"] = c.handleMultiPoint
	c.typeMap["multilinestring"] = c.handleMultiLineString
	c.typeMap["multipolygon"] = c.handleMultiPolygon
	c.typeMap["geometrycollection"] = c.handleGeometryCollection
}

// 字符类型处理函数
func (c *MySQLTypeConverter) handleVarchar(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("varchar(%d)", *col.CharMaxLen)
	}
	return "varchar(255)"
}

func (c *MySQLTypeConverter) handleChar(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("char(%d)", *col.CharMaxLen)
	}
	return "char(1)"
}

func (c *MySQLTypeConverter) handleText(col *conn.Column) string {
	return "text"
}

func (c *MySQLTypeConverter) handleTinyText(col *conn.Column) string {
	return "tinytext"
}

func (c *MySQLTypeConverter) handleMediumText(col *conn.Column) string {
	return "mediumtext"
}

func (c *MySQLTypeConverter) handleLongText(col *conn.Column) string {
	return "longtext"
}

// 数值类型处理函数
func (c *MySQLTypeConverter) handleInt(col *conn.Column) string {
	return "int"
}

func (c *MySQLTypeConverter) handleBigint(col *conn.Column) string {
	return "bigint"
}

func (c *MySQLTypeConverter) handleSmallint(col *conn.Column) string {
	return "smallint"
}

func (c *MySQLTypeConverter) handleTinyint(col *conn.Column) string {
	return "tinyint"
}

func (c *MySQLTypeConverter) handleMediumint(col *conn.Column) string {
	return "mediumint"
}

func (c *MySQLTypeConverter) handleDecimal(col *conn.Column) string {
	if col.NumericPrec != nil && col.NumericScale != nil {
		return fmt.Sprintf("decimal(%d,%d)", *col.NumericPrec, *col.NumericScale)
	} else if col.NumericPrec != nil {
		return fmt.Sprintf("decimal(%d)", *col.NumericPrec)
	}
	return "decimal(10,0)"
}

func (c *MySQLTypeConverter) handleFloat(col *conn.Column) string {
	if col.NumericPrec != nil && col.NumericScale != nil {
		return fmt.Sprintf("float(%d,%d)", *col.NumericPrec, *col.NumericScale)
	}
	return "float"
}

func (c *MySQLTypeConverter) handleDouble(col *conn.Column) string {
	if col.NumericPrec != nil && col.NumericScale != nil {
		return fmt.Sprintf("double(%d,%d)", *col.NumericPrec, *col.NumericScale)
	}
	return "double"
}

// 序列类型处理函数
func (c *MySQLTypeConverter) handleSerial(col *conn.Column) string {
	return "bigint unsigned NOT NULL AUTO_INCREMENT"
}

// 布尔类型处理函数
func (c *MySQLTypeConverter) handleBoolean(col *conn.Column) string {
	return "tinyint(1)"
}

// 日期时间类型处理函数
func (c *MySQLTypeConverter) handleDatetime(col *conn.Column) string {
	return "datetime"
}

func (c *MySQLTypeConverter) handleTimestamp(col *conn.Column) string {
	return "timestamp"
}

func (c *MySQLTypeConverter) handleDate(col *conn.Column) string {
	return "date"
}

func (c *MySQLTypeConverter) handleTime(col *conn.Column) string {
	return "time"
}

func (c *MySQLTypeConverter) handleYear(col *conn.Column) string {
	return "year"
}

// 二进制类型处理函数
func (c *MySQLTypeConverter) handleBlob(col *conn.Column) string {
	return "blob"
}

func (c *MySQLTypeConverter) handleTinyBlob(col *conn.Column) string {
	return "tinyblob"
}

func (c *MySQLTypeConverter) handleMediumBlob(col *conn.Column) string {
	return "mediumblob"
}

func (c *MySQLTypeConverter) handleLongBlob(col *conn.Column) string {
	return "longblob"
}

func (c *MySQLTypeConverter) handleBinary(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("binary(%d)", *col.CharMaxLen)
	}
	return "binary(1)"
}

func (c *MySQLTypeConverter) handleVarbinary(col *conn.Column) string {
	if col.CharMaxLen != nil && *col.CharMaxLen > 0 {
		return fmt.Sprintf("varbinary(%d)", *col.CharMaxLen)
	}
	return "varbinary(255)"
}

// JSON类型处理函数
func (c *MySQLTypeConverter) handleJSON(col *conn.Column) string {
	return "json"
}

// 枚举和集合类型处理函数
func (c *MySQLTypeConverter) handleEnum(col *conn.Column) string {
	// 这里需要从Extra字段解析枚举值
	if col.Extra != "" && strings.HasPrefix(col.Extra, "enum(") {
		return fmt.Sprintf("enum%s", col.Extra[5:]) // 去掉"enum("前缀
	}
	return "enum"
}

func (c *MySQLTypeConverter) handleSet(col *conn.Column) string {
	// 这里需要从Extra字段解析集合值
	if col.Extra != "" && strings.HasPrefix(col.Extra, "set(") {
		return fmt.Sprintf("set%s", col.Extra[4:]) // 去掉"set("前缀
	}
	return "set"
}

// 空间数据类型处理函数
func (c *MySQLTypeConverter) handleGeometry(col *conn.Column) string {
	return "geometry"
}

func (c *MySQLTypeConverter) handlePoint(col *conn.Column) string {
	return "point"
}

func (c *MySQLTypeConverter) handleLineString(col *conn.Column) string {
	return "linestring"
}

func (c *MySQLTypeConverter) handlePolygon(col *conn.Column) string {
	return "polygon"
}

func (c *MySQLTypeConverter) handleMultiPoint(col *conn.Column) string {
	return "multipoint"
}

func (c *MySQLTypeConverter) handleMultiLineString(col *conn.Column) string {
	return "multilinestring"
}

func (c *MySQLTypeConverter) handleMultiPolygon(col *conn.Column) string {
	return "multipolygon"
}

func (c *MySQLTypeConverter) handleGeometryCollection(col *conn.Column) string {
	return "geometrycollection"
}

// ConvertType 转换数据类型
func (c *MySQLTypeConverter) ConvertType(col *conn.Column) string {
	dataType := strings.ToLower(col.DataType)

	if handler, exists := c.typeMap[dataType]; exists {
		return handler(col)
	}

	// 如果找不到对应的处理器，返回原始类型
	return col.DataType
}

// GenerateColumnType 生成列的数据类型（不包含约束）
func (c *MySQLTypeConverter) GenerateColumnType(col *conn.Column) string {
	return c.ConvertType(col)
}

// GenerateColumnDDL 生成列的DDL语句
func (c *MySQLTypeConverter) GenerateColumnDDL(col *conn.Column) string {
	var parts []string

	// 列名（加反引号以处理特殊字符）
	parts = append(parts, fmt.Sprintf("`%s`", col.Name))

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

	// AUTO_INCREMENT
	if strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
		parts = append(parts, "AUTO_INCREMENT")
	}

	// 其他Extra信息（如UNSIGNED、ZEROFILL等）
	if col.Extra != "" && !strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
		parts = append(parts, col.Extra)
	}

	return strings.Join(parts, " ")
}
