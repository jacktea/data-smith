package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// CompareValues 比较两个字段值的大小，返回 -1 (a < b), 0 (a == b), 1 (a > b)
// 能够根据类型或数据特征识别数值、时间、布尔与字符串，防止字典序比较导致的排序错位
func CompareValues(valA, valB any, colType string) int {
	if valA == nil && valB == nil {
		return 0
	}
	if valA == nil {
		return -1
	}
	if valB == nil {
		return 1
	}

	normA := normalizeBytesToString(valA)
	normB := normalizeBytesToString(valB)
	lowerType := strings.ToLower(colType)

	// 1. 真正的二进制字段按字节字典序比对。数据库驱动常用 []byte
	// 返回 numeric/decimal，不能在类型感知比较前走二进制分支。
	if bA, okA := valA.([]byte); okA {
		if bB, okB := valB.([]byte); okB {
			if !isNumericColType(lowerType) && !isTimeColType(lowerType) && !isBoolColType(lowerType) {
				return bytes.Compare(bA, bB)
			}
		}
	}

	// 2. 数值类型比对。整数和 decimal/numeric 保持精度；只有
	// float/double/real 使用 float64 容差。
	if cmp, ok := compareNumericValues(normA, normB, lowerType); ok {
		return cmp
	}

	// 3. 时间类型比对
	isTimeType := isTimeColType(lowerType)
	tA, isTimeA := tryToTime(normA)
	tB, isTimeB := tryToTime(normB)
	if (isTimeType && isTimeA && isTimeB) || (isTimeA && isTimeB) {
		unixA := tA.UTC().UnixNano()
		unixB := tB.UTC().UnixNano()
		if unixA < unixB {
			return -1
		} else if unixA > unixB {
			return 1
		}
		return 0
	}

	// 4. 布尔类型比对
	if isBoolColType(lowerType) {
		boolA, isBoolA := tryToBool(normA)
		boolB, isBoolB := tryToBool(normB)
		if isBoolA && isBoolB {
			if boolA == boolB {
				return 0
			}
			if !boolA && boolB {
				return -1
			}
			return 1
		}
	}

	// 5. 字符串兜底比对
	strA := fmt.Sprintf("%v", normA)
	strB := fmt.Sprintf("%v", normB)
	if strA < strB {
		return -1
	} else if strA > strB {
		return 1
	}
	return 0
}

// AreValuesEqual 判定两个值是否语义相等
// 支持 JSON 结构反序列化深度比对、浮点数容差比对、时间纳秒对齐等
func AreValuesEqual(valA, valB any, colType string) bool {
	if valA == nil && valB == nil {
		return true
	}
	if valA == nil || valB == nil {
		return false
	}

	lowerType := strings.ToLower(colType)

	// 1. JSON / JSONB 语义比较
	if strings.Contains(lowerType, "json") {
		if equalJSON(valA, valB) {
			return true
		}
	}

	// 2. 二进制比对
	bA, okA := valA.([]byte)
	bB, okB := valB.([]byte)
	if okA && okB && !strings.Contains(lowerType, "char") && !strings.Contains(lowerType, "text") && !strings.Contains(lowerType, "json") && !isNumericColType(lowerType) && !isTimeColType(lowerType) && !isBoolColType(lowerType) {
		return bytes.Equal(bA, bB)
	}

	// 3. 数值比较（消除 1.50 与 1.5 的字符串表示差异，同时避免
	// BIGINT 和高精度 decimal 被降为 float64）。
	normA := normalizeBytesToString(valA)
	normB := normalizeBytesToString(valB)
	if cmp, ok := compareNumericValues(normA, normB, lowerType); ok {
		return cmp == 0
	}

	// 4. 时间比较
	tA, isTimeA := tryToTime(normA)
	tB, isTimeB := tryToTime(normB)
	if isTimeA && isTimeB && (isTimeColType(lowerType) || isTimeTypeObject(normA)) {
		return tA.UTC().UnixNano() == tB.UTC().UnixNano()
	}

	// 5. 布尔比较
	if isBoolColType(lowerType) {
		boolA, isBoolA := tryToBool(normA)
		boolB, isBoolB := tryToBool(normB)
		if isBoolA && isBoolB {
			return boolA == boolB
		}
	}

	// 6. 默认通用比对
	return CompareValues(valA, valB, colType) == 0
}

func normalizeBytesToString(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func isNumericColType(t string) bool {
	return numericColumnKind(t) != numericKindNone
}

type numericKind uint8

const (
	numericKindNone numericKind = iota
	numericKindInteger
	numericKindDecimal
	numericKindFloat
)

func numericColumnKind(t string) numericKind {
	base := strings.TrimSpace(t)
	if fields := strings.Fields(base); len(fields) > 0 {
		base = fields[0]
	}
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	switch {
	case strings.Contains(base, "float"), base == "double", base == "real":
		return numericKindFloat
	case base == "decimal", base == "numeric", base == "number":
		return numericKindDecimal
	case base == "integer", strings.HasSuffix(base, "int"), strings.HasSuffix(base, "serial"):
		return numericKindInteger
	default:
		return numericKindNone
	}
}

func compareNumericValues(a, b any, colType string) (int, bool) {
	kind := numericColumnKind(colType)
	if kind == numericKindNone && isNumericGoType(a) && isNumericGoType(b) {
		kind = numericKindInteger
		if isFloatGoType(a) || isFloatGoType(b) {
			kind = numericKindFloat
		}
	}

	switch kind {
	case numericKindInteger:
		aInt, okA := tryToBigInt(a)
		bInt, okB := tryToBigInt(b)
		if okA && okB {
			return aInt.Cmp(bInt), true
		}
	case numericKindDecimal:
		return compareExactDecimals(a, b)
	case numericKindFloat:
		aFloat, okA := tryToFloat(a)
		bFloat, okB := tryToFloat(b)
		if okA && okB {
			return compareFloats(aFloat, bFloat), true
		}
	}
	return 0, false
}

func tryToBigInt(v any) (*big.Int, bool) {
	var s string
	switch val := v.(type) {
	case int:
		s = strconv.FormatInt(int64(val), 10)
	case int8:
		s = strconv.FormatInt(int64(val), 10)
	case int16:
		s = strconv.FormatInt(int64(val), 10)
	case int32:
		s = strconv.FormatInt(int64(val), 10)
	case int64:
		s = strconv.FormatInt(val, 10)
	case uint:
		s = strconv.FormatUint(uint64(val), 10)
	case uint8:
		s = strconv.FormatUint(uint64(val), 10)
	case uint16:
		s = strconv.FormatUint(uint64(val), 10)
	case uint32:
		s = strconv.FormatUint(uint64(val), 10)
	case uint64:
		s = strconv.FormatUint(val, 10)
	case string:
		s = strings.TrimSpace(val)
	default:
		return nil, false
	}
	z, ok := new(big.Int).SetString(s, 10)
	return z, ok
}

// compareExactDecimals uses big.Rat for finite decimal/numeric values. The
// rank also gives PostgreSQL/MySQL special numeric values a stable total order:
// -Infinity < finite < Infinity < NaN.
func compareExactDecimals(a, b any) (int, bool) {
	aRat, aRank, okA := tryToExactDecimal(a)
	bRat, bRank, okB := tryToExactDecimal(b)
	if !okA || !okB {
		return 0, false
	}
	if aRank != bRank {
		if aRank < bRank {
			return -1, true
		}
		return 1, true
	}
	if aRank != 1 {
		return 0, true
	}
	return aRat.Cmp(bRat), true
}

func tryToExactDecimal(v any) (*big.Rat, int, bool) {
	if rank, ok := specialNumberRank(v); ok {
		return nil, rank, true
	}
	var s string
	switch val := v.(type) {
	case int:
		s = strconv.FormatInt(int64(val), 10)
	case int8:
		s = strconv.FormatInt(int64(val), 10)
	case int16:
		s = strconv.FormatInt(int64(val), 10)
	case int32:
		s = strconv.FormatInt(int64(val), 10)
	case int64:
		s = strconv.FormatInt(val, 10)
	case uint:
		s = strconv.FormatUint(uint64(val), 10)
	case uint8:
		s = strconv.FormatUint(uint64(val), 10)
	case uint16:
		s = strconv.FormatUint(uint64(val), 10)
	case uint32:
		s = strconv.FormatUint(uint64(val), 10)
	case uint64:
		s = strconv.FormatUint(val, 10)
	case float32:
		s = strconv.FormatFloat(float64(val), 'g', -1, 32)
	case float64:
		s = strconv.FormatFloat(val, 'g', -1, 64)
	case string:
		s = strings.TrimSpace(val)
	default:
		return nil, 0, false
	}
	rat, ok := new(big.Rat).SetString(s)
	return rat, 1, ok
}

func specialNumberRank(v any) (int, bool) {
	var f float64
	switch val := v.(type) {
	case float32:
		f = float64(val)
	case float64:
		f = val
	case string:
		s := strings.ToLower(strings.TrimSpace(val))
		switch s {
		case "-inf", "-infinity":
			return 0, true
		case "+inf", "inf", "+infinity", "infinity":
			return 2, true
		case "nan", "+nan", "-nan":
			return 3, true
		}
		parsed, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		f = parsed
	default:
		return 0, false
	}
	if math.IsInf(f, -1) {
		return 0, true
	}
	if math.IsInf(f, 1) {
		return 2, true
	}
	if math.IsNaN(f) {
		return 3, true
	}
	return 0, false
}

func compareFloats(a, b float64) int {
	if math.IsNaN(a) || math.IsNaN(b) {
		if math.IsNaN(a) && math.IsNaN(b) {
			return 0
		}
		if math.IsNaN(a) {
			return 1
		}
		return -1
	}
	if a == b { // includes equal infinities and both signed zeroes
		return 0
	}
	if !math.IsInf(a, 0) && !math.IsInf(b, 0) && math.Abs(a-b) < 1e-9 {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

func isTimeColType(t string) bool {
	return strings.Contains(t, "time") ||
		strings.Contains(t, "date") ||
		strings.Contains(t, "year")
}

func isBoolColType(t string) bool {
	return strings.Contains(t, "bool") || strings.HasPrefix(t, "tinyint(1)")
}

func isNumericGoType(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func isFloatGoType(v any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

func isTimeTypeObject(v any) bool {
	_, ok := v.(time.Time)
	return ok
}

func tryToFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		s := strings.TrimSpace(val)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func tryToTime(v any) (time.Time, bool) {
	if t, ok := v.(time.Time); ok {
		return t, true
	}
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		for _, layout := range timeFormats {
			if t, err := time.Parse(layout, s); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func tryToBool(v any) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case int, int8, int16, int32, int64:
		f, _ := tryToFloat(val)
		return f != 0, true
	case string:
		s := strings.ToLower(strings.TrimSpace(val))
		if s == "true" || s == "t" || s == "1" || s == "yes" {
			return true, true
		}
		if s == "false" || s == "f" || s == "0" || s == "no" {
			return false, true
		}
	}
	return false, false
}

func equalJSON(valA, valB any) bool {
	var bytesA, bytesB []byte
	switch a := valA.(type) {
	case []byte:
		bytesA = a
	case string:
		bytesA = []byte(a)
	default:
		if b, err := json.Marshal(a); err == nil {
			bytesA = b
		}
	}
	switch b := valB.(type) {
	case []byte:
		bytesB = b
	case string:
		bytesB = []byte(b)
	default:
		if bData, err := json.Marshal(b); err == nil {
			bytesB = bData
		}
	}

	if len(bytesA) == 0 || len(bytesB) == 0 {
		return false
	}

	var objA, objB any
	if err := json.Unmarshal(bytesA, &objA); err != nil {
		return false
	}
	if err := json.Unmarshal(bytesB, &objB); err != nil {
		return false
	}

	canonA, errA := json.Marshal(objA)
	canonB, errB := json.Marshal(objB)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(canonA, canonB)
}
