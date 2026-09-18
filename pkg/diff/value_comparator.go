package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
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

	// 1. 若两边均为纯 []byte（且未转为 string），按字节字典序比对
	if bA, okA := valA.([]byte); okA {
		if bB, okB := valB.([]byte); okB {
			return bytes.Compare(bA, bB)
		}
	}

	lowerType := strings.ToLower(colType)

	// 2. 数值类型比对（整型、浮点、Decimal、Serial 等）
	isNumericType := isNumericColType(lowerType)
	fA, isNumA := tryToFloat(normA)
	fB, isNumB := tryToFloat(normB)
	if (isNumericType && isNumA && isNumB) || (isNumA && isNumB && isNumericGoType(normA) && isNumericGoType(normB)) {
		if math.Abs(fA-fB) < 1e-9 {
			return 0
		}
		if fA < fB {
			return -1
		}
		return 1
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
	if okA && okB && !strings.Contains(lowerType, "char") && !strings.Contains(lowerType, "text") && !strings.Contains(lowerType, "json") {
		return bytes.Equal(bA, bB)
	}

	// 3. 数值比较（消除 1.50 与 1.5 的字符串表示差异）
	normA := normalizeBytesToString(valA)
	normB := normalizeBytesToString(valB)
	fA, isNumA := tryToFloat(normA)
	fB, isNumB := tryToFloat(normB)
	if isNumA && isNumB && (isNumericColType(lowerType) || (isNumericGoType(normA) && isNumericGoType(normB))) {
		return math.Abs(fA-fB) < 1e-9
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
	return strings.Contains(t, "int") ||
		strings.Contains(t, "float") ||
		strings.Contains(t, "double") ||
		strings.Contains(t, "decimal") ||
		strings.Contains(t, "numeric") ||
		strings.Contains(t, "real") ||
		strings.Contains(t, "serial") ||
		strings.Contains(t, "number")
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
