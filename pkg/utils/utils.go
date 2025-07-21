package utils

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// 预留工具函数

func CleanTransaction(sql string) string {
	re := regexp.MustCompile(`(?i)begin\s*;`)
	sql = re.ReplaceAllString(sql, "")
	re = regexp.MustCompile(`(?i)commit\s*;`)
	sql = re.ReplaceAllString(sql, "")
	re = regexp.MustCompile(`(?i)begin\s+transaction\s*;`)
	sql = re.ReplaceAllString(sql, "")
	re = regexp.MustCompile(`(?i)commit\s+transaction\s*;`)
	sql = re.ReplaceAllString(sql, "")
	return sql
}

func CompareVersion(a, b string) int {
	// 空字符串最小
	if a == "" && b == "" {
		return 0
	}
	if a == "" {
		return -1
	}
	if b == "" {
		return 1
	}

	// 去除 V/v 前缀
	a = strings.TrimPrefix(strings.ToLower(a), "v")
	b = strings.TrimPrefix(strings.ToLower(b), "v")

	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	maxLen := len(aParts)
	if len(bParts) > maxLen {
		maxLen = len(bParts)
	}

	for i := 0; i < maxLen; i++ {
		var ai, bi int
		if i < len(aParts) {
			n, err := strconv.Atoi(aParts[i])
			if err == nil {
				ai = n
			} else {
				ai = 0
			}
		}
		if i < len(bParts) {
			n, err := strconv.Atoi(bParts[i])
			if err == nil {
				bi = n
			} else {
				bi = 0
			}
		}
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
	}
	return 0
}

// ParseMigrationFile 解析迁移文件名，提取版本号、标题和方向
func ParseMigrationFile(path string) (title, version, direction, extension string, err error) {
	re := regexp.MustCompile(`^([vV]?\d+(?:\.\d+)*|\d+)__([^.]+)(?:\.(up|down))?\.(sql|json)$`)
	matches := re.FindStringSubmatch(filepath.Base(path))
	if matches == nil {
		err = fmt.Errorf("无效的迁移文件名格式: %s", path)
		return
	}

	version = matches[1]
	title = matches[2]
	direction = matches[3] // 可能为"up"、"down"或""
	extension = matches[4]

	if direction != "" && direction != "up" && direction != "down" {
		err = fmt.Errorf("无效的方向: %s", direction)
		return
	}

	if extension != "sql" && extension != "json" {
		err = fmt.Errorf("无效的扩展名: %s", extension)
		return
	}
	return
}
