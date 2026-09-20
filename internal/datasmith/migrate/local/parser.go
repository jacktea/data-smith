package local

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jacktea/data-smith/pkg/migrate"
)

// ParseMigrationFile 解析迁移文件名，提取版本号、标题和方向
func ParseMigrationFile(path string) (*migrate.MigrationFile, error) {
	re := regexp.MustCompile(`^([vV]\d+(?:\.\d+)*|\d+)__([^.]+)(?:\.(up|down))?\.(sql|json)$`)
	matches := re.FindStringSubmatch(filepath.Base(path))
	if matches == nil {
		return nil, fmt.Errorf("无效的迁移文件名格式: %s", path)
	}

	version := matches[1]
	title := matches[2]
	direction := matches[3] // 可能为"up"、"down"或""
	extension := matches[4]

	if direction == "" {
		direction = "up"
	}

	if extension != "sql" && extension != "json" {
		return nil, fmt.Errorf("无效的扩展名: %s", extension)
	}

	return &migrate.MigrationFile{
		Version:   version,
		Title:     title,
		Direction: direction,
		Path:      path,
		Ext:       extension,
	}, nil
}

// ScanMigrations 扫描指定目录下的所有迁移文件。除合法文件清单外，还返回
// 被跳过的文件相对路径清单——文件名不符合迁移命名规范的文件（如单下划线的
// V1.0.1_update.up.sql）静默跳过会掩盖「迁移缺版本」问题，调用方必须把
// skipped 清单作为 warning 输出。
func ScanMigrations(dir string) ([]*migrate.MigrationFile, []string, error) {
	fmt.Println("扫描迁移文件目录: ", dir)
	var files []*migrate.MigrationFile
	var skipped []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		file, parseErr := ParseMigrationFile(path)
		if parseErr != nil {
			if rel, relErr := filepath.Rel(dir, path); relErr == nil {
				skipped = append(skipped, rel)
			} else {
				skipped = append(skipped, path)
			}
			return nil
		}
		if file.Ext == "json" {
			return fmt.Errorf("JSON migration is not supported: %s", path)
		}
		if file.Ext == "sql" && (file.Direction == "up" || file.Direction == "down") {
			files = append(files, file)
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	type scriptKey struct {
		version   string
		direction string
	}
	seen := make(map[scriptKey]string, len(files))
	for _, file := range files {
		key := scriptKey{normalizeVersion(file.Version), file.Direction}
		if previous, ok := seen[key]; ok {
			return nil, nil, fmt.Errorf("duplicate migration script %q (%s) in %s and %s", file.Version, file.Direction, previous, file.Path)
		}
		seen[key] = file.Path
	}

	sort.Strings(skipped)
	return files, skipped, nil
}

// FindDownMigration returns the down script for version, or nil when absent.
func FindDownMigration(files []*migrate.MigrationFile, version string) *migrate.MigrationFile {
	normalized := normalizeVersion(version)
	for _, file := range files {
		if file.Direction == "down" && normalizeVersion(file.Version) == normalized {
			return file
		}
	}
	return nil
}

func SortMigrations(files []*migrate.MigrationFile) {
	sort.Slice(files, func(i, j int) bool {
		return CompareVersion(files[i].Version, files[j].Version) < 0
	})
}

func normalizeVersion(version string) string {
	return strings.TrimPrefix(strings.ToLower(version), "v")
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
