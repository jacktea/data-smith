package utils

import (
	"path/filepath"
	"testing"
)

func TestCompareVersion(t *testing.T) {
	tests := []struct {
		name     string
		versionA string
		versionB string
		want     int
	}{
		{
			name:     "空版本比较",
			versionA: "",
			versionB: "",
			want:     0,
		},
		{
			name:     "空版本与有值版本比较",
			versionA: "",
			versionB: "1.0.0",
			want:     -1,
		},
		{
			name:     "有值版本与空版本比较",
			versionA: "1.0.0",
			versionB: "",
			want:     1,
		},
		{
			name:     "相同版本比较",
			versionA: "1.0.0",
			versionB: "1.0.0",
			want:     0,
		},
		{
			name:     "带V前缀版本比较",
			versionA: "v1.0.0",
			versionB: "1.0.0",
			want:     0,
		},
		{
			name:     "带v前缀版本比较",
			versionA: "v1.0.0",
			versionB: "v1.0.0",
			want:     0,
		},
		{
			name:     "主版本号不同",
			versionA: "2.0.0",
			versionB: "1.0.0",
			want:     1,
		},
		{
			name:     "次版本号不同",
			versionA: "1.1.0",
			versionB: "1.0.0",
			want:     1,
		},
		{
			name:     "修订版本号不同",
			versionA: "1.0.1",
			versionB: "1.0.0",
			want:     1,
		},
		{
			name:     "版本号长度不同",
			versionA: "1.0.0.0",
			versionB: "1.0.0",
			want:     0,
		},
		{
			name:     "版本号包含非数字",
			versionA: "1.0.a",
			versionB: "1.0.0",
			want:     0,
		},
		{
			name:     "复杂版本号比较",
			versionA: "1.2.3.4",
			versionB: "1.2.3",
			want:     1,
		},
		{
			name:     "版本号降序比较",
			versionA: "1.0.0",
			versionB: "2.0.0",
			want:     -1,
		},
		{
			name:     "版本号部分缺失",
			versionA: "1",
			versionB: "1.0.0",
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareVersion(tt.versionA, tt.versionB); got != tt.want {
				t.Errorf("CompareVersion(%q, %q) = %v, want %v", tt.versionA, tt.versionB, got, tt.want)
			}
		})
	}
}

func TestParseMigrationFile(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantTitle string
		wantVer   string
		wantDir   string
		wantExt   string
		wantErr   bool
	}{
		{
			name:      "标准SQL升级文件",
			path:      filepath.Join("migrations", "v1.0.0__create_users_table.up.sql"),
			wantTitle: "create_users_table",
			wantVer:   "v1.0.0",
			wantDir:   "up",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "标准SQL降级文件",
			path:      filepath.Join("migrations", "v1.0.0__create_users_table.down.sql"),
			wantTitle: "create_users_table",
			wantVer:   "v1.0.0",
			wantDir:   "down",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "标准JSON升级文件",
			path:      filepath.Join("migrations", "v1.0.0__create_users.up.json"),
			wantTitle: "create_users",
			wantVer:   "v1.0.0",
			wantDir:   "up",
			wantExt:   "json",
			wantErr:   false,
		},
		{
			name:      "无方向SQL文件",
			path:      filepath.Join("migrations", "v1.0.0__create_users.sql"),
			wantTitle: "create_users",
			wantVer:   "v1.0.0",
			wantDir:   "",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "数字版本号",
			path:      filepath.Join("migrations", "1.0.0__create_users.up.sql"),
			wantTitle: "create_users",
			wantVer:   "1.0.0",
			wantDir:   "up",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "大写V前缀",
			path:      filepath.Join("migrations", "V1.0.0__create_users.up.sql"),
			wantTitle: "create_users",
			wantVer:   "V1.0.0",
			wantDir:   "up",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "复杂版本号",
			path:      filepath.Join("migrations", "v1.2.3.4__create_users.up.sql"),
			wantTitle: "create_users",
			wantVer:   "v1.2.3.4",
			wantDir:   "up",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:      "带下划线的标题",
			path:      filepath.Join("migrations", "v1.0.0__create_users_table_with_index.up.sql"),
			wantTitle: "create_users_table_with_index",
			wantVer:   "v1.0.0",
			wantDir:   "up",
			wantExt:   "sql",
			wantErr:   false,
		},
		{
			name:    "无效的文件名格式",
			path:    filepath.Join("migrations", "invalid_file.sql"),
			wantErr: true,
		},
		{
			name:    "无效的方向",
			path:    filepath.Join("migrations", "v1.0.0__create_users.invalid.sql"),
			wantErr: true,
		},
		{
			name:    "无效的扩展名",
			path:    filepath.Join("migrations", "v1.0.0__create_users.up.txt"),
			wantErr: true,
		},
		{
			name:    "缺少版本号",
			path:    filepath.Join("migrations", "__create_users.up.sql"),
			wantErr: true,
		},
		{
			name:    "缺少标题",
			path:    filepath.Join("migrations", "v1.0.0__.up.sql"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTitle, gotVer, gotDir, gotExt, err := ParseMigrationFile(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMigrationFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if gotTitle != tt.wantTitle {
					t.Errorf("ParseMigrationFile() title = %v, want %v", gotTitle, tt.wantTitle)
				}
				if gotVer != tt.wantVer {
					t.Errorf("ParseMigrationFile() version = %v, want %v", gotVer, tt.wantVer)
				}
				if gotDir != tt.wantDir {
					t.Errorf("ParseMigrationFile() direction = %v, want %v", gotDir, tt.wantDir)
				}
				if gotExt != tt.wantExt {
					t.Errorf("ParseMigrationFile() extension = %v, want %v", gotExt, tt.wantExt)
				}
			}
		})
	}
}
