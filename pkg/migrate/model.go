package migrate

import "os"

type MigrationFile struct {
	Version   string
	Title     string
	Direction string // up/down
	Ext       string // sql/json
	Path      string
	Content   string
}

func (m *MigrationFile) GetContent() string {
	if m.Content != "" {
		return m.Content
	}
	content, err := os.ReadFile(m.Path)
	if err != nil {
		return ""
	}
	return string(content)
}
