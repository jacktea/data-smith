package migrate

import "os"

type MigrationFile struct {
	Version   string
	Title     string
	Direction string // up/down
	Ext       string // sql/json
	Path      string
	Content   string
	Checksum  string
}

// ReadContent returns the inline content or reads it from Path, preserving any
// filesystem error so callers cannot mistake an unreadable migration for an
// empty, successful migration.
func (m *MigrationFile) ReadContent() (string, error) {
	if m.Content != "" {
		return m.Content, nil
	}
	content, err := os.ReadFile(m.Path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// GetContent is kept for source compatibility.
// Deprecated: use ReadContent so read failures are handled explicitly.
func (m *MigrationFile) GetContent() string {
	content, err := m.ReadContent()
	if err != nil {
		return ""
	}
	return content
}
