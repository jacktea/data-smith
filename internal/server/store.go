// Package server implements the DataSmith web console backend: a stdlib
// net/http API over the existing diff/exec/migrate engines, with a JSON file
// store for connections/schemes/libraries and an in-memory job registry.
package server

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/consts"
)

// StoredProxy is the persisted SSH proxy configuration. Secret fields are kept
// in plain text on disk (store.json is 0600) but never serialized in responses.
type StoredProxy struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	User            string `json:"user"`
	Type            string `json:"type"`
	Pass            string `json:"pass,omitempty"`
	RsaKey          string `json:"rsaKey,omitempty"`
	RsaKeyPath      string `json:"rsaKeyPath,omitempty"`
	RsaKeyPassword  string `json:"rsaKeyPassword,omitempty"`
	KnownHostsPath  string `json:"knownHostsPath,omitempty"`
	HostFingerprint string `json:"hostFingerprint,omitempty"`
}

// StoredConnection is the persisted connection profile, including secrets.
type StoredConnection struct {
	ID               string       `json:"id"`
	Name             string       `json:"name"`
	Type             string       `json:"type"`
	Host             string       `json:"host"`
	Port             int          `json:"port"`
	User             string       `json:"user"`
	Password         string       `json:"password,omitempty"`
	DBName           string       `json:"dbname"`
	TableSchema      string       `json:"tableSchema,omitempty"`
	SSL              bool         `json:"ssl"`
	Proxy            *StoredProxy `json:"proxy,omitempty"`
	ConnectTimeoutMs int          `json:"connectTimeoutMs"`
	MaxOpenConns     int          `json:"maxOpenConns"`
}

// Clone returns a deep copy so callers can read outside the store lock.
func (c *StoredConnection) Clone() *StoredConnection {
	if c == nil {
		return nil
	}
	out := *c
	if c.Proxy != nil {
		proxy := *c.Proxy
		out.Proxy = &proxy
	}
	return &out
}

// ConnConfig projects the stored profile onto the engine connection config.
func (c *StoredConnection) ConnConfig() *pkgconfig.ConnConfig {
	cfg := &pkgconfig.ConnConfig{
		Type:        consts.DBType(c.Type),
		Host:        c.Host,
		Port:        c.Port,
		User:        c.User,
		Password:    c.Password,
		DBName:      c.DBName,
		TableSchema: c.TableSchema,
		SSL:         c.SSL,
	}
	if c.ConnectTimeoutMs > 0 {
		cfg.ConnectTimeout = time.Duration(c.ConnectTimeoutMs) * time.Millisecond
	}
	cfg.MaxOpenConns = c.MaxOpenConns
	if c.Proxy != nil {
		cfg.Proxy = &pkgconfig.SSHProxy{
			Host:            c.Proxy.Host,
			Port:            c.Proxy.Port,
			User:            c.Proxy.User,
			Type:            c.Proxy.Type,
			Pass:            c.Proxy.Pass,
			RsaKey:          c.Proxy.RsaKey,
			RsaKeyPath:      c.Proxy.RsaKeyPath,
			RsaKeyPassword:  c.Proxy.RsaKeyPassword,
			KnownHostsPath:  c.Proxy.KnownHostsPath,
			HostFingerprint: c.Proxy.HostFingerprint,
		}
	}
	return cfg
}

// SchemeTable names one compared table with its column selection overrides.
type SchemeTable struct {
	Table         string   `json:"table"`
	Columns       []string `json:"columns,omitempty"`
	IgnoreColumns []string `json:"ignoreColumns,omitempty"`
}

// Scheme is a named, reusable data-diff table selection.
type Scheme struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	UpdatedAt time.Time     `json:"updatedAt"`
	Tables    []SchemeTable `json:"tables"`
}

func (s *Scheme) Clone() *Scheme {
	if s == nil {
		return nil
	}
	out := *s
	out.Tables = append([]SchemeTable(nil), s.Tables...)
	for i := range out.Tables {
		out.Tables[i].Columns = append([]string(nil), s.Tables[i].Columns...)
		out.Tables[i].IgnoreColumns = append([]string(nil), s.Tables[i].IgnoreColumns...)
	}
	return &out
}

// VersionMeta records registration metadata for one library version, keyed by
// normalized version.
type VersionMeta struct {
	ExpectedConnectionID string `json:"expectedConnectionId,omitempty"`
}

// LibraryMeta is the store-side record of a migration library. The scripts
// themselves live in <data-dir>/libraries/<id>/.
type LibraryMeta struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	CreatedAt time.Time              `json:"createdAt"`
	Versions  map[string]VersionMeta `json:"versions"`
}

func (l *LibraryMeta) Clone() *LibraryMeta {
	if l == nil {
		return nil
	}
	out := *l
	out.Versions = make(map[string]VersionMeta, len(l.Versions))
	for version, meta := range l.Versions {
		out.Versions[version] = meta
	}
	return &out
}

type storeData struct {
	Connections []*StoredConnection `json:"connections"`
	Schemes     []*Scheme           `json:"schemes"`
	Libraries   []*LibraryMeta      `json:"libraries"`
}

// Store is the JSON-file backed persistence for named entities. Every mutation
// rewrites store.json atomically (temp file + rename, mode 0600).
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
}

// OpenStore loads (or initializes) the store at path.
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("读取存储文件: %w", err)
	}
	if err := strictUnmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("解析存储文件 %s: %w", path, err)
	}
	return s, nil
}

// saveLocked writes the store atomically; callers must hold s.mu.
func (s *Store) saveLocked() error {
	raw, err := marshalIndent(s.data)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".store-*.tmp")
	if err != nil {
		return fmt.Errorf("创建临时存储文件: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("写入临时存储文件: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("设置存储文件权限: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("关闭临时存储文件: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("替换存储文件: %w", err)
	}
	return nil
}

// mutate applies fn under the store lock and persists on success.
func (s *Store) mutate(fn func(data *storeData) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := fn(&s.data); err != nil {
		return err
	}
	return s.saveLocked()
}

// ListConnections returns copies of all stored connections.
func (s *Store) ListConnections() []*StoredConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*StoredConnection, 0, len(s.data.Connections))
	for _, c := range s.data.Connections {
		out = append(out, c.Clone())
	}
	return out
}

// GetConnection returns a copy of the connection with id.
func (s *Store) GetConnection(id string) (*StoredConnection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.data.Connections {
		if c.ID == id {
			return c.Clone(), true
		}
	}
	return nil, false
}

// PutConnection inserts or replaces the connection by ID.
func (s *Store) PutConnection(conn *StoredConnection) error {
	return s.mutate(func(data *storeData) error {
		for i, existing := range data.Connections {
			if existing.ID == conn.ID {
				data.Connections[i] = conn
				return nil
			}
		}
		data.Connections = append(data.Connections, conn)
		return nil
	})
}

// UpdateConnection applies fn to the stored connection under the lock. fn
// returns the replacement value (nil keeps the current one). The second
// return reports whether the connection exists.
func (s *Store) UpdateConnection(id string, fn func(c *StoredConnection) (*StoredConnection, error)) (bool, error) {
	var found bool
	err := s.mutate(func(data *storeData) error {
		for i, existing := range data.Connections {
			if existing.ID == id {
				found = true
				next, err := fn(existing)
				if err != nil {
					return err
				}
				if next != nil {
					data.Connections[i] = next
				}
				return nil
			}
		}
		return nil
	})
	return found, err
}

// DeleteConnection removes the connection; reports whether it existed.
func (s *Store) DeleteConnection(id string) (bool, error) {
	var found bool
	err := s.mutate(func(data *storeData) error {
		for i, existing := range data.Connections {
			if existing.ID == id {
				found = true
				data.Connections = append(data.Connections[:i], data.Connections[i+1:]...)
				return nil
			}
		}
		return nil
	})
	return found, err
}

// ListSchemes returns copies of all stored schemes.
func (s *Store) ListSchemes() []*Scheme {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Scheme, 0, len(s.data.Schemes))
	for _, sc := range s.data.Schemes {
		out = append(out, sc.Clone())
	}
	return out
}

// GetScheme returns a copy of the scheme with id.
func (s *Store) GetScheme(id string) (*Scheme, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sc := range s.data.Schemes {
		if sc.ID == id {
			return sc.Clone(), true
		}
	}
	return nil, false
}

// PutScheme inserts or replaces the scheme by ID.
func (s *Store) PutScheme(sc *Scheme) error {
	return s.mutate(func(data *storeData) error {
		for i, existing := range data.Schemes {
			if existing.ID == sc.ID {
				data.Schemes[i] = sc
				return nil
			}
		}
		data.Schemes = append(data.Schemes, sc)
		return nil
	})
}

// DeleteScheme removes the scheme; reports whether it existed.
func (s *Store) DeleteScheme(id string) (bool, error) {
	var found bool
	err := s.mutate(func(data *storeData) error {
		for i, existing := range data.Schemes {
			if existing.ID == id {
				found = true
				data.Schemes = append(data.Schemes[:i], data.Schemes[i+1:]...)
				return nil
			}
		}
		return nil
	})
	return found, err
}

// ListLibraries returns copies of all library records.
func (s *Store) ListLibraries() []*LibraryMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*LibraryMeta, 0, len(s.data.Libraries))
	for _, lib := range s.data.Libraries {
		out = append(out, lib.Clone())
	}
	return out
}

// GetLibrary returns a copy of the library record with id.
func (s *Store) GetLibrary(id string) (*LibraryMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lib := range s.data.Libraries {
		if lib.ID == id {
			return lib.Clone(), true
		}
	}
	return nil, false
}

// PutLibrary inserts or replaces the library record by ID.
func (s *Store) PutLibrary(lib *LibraryMeta) error {
	return s.mutate(func(data *storeData) error {
		for i, existing := range data.Libraries {
			if existing.ID == lib.ID {
				data.Libraries[i] = lib
				return nil
			}
		}
		data.Libraries = append(data.Libraries, lib)
		return nil
	})
}

// DeleteLibrary removes the library record; reports whether it existed.
func (s *Store) DeleteLibrary(id string) (bool, error) {
	var found bool
	err := s.mutate(func(data *storeData) error {
		for i, existing := range data.Libraries {
			if existing.ID == id {
				found = true
				data.Libraries = append(data.Libraries[:i], data.Libraries[i+1:]...)
				return nil
			}
		}
		return nil
	})
	return found, err
}

// SetVersionMeta records registration metadata for a normalized version.
func (s *Store) SetVersionMeta(libID, version string, meta VersionMeta) error {
	return s.mutate(func(data *storeData) error {
		for _, lib := range data.Libraries {
			if lib.ID == libID {
				if lib.Versions == nil {
					lib.Versions = make(map[string]VersionMeta)
				}
				lib.Versions[version] = meta
				return nil
			}
		}
		return fmt.Errorf("脚本库不存在: %s", libID)
	})
}

// DeleteVersionMeta removes the registration metadata of one version. 清理
// 语义：脚本库或版本条目不存在时同样视为成功，方便幂等调用。
func (s *Store) DeleteVersionMeta(libID, version string) error {
	return s.mutate(func(data *storeData) error {
		for _, lib := range data.Libraries {
			if lib.ID == libID {
				delete(lib.Versions, version)
				return nil
			}
		}
		return nil
	})
}
