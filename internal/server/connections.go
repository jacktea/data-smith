package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
)

// proxyRequest is the incoming proxy payload; nil secret pointers mean
// "keep the previously stored value".
type proxyRequest struct {
	Host            string  `json:"host"`
	Port            int     `json:"port"`
	User            string  `json:"user"`
	Type            string  `json:"type"`
	Pass            *string `json:"pass"`
	RsaKey          *string `json:"rsaKey"`
	RsaKeyPath      string  `json:"rsaKeyPath"`
	RsaKeyPassword  *string `json:"rsaKeyPassword"`
	KnownHostsPath  string  `json:"knownHostsPath"`
	HostFingerprint string  `json:"hostFingerprint"`
}

// connectionRequest is the incoming connection payload; Password nil means
// "keep the previously stored value" (never exposed to clients anyway).
// Proxy stays raw JSON so that an explicit "proxy": null (clear the proxy) is
// distinguishable from an absent field (keep the stored proxy).
type connectionRequest struct {
	Name             string          `json:"name"`
	Type             string          `json:"type"`
	Host             string          `json:"host"`
	Port             int             `json:"port"`
	User             string          `json:"user"`
	Password         *string         `json:"password"`
	DBName           string          `json:"dbname"`
	TableSchema      string          `json:"tableSchema"`
	SSL              bool            `json:"ssl"`
	Proxy            json.RawMessage `json:"proxy"`
	ConnectTimeoutMs int             `json:"connectTimeoutMs"`
	MaxOpenConns     int             `json:"maxOpenConns"`
}

type proxyView struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	User            string `json:"user"`
	Type            string `json:"type"`
	KnownHostsPath  string `json:"knownHostsPath"`
	HostFingerprint string `json:"hostFingerprint"`
	RsaKeyPathSet   bool   `json:"rsaKeyPathSet"`
	PassSet         bool   `json:"passSet"`
}

type connectionView struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Type             string     `json:"type"`
	Host             string     `json:"host"`
	Port             int        `json:"port"`
	User             string     `json:"user"`
	PasswordSet      bool       `json:"passwordSet"`
	DBName           string     `json:"dbname"`
	TableSchema      string     `json:"tableSchema"`
	SSL              bool       `json:"ssl"`
	Proxy            *proxyView `json:"proxy"`
	ConnectTimeoutMs int        `json:"connectTimeoutMs"`
	MaxOpenConns     int        `json:"maxOpenConns"`
}

func proxyFromRequest(req *proxyRequest, previous *StoredProxy) *StoredProxy {
	out := &StoredProxy{
		Host:            req.Host,
		Port:            req.Port,
		User:            req.User,
		Type:            req.Type,
		RsaKeyPath:      req.RsaKeyPath,
		KnownHostsPath:  req.KnownHostsPath,
		HostFingerprint: req.HostFingerprint,
	}
	// Secrets absent from the request keep the previously stored values so
	// clients can round-trip masked views without losing credentials.
	if req.Pass != nil {
		out.Pass = *req.Pass
	} else if previous != nil {
		out.Pass = previous.Pass
	}
	if req.RsaKey != nil {
		out.RsaKey = *req.RsaKey
	} else if previous != nil {
		out.RsaKey = previous.RsaKey
	}
	if req.RsaKeyPassword != nil {
		out.RsaKeyPassword = *req.RsaKeyPassword
	} else if previous != nil {
		out.RsaKeyPassword = previous.RsaKeyPassword
	}
	return out
}

// apply merges a request into a stored connection, preserving secrets that the
// request leaves absent. Validation errors are returned before mutation.
func (req *connectionRequest) apply(existing *StoredConnection) (*StoredConnection, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	out := &StoredConnection{
		ID:               existingOrNewID(existing),
		Name:             req.Name,
		Type:             req.Type,
		Host:             req.Host,
		Port:             req.Port,
		User:             req.User,
		DBName:           req.DBName,
		TableSchema:      req.TableSchema,
		SSL:              req.SSL,
		ConnectTimeoutMs: req.ConnectTimeoutMs,
		MaxOpenConns:     req.MaxOpenConns,
	}
	if req.Password != nil {
		out.Password = *req.Password
	} else if existing != nil {
		out.Password = existing.Password
	}
	switch {
	case req.Proxy == nil:
		// 字段缺省:保留原 proxy(与「缺省密码保持原值」同语义)。
		if existing != nil {
			out.Proxy = existing.Proxy
		}
	case string(req.Proxy) == "null":
		// 显式 null:清除代理。
		out.Proxy = nil
	default:
		var pr proxyRequest
		dec := json.NewDecoder(bytes.NewReader(req.Proxy))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&pr); err != nil {
			return nil, errBad("proxy 字段解析失败: %v", err)
		}
		out.Proxy = proxyFromRequest(&pr, existingProxy(existing))
	}
	return out, nil
}

func existingOrNewID(existing *StoredConnection) string {
	if existing != nil {
		return existing.ID
	}
	return newID("conn")
}

func existingProxy(existing *StoredConnection) *StoredProxy {
	if existing == nil {
		return nil
	}
	return existing.Proxy
}

func (req *connectionRequest) validate() error {
	if strings.TrimSpace(req.Name) == "" {
		return errBad("连接名称不能为空")
	}
	switch req.Type {
	case string(consts.DBTypeMySQL), string(consts.DBTypePostgres):
	default:
		return errBad("连接类型仅支持 mysql 或 postgres")
	}
	if strings.TrimSpace(req.Host) == "" {
		return errBad("主机不能为空")
	}
	if req.Port <= 0 || req.Port > 65535 {
		return errBad("端口必须在 1-65535 之间")
	}
	return nil
}

// errBad marks a client-side validation failure.
func errBad(format string, args ...any) error {
	return &badRequestError{msg: fmt.Sprintf(format, args...)}
}

type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

// respondStoreErr maps store mutation errors onto HTTP responses.
func respondStoreErr(w http.ResponseWriter, err error) {
	var bad *badRequestError
	if asBadRequest(err, &bad) {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "%s", err.Error())
}

func asBadRequest(err error, target **badRequestError) bool {
	if bad, ok := err.(*badRequestError); ok {
		*target = bad
		return true
	}
	return false
}

func connectionToView(c *StoredConnection) connectionView {
	view := connectionView{
		ID:               c.ID,
		Name:             c.Name,
		Type:             c.Type,
		Host:             c.Host,
		Port:             c.Port,
		User:             c.User,
		PasswordSet:      c.Password != "",
		DBName:           c.DBName,
		TableSchema:      c.TableSchema,
		SSL:              c.SSL,
		ConnectTimeoutMs: c.ConnectTimeoutMs,
		MaxOpenConns:     c.MaxOpenConns,
	}
	if c.Proxy != nil {
		view.Proxy = &proxyView{
			Host:            c.Proxy.Host,
			Port:            c.Proxy.Port,
			User:            c.Proxy.User,
			Type:            c.Proxy.Type,
			KnownHostsPath:  c.Proxy.KnownHostsPath,
			HostFingerprint: c.Proxy.HostFingerprint,
			RsaKeyPathSet:   c.Proxy.RsaKeyPath != "",
			PassSet:         c.Proxy.Pass != "",
		}
	}
	return view
}

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	conns := s.store.ListConnections()
	out := make([]connectionView, 0, len(conns))
	for _, c := range conns {
		out = append(out, connectionToView(c))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCreateConnection(w http.ResponseWriter, r *http.Request) {
	var req connectionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	created, err := req.apply(nil)
	if err != nil {
		respondStoreErr(w, err)
		return
	}
	if err := s.store.PutConnection(created); err != nil {
		respondStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, connectionToView(created))
}

func (s *Server) handleGetConnection(w http.ResponseWriter, r *http.Request) {
	conn, ok := s.store.GetConnection(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	writeJSON(w, http.StatusOK, connectionToView(conn))
}

func (s *Server) handleUpdateConnection(w http.ResponseWriter, r *http.Request) {
	var req connectionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	id := r.PathValue("id")
	var updated *StoredConnection
	found, err := s.store.UpdateConnection(id, func(existing *StoredConnection) (*StoredConnection, error) {
		merged, mergeErr := req.apply(existing)
		if mergeErr != nil {
			return nil, mergeErr
		}
		updated = merged
		return merged, nil
	})
	if err != nil {
		respondStoreErr(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	writeJSON(w, http.StatusOK, connectionToView(updated))
}

func (s *Server) handleDeleteConnection(w http.ResponseWriter, r *http.Request) {
	found, err := s.store.DeleteConnection(r.PathValue("id"))
	if err != nil {
		respondStoreErr(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// testConnectionTimeout bounds connection probing in the test endpoint.
const testConnectionTimeout = 5 * time.Second

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	stored, ok := s.store.GetConnection(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), testConnectionTimeout)
	defer cancel()
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	defer adapter.Close()
	var version string
	err = adapter.GetConn().QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "serverVersion": version})
}

type tableColumnView struct {
	Name       string `json:"name"`
	DataType   string `json:"dataType"`
	Nullable   bool   `json:"nullable"`
	Comment    string `json:"comment"`
	PrimaryKey bool   `json:"primaryKey"`
	Position   int    `json:"position"`
}

type tableView struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Columns []tableColumnView `json:"columns"`
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	stored, ok := s.store.GetConnection(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	if kind == "" {
		kind = "table"
	}
	if kind != "table" && kind != "view" && kind != "all" {
		writeError(w, http.StatusBadRequest, "kind 仅支持 table、view 或 all")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), testConnectionTimeout)
	defer cancel()
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		writeError(w, http.StatusBadRequest, "连接数据库失败: %v", err)
		return
	}
	defer adapter.Close()
	schema, err := adapter.ReadSchema()
	if err != nil {
		writeError(w, http.StatusBadRequest, "读取数据库结构失败: %v", err)
		return
	}
	out := make([]tableView, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		parsed := conn.ParseTableType(string(table.Type))
		switch kind {
		case "table":
			if parsed != conn.TableTypeTable {
				continue
			}
		case "view":
			if parsed != conn.TableTypeView {
				continue
			}
		}
		out = append(out, tableViewFromConn(table))
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	writeJSON(w, http.StatusOK, map[string]any{"tables": out})
}

func tableViewFromConn(table *conn.Table) tableView {
	pk := make(map[string]bool)
	if table.PrimaryKey != nil {
		for _, col := range table.PrimaryKey.Columns {
			pk[col] = true
		}
	}
	view := tableView{
		Name:    table.Name,
		Type:    strings.ToLower(string(table.Type)),
		Columns: []tableColumnView{},
	}
	for _, col := range table.GetColumnsByPosition() {
		comment := ""
		if col.Comment != nil {
			comment = *col.Comment
		}
		view.Columns = append(view.Columns, tableColumnView{
			Name:       col.Name,
			DataType:   col.DataType,
			Nullable:   col.Nullable,
			Comment:    comment,
			PrimaryKey: pk[col.Name],
			Position:   col.Position,
		})
	}
	return view
}
