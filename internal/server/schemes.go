package server

import (
	"net/http"
	"strings"
	"time"
)

type schemeRequest struct {
	Name   string        `json:"name"`
	Tables []SchemeTable `json:"tables"`
}

func (req *schemeRequest) validate() error {
	if strings.TrimSpace(req.Name) == "" {
		return errBad("方案名称不能为空")
	}
	seen := make(map[string]bool, len(req.Tables))
	for i, table := range req.Tables {
		if strings.TrimSpace(table.Table) == "" {
			return errBad("第 %d 个数据表名为空", i+1)
		}
		if seen[table.Table] {
			return errBad("数据表 %s 重复", table.Table)
		}
		seen[table.Table] = true
	}
	return nil
}

func schemeFromRequest(id string, req *schemeRequest) *Scheme {
	tables := make([]SchemeTable, 0, len(req.Tables))
	for _, table := range req.Tables {
		tables = append(tables, SchemeTable{
			Table:         table.Table,
			Columns:       append([]string(nil), table.Columns...),
			IgnoreColumns: append([]string(nil), table.IgnoreColumns...),
		})
	}
	return &Scheme{ID: id, Name: req.Name, UpdatedAt: time.Now(), Tables: tables}
}

func (s *Server) handleListSchemes(w http.ResponseWriter, r *http.Request) {
	schemes := s.store.ListSchemes()
	writeJSON(w, http.StatusOK, schemes)
}

func (s *Server) handleCreateScheme(w http.ResponseWriter, r *http.Request) {
	var req schemeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	if err := req.validate(); err != nil {
		respondStoreErr(w, err)
		return
	}
	scheme := schemeFromRequest(newID("scheme"), &req)
	if err := s.store.PutScheme(scheme); err != nil {
		respondStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, scheme)
}

func (s *Server) handleGetScheme(w http.ResponseWriter, r *http.Request) {
	scheme, ok := s.store.GetScheme(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "方案不存在")
		return
	}
	writeJSON(w, http.StatusOK, scheme)
}

func (s *Server) handleUpdateScheme(w http.ResponseWriter, r *http.Request) {
	var req schemeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	id := r.PathValue("id")
	if _, ok := s.store.GetScheme(id); !ok {
		writeError(w, http.StatusNotFound, "方案不存在")
		return
	}
	if err := req.validate(); err != nil {
		respondStoreErr(w, err)
		return
	}
	scheme := schemeFromRequest(id, &req)
	if err := s.store.PutScheme(scheme); err != nil {
		respondStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, scheme)
}

func (s *Server) handleDeleteScheme(w http.ResponseWriter, r *http.Request) {
	found, err := s.store.DeleteScheme(r.PathValue("id"))
	if err != nil {
		respondStoreErr(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "方案不存在")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
