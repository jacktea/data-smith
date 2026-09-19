package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jacktea/data-smith/pkg/consts"
)

const (
	queryTimeout   = 30 * time.Second
	queryRowLimit  = 1000
	emptyRunes     = " \t\r\n"
	queryErrorOnly = "仅允许单条 SELECT/WITH/SHOW/EXPLAIN 查询语句"
)

// allowedQueryVerbs lists the statement-leading keywords permitted by the
// query channel per dialect.
var allowedQueryVerbs = map[consts.DBType]map[string]bool{
	consts.DBTypeMySQL:    {"SELECT": true, "WITH": true, "SHOW": true, "EXPLAIN": true, "DESC": true, "DESCRIBE": true},
	consts.DBTypePostgres: {"SELECT": true, "WITH": true, "SHOW": true, "EXPLAIN": true},
}

// errQueryRejected marks a guard rejection (HTTP 400).
type errQueryRejected struct{ msg string }

func (e *errQueryRejected) Error() string { return e.msg }

// checkQueryStatement lexes sqlText after stripping comments and enforces the
// query-channel contract: exactly one statement, starting with an allowed
// read-only keyword. String literals, quoted identifiers, and (for postgres)
// dollar-quoted strings are honored so semicolons inside them do not split.
func checkQueryStatement(sqlText string, dbType consts.DBType) error {
	allowed := allowedQueryVerbs[dbType]
	if allowed == nil {
		return &errQueryRejected{"不支持的数据库类型"}
	}

	type lexerState int
	const (
		stateNormal lexerState = iota
		stateSingleQuote
		stateDoubleQuote
		stateBacktick
		stateLineComment
		stateBlockComment
	)

	state := stateNormal
	blockDepth := 0
	dollarTag := ""   // active $tag$ delimiter for postgres dollar quotes
	firstWord := ""   // first identifier of the statement
	wordDone := false // firstWord already terminated by a non-ident char

	isIdentChar := func(ch byte) bool {
		return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
	}

	trimmed := strings.TrimLeft(sqlText, emptyRunes)
	if trimmed == "" {
		return &errQueryRejected{"SQL 语句不能为空"}
	}

	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		switch state {
		case stateLineComment:
			if ch == '\n' {
				state = stateNormal
			}
			continue
		case stateBlockComment:
			if ch == '*' && i+1 < len(sqlText) && sqlText[i+1] == '/' {
				blockDepth--
				i++
				if blockDepth == 0 {
					state = stateNormal
				}
			} else if ch == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
				blockDepth++
				i++
			}
			continue
		case stateSingleQuote:
			switch {
			case ch == '\\' && dbType == consts.DBTypeMySQL:
				i++ // MySQL backslash escape inside strings
			case ch == '\'':
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i++ // '' escape
				} else {
					state = stateNormal
				}
			}
			continue
		case stateDoubleQuote:
			if ch == '"' {
				if i+1 < len(sqlText) && sqlText[i+1] == '"' {
					i++
				} else {
					state = stateNormal
				}
			}
			continue
		case stateBacktick:
			if ch == '`' {
				state = stateNormal
			}
			continue
		}

		// stateNormal
		if dollarTag != "" {
			if strings.HasPrefix(sqlText[i:], dollarTag) {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}
		switch {
		case ch == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-':
			state = stateLineComment
			i++
		case ch == '#' && dbType == consts.DBTypeMySQL:
			state = stateLineComment
		case ch == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*':
			state = stateBlockComment
			blockDepth = 1
			i++
		case ch == '\'':
			state = stateSingleQuote
			wordDone = true
		case ch == '"':
			state = stateDoubleQuote
			wordDone = true
		case ch == '`':
			state = stateBacktick
			wordDone = true
		case ch == '$' && dbType == consts.DBTypePostgres:
			if tag, ok := dollarQuoteTag(sqlText, i); ok {
				dollarTag = tag
				i += len(tag) - 1
				wordDone = true
			}
		case ch == ';':
			if rest := strings.TrimLeft(sqlText[i+1:], emptyRunes); rest != "" {
				return &errQueryRejected{queryErrorOnly}
			}
		case isIdentChar(ch):
			if !wordDone {
				firstWord += string(ch)
			}
		case ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n':
			if firstWord != "" {
				wordDone = true
			}
		default:
			wordDone = true
		}
	}
	if state == stateBlockComment || state == stateSingleQuote || state == stateDoubleQuote || state == stateBacktick {
		return &errQueryRejected{"SQL 语句存在未闭合的引号或注释"}
	}
	word := strings.ToUpper(firstWord)
	if !allowed[word] {
		return &errQueryRejected{queryErrorOnly}
	}
	return nil
}

// dollarQuoteTag returns the $tag$ delimiter starting at offset, if present.
func dollarQuoteTag(sqlText string, offset int) (string, bool) {
	end := offset + 1
	for end < len(sqlText) {
		ch := sqlText[end]
		if ch == '$' {
			return sqlText[offset : end+1], true
		}
		if !isDollarTagChar(ch) {
			return "", false
		}
		end++
	}
	return "", false
}

func isDollarTagChar(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

type queryRequest struct {
	ConnectionID string `json:"connectionId"`
	SQL          string `json:"sql"`
}

type queryResponse struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"rowCount"`
	Truncated bool     `json:"truncated"`
	ElapsedMs int64    `json:"elapsedMs"`
}

func (s *Server) handleSQLQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	stored, ok := s.store.GetConnection(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusNotFound, "连接不存在")
		return
	}
	if err := checkQueryStatement(req.SQL, consts.DBType(stored.Type)); err != nil {
		var rejected *errQueryRejected
		if errors.As(err, &rejected) {
			writeError(w, http.StatusBadRequest, "%s", rejected.msg)
			return
		}
		writeError(w, http.StatusBadRequest, "%s", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()
	adapter, err := s.openAdapter(ctx, stored.ConnConfig())
	if err != nil {
		writeError(w, http.StatusBadRequest, "连接数据库失败: %v", err)
		return
	}
	defer adapter.Close()

	started := time.Now()
	rows, err := adapter.GetConn().QueryContext(ctx, req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "查询失败: %v", err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		writeError(w, http.StatusBadRequest, "读取结果列失败: %v", err)
		return
	}
	result := queryResponse{Columns: columns, Rows: [][]any{}}
	for rows.Next() {
		if result.RowCount >= queryRowLimit {
			result.Truncated = true
			break
		}
		scan := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range scan {
			ptrs[i] = &scan[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeError(w, http.StatusBadRequest, "读取结果行失败: %v", err)
			return
		}
		result.Rows = append(result.Rows, jsonValues(scan))
		result.RowCount++
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusBadRequest, "查询失败: %v", err)
		return
	}
	result.ElapsedMs = time.Since(started).Milliseconds()
	writeJSON(w, http.StatusOK, result)
}

// jsonValues normalizes driver values into JSON-friendly ones.
func jsonValues(row []any) []any {
	out := make([]any, len(row))
	for i, v := range row {
		switch value := v.(type) {
		case nil:
			out[i] = nil
		case []byte:
			out[i] = string(value)
		case time.Time:
			out[i] = value.Format(time.RFC3339Nano)
		default:
			out[i] = value
		}
	}
	return out
}
