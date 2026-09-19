package server

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"
)

// newID returns prefix_hex, where hex is 8 random bytes; falls back to a
// nanosecond suffix if crypto/rand fails.
func newID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return prefix + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return prefix + "_" + hex.EncodeToString(buf)
}

func joinPath(elem ...string) string { return filepath.Join(elem...) }

func queryInt(r *http.Request, name string, fallback int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func urlPathEscape(name string) string { return url.PathEscape(name) }
