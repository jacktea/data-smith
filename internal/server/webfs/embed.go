// Package webfs embeds the compiled web console assets so the datasmith web
// subcommand serves a single self-contained binary. scripts/build.sh replaces
// the placeholder dist with the real frontend build output.
package webfs

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// Dist returns the filesystem rooted at the embedded dist directory.
func Dist() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		// Unreachable: dist is embedded at compile time.
		panic(err)
	}
	return sub
}
