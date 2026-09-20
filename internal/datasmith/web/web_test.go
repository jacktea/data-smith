package web

import (
	"bytes"
	"strings"
	"testing"
)

func TestWebCommandDefaultsToLoopback(t *testing.T) {
	cmd := newWebCommand()

	flag := cmd.Flags().Lookup("addr")
	if flag == nil {
		t.Fatal("web command must expose --addr")
	}
	if got, want := flag.DefValue, "127.0.0.1:8080"; got != want {
		t.Fatalf("--addr default = %q, want %q", got, want)
	}
}

func TestWebCommandWarnsForExplicitNonLoopbackListen(t *testing.T) {
	cmd := newWebCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	if err := cmd.ParseFlags([]string{"--addr", "0.0.0.0:8080"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if cmd.PreRunE == nil {
		t.Fatal("web command must check the listen address before starting the server")
	}
	if err := cmd.PreRunE(cmd, nil); err != nil {
		t.Fatalf("pre-run validation: %v", err)
	}
	if got := stderr.String(); !strings.Contains(got, "非 loopback") || !strings.Contains(got, "认证") {
		t.Fatalf("stderr = %q, want explicit non-loopback authentication warning", got)
	}
}
