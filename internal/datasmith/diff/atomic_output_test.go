package diff

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type faultAtomicFile struct {
	atomicOutputFile
	writeErr error
	syncErr  error
	closeErr error
}

func (f *faultAtomicFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.atomicOutputFile.Write(data)
}

func (f *faultAtomicFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.atomicOutputFile.Sync()
}

func (f *faultAtomicFile) Close() error {
	err := f.atomicOutputFile.Close()
	if f.closeErr != nil {
		return errors.Join(err, f.closeErr)
	}
	return err
}

func TestAtomicPairPropagatesOutputFailuresAndPreservesFinals(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*faultAtomicFile)
		wantMessage string
	}{
		{
			name: "flush",
			configure: func(file *faultAtomicFile) {
				file.writeErr = errors.New("injected write failure")
			},
			wantMessage: "flush temporary output",
		},
		{
			name: "sync",
			configure: func(file *faultAtomicFile) {
				file.syncErr = errors.New("injected sync failure")
			},
			wantMessage: "sync temporary output",
		},
		{
			name: "close",
			configure: func(file *faultAtomicFile) {
				file.closeErr = errors.New("injected close failure")
			},
			wantMessage: "close temporary output",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			forward := filepath.Join(dir, "forward.sql")
			rollback := filepath.Join(dir, "rollback.sql")
			writeOldFinals(t, forward, rollback)

			ops := defaultAtomicOutputOps
			created := 0
			ops.createTemp = func(dir, pattern string) (atomicOutputFile, error) {
				file, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				created++
				wrapped := &faultAtomicFile{atomicOutputFile: file}
				if created == 1 {
					test.configure(wrapped)
				}
				return wrapped, nil
			}

			err := writeAtomicPairWithOps(forward, rollback, func(forwardWriter, rollbackWriter io.Writer) error {
				if _, err := fmt.Fprintln(forwardWriter, "new forward"); err != nil {
					return err
				}
				_, err := fmt.Fprintln(rollbackWriter, "new rollback")
				return err
			}, ops)
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("expected %q error, got %v", test.wantMessage, err)
			}
			assertOldFinals(t, forward, rollback)
			assertNoTemporaryOutputs(t, dir)
		})
	}
}

func TestAtomicPairCallbackWriteErrorPreservesFinals(t *testing.T) {
	dir := t.TempDir()
	forward := filepath.Join(dir, "forward.sql")
	rollback := filepath.Join(dir, "rollback.sql")
	writeOldFinals(t, forward, rollback)

	err := writeAtomicPair(forward, rollback, func(forwardWriter, rollbackWriter io.Writer) error {
		if _, err := fmt.Fprintln(forwardWriter, "partial forward"); err != nil {
			return err
		}
		return errors.New("injected generation failure")
	})
	if err == nil || !strings.Contains(err.Error(), "injected generation failure") {
		t.Fatalf("expected generation error, got %v", err)
	}
	assertOldFinals(t, forward, rollback)
	assertNoTemporaryOutputs(t, dir)
}

func TestAtomicPairPropagatesImmediateWriteError(t *testing.T) {
	dir := t.TempDir()
	forward := filepath.Join(dir, "forward.sql")
	rollback := filepath.Join(dir, "rollback.sql")
	writeOldFinals(t, forward, rollback)

	ops := defaultAtomicOutputOps
	created := 0
	ops.createTemp = func(dir, pattern string) (atomicOutputFile, error) {
		file, err := os.CreateTemp(dir, pattern)
		if err != nil {
			return nil, err
		}
		created++
		wrapped := &faultAtomicFile{atomicOutputFile: file}
		if created == 1 {
			wrapped.writeErr = errors.New("injected immediate write failure")
		}
		return wrapped, nil
	}

	err := writeAtomicPairWithOps(forward, rollback, func(forwardWriter, rollbackWriter io.Writer) error {
		_, err := forwardWriter.Write(bytes.Repeat([]byte("x"), 64*1024))
		return err
	}, ops)
	if err == nil || !strings.Contains(err.Error(), "injected immediate write failure") {
		t.Fatalf("expected immediate write error, got %v", err)
	}
	assertOldFinals(t, forward, rollback)
	assertNoTemporaryOutputs(t, dir)
}

func TestAtomicPairSecondRenameFailureRestoresBothFinals(t *testing.T) {
	dir := t.TempDir()
	forward := filepath.Join(dir, "forward.sql")
	rollback := filepath.Join(dir, "rollback.sql")
	writeOldFinals(t, forward, rollback)

	ops := defaultAtomicOutputOps
	ops.rename = func(oldPath, newPath string) error {
		if newPath == rollback && strings.Contains(filepath.Base(oldPath), ".rollback.sql.tmp-") {
			return errors.New("injected second rename failure")
		}
		return os.Rename(oldPath, newPath)
	}
	err := writeAtomicPairWithOps(forward, rollback, func(forwardWriter, rollbackWriter io.Writer) error {
		if _, err := fmt.Fprintln(forwardWriter, "new forward"); err != nil {
			return err
		}
		_, err := fmt.Fprintln(rollbackWriter, "new rollback")
		return err
	}, ops)
	if err == nil || !strings.Contains(err.Error(), "publish rollback output") {
		t.Fatalf("expected rollback publish error, got %v", err)
	}
	assertOldFinals(t, forward, rollback)
	assertNoTemporaryOutputs(t, dir)
}

func TestAtomicPairSuccessReplacesBothFinals(t *testing.T) {
	dir := t.TempDir()
	forward := filepath.Join(dir, "forward.sql")
	rollback := filepath.Join(dir, "rollback.sql")
	writeOldFinals(t, forward, rollback)

	err := writeAtomicPair(forward, rollback, func(forwardWriter, rollbackWriter io.Writer) error {
		if _, err := fmt.Fprint(forwardWriter, "new forward"); err != nil {
			return err
		}
		_, err := fmt.Fprint(rollbackWriter, "new rollback")
		return err
	})
	if err != nil {
		t.Fatalf("write atomic pair: %v", err)
	}
	assertFileContent(t, forward, "new forward")
	assertFileContent(t, rollback, "new rollback")
	assertNoTemporaryOutputs(t, dir)
}

func writeOldFinals(t *testing.T, forward, rollback string) {
	t.Helper()
	if err := os.WriteFile(forward, []byte("old forward"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rollback, []byte("old rollback"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertOldFinals(t *testing.T, forward, rollback string) {
	t.Helper()
	assertFileContent(t, forward, "old forward")
	assertFileContent(t, rollback, "old rollback")
}

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(data, []byte(expected)) {
		t.Fatalf("%s content = %q, want %q", path, data, expected)
	}
}

func assertNoTemporaryOutputs(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") || strings.Contains(entry.Name(), ".backup-") {
			t.Fatalf("temporary output was not cleaned up: %s", entry.Name())
		}
	}
}
