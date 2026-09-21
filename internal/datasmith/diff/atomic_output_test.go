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

func TestFullOutputTransactionDataPhaseFailurePreservesPreviousGeneration(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	tx, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range SchemaDiffFileNames() {
		writeArtifact(t, tx.stagingDir, name, "new "+name)
	}
	if err := tx.abort(); err != nil {
		t.Fatalf("abort after injected data-phase failure: %v", err)
	}
	assertOldFullFinals(t, dir)
	assertNoTemporaryOutputs(t, dir)
}

func TestFullOutputTransactionDiskWriteFailurePreservesPreviousGeneration(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	tx, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatal(err)
	}
	faultOps := defaultAtomicOutputOps
	created := 0
	faultOps.createTemp = func(dir, pattern string) (atomicOutputFile, error) {
		file, err := os.CreateTemp(dir, pattern)
		if err != nil {
			return nil, err
		}
		created++
		wrapped := &faultAtomicFile{atomicOutputFile: file}
		if created == 1 {
			wrapped.writeErr = errors.New("injected full-diff disk write failure")
		}
		return wrapped, nil
	}
	err = writeAtomicPairWithOps(
		filepath.Join(tx.stagingDir, DataDiffForwardFile),
		filepath.Join(tx.stagingDir, DataDiffRollbackFile),
		func(forward, rollback io.Writer) error {
			if _, err := fmt.Fprintln(forward, "new data forward"); err != nil {
				return err
			}
			_, err := fmt.Fprintln(rollback, "new data rollback")
			return err
		},
		faultOps,
	)
	if err == nil || !strings.Contains(err.Error(), "injected full-diff disk write failure") {
		t.Fatalf("expected staged disk write failure, got %v", err)
	}
	if err := tx.abort(); err != nil {
		t.Fatalf("abort after injected disk write failure: %v", err)
	}
	assertOldFullFinals(t, dir)
	assertNoTemporaryOutputs(t, dir)
}

func TestFullOutputTransactionPublishFailureRestoresAllFourFinals(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	ops := defaultAtomicOutputOps
	ops.rename = func(oldPath, newPath string) error {
		if filepath.Base(newPath) == DataDiffForwardFile && strings.Contains(oldPath, ".datasmith-full-") {
			return errors.New("injected third publish failure")
		}
		return os.Rename(oldPath, newPath)
	}
	tx, err := beginFullOutputTransaction(dir, ops)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range FullDiffFileNames() {
		writeArtifact(t, tx.stagingDir, name, "new "+name)
	}
	err = tx.commit()
	if err == nil || !strings.Contains(err.Error(), "injected third publish failure") {
		t.Fatalf("expected publish failure, got %v", err)
	}
	if err := tx.abort(); err != nil {
		t.Fatalf("abort after publish failure: %v", err)
	}
	assertOldFullFinals(t, dir)
	assertNoTemporaryOutputs(t, dir)
}

func TestFullOutputTransactionRecoversInterruptedPublicationToCompleteOldGeneration(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	tx, journal := prepareInterruptedFullOutputTransaction(t, dir)

	// Simulate SIGKILL after every old file was backed up and only half of the
	// new fixed-name files were published. No in-process error path runs.
	for _, entry := range journal.Outputs {
		if err := os.Rename(filepath.Join(dir, entry.Name), fullOutputBackupPath(dir, entry.Name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range FullDiffFileNames()[:2] {
		if err := os.Rename(filepath.Join(tx.stagingDir, name), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	next, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatalf("recover interrupted publication: %v", err)
	}
	assertOldFullFinals(t, dir)
	if err := next.abort(); err != nil {
		t.Fatal(err)
	}
	assertNoTemporaryOutputs(t, dir)
}

func TestFullOutputTransactionRecoversCommittedPublicationToCompleteNewGeneration(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	tx, journal := prepareInterruptedFullOutputTransaction(t, dir)

	// Simulate SIGKILL after the durable commit marker but before journal and
	// backup cleanup. Recovery must retain the complete new generation.
	for _, entry := range journal.Outputs {
		if err := os.Rename(filepath.Join(dir, entry.Name), fullOutputBackupPath(dir, entry.Name)); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(tx.stagingDir, entry.Name), filepath.Join(dir, entry.Name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeFullOutputControlFile(dir, fullOutputCommitName, map[string]bool{"committed": true}, defaultAtomicOutputOps); err != nil {
		t.Fatal(err)
	}

	next, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatalf("recover committed publication: %v", err)
	}
	for _, name := range FullDiffFileNames() {
		assertFileContent(t, filepath.Join(dir, name), "new "+name)
	}
	if err := next.abort(); err != nil {
		t.Fatal(err)
	}
	assertNoTemporaryOutputs(t, dir)
}

func TestFullOutputTransactionRemovesOrphanCommitMarkerBeforeStarting(t *testing.T) {
	dir := t.TempDir()
	writeOldFullFinals(t, dir)
	if err := writeFullOutputControlFile(dir, fullOutputCommitName, map[string]bool{"committed": true}, defaultAtomicOutputOps); err != nil {
		t.Fatal(err)
	}

	tx, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatal(err)
	}
	assertOldFullFinals(t, dir)
	if err := tx.abort(); err != nil {
		t.Fatal(err)
	}
	assertNoTemporaryOutputs(t, dir)
}

func prepareInterruptedFullOutputTransaction(t *testing.T, dir string) (*fullOutputTransaction, fullOutputJournal) {
	t.Helper()
	tx, err := beginFullOutputTransaction(dir, defaultAtomicOutputOps)
	if err != nil {
		t.Fatal(err)
	}
	journal := fullOutputJournal{StagingDir: filepath.Base(tx.stagingDir)}
	for _, name := range FullDiffFileNames() {
		writeArtifact(t, tx.stagingDir, name, "new "+name)
		journal.Outputs = append(journal.Outputs, fullOutputJournalEntry{Name: name, Existed: true})
	}
	if err := writeFullOutputControlFile(dir, fullOutputJournalName, journal, defaultAtomicOutputOps); err != nil {
		t.Fatal(err)
	}
	return tx, journal
}

func writeOldFullFinals(t *testing.T, dir string) {
	t.Helper()
	for _, name := range FullDiffFileNames() {
		writeArtifact(t, dir, name, "old "+name)
	}
}

func assertOldFullFinals(t *testing.T, dir string) {
	t.Helper()
	for _, name := range FullDiffFileNames() {
		assertFileContent(t, filepath.Join(dir, name), "old "+name)
	}
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
		if strings.Contains(entry.Name(), ".tmp-") || strings.Contains(entry.Name(), ".backup-") || strings.Contains(entry.Name(), ".previous-") || strings.Contains(entry.Name(), ".datasmith-full-") {
			t.Fatalf("temporary output was not cleaned up: %s", entry.Name())
		}
	}
}
