package diff

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type atomicOutputFile interface {
	io.Writer
	Name() string
	Sync() error
	Close() error
}

type atomicOutputOps struct {
	createTemp func(string, string) (atomicOutputFile, error)
	rename     func(string, string) error
	remove     func(string) error
	stat       func(string) (os.FileInfo, error)
	openDir    func(string) (atomicOutputFile, error)
}

var defaultAtomicOutputOps = atomicOutputOps{
	createTemp: func(dir, pattern string) (atomicOutputFile, error) {
		return os.CreateTemp(dir, pattern)
	},
	rename: os.Rename,
	remove: os.Remove,
	stat:   os.Stat,
	openDir: func(path string) (atomicOutputFile, error) {
		return os.Open(path)
	},
}

type stagedOutput struct {
	file   atomicOutputFile
	writer *bufio.Writer
	path   string
}

type finalBackup struct {
	finalPath  string
	backupPath string
	existed    bool
}

func writeAtomicPair(forwardPath, rollbackPath string, write func(io.Writer, io.Writer) error) error {
	return writeAtomicPairWithOps(forwardPath, rollbackPath, write, defaultAtomicOutputOps)
}

func writeAtomicPairWithOps(forwardPath, rollbackPath string, write func(io.Writer, io.Writer) error, ops atomicOutputOps) error {
	if write == nil {
		return fmt.Errorf("atomic output writer is required")
	}
	forwardPath = filepath.Clean(forwardPath)
	rollbackPath = filepath.Clean(rollbackPath)
	if forwardPath == rollbackPath {
		return fmt.Errorf("forward and rollback output paths must be different: %s", forwardPath)
	}

	forward, err := createStagedOutput(forwardPath, ops)
	if err != nil {
		return fmt.Errorf("create forward temporary output: %w", err)
	}
	rollback, err := createStagedOutput(rollbackPath, ops)
	if err != nil {
		return errors.Join(
			fmt.Errorf("create rollback temporary output: %w", err),
			abortStagedOutputs(ops, forward),
		)
	}

	if err := write(forward.writer, rollback.writer); err != nil {
		return errors.Join(err, abortStagedOutputs(ops, forward, rollback))
	}
	if err := finishStagedOutputs(forward, rollback); err != nil {
		return errors.Join(err, cleanupPaths(ops, forward.path, rollback.path))
	}
	if err := commitStagedPair(forward.path, forwardPath, rollback.path, rollbackPath, ops); err != nil {
		return errors.Join(err, cleanupPaths(ops, forward.path, rollback.path))
	}
	return nil
}

func createStagedOutput(finalPath string, ops atomicOutputOps) (*stagedOutput, error) {
	dir := filepath.Dir(finalPath)
	file, err := ops.createTemp(dir, "."+filepath.Base(finalPath)+".tmp-*")
	if err != nil {
		return nil, err
	}
	return &stagedOutput{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   file.Name(),
	}, nil
}

func finishStagedOutputs(outputs ...*stagedOutput) error {
	var result error
	flushOK := true
	for _, output := range outputs {
		if err := output.writer.Flush(); err != nil {
			flushOK = false
			result = errors.Join(result, fmt.Errorf("flush temporary output %s: %w", output.path, err))
		}
	}
	if flushOK {
		for _, output := range outputs {
			if err := output.file.Sync(); err != nil {
				result = errors.Join(result, fmt.Errorf("sync temporary output %s: %w", output.path, err))
			}
		}
	}
	for _, output := range outputs {
		if err := output.file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close temporary output %s: %w", output.path, err))
		}
	}
	return result
}

func abortStagedOutputs(ops atomicOutputOps, outputs ...*stagedOutput) error {
	var result error
	for _, output := range outputs {
		if output == nil {
			continue
		}
		if err := output.file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close temporary output %s: %w", output.path, err))
		}
	}
	paths := make([]string, 0, len(outputs))
	for _, output := range outputs {
		if output != nil {
			paths = append(paths, output.path)
		}
	}
	return errors.Join(result, cleanupPaths(ops, paths...))
}

func commitStagedPair(forwardTemp, forwardFinal, rollbackTemp, rollbackFinal string, ops atomicOutputOps) error {
	forwardBackup, err := backupFinal(forwardFinal, ops)
	if err != nil {
		return fmt.Errorf("prepare forward output replacement: %w", err)
	}
	rollbackBackup, err := backupFinal(rollbackFinal, ops)
	if err != nil {
		return errors.Join(
			fmt.Errorf("prepare rollback output replacement: %w", err),
			restoreFinals(ops, forwardBackup),
		)
	}

	backups := []finalBackup{forwardBackup, rollbackBackup}
	if err := ops.rename(forwardTemp, forwardFinal); err != nil {
		return errors.Join(
			fmt.Errorf("publish forward output: %w", err),
			restoreFinals(ops, backups...),
		)
	}
	if err := ops.rename(rollbackTemp, rollbackFinal); err != nil {
		return errors.Join(
			fmt.Errorf("publish rollback output: %w", err),
			restoreFinals(ops, backups...),
		)
	}
	if err := syncOutputDirs(ops, forwardFinal, rollbackFinal); err != nil {
		return errors.Join(err, restoreFinals(ops, backups...))
	}

	// The final files are durable at this point. Backup cleanup cannot make the
	// committed pair partial, so cleanup is deliberately best-effort.
	for _, backup := range backups {
		if backup.existed {
			_ = removeIfExists(ops, backup.backupPath)
		}
	}
	return nil
}

func backupFinal(finalPath string, ops atomicOutputOps) (finalBackup, error) {
	backup := finalBackup{finalPath: finalPath}
	if _, err := ops.stat(finalPath); err != nil {
		if os.IsNotExist(err) {
			return backup, nil
		}
		return backup, err
	}

	placeholder, err := ops.createTemp(filepath.Dir(finalPath), "."+filepath.Base(finalPath)+".backup-*")
	if err != nil {
		return backup, err
	}
	backup.backupPath = placeholder.Name()
	if err := placeholder.Close(); err != nil {
		return backup, errors.Join(err, removeIfExists(ops, backup.backupPath))
	}
	if err := removeIfExists(ops, backup.backupPath); err != nil {
		return backup, err
	}
	if err := ops.rename(finalPath, backup.backupPath); err != nil {
		return backup, err
	}
	backup.existed = true
	return backup, nil
}

func restoreFinals(ops atomicOutputOps, backups ...finalBackup) error {
	var result error
	for i := len(backups) - 1; i >= 0; i-- {
		backup := backups[i]
		if err := removeIfExists(ops, backup.finalPath); err != nil {
			result = errors.Join(result, fmt.Errorf("remove incomplete output %s: %w", backup.finalPath, err))
			continue
		}
		if backup.existed {
			if err := ops.rename(backup.backupPath, backup.finalPath); err != nil {
				result = errors.Join(result, fmt.Errorf("restore previous output %s: %w", backup.finalPath, err))
			}
		}
	}
	if err := syncOutputDirs(ops, finalPaths(backups)...); err != nil {
		result = errors.Join(result, fmt.Errorf("sync restored output directories: %w", err))
	}
	return result
}

func finalPaths(backups []finalBackup) []string {
	paths := make([]string, 0, len(backups))
	for _, backup := range backups {
		paths = append(paths, backup.finalPath)
	}
	return paths
}

func syncOutputDirs(ops atomicOutputOps, paths ...string) error {
	seen := make(map[string]struct{})
	var result error
	for _, path := range paths {
		dir := filepath.Dir(path)
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		dirFile, err := ops.openDir(dir)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("open output directory %s: %w", dir, err))
			continue
		}
		if err := dirFile.Sync(); err != nil {
			result = errors.Join(result, fmt.Errorf("sync output directory %s: %w", dir, err))
		}
		if err := dirFile.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close output directory %s: %w", dir, err))
		}
	}
	return result
}

func cleanupPaths(ops atomicOutputOps, paths ...string) error {
	var result error
	for _, path := range paths {
		if path == "" {
			continue
		}
		if err := removeIfExists(ops, path); err != nil {
			result = errors.Join(result, fmt.Errorf("remove temporary output %s: %w", path, err))
		}
	}
	return result
}

func removeIfExists(ops atomicOutputOps, path string) error {
	err := ops.remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
