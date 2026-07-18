package spool

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var ErrAlreadyLocked = errors.New("spool: directory already locked or stale lock cannot be safely reclaimed")

func acquireLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		if err := platformLock(f); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("spool: lock new file: %w", err)
		}
		if err := writeLockInfo(f); err != nil {
			_ = releaseLock(f, dir)
			return nil, err
		}
		if err := fsyncDir(dir); err != nil {
			_ = releaseLock(f, dir)
			return nil, fmt.Errorf("spool: sync lock creation: %w", err)
		}
		return f, nil
	}
	if !os.IsExist(err) {
		return nil, fmt.Errorf("spool: create lock: %w", err)
	}
	if !supportsExistingLockReclaim() {
		return nil, ErrAlreadyLocked
	}

	// Unix: open the existing inode and acquire flock before treating it as a
	// stale crash remnant. A live holder keeps this operation fail-closed.
	f, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spool: open existing lock: %w", err)
	}
	if err := platformLock(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %v", ErrAlreadyLocked, err)
	}
	if err := f.Truncate(0); err != nil {
		_ = platformUnlock(f)
		_ = f.Close()
		return nil, fmt.Errorf("spool: truncate reclaimed lock: %w", err)
	}
	if err := writeLockInfo(f); err != nil {
		_ = platformUnlock(f)
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func writeLockInfo(f *os.File) error {
	data := []byte(fmt.Sprintf("pid=%d\n", os.Getpid()))
	n, err := f.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("spool: write lock info: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("spool: write lock info: %w", io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("spool: sync lock info: %w", err)
	}
	return nil
}

// releaseLock unlinks and directory-syncs the lock name while the kernel lock
// is still held. Only then does it unlock and close, eliminating the window in
// which another opener could lock the old inode before its path was removed.
func releaseLock(f *os.File, dir string) error {
	if f == nil {
		return nil
	}
	path := filepath.Join(dir, lockName)
	var errs []error
	pathInfo, pathErr := os.Stat(path)
	fileInfo, fileErr := f.Stat()
	if pathErr != nil {
		errs = append(errs, fmt.Errorf("spool: stat lock path during release: %w", pathErr))
	} else if fileErr != nil {
		errs = append(errs, fmt.Errorf("spool: stat held lock during release: %w", fileErr))
	} else if !os.SameFile(pathInfo, fileInfo) {
		errs = append(errs, errors.New("spool: lock path was replaced while held"))
	} else if supportsExistingLockReclaim() {
		if err := os.Remove(path); err != nil {
			errs = append(errs, fmt.Errorf("spool: unlink lock while held: %w", err))
		} else if err := fsyncDir(dir); err != nil {
			errs = append(errs, fmt.Errorf("spool: sync directory after lock unlink: %w", err))
		}
	}
	if err := platformUnlock(f); err != nil {
		errs = append(errs, fmt.Errorf("spool: unlock: %w", err))
	}
	if err := f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("spool: close lock: %w", err))
	}
	if !supportsExistingLockReclaim() && pathErr == nil && fileErr == nil && os.SameFile(pathInfo, fileInfo) {
		// There is no unlock/reclaim race here: on these platforms every Open
		// fails closed while the path exists. Remove it only after Windows has
		// released the open handle, then a new owner may O_EXCL-create it.
		if err := os.Remove(path); err != nil {
			errs = append(errs, fmt.Errorf("spool: remove closed non-Unix lock: %w", err))
		} else if err := fsyncDir(dir); err != nil {
			errs = append(errs, fmt.Errorf("spool: sync directory after lock removal: %w", err))
		}
	}
	return errors.Join(errs...)
}
