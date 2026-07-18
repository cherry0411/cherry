//go:build linux || darwin

package spool

import (
	"fmt"
	"os"
	"syscall"
)

// platformLock takes a non-blocking exclusive advisory lock via the stdlib
// syscall package (no external dependency). On Linux this is the production
// guarantee: the lock is released by the kernel when the process dies, so a
// crashed crawler's lock file is reclaimable on restart.
func platformLock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock: %w", err)
	}
	return nil
}

func platformUnlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

func supportsExistingLockReclaim() bool { return true }
