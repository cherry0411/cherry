//go:build !linux && !darwin

package spool

import "os"

// A freshly O_EXCL-created lock file is sufficient to prevent a second Open.
// If that file already exists, platforms without a kernel advisory lock cannot
// distinguish a live holder from a stale crash remnant and must fail closed.
func platformLock(_ *os.File) error { return nil }

func platformUnlock(_ *os.File) error { return nil }

func supportsExistingLockReclaim() bool { return false }
