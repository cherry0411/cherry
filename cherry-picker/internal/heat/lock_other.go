//go:build !linux && !darwin

package heat

import "os"

func lockFile(*os.File) error   { return nil }
func unlockFile(*os.File) error { return nil }
func canReclaimLockFile() bool  { return false }
