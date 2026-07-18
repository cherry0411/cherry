//go:build !linux && !darwin

package spool

// fsyncDir is a no-op on platforms (notably Windows) where a directory handle
// cannot be fsynced. The crash-safe power-loss guarantee for directory metadata
// (create/rename/delete durability) is therefore Linux/darwin-only. Production
// runs on Linux; this build exists so development/tests compile and run on
// other platforms without pretending to offer a guarantee the OS does not.
func fsyncDir(_ string) error { return nil }
