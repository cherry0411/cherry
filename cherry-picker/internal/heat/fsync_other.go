//go:build !linux && !darwin

package heat

// Production is Linux. Windows cannot fsync a directory handle through the Go
// standard library, so development builds cannot claim directory-entry
// power-loss durability even though file contents are still synchronized.
func fsyncDir(string) error { return nil }
