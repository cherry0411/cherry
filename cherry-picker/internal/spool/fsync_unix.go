//go:build linux || darwin

package spool

import "os"

// fsyncDir performs a real directory fsync. On Linux this is required so that a
// file create/rename/delete survives power loss; without it the directory entry
// may be lost even though the file's data was synced.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
