//go:build !linux && !windows

package sysres

func totalMemory() uint64 {
	return 0
}
