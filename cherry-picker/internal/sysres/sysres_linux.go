//go:build linux

package sysres

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

func totalMemory() uint64 {
	system := readMemInfoTotal()
	cgroup := readCgroupLimit()

	switch {
	case system == 0:
		return cgroup
	case cgroup == 0:
		return system
	case cgroup < system:
		return cgroup
	default:
		return system
	}
}

// readMemInfoTotal 解析 /proc/meminfo 的 MemTotal（kB）。
func readMemInfoTotal() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

// readCgroupLimit 读取 cgroup v2 或 v1 的内存限制，无限制返回 0。
func readCgroupLimit() uint64 {
	// cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "max" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				return v
			}
		}
		return 0
	}
	// cgroup v1
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		s := strings.TrimSpace(string(data))
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			// v1 无限制时返回一个接近 int64 最大值的数
			if v < 1<<60 {
				return v
			}
		}
	}
	return 0
}
