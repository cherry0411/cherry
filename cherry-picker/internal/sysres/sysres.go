// Package sysres 探测机器/容器的物理资源，供启动时按资源规模推导参数。
//
// TotalMemory 返回对进程生效的内存上限（字节）：
//   - Linux：min(物理内存, cgroup 内存限制)（容器内取 cgroup 值）
//   - Windows：物理内存
//   - 探测失败返回 0，调用方应使用保守默认值
package sysres

// TotalMemory 返回对进程生效的内存上限（字节），未知返回 0。
func TotalMemory() uint64 {
	return totalMemory()
}
