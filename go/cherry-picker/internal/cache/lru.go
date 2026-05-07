// Package cache 提供线程安全的有界 LRU 缓存，用于替换无限增长的 seenSet 和 remoteKnown。
package cache

import (
	"container/list"
	"sync"
)

// LRU 是线程安全的有界 LRU 缓存，使用双向链表 + map 实现 O(1) set/get。
//
// 语义：
//   - Set(key) bool：key 不存在时插入并返回 true（新条目），存在时将其移至链表头部并返回 false（已见过）
//   - Contains(key) bool：检查 key 是否存在，不更新 LRU 顺序（用于只读检查）
//   - Add(key) bool：同 Set，语义一致，供外部调用
type LRU struct {
	mu       sync.Mutex
	capacity int
	list     *list.List
	items    map[string]*list.Element
}

type entry struct {
	key string
}

// NewLRU 创建一个容量为 capacity 的 LRU 缓存。capacity 必须 >= 1。
func NewLRU(capacity int) *LRU {
	if capacity < 1 {
		capacity = 1
	}
	return &LRU{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[string]*list.Element, capacity),
	}
}

// Set 插入 key。若 key 不存在，插入并返回 true；若已存在，将其移至头部并返回 false。
// 当缓存满时，自动淘汰最久未使用（链表尾部）的条目。
//
// "seen" 语义：返回 false 表示已见过，返回 true 表示首次出现。
func (c *LRU) Set(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 已存在：移至头部，返回 false（已见过）
	if elem, ok := c.items[key]; ok {
		c.list.MoveToFront(elem)
		return false
	}

	// 容量已满：淘汰链表尾部（最旧）的条目
	if c.list.Len() >= c.capacity {
		c.evict()
	}

	// 插入新条目到链表头部
	elem := c.list.PushFront(&entry{key: key})
	c.items[key] = elem
	return true
}

// Contains 检查 key 是否存在，不更新 LRU 顺序。O(1)。
func (c *LRU) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[key]
	return ok
}

// Add 插入 key，与 Set 语义相同。返回 true 表示 key 是新插入的。
func (c *LRU) Add(key string) bool {
	return c.Set(key)
}

// Len 返回当前缓存中的条目数。
func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

// evict 淘汰链表尾部最旧的条目，调用方必须已持有锁。
func (c *LRU) evict() {
	back := c.list.Back()
	if back == nil {
		return
	}
	c.list.Remove(back)
	delete(c.items, back.Value.(*entry).key)
}
