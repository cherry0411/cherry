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
	shards []*lruShard
}

type lruShard struct {
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

	shardCount := 64
	if capacity < shardCount {
		shardCount = capacity
	}
	if shardCount < 1 {
		shardCount = 1
	}

	shards := make([]*lruShard, 0, shardCount)
	baseCap := capacity / shardCount
	remainder := capacity % shardCount
	for i := 0; i < shardCount; i++ {
		shardCap := baseCap
		if i < remainder {
			shardCap++
		}
		if shardCap < 1 {
			shardCap = 1
		}
		// map 初始 hint 封顶：make(map, n) 会立即预分配 n 容量的桶数组，
		// 百万级容量下 5 个 LRU 会在启动时空耗数百 MB。让 map 按实际
		// 填充增长，容量上限仍由 evict 保证。
		hint := shardCap
		if hint > 4096 {
			hint = 4096
		}
		shards = append(shards, &lruShard{
			capacity: shardCap,
			list:     list.New(),
			items:    make(map[string]*list.Element, hint),
		})
	}

	return &LRU{shards: shards}
}

// Set 插入 key。若 key 不存在，插入并返回 true；若已存在，将其移至头部并返回 false。
// 当缓存满时，自动淘汰最久未使用（链表尾部）的条目。
//
// "seen" 语义：返回 false 表示已见过，返回 true 表示首次出现。
func (c *LRU) Set(key string) bool {
	shard := c.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 已存在：移至头部，返回 false（已见过）
	if elem, ok := shard.items[key]; ok {
		shard.list.MoveToFront(elem)
		return false
	}

	// 容量已满：淘汰链表尾部（最旧）的条目
	if shard.list.Len() >= shard.capacity {
		shard.evict()
	}

	// 插入新条目到链表头部
	elem := shard.list.PushFront(&entry{key: key})
	shard.items[key] = elem
	return true
}

// Contains 检查 key 是否存在，不更新 LRU 顺序。O(1)。
func (c *LRU) Contains(key string) bool {
	shard := c.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	_, ok := shard.items[key]
	return ok
}

// Add 插入 key，与 Set 语义相同。返回 true 表示 key 是新插入的。
func (c *LRU) Add(key string) bool {
	return c.Set(key)
}

// Delete 删除 key。若 key 存在，返回 true。
func (c *LRU) Delete(key string) bool {
	shard := c.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, ok := shard.items[key]
	if !ok {
		return false
	}

	shard.list.Remove(elem)
	delete(shard.items, key)
	return true
}

// Cap 返回缓存的总容量。
func (c *LRU) Cap() int {
	total := 0
	for _, shard := range c.shards {
		total += shard.capacity
	}
	return total
}

// Len 返回当前缓存中的条目数。
func (c *LRU) Len() int {
	total := 0
	for _, shard := range c.shards {
		shard.mu.Lock()
		total += shard.list.Len()
		shard.mu.Unlock()
	}
	return total
}

// evict 淘汰链表尾部最旧的条目，调用方必须已持有锁。
func (s *lruShard) evict() {
	back := s.list.Back()
	if back == nil {
		return
	}
	s.list.Remove(back)
	delete(s.items, back.Value.(*entry).key)
}

func (c *LRU) shardFor(key string) *lruShard {
	if len(c.shards) == 1 {
		return c.shards[0]
	}

	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return c.shards[uint(h)%uint(len(c.shards))]
}
