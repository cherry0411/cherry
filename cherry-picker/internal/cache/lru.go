// Package cache 提供线程安全的有界 LRU 缓存，用于替换无限增长的 seenSet 和 remoteKnown。
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// LRU 是线程安全的有界 LRU 缓存，使用双向链表 + map 实现 O(1) set/get。
//
// 语义：
//   - Set(key) bool：key 不存在时插入并返回 true（新条目），存在时将其移至链表头部并返回 false（已见过）
//   - Contains(key) bool：检查 key 是否存在，不更新 LRU 顺序（用于只读检查）
//   - ContainsAndTouch(key) bool：检查 key，并在命中时刷新 LRU 顺序
//   - Add(key) bool：同 Set，语义一致，供外部调用
type LRU struct {
	shards []*lruShard
	// ttlSeconds is zero for the legacy non-expiring cache. Expiration is
	// checked lazily on key access; cold expired entries remain bounded by the
	// normal LRU capacity and disappear on access or capacity eviction.
	ttlSeconds uint32

	// observedUnix is a deliberately coarse clock. Snapshot advances it once
	// per telemetry interval; hot Set/ContainsAndTouch operations only perform
	// an atomic load instead of calling time.Now. OldestAgeSeconds is therefore
	// accurate to one telemetry interval while avoiding time.Now on the request
	// path.
	observedUnix atomic.Uint32
}

type lruShard struct {
	mu           sync.Mutex
	capacity     int
	list         *list.List
	items        map[string]*list.Element
	hits         uint64
	misses       uint64
	inserts      uint64
	evicts       uint64
	deleteMisses uint64
	expirations  uint64
}

type entry struct {
	key         string
	insertedAt  uint32
	lastTouched uint32
}

// LRUStats is a cumulative, race-free cache snapshot. Len, Capacity and
// OldestAgeSeconds are gauges; all other fields are monotonic counters.
// Snapshot visits only the fixed shard set (at most 64), never every entry.
type LRUStats struct {
	Len              int
	Capacity         int
	OldestAgeSeconds uint64
	Hits             uint64
	Misses           uint64
	Inserts          uint64
	Evicts           uint64
	DeleteMisses     uint64
	Expirations      uint64
}

// NewLRU 创建一个容量为 capacity 的 LRU 缓存。capacity 必须 >= 1。
func NewLRU(capacity int) *LRU {
	return newLRU(capacity, 0)
}

// NewLRUWithTTL creates a bounded LRU whose entries become absent ttl after
// their first insertion. Duplicate observations do not extend the deadline:
// this is deliberate for attempt/failure cooldowns, where a noisy dead peer
// must eventually become retryable. A non-positive ttl preserves NewLRU's
// non-expiring behavior.
func NewLRUWithTTL(capacity int, ttl time.Duration) *LRU {
	var ttlSeconds uint32
	if ttl > 0 {
		seconds := (ttl + time.Second - 1) / time.Second
		if seconds > time.Duration(^uint32(0)) {
			seconds = time.Duration(^uint32(0))
		}
		ttlSeconds = uint32(seconds)
	}
	return newLRU(capacity, ttlSeconds)
}

func newLRU(capacity int, ttlSeconds uint32) *LRU {
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

	c := &LRU{shards: shards, ttlSeconds: ttlSeconds}
	c.observedUnix.Store(uint32(time.Now().Unix()))
	return c
}

// Set 插入 key。若 key 不存在，插入并返回 true；若已存在，将其移至头部并返回 false。
// 当缓存满时，自动淘汰最久未使用（链表尾部）的条目。
//
// "seen" 语义：返回 false 表示已见过，返回 true 表示首次出现。
func (c *LRU) Set(key string) bool {
	shard := c.shardFor(key)
	now := c.observedUnix.Load()
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 已存在：移至头部，返回 false（已见过）
	if elem, ok := shard.items[key]; ok {
		if c.expired(now, elem.Value.(*entry)) {
			shard.list.Remove(elem)
			delete(shard.items, key)
			shard.expirations++
		} else {
			shard.hits++
			elem.Value.(*entry).lastTouched = now
			shard.list.MoveToFront(elem)
			return false
		}
	}
	shard.misses++

	// 容量已满：淘汰链表尾部（最旧）的条目
	if shard.list.Len() >= shard.capacity {
		shard.evict()
	}

	// 插入新条目到链表头部
	elem := shard.list.PushFront(&entry{key: key, insertedAt: now, lastTouched: now})
	shard.items[key] = elem
	shard.inserts++
	return true
}

// Contains 检查 key 是否存在，不更新 LRU 顺序。O(1)。
func (c *LRU) Contains(key string) bool {
	shard := c.shardFor(key)
	now := c.observedUnix.Load()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	elem, ok := shard.items[key]
	if ok && c.expired(now, elem.Value.(*entry)) {
		shard.list.Remove(elem)
		delete(shard.items, key)
		shard.expirations++
		ok = false
	}
	if ok {
		shard.hits++
	} else {
		shard.misses++
	}
	return ok
}

// ContainsAndTouch 检查 key 是否存在；命中时将其移到链表头部。
// 适用于 remote-known 之类的热点集合，避免持续命中的 key 因插入洪峰
// 仍被当成冷数据淘汰。
func (c *LRU) ContainsAndTouch(key string) bool {
	shard := c.shardFor(key)
	now := c.observedUnix.Load()
	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, ok := shard.items[key]
	if ok && c.expired(now, elem.Value.(*entry)) {
		shard.list.Remove(elem)
		delete(shard.items, key)
		shard.expirations++
		ok = false
	}
	if ok {
		shard.hits++
		elem.Value.(*entry).lastTouched = now
		shard.list.MoveToFront(elem)
	} else {
		shard.misses++
	}
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
		shard.deleteMisses++
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

// Snapshot returns cache health without an O(N) entry scan. Each shard's list
// tail is its least recently used resident, so reading the coarse last-touch
// timestamp from at most 64 tails is sufficient. The crawler advances the
// clock every 30 seconds, bounding age precision without time.Now on hot paths.
func (c *LRU) Snapshot() LRUStats {
	now := uint32(time.Now().Unix())
	c.observedUnix.Store(now)

	var stats LRUStats
	for _, shard := range c.shards {
		shard.mu.Lock()
		stats.Len += shard.list.Len()
		stats.Capacity += shard.capacity
		stats.Hits += shard.hits
		stats.Misses += shard.misses
		stats.Inserts += shard.inserts
		stats.Evicts += shard.evicts
		stats.DeleteMisses += shard.deleteMisses
		stats.Expirations += shard.expirations
		if tail := shard.list.Back(); tail != nil {
			touched := tail.Value.(*entry).lastTouched
			if now >= touched {
				age := uint64(now - touched)
				if age > stats.OldestAgeSeconds {
					stats.OldestAgeSeconds = age
				}
			}
		}
		shard.mu.Unlock()
	}
	return stats
}

// expired uses unsigned subtraction so normal uint32 Unix-second wraparound
// remains well-defined for TTLs below half the uint32 range (all supported
// production cooldowns are minutes, not decades).
func (c *LRU) expired(now uint32, value *entry) bool {
	return c.ttlSeconds > 0 && now-value.insertedAt >= c.ttlSeconds
}

// evict 淘汰链表尾部最旧的条目，调用方必须已持有锁。
func (s *lruShard) evict() {
	back := s.list.Back()
	if back == nil {
		return
	}
	s.list.Remove(back)
	delete(s.items, back.Value.(*entry).key)
	s.evicts++
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
