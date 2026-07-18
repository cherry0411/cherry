package dht

import (
	"sync/atomic"
	"time"
)

// blockedItem represents a blocked node.
type blockedItem struct {
	ip         string
	port       int
	createTime time.Time
}

// blackList manages the blocked nodes including which sends bad information
// and can't ping out.
type blackList struct {
	list         *syncedMap
	maxSize      int
	expiredAfter time.Duration

	// 诊断计数器（累计，原子）。用于回答"黑名单是否已满、是否在静默丢弃
	// insert、有多少 entry 过期回收"——这些决定了 wire 漏斗是被真实的
	// 坏 peer 保护，还是被一个满载失效的黑名单拖累。
	insertAccepted atomic.Int64 // 成功写入的 insert 次数
	insertRejected atomic.Int64 // 因 Len>=maxSize 被静默放弃的 insert 次数
	expiredEvicted atomic.Int64 // clear() 回收的过期 entry 数
}

// newBlackList returns a blackList pointer.
func newBlackList(size int) *blackList {
	return &blackList{
		list:         newSyncedMap(),
		maxSize:      size,
		expiredAfter: time.Minute * 15,
	}
}

// BlacklistStats 是黑名单的诊断快照。
type BlacklistStats struct {
	Size           int   // 当前 entry 数
	MaxSize        int   // 容量上限
	InsertAccepted int64 // 累计成功写入
	InsertRejected int64 // 累计因满载被丢弃（静默 no-op 的次数）
	ExpiredEvicted int64 // 累计过期回收
}

// stats 返回黑名单诊断快照（Size 读一次 map 长度，其余为原子计数）。
func (bl *blackList) stats() BlacklistStats {
	return BlacklistStats{
		Size:           bl.list.Len(),
		MaxSize:        bl.maxSize,
		InsertAccepted: bl.insertAccepted.Load(),
		InsertRejected: bl.insertRejected.Load(),
		ExpiredEvicted: bl.expiredEvicted.Load(),
	}
}

// genKey returns a key. If port is less than 0, the key wil be ip. Ohterwise
// it will be `ip:port` format.
func (bl *blackList) genKey(ip string, port int) string {
	key := ip
	if port >= 0 {
		key = genAddress(ip, port)
	}
	return key
}

// insert adds a blocked item to the blacklist.
func (bl *blackList) insert(ip string, port int) {
	if bl.list.Len() >= bl.maxSize {
		// 满载：静默放弃。这本身是一个盲点信号——记录下来以便观测。
		bl.insertRejected.Add(1)
		return
	}

	bl.list.Set(bl.genKey(ip, port), &blockedItem{
		ip:         ip,
		port:       port,
		createTime: time.Now(),
	})
	bl.insertAccepted.Add(1)
}

// delete removes blocked item form the blackList.
func (bl *blackList) delete(ip string, port int) {
	bl.list.Delete(bl.genKey(ip, port))
}

// validate checks whether ip-port pair is in the block nodes list.
func (bl *blackList) in(ip string, port int) bool {
	if _, ok := bl.list.Get(ip); ok {
		return true
	}

	key := bl.genKey(ip, port)

	v, ok := bl.list.Get(key)
	if ok {
		if time.Now().Sub(v.(*blockedItem).createTime) < bl.expiredAfter {
			return true
		}
		if bl.list.Delete(key) {
			bl.expiredEvicted.Add(1)
		}
	}
	return false
}

// clear cleans the expired items every 10 minutes.
func (bl *blackList) clear() {
	for _ = range time.Tick(time.Minute * 3) {
		keys := make([]interface{}, 0, 100)

		for item := range bl.list.Iter() {
			if time.Now().Sub(
				item.val.(*blockedItem).createTime) > bl.expiredAfter {

				keys = append(keys, item.key)
			}
		}

		deleted := bl.list.DeleteMulti(keys)
		if deleted > 0 {
			bl.expiredEvicted.Add(int64(deleted))
		}
	}
}
