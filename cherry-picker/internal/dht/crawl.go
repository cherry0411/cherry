// Package dht — crawl 模式高性能响应处理。
//
// 在爬虫模式下，所有出站请求使用"火发即忘"（fire-and-forget）策略，
// 不通过 transactionManager 跟踪事务，因此：
//
//  1. 标准 handleResponse 中的 filterOne() 检查始终返回 nil（没有已注册的事务）
//     → 所有响应被静默丢弃。
//  2. 本文件提供 handleResponseCrawl() 替代标准路径：
//     - 解析并插入响应中的 compact node 信息
//     - 对 get_peers 响应的 values（peers）通过 crawlTxBuf 环形缓冲
//     还原原始 info_hash，并触发 OnGetPeersResponse 回调。
package dht

import (
	"net"
	"sync/atomic"
)

// crawlToken 是爬虫模式 get_peers 响应中使用的固定 token。
// 由于爬虫模式对所有 announce_peer 均接受（跳过 token 验证），
// 这里使用固定值可完全避免 tokenManager 映射表无限增长的内存问题。
const crawlToken = "\x00\x01\x02\x03\x04"

// handleResponseCrawl 在爬虫模式下处理入站响应，无需事务匹配。
//
// 处理逻辑：
//   - 解析响应中的 compact node 信息并插入路由表（供下次 Fresh() 联系）
//   - 若响应包含 values（peer 列表），通过 crawlTxBuf 环形缓冲还原 info_hash
//     并触发 OnGetPeersResponse 回调
func handleResponseCrawl(dht *DHT, addr *net.UDPAddr, response map[string]interface{}) bool {
	if err := ParseKey(response, "r", "map"); err != nil {
		return false
	}
	r := response["r"].(map[string]interface{})

	// 快速路径：路由表已满时，只处理包含 values 的 get_peers 响应。
	// find_node 响应（只有 nodes、没有 values）占全部响应的 ~80-90%，
	// 当表满时它们只会白白消耗 CPU（解析节点 → Insert 立即失败）。
	// 跳过它们可节省：bencode node 解析 × 8 + newNode 分配 × 9 + Insert 写锁竞争。
	tableFull := atomic.LoadInt64(&dht.routingTable.nodeCount) >= int64(dht.MaxNodes)

	if dht.OnGetPeersResponse != nil {
		if values, ok := r["values"].([]interface{}); ok {
			t := response["t"].(string)
			idx := crawlTxIdx(t)
			ih := dht.crawlTxBuf[idx]
			if ih != [20]byte{} {
				infoHash := string(ih[:])
				token, _ := r["token"].(string)
				for _, v := range values {
					peer, err := newPeerFromCompactIPPortInfo(v.(string), token)
					if err == nil {
						dht.OnGetPeersResponse(infoHash, peer)
					}
				}
			}
			return true
		}
	}

	// 路由表已满且没有 values → 这个响应没有价值，直接跳过全部处理。
	if tableFull {
		return true
	}

	// 路由表未满：解析 compact nodes 并插入路由表以填充表。
	id, ok := r["id"].(string)
	if !ok || len(id) != 20 {
		return false
	}

	if nodesStr, ok := r["nodes"].(string); ok && len(nodesStr) > 0 && len(nodesStr)%26 == 0 {
		for i := 0; i < len(nodesStr)/26; i++ {
			no, err := newNodeFromCompactInfo(nodesStr[i*26:(i+1)*26], dht.Network)
			if err == nil {
				dht.routingTable.Insert(no)
			}
		}
	}

	if no, err := newNodeFromAddr(id, addr); err == nil {
		dht.routingTable.Insert(no)
	}
	return true
}

// crawlTxIdx 将 KRPC 事务 ID 字符串解码为 crawlTxBuf 的 16 位环形索引。
// KRPC 事务 ID 是变长字节串；爬虫模式下我们用 2 字节（大端）编码 uint16 计数器。
func crawlTxIdx(t string) uint16 {
	switch len(t) {
	case 0:
		return 0
	case 1:
		return uint16(t[0])
	default:
		return uint16(t[0])<<8 | uint16(t[1])
	}
}
