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

	if samples, ok := r["samples"].(string); ok {
		if interval, ok := r["interval"].(int); ok {
			dht.markSampleInterval(addr.String(), interval)
		}
		if dht.OnSampleInfoHashes != nil && len(samples)%20 == 0 {
			dht.OnSampleInfoHashes(samples)
		}
		return true
	}

	if dht.OnGetPeersResponse != nil {
		if values, ok := r["values"].([]interface{}); ok {
			t := response["t"].(string)
			if ih, ok := dht.crawlInfoHash(t); ok {
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
	if nodesStr, ok := r["nodes"].(string); ok {
		followCrawlGetPeersResponse(dht, response["t"].(string), nodesStr)
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

func handleResponseCrawlFast(dht *DHT, addr *net.UDPAddr, pkt crawlPacket) bool {
	if !pkt.hasPayload {
		return false
	}

	tableFull := atomic.LoadInt64(&dht.routingTable.nodeCount) >= int64(dht.MaxNodes)

	if pkt.hasSamples {
		dht.markSampleInterval(addr.String(), pkt.interval)
		if dht.OnSampleInfoHashes != nil && len(pkt.samples)%20 == 0 {
			dht.OnSampleInfoHashes(pkt.samples)
		}
		return true
	}

	if dht.OnGetPeersResponse != nil && pkt.hasValues {
		if ih, ok := dht.crawlInfoHash(pkt.t); ok {
			infoHash := string(ih[:])
			for _, v := range pkt.values {
				peer, err := newPeerFromCompactIPPortInfo(v, pkt.token)
				if err == nil {
					dht.OnGetPeersResponse(infoHash, peer)
				}
			}
		}
		return true
	}
	followCrawlGetPeersResponse(dht, pkt.t, pkt.nodes)

	if tableFull {
		return true
	}

	if len(pkt.nodes) > 0 {
		if len(pkt.nodes)%26 != 0 {
			return false
		}
		for i := 0; i < len(pkt.nodes)/26; i++ {
			no, err := newNodeFromCompactInfo(pkt.nodes[i*26:(i+1)*26], dht.Network)
			if err == nil {
				dht.routingTable.Insert(no)
			}
		}
	}

	if len(pkt.id) == 20 {
		if no, err := newNodeFromAddr(pkt.id, addr); err == nil {
			dht.routingTable.Insert(no)
		}
	}
	return true
}

// followCrawlGetPeersResponse advances one bounded step toward the target.
// BEP 5 responses contain the closest known nodes; deployed implementations
// conventionally order that compact list by distance, so the first valid,
// non-blacklisted endpoint gives a cheap alpha=1 continuation. A response can
// trigger at most one query, preventing amplification.
func followCrawlGetPeersResponse(dht *DHT, txID, nodes string) bool {
	if len(nodes) < 26 || len(nodes)%26 != 0 {
		return false
	}
	entry, ok := dht.crawlTransaction(txID)
	if !ok || entry.followups == 0 {
		return false
	}
	nodeCount := len(nodes) / 26
	start := 0
	if dht.SpreadFollowups {
		start = int(entry.counter % uint32(nodeCount))
	}
	for offset := 0; offset < nodeCount; offset++ {
		i := ((start + offset) % nodeCount) * 26
		if !crawlNodeIsCloser(nodes[i:i+20], entry.nodeID, entry.infoHash) {
			continue
		}
		no, err := newNodeFromCompactInfo(nodes[i:i+26], dht.Network)
		if err != nil || dht.blackList.in(no.addr.IP.String(), no.addr.Port) {
			continue
		}
		sendCrawlGetPeersQueryWithFollowups(dht, no, string(entry.infoHash[:]), entry.followups-1)
		dht.stats.followupsSent.Add(1)
		return true
	}
	return false
}

// crawlNodeIsCloser enforces monotonic progress toward the infohash without
// allocating bitmaps. It prevents a malicious or stale response from bouncing
// a bounded lookup between the same nodes, which makes deeper chains safe.
func crawlNodeIsCloser(candidate string, current, target [20]byte) bool {
	if len(candidate) != 20 {
		return false
	}
	if current == [20]byte{} {
		return true // compatibility for transactions created without a node ID
	}
	for i := range target {
		candidateDistance := candidate[i] ^ target[i]
		currentDistance := current[i] ^ target[i]
		if candidateDistance < currentDistance {
			return true
		}
		if candidateDistance > currentDistance {
			return false
		}
	}
	return false
}
