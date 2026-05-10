// Package dht — 爬虫模式零分配快速路径。
//
// 本文件包含 crawl 模式下的所有性能关键路径优化：
//   - 响应报文直接构造为 bencode 字节流，跳过 map 分配 + Encode() 字符串拼接
//   - handleRequestCrawl 精简请求处理器：无错误响应、跳过不必要的节点创建
//   - 所有辅助函数均设计为零堆分配
//
// 性能影响：对每个入站报文节省 3-6 次堆分配 + 1 次 Encode 递归 + 1 次 string→[]byte 拷贝。
package dht

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// crawlResponsePool 为爬虫模式响应提供可复用的字节缓冲区。
// 消除 makeResponse() map 分配 + Encode() 字符串拼接的 CPU 和 GC 开销。
var crawlResponsePool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 512)
	},
}

// appendCrawlID 将 20 字节伪装节点 ID 追加到 buf 中。
// 爬虫模式下 id(target) = target[:15] + ownID[15:]，使自身看起来靠近目标。
// 避免 dht.id() 的字符串拼接分配。
func (dht *DHT) appendCrawlID(buf []byte, target string) []byte {
	if target == "" || len(target) < 15 {
		return append(buf, dht.node.id.RawString()...)
	}
	buf = append(buf, target[:15]...)
	buf = append(buf, dht.node.id.RawString()[15:]...)
	return buf
}

// --- 直接字节构造响应（零 map 分配、零 Encode 调用） ---

// sendCrawlPingResp 构造并发送 ping/announce_peer 响应。
// bencode: d1:rd2:id20:<id>e1:t<tlen>:<t>1:y1:re
func sendCrawlPingResp(dht *DHT, addr *net.UDPAddr, t, targetID string) {
	buf := crawlResponsePool.Get().([]byte)[:0]
	buf = append(buf, "d1:rd2:id20:"...)
	buf = dht.appendCrawlID(buf, targetID)
	buf = append(buf, "e1:t"...)
	buf = strconv.AppendInt(buf, int64(len(t)), 10)
	buf = append(buf, ':')
	buf = append(buf, t...)
	buf = append(buf, "1:y1:re"...)
	dht.conn.WriteToUDP(buf, addr)
	crawlResponsePool.Put(buf)
}

// sendCrawlNodesResp 构造并发送包含 nodes 的响应（find_node）。
// bencode: d1:rd2:id20:<id>5:nodes<nlen>:<nodes>e1:t<tlen>:<t>1:y1:re
func sendCrawlNodesResp(dht *DHT, addr *net.UDPAddr, t, targetID, nodes string) {
	buf := crawlResponsePool.Get().([]byte)[:0]
	buf = append(buf, "d1:rd2:id20:"...)
	buf = dht.appendCrawlID(buf, targetID)
	buf = append(buf, "5:nodes"...)
	buf = strconv.AppendInt(buf, int64(len(nodes)), 10)
	buf = append(buf, ':')
	buf = append(buf, nodes...)
	buf = append(buf, "e1:t"...)
	buf = strconv.AppendInt(buf, int64(len(t)), 10)
	buf = append(buf, ':')
	buf = append(buf, t...)
	buf = append(buf, "1:y1:re"...)
	dht.conn.WriteToUDP(buf, addr)
	crawlResponsePool.Put(buf)
}

// sendCrawlGetPeersResp 构造并发送 get_peers 响应（包含 nodes + token）。
// bencode: d1:rd2:id20:<id>5:nodes<nlen>:<nodes>5:token5:<tok>e1:t<tlen>:<t>1:y1:re
func sendCrawlGetPeersResp(dht *DHT, addr *net.UDPAddr, t, infoHash, nodes string) {
	buf := crawlResponsePool.Get().([]byte)[:0]
	buf = append(buf, "d1:rd2:id20:"...)
	buf = dht.appendCrawlID(buf, infoHash)
	buf = append(buf, "5:nodes"...)
	buf = strconv.AppendInt(buf, int64(len(nodes)), 10)
	buf = append(buf, ':')
	buf = append(buf, nodes...)
	buf = append(buf, "5:token5:\x00\x01\x02\x03\x04e1:t"...)
	buf = strconv.AppendInt(buf, int64(len(t)), 10)
	buf = append(buf, ':')
	buf = append(buf, t...)
	buf = append(buf, "1:y1:re"...)
	dht.conn.WriteToUDP(buf, addr)
	crawlResponsePool.Put(buf)
}

// --- 爬虫模式出站请求直接字节构造 ---

// sendCrawlFindNodeQuery 直接构造并发送 find_node 请求，
// 跳过 queryChan → transactionManager.run() → send() → Encode() 的完整链路。
// bencode: d1:ad2:id20:<id>6:target20:<target>e1:q9:find_node1:t2:<txid>1:y1:qe
func sendCrawlFindNodeQuery(dht *DHT, addr *net.UDPAddr, target string) {
	txID := dht.crawlGenTxID()
	buf := crawlResponsePool.Get().([]byte)[:0]
	buf = append(buf, "d1:ad2:id20:"...)
	buf = dht.appendCrawlID(buf, target)
	buf = append(buf, "6:target20:"...)
	buf = append(buf, target...)
	buf = append(buf, "e1:q9:find_node1:t"...)
	buf = strconv.AppendInt(buf, int64(len(txID)), 10)
	buf = append(buf, ':')
	buf = append(buf, txID...)
	buf = append(buf, "1:y1:qe"...)
	dht.conn.WriteToUDP(buf, addr)
	crawlResponsePool.Put(buf)
}

// sendCrawlGetPeersQuery 直接构造并发送 get_peers 请求。
// 同时将 info_hash 存入 crawlTxBuf，供 handleResponseCrawl 还原。
// bencode: d1:ad2:id20:<id>9:info_hash20:<ih>e1:q9:get_peers1:t2:<txid>1:y1:qe
func sendCrawlGetPeersQuery(dht *DHT, no *node, infoHash string) {
	if len(infoHash) != 20 {
		return
	}
	txID := dht.crawlGenTxID()
	idx := crawlTxIdx(txID)
	copy(dht.crawlTxBuf[idx][:], infoHash)

	buf := crawlResponsePool.Get().([]byte)[:0]
	buf = append(buf, "d1:ad2:id20:"...)
	buf = dht.appendCrawlID(buf, infoHash)
	buf = append(buf, "9:info_hash20:"...)
	buf = append(buf, infoHash...)
	buf = append(buf, "e1:q9:get_peers1:t"...)
	buf = strconv.AppendInt(buf, int64(len(txID)), 10)
	buf = append(buf, ':')
	buf = append(buf, txID...)
	buf = append(buf, "1:y1:qe"...)
	dht.conn.WriteToUDP(buf, no.addr)
	crawlResponsePool.Put(buf)
}

// --- 爬虫模式请求处理器 ---

// handleRequestCrawl 是爬虫模式的精简请求处理器。
//
// 相比通用 handleRequest 的优化：
//   - 使用直接字节构造响应，不创建 map、不调用 Encode()
//   - 不发送错误响应（爬虫不关心协议规范性）
//   - 路由表满时跳过节点创建和插入（避免无效的 lock + alloc）
//   - 不查询黑名单中是否存在当前节点（在 handle() 中已跳过）
func handleRequestCrawl(dht *DHT, addr *net.UDPAddr,
	response map[string]interface{}) bool {

	t, _ := response["t"].(string)
	if t == "" {
		return false
	}

	a, ok := response["a"].(map[string]interface{})
	if !ok {
		return false
	}
	q, _ := response["q"].(string)
	id, _ := a["id"].(string)
	if len(id) != 20 {
		return false
	}

	switch q {
	case pingType:
		sendCrawlPingResp(dht, addr, t, id)

	case findNodeType:
		target, _ := a["target"].(string)
		if len(target) != 20 {
			return false
		}
		sendCrawlNodesResp(dht, addr, t, target,
			dht.routingTable.GetCachedNeighborNodes())

	case getPeersType:
		infoHash, _ := a["info_hash"].(string)
		if len(infoHash) != 20 {
			return false
		}
		sendCrawlGetPeersResp(dht, addr, t, infoHash,
			dht.routingTable.GetCachedNeighborNodes())
		if dht.OnGetPeers != nil {
			dht.OnGetPeers(infoHash, addr.IP.String(), addr.Port)
		}

	case announcePeerType:
		infoHash, _ := a["info_hash"].(string)
		port, _ := a["port"].(int)
		if len(infoHash) != 20 {
			return false
		}
		if impliedPort, ok := a["implied_port"]; ok {
			if ip, ok := impliedPort.(int); ok && ip != 0 {
				port = addr.Port
			}
		}
		sendCrawlPingResp(dht, addr, t, id)
		if dht.OnAnnouncePeer != nil {
			dht.OnAnnouncePeer(infoHash, addr.IP.String(), port)
		}

	default:
		return false
	}

	// 路由表满时完全跳过节点创建和插入。
	// 避免 newBitmapFromString + newNode + 20字节 data 分配 + routing table 写锁。
	if atomic.LoadInt64(&dht.routingTable.nodeCount) < int64(dht.MaxNodes) {
		if no, err := newNodeFromAddr(id, addr); err == nil {
			dht.routingTable.Insert(no)
		}
	}
	return true
}

// newNodeFromAddr 从原始 ID 和 UDPAddr 创建节点，避免 newNode 中
// net.ResolveUDPAddr(addr.String()) 的字符串往返分配。
func newNodeFromAddr(id string, addr *net.UDPAddr) (*node, error) {
	if len(id) != 20 {
		return nil, errInvalidNodeID
	}
	bm := newBitmapFromString(id)
	return &node{
		id:             bm,
		addr:           addr,
		lastActiveTime: time.Now(),
		compactInfo:    id + compactIPPortInfo(addr.IP, addr.Port),
	}, nil
}

var errInvalidNodeID = errors.New("node id should be a 20-length string")
