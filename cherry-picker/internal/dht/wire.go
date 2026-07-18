// Package dht — peer wire protocol 实现（高性能重写版）
//
// 修复的问题：
//  1. wire.queue 改为有界 LRU，防止 syncedMap 无限增长
//  2. fetchMetadata 完成后（成功/失败）从 queue 中删除 key，原版不删除
//  3. Wire.Run() 使用固定 worker 池，减少极高负载下的 goroutine 调度开销
//  4. 用 io.LimitReader 替换 ioutil.ReadAll 的无限读取
//  5. 去除 ioutil（已废弃），改用 io 包
package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cherry-picker/internal/cache"
)

const (
	// REQUEST 表示请求消息类型
	REQUEST = iota
	// DATA 表示数据消息类型
	DATA
	// REJECT 表示拒绝消息类型
	REJECT
)

const (
	// BLOCK 是 2^14，BitTorrent 标准块大小
	BLOCK = 16384
	// MaxMetadataSize 限制单个 metadata 最大尺寸（1 MB）
	MaxMetadataSize = BLOCK * 64
	// MaxWireMessageSize 限制单条 peer-wire 消息长度，避免异常 peer 用长度前缀放大内存。
	MaxWireMessageSize = MaxMetadataSize + 4096
	// EXTENDED 表示扩展消息类型
	EXTENDED = 20
	// HANDSHAKE 表示握手位
	HANDSHAKE = 0
)

var handshakePrefix = []byte{
	19, 66, 105, 116, 84, 111, 114, 114, 101, 110, 116, 32, 112, 114,
	111, 116, 111, 99, 111, 108, 0, 0, 0, 0, 0, 16, 0, 1,
}

// read 从 conn 读取 size 字节写入 data。
func read(conn *net.TCPConn, size int, data *bytes.Buffer) error {
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	n, err := io.CopyN(data, conn, int64(size))
	if err != nil || n != int64(size) {
		return errors.New("read error")
	}
	return nil
}

// readMessage 从 TCP 连接读取一条消息（4字节长度前缀 + payload）。
func readMessage(conn *net.TCPConn, data *bytes.Buffer) (length int, err error) {
	if err = read(conn, 4, data); err != nil {
		return
	}

	length = int(bytes2int(data.Next(4)))
	if length == 0 {
		return
	}
	if length > MaxWireMessageSize {
		err = errors.New("wire message too long")
		return
	}

	if err = read(conn, length, data); err != nil {
		return
	}
	return
}

// sendMessage 向连接发送带长度前缀的消息。
func sendMessage(conn *net.TCPConn, data []byte) error {
	conn.SetWriteDeadline(time.Now().Add(time.Second * 2))
	buffer := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buffer[:4], uint32(len(data)))
	copy(buffer[4:], data)
	_, err := conn.Write(buffer)
	return err
}

// sendHandshake 发送 BitTorrent 握手消息。
func sendHandshake(conn *net.TCPConn, infoHash, peerID []byte) error {
	data := make([]byte, 68)
	copy(data[:28], handshakePrefix)
	copy(data[28:48], infoHash)
	copy(data[48:], peerID)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 2))
	_, err := conn.Write(data)
	return err
}

// onHandshake 验证握手响应。
func onHandshake(data []byte) (err error) {
	if !(bytes.Equal(handshakePrefix[:20], data[:20]) && data[25]&0x10 != 0) {
		err = errors.New("invalid handshake response")
	}
	return
}

// sendExtHandshake 请求 ut_metadata 和 metadata_size。
func sendExtHandshake(conn *net.TCPConn) error {
	data := append(
		[]byte{EXTENDED, HANDSHAKE},
		Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": 1},
		})...,
	)

	return sendMessage(conn, data)
}

// getUTMetaSize 解析 ut_metadata ID 和 metadata_size。
func getUTMetaSize(data []byte) (utMetadata int, metadataSize int, err error) {
	v, err := Decode(data)
	if err != nil {
		return
	}

	dict, ok := v.(map[string]interface{})
	if !ok {
		err = errors.New("invalid dict")
		return
	}

	if err = ParseKeys(
		dict, [][]string{{"metadata_size", "int"}, {"m", "map"}}); err != nil {
		return
	}

	m := dict["m"].(map[string]interface{})
	if err = ParseKey(m, "ut_metadata", "int"); err != nil {
		return
	}

	utMetadata = m["ut_metadata"].(int)
	metadataSize = dict["metadata_size"].(int)

	if metadataSize > MaxMetadataSize {
		err = errors.New("metadata_size too long")
	}
	return
}

// PeerSource 标记一个 metadata 下载请求的 peer 来源，用于按来源分解 wire
// 漏斗（诊断用，不改变下载行为）。核心假设：announce_peer 的 peer 刚刚主动
// 联系过本节点（NAT pinhole 打开、当前存活），其 dial→connect 成功率应显著
// 高于第三方 get_peers 响应里 report 的 values（可能陈旧/已死）。这个计数器
// 就是用来证实或证伪该假设的。
type PeerSource uint8

const (
	// PeerSourceUnknown 表示来源未标注（默认零值，兼容旧调用）。
	PeerSourceUnknown PeerSource = iota
	// PeerSourceGetPeers 来自 get_peers 响应中的 values（第三方转述的 peer）。
	PeerSourceGetPeers
	// PeerSourceAnnounce 来自 announce_peer（peer 主动联系本节点，存活证据最强）。
	PeerSourceAnnounce
	// peerSourceCount 是来源枚举的基数，用于定长计数数组。
	peerSourceCount
)

// Request 表示一个 metadata 下载请求的上下文。
type Request struct {
	InfoHash []byte
	IP       string
	Port     int
	Source   PeerSource
}

// Response 包含请求上下文和下载到的 metadata 内容。
type Response struct {
	Request
	MetadataInfo []byte
}

// WireStats 提供 wire 层的诊断计数器，所有字段均为原子操作安全。
type WireStats struct {
	QueueDropped    atomic.Int64 // request channel 满时、拨号前丢弃
	DialAttempts    atomic.Int64 // TCP dial 尝试次数
	DialOK          atomic.Int64 // TCP dial 成功
	DialFailed      atomic.Int64 // TCP dial 失败
	HandshakeOK     atomic.Int64 // BT + extension 握手成功
	HandshakeFailed atomic.Int64 // BT 或 extension 握手失败
	DownloadOK      atomic.Int64 // metadata 下载并校验成功
	DownloadFailed  atomic.Int64 // 握手后 metadata 下载或校验失败
	Blacklisted     atomic.Int64 // 被黑名单跳过的请求数

	// 按 peer 来源分解的漏斗计数（诊断用）。下标为 PeerSource。
	// 用于验证 announce_peer 来源是否比 get_peers values 有更高的
	// dial/connect 成功率——这是"刷新 peer 而非刷新 infohash"策略的前提。
	//
	// 完整漏斗（每级都按来源分解，才能定位 supply 在哪一步流失）：
	//   queued → (blacklisted | inflightDeduped | dialAttempts)
	//   dialAttempts → dialOK → downloadOK
	// queued 是进入 handleRequest 的请求；blacklisted/inflightDeduped 是
	// 拨号前就被丢弃的两条路径（坏 peer 保护 / 同 (hash,peer) 正在下载）。
	queuedBySource          [peerSourceCount]atomic.Int64
	blacklistedBySource     [peerSourceCount]atomic.Int64
	inflightDedupedBySource [peerSourceCount]atomic.Int64
	dialAttemptsBySource    [peerSourceCount]atomic.Int64
	dialOKBySource          [peerSourceCount]atomic.Int64
	downloadOKBySource      [peerSourceCount]atomic.Int64
}

// SourceFunnel 是某一 peer 来源的完整漏斗快照。
type SourceFunnel struct {
	Queued          int64
	Blacklisted     int64
	InflightDeduped int64
	DialAttempts    int64
	DialOK          int64
	DownloadOK      int64
}

// FunnelBySource 返回各 peer 来源的漏斗快照，供上层按来源比较转化率。
func (wire *Wire) FunnelBySource() map[PeerSource]SourceFunnel {
	out := make(map[PeerSource]SourceFunnel, peerSourceCount)
	for s := PeerSource(0); s < peerSourceCount; s++ {
		out[s] = SourceFunnel{
			Queued:          wire.Stats.queuedBySource[s].Load(),
			Blacklisted:     wire.Stats.blacklistedBySource[s].Load(),
			InflightDeduped: wire.Stats.inflightDedupedBySource[s].Load(),
			DialAttempts:    wire.Stats.dialAttemptsBySource[s].Load(),
			DialOK:          wire.Stats.dialOKBySource[s].Load(),
			DownloadOK:      wire.Stats.downloadOKBySource[s].Load(),
		}
	}
	return out
}

// BlacklistStats 返回黑名单诊断快照。
func (wire *Wire) BlacklistStats() BlacklistStats {
	return wire.blackList.stats()
}

// Wire 表示 peer wire 协议下载器。
//
// 改动：
//   - queue 改为有界 LRU（防止无限增长）
//   - Run() 使用固定 worker 池，降低高负载调度开销
type Wire struct {
	blackList   *blackList
	queue       *cache.LRU // 有界 LRU，替代原来的 syncedMap
	requests    chan Request
	responses   chan Response
	workerCount int
	active      atomic.Int64
	busy        atomic.Int64
	peerID      []byte
	Stats       WireStats
}

// NewWire 创建一个 Wire 实例。
//   - blackListSize: 黑名单大小
//   - requestQueueSize: 请求队列大小（channel 容量）
//   - workerQueueSize: 最大并发下载 goroutine 数量
func NewWire(blackListSize, requestQueueSize, workerQueueSize int) *Wire {
	if workerQueueSize <= 0 {
		workerQueueSize = 1
	}
	if requestQueueSize <= 0 {
		requestQueueSize = workerQueueSize * 128
	}
	// queue 容量设置为 workerQueueSize 的 4 倍，避免频繁淘汰正在下载的 key
	queueCap := workerQueueSize * 4
	if queueCap < 4096 {
		queueCap = 4096
	}
	responseQueueSize := workerQueueSize * 16
	if responseQueueSize < 4096 {
		responseQueueSize = 4096
	}
	w := &Wire{
		blackList:   newBlackList(blackListSize),
		queue:       cache.NewLRU(queueCap),
		requests:    make(chan Request, requestQueueSize),
		responses:   make(chan Response, responseQueueSize),
		workerCount: workerQueueSize,
		peerID:      []byte("-CH0001-" + randomString(12)),
	}
	w.active.Store(int64(workerQueueSize))
	return w
}

// Request 将请求放入队列。队列满时直接丢弃（调用方是 UDP 包处理 goroutine，不能阻塞）。
// 来源标记为 Unknown，保留给不关心来源分解的旧调用方。
func (wire *Wire) Request(infoHash []byte, ip string, port int) {
	_ = wire.RequestFromSource(infoHash, ip, port, PeerSourceUnknown)
}

// RequestFromSource 与 Request 相同，但标注 peer 来源用于漏斗分解。返回 true
// 表示请求已被 channel 接纳；返回 false 表示 channel 已满且请求已丢弃。
// 调用方可据此撤销在入队前创建的 reservation，避免一个从未拨号的 peer
// 长时间占用上层去重缓存。
func (wire *Wire) RequestFromSource(infoHash []byte, ip string, port int, source PeerSource) bool {
	select {
	case wire.requests <- Request{InfoHash: infoHash, IP: ip, Port: port, Source: source}:
		return true
	default:
		wire.Stats.QueueDropped.Add(1)
		return false
	}
}

// Response 返回 metadata 响应 channel，供上层消费。
func (wire *Wire) Response() <-chan Response {
	return wire.responses
}

func (wire *Wire) SetActiveWorkers(n int) {
	if n < 1 {
		n = 1
	}
	if n > wire.workerCount {
		n = wire.workerCount
	}
	wire.active.Store(int64(n))
}

func (wire *Wire) ActiveWorkers() int {
	return int(wire.active.Load())
}

func (wire *Wire) MaxWorkers() int {
	return wire.workerCount
}

func (wire *Wire) RequestDepth() int {
	return len(wire.requests)
}

func (wire *Wire) ResponseDepth() int {
	return len(wire.responses)
}

func (wire *Wire) RequestCapacity() int {
	return cap(wire.requests)
}

func (wire *Wire) ResponseCapacity() int {
	return cap(wire.responses)
}

// BusyWorkers is the instantaneous number of workers executing a request.
// ActiveWorkers is the autotuner's admission ceiling; reporting both
// distinguishes an idle supply-side queue from a saturated worker pool.
func (wire *Wire) BusyWorkers() int {
	return int(wire.busy.Load())
}

// isDone 检查所有分片是否已下载完成。
func (wire *Wire) isDone(pieces [][]byte) bool {
	for _, piece := range pieces {
		if len(piece) == 0 {
			return false
		}
	}
	return true
}

// requestPieces 发送所有分片的请求。
func (wire *Wire) requestPieces(
	conn *net.TCPConn, utMetadata int, metadataSize int, piecesNum int) {

	buffer := make([]byte, 1024)
	for i := 0; i < piecesNum; i++ {
		buffer[0] = EXTENDED
		buffer[1] = byte(utMetadata)

		length := appendMetadataRequest(buffer[:2], i)

		_ = sendMessage(conn, buffer[:length])
	}
	buffer = nil
}

func appendMetadataRequest(buf []byte, piece int) int {
	buf = append(buf, "d8:msg_typei0e5:piecei"...)
	buf = strconv.AppendInt(buf, int64(piece), 10)
	buf = append(buf, "ee"...)
	return len(buf)
}

// extractInfoBytes 从原始 .torrent 文件字节中提取 info dict 的原始 bencode 字节。
// .torrent 文件的外层结构是 bencoded dict，info 键对应的值即为我们需要的部分。
// 提取后的字节 SHA1 应等于 infohash。
func extractInfoBytes(torrentData []byte) ([]byte, error) {
	const sep = "4:info"
	idx := bytes.Index(torrentData, []byte(sep))
	if idx < 0 {
		return nil, errors.New("info key not found")
	}
	start := idx + len(sep)
	if start >= len(torrentData) || torrentData[start] != 'd' {
		return nil, errors.New("info value is not a dict")
	}
	_, endIdx, err := DecodeDict(torrentData[start:], 0)
	if err != nil {
		return nil, err
	}
	return torrentData[start : start+endIdx], nil
}

// fetchMetadata 连接 peer，通过 extension protocol 下载 info dict。
// 完成后（成功或失败）都会从 queue 中删除 key，避免泄漏。
func (wire *Wire) fetchMetadata(r Request, key string) {
	var (
		length       int
		msgType      byte
		piecesNum    int
		pieces       [][]byte
		utMetadata   int
		metadataSize int
	)

	const (
		wireStageDial = iota
		wireStageHandshake
		wireStageDownload
		wireStageDone
	)
	stage := wireStageDial

	defer func() {
		// 请求完成后释放 inflight 去重键，允许同一 peer 在后续重新尝试。
		wire.queue.Delete(key)
		pieces = nil
		_ = recover()

		switch stage {
		case wireStageDial:
			wire.Stats.DialFailed.Add(1)
			wire.blackList.insert(r.IP, r.Port)
		case wireStageHandshake:
			wire.Stats.HandshakeFailed.Add(1)
			// 一个不支持 BT/extension 握手的 endpoint 对其它 infohash 也没有
			// 下载价值；尽早淘汰，避免热点坏节点反复占用 worker。
			wire.blackList.insert(r.IP, r.Port)
		case wireStageDownload:
			wire.Stats.DownloadFailed.Add(1)
		}
	}()

	infoHash := r.InfoHash
	address := genAddress(r.IP, r.Port)
	src := r.Source
	if src >= peerSourceCount {
		src = PeerSourceUnknown
	}

	wire.Stats.DialAttempts.Add(1)
	wire.Stats.dialAttemptsBySource[src].Add(1)
	dial, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err != nil {
		return
	}
	wire.Stats.DialOK.Add(1)
	wire.Stats.dialOKBySource[src].Add(1)
	stage = wireStageHandshake
	conn := dial.(*net.TCPConn)
	conn.SetLinger(0)
	defer conn.Close()

	// 整体 deadline：防止慢速 peer 长时间占用 worker。
	// 注意不能用 conn.SetDeadline() 替代，因为 per-op SetReadDeadline/SetWriteDeadline
	// 会覆盖 SetDeadline 设置的值。用 time 判断更可靠。
	deadline := time.Now().Add(15 * time.Second)

	// 使用固定大小的 buffer，避免无限增长
	data := bytes.NewBuffer(nil)
	data.Grow(BLOCK)

	if sendHandshake(conn, infoHash, wire.peerID) != nil ||
		read(conn, 68, data) != nil ||
		onHandshake(data.Next(68)) != nil ||
		sendExtHandshake(conn) != nil {
		return
	}
	wire.Stats.HandshakeOK.Add(1)
	stage = wireStageDownload

	for {
		if time.Now().After(deadline) {
			return
		}

		length, err = readMessage(conn, data)
		if err != nil {
			return
		}

		if length == 0 {
			continue
		}

		msgType, err = data.ReadByte()
		if err != nil {
			return
		}

		switch msgType {
		case EXTENDED:
			extendedID, err := data.ReadByte()
			if err != nil {
				return
			}

			// 用 LimitReader 限制单次读取大小，防止内存峰值（修复问题8）
			limitedReader := io.LimitReader(data, MaxMetadataSize)
			payload, err := io.ReadAll(limitedReader)
			if err != nil {
				return
			}

			if extendedID == 0 {
				// 扩展握手：获取 ut_metadata ID 和 metadata 总大小
				if pieces != nil {
					return
				}

				utMetadata, metadataSize, err = getUTMetaSize(payload)
				if err != nil {
					return
				}

				piecesNum = metadataSize / BLOCK
				if metadataSize%BLOCK != 0 {
					piecesNum++
				}

				pieces = make([][]byte, piecesNum)
				go wire.requestPieces(conn, utMetadata, metadataSize, piecesNum)

				continue
			}

			if pieces == nil {
				return
			}

			d, index, err := DecodeDict(payload, 0)
			if err != nil {
				return
			}
			dict := d.(map[string]interface{})

			if err = ParseKeys(dict, [][]string{
				{"msg_type", "int"},
				{"piece", "int"}}); err != nil {
				return
			}

			if dict["msg_type"].(int) != DATA {
				continue
			}

			piece := dict["piece"].(int)
			pieceLen := length - 2 - index

			if piece < 0 || piece >= piecesNum {
				return
			}

			expectedPieceLen := BLOCK
			if piece == piecesNum-1 && metadataSize%BLOCK != 0 {
				expectedPieceLen = metadataSize % BLOCK
			}
			if pieceLen != expectedPieceLen {
				return
			}

			pieces[piece] = payload[index:]

			if wire.isDone(pieces) {
				metadataInfo := bytes.Join(pieces, nil)

				info := sha1.Sum(metadataInfo)
				if !bytes.Equal(infoHash, info[:]) {
					return
				}

				wire.Stats.DownloadOK.Add(1)
				wire.Stats.downloadOKBySource[src].Add(1)
				wire.responses <- Response{
					Request:      r,
					MetadataInfo: metadataInfo,
				}
				stage = wireStageDone
				return
			}
		default:
			data.Reset()
		}
	}
}

// Run 启动 peer wire 协议处理循环。
//
// 固定 worker 池比“每个请求一个 goroutine + semaphore”更适合极高负载：
// 并发上限严格固定，调度开销低，请求 channel 的背压仍然自然传导给上游。
func (wire *Wire) Run() {
	go wire.blackList.clear()

	for i := 0; i < wire.workerCount; i++ {
		go wire.runWorker(i)
	}

	select {}
}

func (wire *Wire) runWorker(workerID int) {
	for {
		if workerID >= wire.ActiveWorkers() {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		r, ok := <-wire.requests
		if !ok {
			return
		}
		wire.runRequest(r)
	}
}

func (wire *Wire) runRequest(r Request) {
	wire.busy.Add(1)
	defer wire.busy.Add(-1)
	wire.handleRequest(r)
}

func (wire *Wire) handleRequest(r Request) {
	src := r.Source
	if src >= peerSourceCount {
		src = PeerSourceUnknown
	}
	// queued：进入处理的请求（无效 infohash 不计，它不是真实 supply）。
	if len(r.InfoHash) == 20 {
		wire.Stats.queuedBySource[src].Add(1)
	}

	key := strings.Join([]string{
		string(r.InfoHash), genAddress(r.IP, r.Port),
	}, ":")

	if len(r.InfoHash) != 20 || wire.blackList.in(r.IP, r.Port) {
		if len(r.InfoHash) == 20 {
			wire.Stats.Blacklisted.Add(1)
			wire.Stats.blacklistedBySource[src].Add(1)
		}
		return
	}

	// inflight dedup：同一 (infohash, peer) 已在下载中，LRU.Set 返回 false。
	// 这是"重复 supply"而非"流失 supply"，单独计数以便和黑名单区分。
	if !wire.queue.Set(key) {
		wire.Stats.inflightDedupedBySource[src].Add(1)
		return
	}

	wire.fetchMetadata(r, key)
}
