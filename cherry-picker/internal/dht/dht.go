// Package dht implements the bittorrent dht protocol. For more information
// see http://www.bittorrent.org/beps/bep_0005.html.
package dht

import (
	"encoding/hex"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// StandardMode follows the standard protocol
	StandardMode = iota
	// CrawlMode for crawling the dht network.
	CrawlMode
)

var (
	// ErrNotReady is the error when DHT is not initialized.
	ErrNotReady = errors.New("dht is not ready")
	// ErrOnGetPeersResponseNotSet is the error that config
	// OnGetPeersResponseNotSet is not set when call dht.GetPeers.
	ErrOnGetPeersResponseNotSet = errors.New("OnGetPeersResponse is not set")
)

// Config represents the configure of dht.
type Config struct {
	// in mainline dht, k = 8
	K int
	// for crawling mode, we put all nodes in one bucket, so KBucketSize may
	// not be K
	KBucketSize int
	// candidates are udp, udp4, udp6
	Network string
	// format is `ip:port`
	Address string
	// the prime nodes through which we can join in dht network
	PrimeNodes []string
	// the kbucket expired duration
	KBucketExpiredAfter time.Duration
	// the node expired duration
	NodeExpriedAfter time.Duration
	// how long it checks whether the bucket is expired
	CheckKBucketPeriod time.Duration
	// peer token expired duration
	TokenExpiredAfter time.Duration
	// the max transaction id
	MaxTransactionCursor uint64
	// how many nodes routing table can hold
	MaxNodes int
	// callback when got get_peers request
	OnGetPeers func(string, string, int)
	// callback when receive get_peers response
	OnGetPeersResponse func(string, *Peer)
	// callback when got announce_peer request
	OnAnnouncePeer func(string, string, int)
	// blcoked ips
	BlockedIPs []string
	// blacklist size
	BlackListMaxSize int
	// StandardMode or CrawlMode
	Mode int
	// the times it tries when send fails
	Try int
	// the size of packet need to be dealt with
	PacketJobLimit int
	// the size of packet handler
	PacketWorkerLimit int
	// PacketReadWorkers 是并发从 UDP socket 读取的 goroutine 数。
	// 单读取 goroutine 时"read syscall → 入队 → 再 read"串行执行，高包率下
	// 会在内核 recv buffer 溢出前先卡在用户态；多读取 goroutine 让多个
	// recvfrom syscall 并发在飞，把内核缓冲的包及时取走。0 = 自动。
	PacketReadWorkers int
	// the nodes num to be fresh in a kbucket
	RefreshNodeNum int
	// NodeIDFile is the path to persist the node ID across restarts.
	// If empty or the file doesn't exist, a new random ID is generated and saved.
	NodeIDFile string
}

// NewStandardConfig returns a Config pointer with default values.
func NewStandardConfig() *Config {
	return &Config{
		K:           8,
		KBucketSize: 8,
		Network:     "udp4",
		Address:     ":6881",
		PrimeNodes: []string{
			"router.bittorrent.com:6881",
			"router.utorrent.com:6881",
			"dht.transmissionbt.com:6881",
		},
		NodeExpriedAfter:     time.Duration(time.Minute * 15),
		KBucketExpiredAfter:  time.Duration(time.Minute * 15),
		CheckKBucketPeriod:   time.Duration(time.Second * 30),
		TokenExpiredAfter:    time.Duration(time.Minute * 10),
		MaxTransactionCursor: math.MaxUint32,
		MaxNodes:             5000,
		BlockedIPs:           make([]string, 0),
		BlackListMaxSize:     65536,
		Try:                  2,
		Mode:                 StandardMode,
		PacketJobLimit:       1024,
		PacketWorkerLimit:    256,
		RefreshNodeNum:       8,
	}
}

// NewCrawlConfig returns a config in crawling mode, tuned for maximum throughput.
func NewCrawlConfig() *Config {
	config := NewStandardConfig()
	config.NodeExpriedAfter = 0
	config.KBucketExpiredAfter = 0
	config.CheckKBucketPeriod = 2 * time.Second // 每 2 秒刷新一次路由表
	config.KBucketSize = math.MaxInt32
	config.Mode = CrawlMode
	config.RefreshNodeNum = 256   // 每次刷新联系的节点数
	config.MaxNodes = 5_000       // 路由表上限（节省内存）
	config.PacketJobLimit = 4_096 // 数据包 channel 容量
	config.PacketWorkerLimit = 0  // 0 = 自动检测（NumCPU × 4）
	// 更多 bootstrap 节点，加快初始入网
	config.PrimeNodes = append(config.PrimeNodes,
		"router.bitcomet.com:6881",
		"dht.aelitis.com:6881",
	)
	return config
}

// DHT represents a DHT node.
type DHT struct {
	*Config
	node               *node
	conn               *net.UDPConn
	routingTable       *routingTable
	transactionManager *transactionManager
	peersManager       *peersManager
	tokenManager       *tokenManager
	blackList          *blackList
	Ready              bool
	packets            chan packet
	packetPool         sync.Pool
	stats              packetStats

	// crawl 模式轻量级事务环形缓冲（无锁，1.28MB）。
	// 存储 get_peers 出站请求的 info_hash，用于响应到达时还原原始 info_hash
	// 并触发 OnGetPeersResponse 回调。索引为 16 位计数器取模。
	crawlTxBuf [1 << 16][20]byte
	crawlTxCtr atomic.Uint32
}

type packetStats struct {
	received      atomic.Uint64
	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	handled       atomic.Uint64
	decodeErrors  atomic.Uint64
	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64
}

type PacketStats struct {
	Received      uint64
	Enqueued      uint64
	Dropped       uint64
	Handled       uint64
	DecodeErrors  uint64
	BytesReceived uint64
	BytesSent     uint64
}

// New returns a DHT pointer. If config is nil, then config will be set to
// the default config.
func New(config *Config) *DHT {
	if config == nil {
		config = NewStandardConfig()
	}

	node, err := newNode(loadOrGenerateNodeID(config.NodeIDFile), config.Network, config.Address)
	if err != nil {
		panic(err)
	}

	d := &DHT{
		Config:    config,
		node:      node,
		blackList: newBlackList(config.BlackListMaxSize),
		packets:   make(chan packet, config.PacketJobLimit),
		packetPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 8192)
			},
		},
	}

	for _, ip := range config.BlockedIPs {
		d.blackList.insert(ip, -1)
	}

	go func() {
		for _, ip := range getLocalIPs() {
			d.blackList.insert(ip, -1)
		}

		ip, err := getRemoteIP()
		if err != nil {
			d.blackList.insert(ip, -1)
		}
	}()

	return d
}

// IsStandardMode returns whether mode is StandardMode.
func (dht *DHT) IsStandardMode() bool {
	return dht.Mode == StandardMode
}

// IsCrawlMode returns whether mode is CrawlMode.
func (dht *DHT) IsCrawlMode() bool {
	return dht.Mode == CrawlMode
}

// init initializes global varables.
func (dht *DHT) init() {
	listener, err := net.ListenPacket(dht.Network, dht.Address)
	if err != nil {
		panic(err)
	}

	dht.conn = listener.(*net.UDPConn)

	// 扩大内核 socket 缓冲区，减少高流量下的内核级丢包
	dht.conn.SetReadBuffer(8 * 1024 * 1024)  // 8MB 接收缓冲
	dht.conn.SetWriteBuffer(4 * 1024 * 1024) // 4MB 发送缓冲

	dht.routingTable = newRoutingTable(dht.KBucketSize, dht)
	dht.transactionManager = newTransactionManager(
		dht.MaxTransactionCursor, dht)

	// 爬虫模式跳过未使用的子系统，减少内存和 GC 压力。
	// peersManager: 仅标准模式存储 peer（爬虫模式不维护 peer 列表）
	// tokenManager: 爬虫模式使用固定 crawlToken，不需要 token 生成/验证
	if dht.IsStandardMode() {
		dht.peersManager = newPeersManager(dht)
		dht.tokenManager = newTokenManager(dht.TokenExpiredAfter, dht)
		go dht.tokenManager.clear()
	}

	go dht.transactionManager.run()
	go dht.blackList.clear()
	dht.startPacketWorkers()
}

func (dht *DHT) startPacketWorkers() {
	workers := dht.PacketWorkerLimit
	if dht.IsCrawlMode() && workers <= 0 {
		// 爬虫模式：每核 4 个 worker 即可饱和处理。
		// 超过此数只增加调度开销和内存，不增加吞吐。
		workers = runtime.NumCPU() * 4
		if workers < 4 {
			workers = 4
		}
	}
	if workers <= 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		go func() {
			for pkt := range dht.packets {
				handle(dht, pkt)
			}
		}()
	}
}

// join makes current node join the dht network.
func (dht *DHT) join() {
	for _, addr := range dht.PrimeNodes {
		raddr, err := net.ResolveUDPAddr(dht.Network, addr)
		if err != nil {
			continue
		}

		// NOTE: Temporary node has NOT node id.
		dht.transactionManager.findNode(
			&node{addr: raddr},
			dht.node.id.RawString(),
		)
	}
}

// listen receives message from udp. It runs readWorkerCount() concurrent
// reader goroutines, each pulling datagrams off the shared socket. ReadFromUDP
// is safe for concurrent use; each call returns a distinct datagram.
func (dht *DHT) listen() {
	readers := dht.readWorkerCount()
	for i := 0; i < readers; i++ {
		go dht.readLoop()
	}
}

func (dht *DHT) readLoop() {
	for {
		buff := dht.packetPool.Get().([]byte)
		n, raddr, err := dht.conn.ReadFromUDP(buff)
		if err != nil {
			dht.packetPool.Put(buff)
			continue
		}
		dht.stats.received.Add(1)
		dht.stats.bytesReceived.Add(uint64(n))

		pkt := packet{
			data:   buff[:n],
			buffer: buff,
			raddr:  raddr,
		}

		select {
		case dht.packets <- pkt:
			dht.stats.enqueued.Add(1)
		default:
			dht.stats.dropped.Add(1)
			pkt.release(dht)
		}
	}
}

// readWorkerCount returns how many concurrent socket readers to run.
func (dht *DHT) readWorkerCount() int {
	if dht.PacketReadWorkers > 0 {
		return dht.PacketReadWorkers
	}
	if !dht.IsCrawlMode() {
		return 1
	}
	// 爬虫模式：默认 2 个读取 goroutine/socket。部署通常已有多个监听端口
	// （各自独立 socket），无需在单 socket 上堆太多读取协程。
	readers := runtime.NumCPU() / 2
	if readers < 2 {
		readers = 2
	}
	if readers > 8 {
		readers = 8
	}
	return readers
}

func (dht *DHT) PacketStats() PacketStats {
	return PacketStats{
		Received:      dht.stats.received.Load(),
		Enqueued:      dht.stats.enqueued.Load(),
		Dropped:       dht.stats.dropped.Load(),
		Handled:       dht.stats.handled.Load(),
		DecodeErrors:  dht.stats.decodeErrors.Load(),
		BytesReceived: dht.stats.bytesReceived.Load(),
		BytesSent:     dht.stats.bytesSent.Load(),
	}
}

// writeToUDP 发送 UDP 报文并累加出站字节计数（带宽可观测性）。
// 所有出站路径（快速路径直接构造 + 标准 Encode 路径）都应经由此方法。
func (dht *DHT) writeToUDP(buf []byte, addr *net.UDPAddr) (int, error) {
	n, err := dht.conn.WriteToUDP(buf, addr)
	if n > 0 {
		dht.stats.bytesSent.Add(uint64(n))
	}
	return n, err
}

// id returns a id near to target if target is not null, otherwise it returns
// the dht's node id.
func (dht *DHT) id(target string) string {
	if dht.IsStandardMode() || target == "" {
		return dht.node.id.RawString()
	}
	return target[:15] + dht.node.id.RawString()[15:]
}

// loadOrGenerateNodeID loads a node ID from file, or generates a new random one
// and saves it. This preserves DHT network identity across restarts.
func loadOrGenerateNodeID(path string) string {
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			id := string(data)
			for len(id) > 0 && (id[len(id)-1] == '\n' || id[len(id)-1] == '\r') {
				id = id[:len(id)-1]
			}
			if len(id) == 40 {
				if b, err := hex.DecodeString(id); err == nil && len(b) == 20 {
					return string(b)
				}
			}
		}
		id := randomString(20)
		_ = os.MkdirAll(filepath.Dir(path), 0700)
		_ = os.WriteFile(path, []byte(hex.EncodeToString([]byte(id))+"\n"), 0600)
		return id
	}
	return randomString(20)
}

// crawlGenTxID 生成 2 字节爬虫模式事务 ID（无锁，原子计数器）。
// 返回的字符串可通过 crawlTxIdx() 还原为 crawlTxBuf 的索引。
func (dht *DHT) crawlGenTxID() string {
	ctr := dht.crawlTxCtr.Add(1)
	idx := uint16(ctr)
	return string([]byte{byte(idx >> 8), byte(idx)})
}

// GetPeers returns peers who have announced having infoHash.
func (dht *DHT) GetPeers(infoHash string) error {
	if !dht.Ready {
		return ErrNotReady
	}

	if dht.OnGetPeersResponse == nil {
		return ErrOnGetPeersResponseNotSet
	}

	if len(infoHash) == 40 {
		data, err := hex.DecodeString(infoHash)
		if err != nil {
			return err
		}
		infoHash = string(data)
	}

	neighbors := dht.routingTable.AllNodes()

	for _, no := range neighbors {
		dht.transactionManager.getPeers(no, infoHash)
	}

	return nil
}

// Run starts the dht.
func (dht *DHT) Run() {
	dht.init()
	dht.listen()
	dht.join()

	dht.Ready = true

	tick := time.Tick(dht.CheckKBucketPeriod)

	for {
		select {
		case <-tick:
			// 爬虫模式：每 tick 刷新邻居缓存，供热路径 O(1) 读取
			if dht.IsCrawlMode() {
				dht.routingTable.refreshCachedNeighbors()
			}
			if dht.routingTable.Len() == 0 {
				dht.join()
			} else if dht.transactionManager.len() == 0 {
				go dht.routingTable.Fresh()
			}
		}
	}
}
