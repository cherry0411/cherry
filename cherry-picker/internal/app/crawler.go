// Package app 实现 Cherry DHT 爬虫的核心应用逻辑（高性能重写版）。
//
// 主要修复（相比 app.go）：
//  1. seenSet（无限增长 map）→ 有界 LRU cache，内存严格上限
//  2. remoteKnown sync.Map（无限增长）→ 有界 LRU cache
//  3. peerCounts sync.Map（高并发内存碎片）→ mutex + map，定期 swap 替换
//  4. checkQueue 容量从 4096 增大到 100000
//  5. 消除全局变量，所有状态内聚到 Application 结构体
//  6. 自适应调优：根据内存压力自动暂停/恢复 metadata 请求入队
package app

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"cherry-picker/internal/cache"
	"cherry-picker/internal/config"
	dht "cherry-picker/internal/dht"
	"cherry-picker/internal/export"
	"cherry-picker/internal/filter"
	"cherry-picker/internal/pipeline"
	"cherry-picker/internal/sysres"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// lruCapConfig 保存各 LRU 的容量配置。
type lruCapConfig struct {
	infohashSeen        int // infohash 去重 LRU 容量
	peerSeen            int // peer 去重 LRU 容量
	metadataRequestSeen int // metadata 请求去重 LRU 容量
	metadataResultSeen  int // metadata 结果去重 LRU 容量
	remoteKnown         int // 远端已知 hash LRU 容量
}

// Application 是爬虫应用实例，封装所有状态（不使用全局变量）。
type Application struct {
	cfg    config.Config
	logger *log.Logger
	dhts   []*dht.DHT

	// 有界 LRU，替代无限增长的 seenSet / remoteKnown
	infohashSeen        *cache.LRU // infohash 去重（peer 发现）
	peerSeen            *cache.LRU // peer 去重（peer 事件）
	metadataRequestSeen *cache.LRU // metadata 请求去重
	metadataResultSeen  *cache.LRU // metadata 结果去重
	remoteKnown         *cache.LRU // 远端 API 已确认存在的 hash

	// peerCounts：替代 sync.Map，用 mutex+map 减少内存碎片。
	// 每60秒 flush 时原子替换整个 map，避免在高并发 LoadOrStore 下 sync.Map 的 dirty 翻倍问题。
	peerCountsMu sync.Mutex
	peerCounts   map[string]int

	// 自适应调优：内存压力高时暂停 metadata 请求入队
	metaPaused atomic.Bool

	// memLimit 是启动时探测的进程内存上限（字节），用于推导 LRU 容量、
	// GC 软上限和 autotune 阈值。
	memLimit uint64
	// cpuUtilPct 是 autotune 采样的 Go 进程 CPU 利用率（百分比 ×100，
	// 即 9550 = 95.50%），供 tuneWireWorkers 等其他调优回路无锁读取。
	cpuUtilPct atomic.Uint64

	// filterChain 在 metadata 上报前执行规则过滤。
	filterChain *filter.Chain
	// rejectQueue 将被过滤的 hash 异步批量上报给后端 cuckoo filter。
	rejectQueue chan string

	// API 客户端（不再是全局变量）
	apiClient *http.Client
}

// runtimeStats 运行时统计（原子计数器）。
type runtimeStats struct {
	infohashEventsSent      atomic.Uint64
	infohashEventsDropped   atomic.Uint64
	infohashEventsDeduped   atomic.Uint64
	peerEventsDropped       atomic.Uint64
	metadataEventsDropped   atomic.Uint64
	peerEventsSent          atomic.Uint64
	metadataEventsSent      atomic.Uint64
	peerEventsDeduped       atomic.Uint64
	metadataEventsDeduped   atomic.Uint64
	metadataEventsFiltered  atomic.Uint64
	metadataRequestsQueued  atomic.Uint64
	metadataRequestsDeduped atomic.Uint64
	checkBatchesQueued      atomic.Uint64
	checkBatchesDropped     atomic.Uint64
	checkBatchesProcessed   atomic.Uint64
	activeLookupsQueued     atomic.Uint64
	activeLookupsDropped    atomic.Uint64
	activeLookupsSent       atomic.Uint64
	sampleQueriesSent       atomic.Uint64
	sampleResponses         atomic.Uint64
	sampleHashesReceived    atomic.Uint64
	sampleHashesQueued      atomic.Uint64
	metadataLocale          metadataLocaleCounters
}

type statsSnapshot struct {
	infohashEventsSent      uint64
	infohashEventsDropped   uint64
	infohashEventsDeduped   uint64
	peerEventsSent          uint64
	peerEventsDropped       uint64
	peerEventsDeduped       uint64
	metadataRequestsQueued  uint64
	metadataRequestsDeduped uint64
	checkBatchesQueued      uint64
	checkBatchesDropped     uint64
	checkBatchesProcessed   uint64
	activeLookupsQueued     uint64
	activeLookupsDropped    uint64
	activeLookupsSent       uint64
	sampleQueriesSent       uint64
	sampleResponses         uint64
	sampleHashesReceived    uint64
	sampleHashesQueued      uint64
	metadataEventsSent      uint64
	metadataEventsDropped   uint64
	metadataEventsDeduped   uint64
	metadataEventsFiltered  uint64
	metadataLocale          metadataLocaleSnapshot
	dhtPacketsReceived      uint64
	dhtPacketsEnqueued      uint64
	dhtPacketsDropped       uint64
	dhtPacketsHandled       uint64
	dhtPacketDecodeErrors   uint64
	dhtBytesReceived        uint64
	dhtBytesSent            uint64
	dhtFollowupsSent        uint64
	dhtRoutingNodes         uint64
	dhtNodesInserted        uint64
	dhtNodesRemoved         uint64
	dhtRefreshQueries       uint64
	wireQueueDropped        int64
	wireDialAttempts        int64
	wireDialOK              int64
	wireDialFailed          int64
	wireHandshakeOK         int64
	wireHandshakeFailed     int64
	wireDownloadOK          int64
	wireDownloadFailed      int64
	wireBlacklisted         int64

	// 按 peer 来源分解的漏斗（announce_peer vs get_peers values）。
	wireAnnounceQueued    int64
	wireAnnounceBlacklist int64
	wireAnnounceInflight  int64
	wireAnnounceDial      int64
	wireAnnounceDialOK    int64
	wireAnnounceDownload  int64
	wireGetPeersQueued    int64
	wireGetPeersBlacklist int64
	wireGetPeersInflight  int64
	wireGetPeersDial      int64
	wireGetPeersDialOK    int64
	wireGetPeersDownload  int64

	// 黑名单诊断（当前值，非累计增量；size/max 直接反映盲点）。
	wireBlacklistSize     int64
	wireBlacklistMax      int64
	wireBlacklistRejected int64
	wireBlacklistExpired  int64
}

const (
	checkBatchSize         = 512
	checkFlushInterval     = 250 * time.Millisecond
	checkWorkerBacklog     = 64
	defaultCheckWorkersCap = 16
	autoTunePauseSamples   = 3
	autoTuneResumeSamples  = 2
	autoTuneMinPause       = 45 * time.Second
)

type autoTuneAction uint8

const (
	autoTuneNoop autoTuneAction = iota
	autoTunePause
	autoTuneResume
)

type autoTuneController struct {
	overLimitSamples  int
	underLimitSamples int
	pausedAt          time.Time
}

// New 创建一个新的 Application 实例。
func New(cfg config.Config, logger *log.Logger) *Application {
	if logger == nil {
		logger = log.Default()
	}
	memLimit := resolveMemLimit(cfg)
	lruCaps := newLRUCaps(cfg, memLimit)
	return &Application{
		cfg:      cfg,
		logger:   logger,
		memLimit: memLimit,

		infohashSeen:        cache.NewLRU(lruCaps.infohashSeen),
		peerSeen:            cache.NewLRU(lruCaps.peerSeen),
		metadataRequestSeen: cache.NewLRU(lruCaps.metadataRequestSeen),
		metadataResultSeen:  cache.NewLRU(lruCaps.metadataResultSeen),
		remoteKnown:         cache.NewLRU(lruCaps.remoteKnown),

		peerCounts:  make(map[string]int, 4096),
		filterChain: buildFilterChain(cfg.Filter),
		rejectQueue: make(chan string, 10_000),
		// check/peers/reject 三条上报回路共用此 client，均为对同一后端 host
		// 的高频小 POST。默认 Transport 的 MaxIdleConnsPerHost=2 会导致
		// 并发 worker 不断新建/关闭 TCP 连接，这里放大空闲连接池。
		apiClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// resolveMemLimit 返回进程可用内存上限（字节）。
// 优先级：显式配置 > 系统/cgroup 探测（取 70%，为 OS 和其他进程留余量）> 2GB 兜底。
func resolveMemLimit(cfg config.Config) uint64 {
	if cfg.MemLimitMB > 0 {
		return uint64(cfg.MemLimitMB) * 1024 * 1024
	}
	if total := sysres.TotalMemory(); total > 0 {
		limit := total * 70 / 100
		return clampUint64(limit, 1<<30, 64<<30) // [1GB, 64GB]
	}
	return 2 << 30
}

// Run 启动爬虫，阻塞直到 ctx 取消。
func (a *Application) Run(ctx context.Context) error {
	events := make(chan pipeline.Event, a.cfg.EventQueue)
	sink, err := export.NewSink(a.cfg.Exporter)
	if err != nil {
		return err
	}

	exporter := export.NewBatchExporter(a.logger, sink, a.cfg.Exporter.BatchSize, a.cfg.Exporter.FlushInterval, events)
	go func() { _ = exporter.Run(ctx) }()

	stats := &runtimeStats{}

	// checkQueue：批量向 API 检查 hash 是否已存在
	checkQueue := make(chan string, 16_384)
	go a.runCheckLoop(ctx, checkQueue, stats)
	lookupQueue := make(chan string, a.cfg.Discovery.LookupQueue)
	sampleLookupQueue := make(chan string, a.cfg.Discovery.LookupQueue)

	// peer wire metadata 下载器
	downloader := dht.NewWire(a.cfg.Metadata.BlackListSize, a.cfg.Metadata.RequestQueueSize, a.cfg.Metadata.WorkerQueueSize)
	if a.cfg.Metadata.Enabled {
		a.startMetadataConsumers(ctx, downloader, events, stats)
		go downloader.Run()
		go a.tuneWireWorkers(ctx, downloader)
	}
	a.logger.Printf("metadata workers: %d, queue: %d", a.cfg.Metadata.WorkerQueueSize, a.cfg.Metadata.RequestQueueSize)

	// 共享回调（所有 DHT 实例共用同一套 handler，共享下载器和 LRU）
	onGetPeers := func(infoHash, ip string, port int) {
		now := time.Now().UTC()
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitInfohashEvent(events, ihHex, ip, port, "get_peers", stats, now)
		}
		a.queueActiveLookup(lookupQueue, ihHex, "lookup|", stats)
	}
	onSampleInfoHashes := func(samples string) {
		stats.sampleResponses.Add(1)
		stats.sampleHashesReceived.Add(uint64(len(samples) / 20))
		for i := 0; i+20 <= len(samples); i += 20 {
			ihHex := hex.EncodeToString([]byte(samples[i : i+20]))
			if a.queueActiveLookup(sampleLookupQueue, ihHex, "sample|", stats) {
				stats.sampleHashesQueued.Add(1)
			}
		}
	}
	onGetPeersResponse := func(infoHash string, peer *dht.Peer) {
		now := time.Now().UTC()
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, peer.IP.String(), peer.Port, "get_peers_response", stats, now)
		}
		a.queueMetadataRequest(downloader, ihHex, peer.IP.String(), peer.Port, dht.PeerSourceGetPeers, stats, checkQueue)
	}
	onAnnouncePeer := func(infoHash, ip string, port int) {
		now := time.Now().UTC()
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, ip, port, "announce_peer", stats, now)
		}
		a.queueMetadataRequest(downloader, ihHex, ip, port, dht.PeerSourceAnnounce, stats, checkQueue)
	}

	addrs, err := a.listenAddrs()
	if err != nil {
		return err
	}
	dhtErrors := make(chan error, len(addrs))
	for i, addr := range addrs {
		dhtCfg := dht.NewCrawlConfig()
		if a.cfg.Discovery.Mode == "standard" {
			dhtCfg = dht.NewStandardConfig()
		}
		dhtCfg.Address = addr
		if primeNodes := splitCommaSeparated(a.cfg.Discovery.PrimeNodes); len(primeNodes) > 0 {
			dhtCfg.PrimeNodes = primeNodes
		}
		dhtCfg.PacketWorkerLimit = a.cfg.Discovery.PacketWorkers
		dhtCfg.PacketJobLimit = a.cfg.Discovery.PacketJobs
		dhtCfg.PacketReadWorkers = a.cfg.Discovery.ReadWorkers
		dhtCfg.MaxNodes = a.cfg.Discovery.MaxNodes
		dhtCfg.RefreshNodeNum = a.cfg.Discovery.RefreshNodes
		dhtCfg.SpreadFollowups = a.cfg.Discovery.LookupSpread
		dhtCfg.NodeIDFile = a.nodeIDPath(i)
		dhtCfg.OnGetPeers = onGetPeers
		dhtCfg.OnGetPeersResponse = onGetPeersResponse
		dhtCfg.OnSampleInfoHashes = onSampleInfoHashes
		dhtCfg.OnAnnouncePeer = onAnnouncePeer
		d := dht.New(dhtCfg)
		a.dhts = append(a.dhts, d)
		go func(addr string, d *dht.DHT) {
			if runErr := d.Run(); runErr != nil {
				dhtErrors <- fmt.Errorf("start DHT listener %s: %w", addr, runErr)
			}
		}(addr, d)
	}
	if a.cfg.Metadata.Enabled && a.cfg.Discovery.ActiveLookup {
		go a.runActiveLookupLoop(ctx, lookupQueue, sampleLookupQueue, stats)
		if a.cfg.Discovery.SampleInfohashes && a.cfg.Discovery.Mode == "crawl" {
			go a.runSampleInfohashesLoop(ctx, stats)
		}
	}

	// GC 策略：软上限设为探测到的内存上限（cgroup/物理内存的 70%），
	// GOGC 初始 200 拿内存换 CPU；autoTuneLoop 会根据 heap 水位在
	// 100~400 之间动态调节（见 tuneGC）。必须在 autoTuneLoop 启动前设置，
	// 因为 autoTuneThresholds 会读取当前 memory limit。
	debug.SetMemoryLimit(int64(a.memLimit))
	debug.SetGCPercent(gogcDefault)
	a.logger.Printf("resources: mem_limit=%dMB gogc=%d lru=[ih=%d peer=%d metaReq=%d metaRes=%d remote=%d]",
		a.memLimit/1024/1024, gogcDefault,
		a.infohashSeen.Cap(), a.peerSeen.Cap(),
		a.metadataRequestSeen.Cap(), a.metadataResultSeen.Cap(), a.remoteKnown.Cap())

	// 后台 goroutines
	go a.emitStats(ctx, events, stats, downloader)
	go a.flushPeerCountsLoop(ctx)
	go a.flushRejectLoop(ctx)
	go a.pollPendingRequests(ctx)
	if a.cfg.AutoTune {
		go a.autoTuneLoop(ctx)
	}

	a.logger.Printf("started: instance=%s listen=%v meta_workers=%d nodes_per_dht=%d dht_count=%d",
		a.cfg.InstanceID, addrs,
		a.cfg.Metadata.WorkerQueueSize, a.cfg.Discovery.MaxNodes, len(a.dhts))

	select {
	case <-ctx.Done():
		a.logger.Printf("shutdown")
		return nil
	case err := <-dhtErrors:
		return err
	}
}

func (a *Application) queueActiveLookup(queue chan<- string, infoHash, dedupePrefix string, stats *runtimeStats) bool {
	if !a.cfg.Metadata.Enabled || !a.cfg.Discovery.ActiveLookup || a.remoteKnown.ContainsAndTouch(infoHash) {
		return false
	}
	// Prefix keeps these keys distinct from the source/peer keys that share the
	// same bounded LRU in discovery and combined modes.
	if !a.infohashSeen.Set(dedupePrefix + infoHash) {
		return false
	}
	select {
	case queue <- infoHash:
		stats.activeLookupsQueued.Add(1)
		return true
	default:
		// Admission failed, so this hash has not actually been processed. Undo
		// the seen reservation; otherwise a transient full queue suppresses all
		// later observations until unrelated LRU churn eventually evicts it.
		a.infohashSeen.Delete(dedupePrefix + infoHash)
		stats.activeLookupsDropped.Add(1)
		return false
	}
}

func (a *Application) runSampleInfohashesLoop(ctx context.Context, stats *runtimeStats) {
	rate := a.cfg.Discovery.SampleRate
	interval := time.Second / time.Duration(rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	cursor := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(a.dhts) == 0 {
				continue
			}
			var target [20]byte
			if _, err := cryptorand.Read(target[:]); err != nil {
				continue
			}
			d := a.dhts[cursor%len(a.dhts)]
			cursor++
			sent, err := d.SampleInfohashesLimit(string(target[:]), 1)
			if err == nil && sent > 0 {
				stats.sampleQueriesSent.Add(uint64(sent))
			}
		}
	}
}

func (a *Application) runActiveLookupLoop(ctx context.Context, priority, background <-chan string, stats *runtimeStats) {
	rate := a.cfg.Discovery.LookupRate
	interval := time.Second / time.Duration(rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var cursor atomic.Uint64
	var workers sync.WaitGroup
	for range a.cfg.Discovery.LookupWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				infoHash, ok := nextActiveLookup(ctx, priority, background)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				if len(a.dhts) == 0 || a.remoteKnown.ContainsAndTouch(infoHash) {
					continue
				}
				count := min(a.cfg.Discovery.LookupDHTs, len(a.dhts))
				start := int(cursor.Add(uint64(count)) - uint64(count))
				for i := 0; i < count; i++ {
					d := a.dhts[(start+i)%len(a.dhts)]
					var err error
					if a.cfg.Discovery.LookupFollowups > 0 {
						err = d.GetPeersIterativeLimit(infoHash, a.cfg.Discovery.LookupNodes, a.cfg.Discovery.LookupFollowups)
					} else {
						err = d.GetPeersLimit(infoHash, a.cfg.Discovery.LookupNodes)
					}
					if err == nil {
						stats.activeLookupsSent.Add(1)
					}
				}
			}
		}()
	}
	<-ctx.Done()
	workers.Wait()
}

// nextActiveLookup gives hashes observed in live get_peers traffic strict
// preference over BEP 51 samples. Sampling can consume otherwise-idle lookup
// capacity without pushing fresher, higher-conversion work to the back of a
// shared FIFO.
func nextActiveLookup(ctx context.Context, priority, background <-chan string) (string, bool) {
	select {
	case infoHash := <-priority:
		return infoHash, true
	default:
	}
	select {
	case <-ctx.Done():
		return "", false
	case infoHash := <-priority:
		return infoHash, true
	case infoHash := <-background:
		return infoHash, true
	}
}

// --- 内部 goroutine ---

// runCheckLoop 每2秒批量向 API 检查哪些 hash 已存在，结果写入 remoteKnown LRU。
func (a *Application) runCheckLoop(ctx context.Context, checkQueue <-chan string, stats *runtimeStats) {
	workerCount := a.checkWorkerCount()
	batchQueue := make(chan []string, workerCount*checkWorkerBacklog)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchQueue {
				a.checkBatchExists(batch)
				stats.checkBatchesProcessed.Add(1)
			}
		}()
	}

	ticker := time.NewTicker(checkFlushInterval)
	defer ticker.Stop()
	buf := make([]string, 0, checkBatchSize)
	seen := make(map[string]struct{}, checkBatchSize)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		batch := append([]string(nil), buf...)
		select {
		case batchQueue <- batch:
			stats.checkBatchesQueued.Add(1)
		default:
			stats.checkBatchesDropped.Add(1)
		}
		buf = buf[:0]
		clear(seen)
	}
	defer func() {
		flush()
		close(batchQueue)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case h := <-checkQueue:
			if h == "" {
				continue
			}
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			buf = append(buf, h)
			if len(buf) >= checkBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flushPeerCountsLoop flushes peer counts every 20s or when count exceeds threshold.
func (a *Application) flushPeerCountsLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.flushPeerCounts()
			return
		case <-ticker.C:
			a.flushPeerCounts()
		}
	}
}

// flushPeerCounts 原子替换 peerCounts map 并批量上报，避免上报期间的锁竞争。
func (a *Application) flushPeerCounts() {
	a.peerCountsMu.Lock()
	if len(a.peerCounts) == 0 {
		a.peerCountsMu.Unlock()
		return
	}
	counts := a.peerCounts
	a.peerCounts = make(map[string]int, 4096)
	a.peerCountsMu.Unlock()

	body, _ := json.Marshal(map[string]interface{}{"hashes": counts})
	req, err := http.NewRequest(http.MethodPost,
		a.baseURL()+"/api/v1/torrents/peers",
		bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.Exporter.APIKey != "" {
		req.Header.Set("X-API-Key", a.cfg.Exporter.APIKey)
	}
	resp, err := a.apiClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// pollPendingRequests 每30秒拉取 API 中待抓取的 infohash 列表，触发 DHT get_peers。
func (a *Application) pollPendingRequests(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := a.apiClient.Get(a.baseURL() + "/api/v1/torrents/pending")
			if err != nil {
				continue
			}
			var pending []string
			if json.NewDecoder(resp.Body).Decode(&pending) != nil {
				resp.Body.Close()
				continue
			}
			resp.Body.Close()
			for _, h := range pending {
				for _, d := range a.dhts {
					_ = d.GetPeers(h)
				}
			}
		}
	}
}

// gogcDefault 是启动时的 GOGC；autotune 在 gogcMin~gogcMax 间动态调节。
const (
	gogcDefault = 200
	gogcMin     = 100
	gogcMax     = 400
)

// autoTuneLoop 每30秒采样内存、CPU 和队列状态，动态调整运行参数。
//
// 调节的参数：
//   - metaPaused：heap 接近内存上限时暂停 metadata 请求入队（防 OOM 最后防线）
//   - GOGC：按 heap 水位在 100~400 间调节——内存富余时少跑 GC 省 CPU，
//     内存紧张时勤跑 GC 压 heap（见 tuneGC）
//   - cpuUtilPct：采样进程 CPU 利用率，供 tuneWireWorkers 读取
func (a *Application) autoTuneLoop(ctx context.Context) {
	const tickInterval = 30 * time.Second
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	pauseThreshold, resumeThreshold := a.autoTuneThresholds()
	controller := &autoTuneController{}
	gogcCurrent := gogcDefault
	var prevIdleCPU, prevTotalCPU float64
	// runtime/metrics has no /memory/classes/heap/inuse metric. HeapInuse is
	// equivalent to object bytes plus the unused tail space in in-use spans.
	// Allocate this slice once; the 30s control loop can safely reuse it.
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	a.logger.Printf("autotune: enabled pause=%dMB resume=%dMB", pauseThreshold/1024/1024, resumeThreshold/1024/1024)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// G11: runtime/metrics avoids STW unlike ReadMemStats.
			heapAlloc, heapInUse, idleCPU, totalCPU, ok := readRuntimeResources(samples)
			if !ok {
				a.logger.Printf("autotune: runtime metrics unavailable; skipping sample")
				continue
			}
			paused := a.metaPaused.Load()

			// CPU 利用率 = 1 - idle/total（本采样周期的增量）
			cpuUtil := 0.0
			if dTotal := totalCPU - prevTotalCPU; dTotal > 0 {
				cpuUtil = 1 - (idleCPU-prevIdleCPU)/dTotal
				if cpuUtil < 0 {
					cpuUtil = 0
				}
				if cpuUtil > 1 {
					cpuUtil = 1
				}
			}
			prevIdleCPU, prevTotalCPU = idleCPU, totalCPU
			a.cpuUtilPct.Store(uint64(cpuUtil * 10_000))

			// 按 heap 水位调节 GOGC（内存换 CPU 的核心旋钮）
			gogcCurrent = a.tuneGC(gogcCurrent, heapInUse)

			action := controller.nextAction(time.Now(), paused, heapAlloc, pauseThreshold, resumeThreshold)

			switch action {
			case autoTunePause:
				a.metaPaused.Store(true)
				debug.FreeOSMemory()
				a.logger.Printf("autotune: metadata paused (heap_alloc=%dMB > %dMB, heap_inuse=%dMB)",
					heapAlloc/1024/1024, pauseThreshold/1024/1024, heapInUse/1024/1024)
			case autoTuneResume:
				a.metaPaused.Store(false)
				a.logger.Printf("autotune: metadata resumed (heap_alloc=%dMB < %dMB, heap_inuse=%dMB)",
					heapAlloc/1024/1024, resumeThreshold/1024/1024, heapInUse/1024/1024)
			default:
				a.logger.Printf("autotune: heap_alloc=%dMB heap_inuse=%dMB cpu=%.0f%% gogc=%d paused=%v seenSizes=[ih=%d peer=%d metaReq=%d remote=%d]",
					heapAlloc/1024/1024, heapInUse/1024/1024, cpuUtil*100, gogcCurrent, paused,
					a.infohashSeen.Len(), a.peerSeen.Len(),
					a.metadataRequestSeen.Len(), a.remoteKnown.Len())
			}
		}
	}
}

// readRuntimeResources reads the small set of stable runtime metrics used by
// the controller. Checking Kind before accessing values keeps a renamed or
// unavailable metric from crashing a long-running crawler.
func readRuntimeResources(samples []runtimemetrics.Sample) (heapAlloc, heapInUse uint64, idleCPU, totalCPU float64, ok bool) {
	if len(samples) != 4 {
		return 0, 0, 0, 0, false
	}
	runtimemetrics.Read(samples)
	if samples[0].Value.Kind() != runtimemetrics.KindUint64 ||
		samples[1].Value.Kind() != runtimemetrics.KindUint64 ||
		samples[2].Value.Kind() != runtimemetrics.KindFloat64 ||
		samples[3].Value.Kind() != runtimemetrics.KindFloat64 {
		return 0, 0, 0, 0, false
	}
	heapAlloc = samples[0].Value.Uint64()
	heapInUse = heapAlloc + samples[1].Value.Uint64()
	idleCPU = samples[2].Value.Float64()
	totalCPU = samples[3].Value.Float64()
	return heapAlloc, heapInUse, idleCPU, totalCPU, true
}

// tuneGC 按 heap 占内存上限的比例调节 GOGC，返回生效值。
//
//	< 50% → GOGC 400：内存富余，让 heap 涨、GC 少跑，省下的 CPU 给收发包
//	50~70% → GOGC 200：过渡区
//	> 70% → GOGC 100：接近上限，勤跑 GC 压住 heap（配合 SetMemoryLimit 兜底）
func (a *Application) tuneGC(current int, heapInUse uint64) int {
	target := gogcDefault
	switch {
	case heapInUse > a.memLimit*70/100:
		target = gogcMin
	case heapInUse < a.memLimit*50/100:
		target = gogcMax
	}
	if target != current {
		debug.SetGCPercent(target)
		a.logger.Printf("autotune: gogc %d -> %d (heap_inuse=%dMB / limit=%dMB)",
			current, target, heapInUse/1024/1024, a.memLimit/1024/1024)
	}
	return target
}

func (c *autoTuneController) nextAction(now time.Time, paused bool, heapAlloc, pauseThreshold, resumeThreshold uint64) autoTuneAction {
	if paused {
		c.overLimitSamples = 0
		if c.pausedAt.IsZero() {
			c.pausedAt = now
		}
		if heapAlloc < resumeThreshold {
			c.underLimitSamples++
		} else {
			c.underLimitSamples = 0
		}
		if c.underLimitSamples >= autoTuneResumeSamples && now.Sub(c.pausedAt) >= autoTuneMinPause {
			c.underLimitSamples = 0
			c.pausedAt = time.Time{}
			return autoTuneResume
		}
		return autoTuneNoop
	}

	c.pausedAt = time.Time{}
	c.underLimitSamples = 0
	if heapAlloc > pauseThreshold {
		c.overLimitSamples++
	} else {
		c.overLimitSamples = 0
	}
	if c.overLimitSamples >= autoTunePauseSamples {
		c.overLimitSamples = 0
		c.pausedAt = now
		return autoTunePause
	}
	return autoTuneNoop
}

func (a *Application) tuneWireWorkers(ctx context.Context, downloader *dht.Wire) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	maxWorkers := downloader.MaxWorkers()
	minWorkers := clampInt(maxWorkers/8, 64, maxWorkers)
	if maxWorkers < 64 {
		minWorkers = 1
	}

	var prevDialAttempts, prevDialOK, prevHandshakeOK, prevDownloadOK int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dialAttempts := downloader.Stats.DialAttempts.Load()
			dialOK := downloader.Stats.DialOK.Load()
			handshakeOK := downloader.Stats.HandshakeOK.Load()
			downloadOK := downloader.Stats.DownloadOK.Load()

			attemptsDelta := dialAttempts - prevDialAttempts
			dialOKDelta := dialOK - prevDialOK
			handshakeDelta := handshakeOK - prevHandshakeOK
			downloadDelta := downloadOK - prevDownloadOK

			prevDialAttempts = dialAttempts
			prevDialOK = dialOK
			prevHandshakeOK = handshakeOK
			prevDownloadOK = downloadOK

			active := downloader.ActiveWorkers()
			target := active
			reqDepth := downloader.RequestDepth()
			respDepth := downloader.ResponseDepth()

			// CPU 利用率（由 autoTuneLoop 采样；autotune 关闭时恒为 0，不影响判断）
			cpuUtil := float64(a.cpuUtilPct.Load()) / 10_000

			if a.metaPaused.Load() {
				target = minWorkers
			} else if cpuUtil > 0.95 && active > minWorkers {
				// CPU 过载：wire worker 的握手/SHA1/bencode 解析在抢占
				// UDP 收包和 DHT 处理的 CPU，收缩让位给发现侧
				target = active * 3 / 4
			} else if respDepth > active*8 && active > minWorkers {
				target = active * 3 / 4
			} else if attemptsDelta > 0 {
				dialRate := float64(dialOKDelta) / float64(attemptsDelta)
				handshakeRate := 0.0
				if dialOKDelta > 0 {
					handshakeRate = float64(handshakeDelta) / float64(dialOKDelta)
				}

				switch {
				case attemptsDelta > int64(active*2) && (dialRate < 0.005 || (dialOKDelta > 32 && handshakeRate < 0.01)):
					target = active * 3 / 4
				case cpuUtil > 0.90:
					// CPU 接近饱和：保持现状，不再扩容
				case reqDepth > active*4 && dialRate >= 0.02 && handshakeRate >= 0.03:
					target = active + maxInt(16, active/4)
				case reqDepth > active && downloadDelta > int64(maxInt(8, active/2)):
					target = active + maxInt(8, active/8)
				}
			}

			target = clampInt(target, minWorkers, maxWorkers)
			if target != active {
				downloader.SetActiveWorkers(target)
				a.logger.Printf(
					"wire autotune: active=%d target=%d req_depth=%d resp_depth=%d attempts=%d dial_ok=%d handshake_ok=%d download_ok=%d paused=%v",
					active, target, reqDepth, respDepth, attemptsDelta, dialOKDelta, handshakeDelta, downloadDelta, a.metaPaused.Load())
			}
		}
	}
}

// lruEntryBytes 是单个 LRU 条目的估算内存占用：
// map bucket + list.Element + entry 结构 + key 字符串（40~90 字节）。
const lruEntryBytes = 192

// newLRUCaps 按内存上限缩放各去重 LRU 的容量。
//
// 去重缓存是"内存换带宽"最直接的杠杆：容量不足时 remoteKnown /
// metadataRequestSeen 频繁淘汰，同一 infohash 被反复拨号、重复下载，
// 浪费 wire worker 和带宽。这里拿内存上限的 ~15% 做去重预算，
// 一台 8GB 机器（limit≈5.6GB）可支撑约 440 万条目。
func newLRUCaps(cfg config.Config, memLimit uint64) lruCapConfig {
	if memLimit == 0 {
		memLimit = 2 << 30
	}
	budget := memLimit * 15 / 100
	totalEntries := int(budget / lruEntryBytes)
	// 下限保持旧版最小值量级，避免极小内存机器上容量过低
	totalEntries = clampInt(totalEntries, 200_000, 50_000_000)

	// 分配比例：拦截重复下载的两个缓存（remoteKnown、metadataRequestSeen）
	// 直接节省 wire worker，拿最大份额。
	remoteKnownCap := totalEntries * 30 / 100
	metadataRequestCap := totalEntries * 30 / 100
	infohashCap := totalEntries * 15 / 100
	peerCap := totalEntries * 15 / 100
	metadataResultCap := totalEntries * 10 / 100

	switch cfg.Role {
	case "discovery":
		// discovery 不下载 metadata，把预算让给 peer/infohash 去重
		metadataRequestCap = totalEntries * 5 / 100
		metadataResultCap = totalEntries * 5 / 100
		infohashCap = totalEntries * 30 / 100
		peerCap = totalEntries * 30 / 100
	case "metadata":
		peerCap /= 2
		infohashCap /= 2
	}

	return lruCapConfig{
		infohashSeen:        infohashCap,
		peerSeen:            peerCap,
		metadataRequestSeen: metadataRequestCap,
		metadataResultSeen:  metadataResultCap,
		remoteKnown:         remoteKnownCap,
	}
}

func (a *Application) autoTuneThresholds() (pauseThreshold uint64, resumeThreshold uint64) {
	if memoryLimit := debug.SetMemoryLimit(-1); memoryLimit > 0 && memoryLimit < 1<<60 {
		pauseThreshold = uint64(memoryLimit * 80 / 100)
		resumeThreshold = uint64(memoryLimit * 64 / 100)
		return pauseThreshold, resumeThreshold
	}

	pauseThreshold = 512 * 1024 * 1024
	if a.cfg.Role != "discovery" {
		pauseThreshold = 768 * 1024 * 1024
	}
	pauseThreshold += uint64(maxInt(a.cfg.EventQueue-4_096, 0)) * 1024
	pauseThreshold += uint64(maxInt(a.cfg.Metadata.RequestQueueSize-8_192, 0)) * 256
	pauseThreshold += uint64(maxInt(a.cfg.Metadata.WorkerQueueSize-256, 0)) * 512 * 1024
	pauseThreshold = clampUint64(pauseThreshold, 512*1024*1024, 2*1024*1024*1024)

	resumeThreshold = pauseThreshold * 70 / 100
	resumeThreshold = clampUint64(resumeThreshold, 512*1024*1024, pauseThreshold-128*1024*1024)
	return pauseThreshold, resumeThreshold
}

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func clampUint64(value, minValue, maxValue uint64) uint64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// --- 事件提交 ---

func (a *Application) submitInfohashEvent(events chan<- pipeline.Event, ihHex, ip string, port int, source string, stats *runtimeStats, now time.Time) {
	if !a.shouldEmitPeerEvents() {
		return
	}

	key := buildInfohashSourceKey(ihHex, source, ip, port)
	// LRU.Set 返回 false = 已存在（已见过）
	if !a.infohashSeen.Set(key) {
		stats.infohashEventsDeduped.Add(1)
		return
	}
	a.submitEvent(events, pipeline.Event{
		Type:       pipeline.EventPeerDiscovered,
		Timestamp:  now,
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   ihHex,
		IP:         ip,
		Port:       port,
	}, stats.infohashEventsDropped.Add, stats.infohashEventsSent.Add)
}

func (a *Application) submitPeerEvent(events chan<- pipeline.Event, ihHex, ip string, port int, source string, stats *runtimeStats, now time.Time) {
	if !a.shouldEmitPeerEvents() {
		return
	}

	key := buildInfohashPeerKey(ihHex, ip, port)
	if !a.peerSeen.Set(key) {
		stats.peerEventsDeduped.Add(1)
		return
	}
	a.submitEvent(events, pipeline.Event{
		Type:       pipeline.EventPeerDiscovered,
		Timestamp:  now,
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   ihHex,
		IP:         ip,
		Port:       port,
	}, stats.peerEventsDropped.Add, stats.peerEventsSent.Add)
}

func (a *Application) queueMetadataRequest(downloader *dht.Wire, ihHex, ip string, port int, source dht.PeerSource, stats *runtimeStats, checkQueue chan<- string) {
	if !a.cfg.Metadata.Enabled {
		return
	}

	requestKey := buildInfohashPeerKey(ihHex, ip, port)
	if !a.metadataRequestSeen.Set(requestKey) {
		stats.metadataRequestsDeduped.Add(1)
		// 累加 peer count（该 hash 再次被目击，但不重复下载）
		a.incPeerCount(ihHex)
		return
	}

	// 已被远端 API 确认存在：跳过下载，只累加 peer count
	if a.remoteKnown.ContainsAndTouch(ihHex) {
		stats.metadataRequestsDeduped.Add(1)
		a.incPeerCount(ihHex)
		return
	}

	infoHashBytes, err := hex.DecodeString(ihHex)
	if err != nil {
		return
	}

	// 加入远端检查队列（non-blocking，满了就跳过，不阻塞 UDP 处理）
	select {
	case checkQueue <- ihHex:
	default:
	}

	// 自适应调优：内存压力高时暂停 metadata 入队
	if a.metaPaused.Load() {
		return
	}

	stats.metadataRequestsQueued.Add(1)
	downloader.RequestFromSource(infoHashBytes, ip, port, source)
}

func (a *Application) incPeerCount(ihHex string) {
	a.peerCountsMu.Lock()
	a.peerCounts[ihHex]++
	a.peerCountsMu.Unlock()
}

func (a *Application) startMetadataConsumers(ctx context.Context, downloader *dht.Wire, events chan<- pipeline.Event, stats *runtimeStats) {
	workers := runtime.GOMAXPROCS(0) * 2
	workers = clampInt(workers, 4, 64)
	var ok, fail atomic.Uint64
	for i := 0; i < workers; i++ {
		go a.consumeMetadata(ctx, downloader, events, stats, &ok, &fail)
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.logger.Printf("metadata download: ok=%d fail=%d (30s)", ok.Swap(0), fail.Swap(0))
			}
		}
	}()
}

func (a *Application) consumeMetadata(
	ctx context.Context,
	downloader *dht.Wire,
	events chan<- pipeline.Event,
	stats *runtimeStats,
	ok *atomic.Uint64,
	fail *atomic.Uint64,
) {
	responses := downloader.Response()

	for {
		select {
		case <-ctx.Done():
			return
		case response := <-responses:
			ihHex := hex.EncodeToString(response.InfoHash)
			// G7: dedupe by infohash only; one result per hash is sufficient.
			if !a.metadataResultSeen.Set(ihHex) {
				stats.metadataEventsDeduped.Add(1)
				fail.Add(1)
				continue
			}
			metadata, err := normalizeMetadata(response.MetadataInfo)
			if err != nil {
				stats.metadataEventsDeduped.Add(1)
				fail.Add(1)
				continue
			}
			// Record script-level regional signals before content filtering so
			// filter policy cannot bias cross-region comparisons.
			stats.metadataLocale.observe(metadata)
			// Apply content filter rules before reporting to the backend.
			if reason := a.filterChain.Apply(metadata); reason != filter.ReasonPass {
				stats.metadataEventsFiltered.Add(1)
				// Prevent re-requesting this hash within the current session.
				a.remoteKnown.Set(ihHex)
				// Asynchronously notify the backend so it can persist the
				// rejection in the cuckoo filter, preventing re-requests across
				// process restarts. Drop silently when the channel is full.
				select {
				case a.rejectQueue <- ihHex:
				default:
				}
				continue
			}
			ok.Add(1)
			// 下载成功后立即标记为已知：后续同一 hash 的其他 peer 宣告会在
			// queueMetadataRequest 的 remoteKnown 检查处被短路（跳过重复下载，
			// 只累加 peer count）。热门种子有成百上千个宣告 peer，缺少这一行
			// 会导致同一 metadata 被完整重复下载，浪费稀缺的 wire worker。
			a.remoteKnown.Set(ihHex)
			// metadata 是整条流水线的产物：下载它花了一个稀缺 wire worker 的
			// 拨号+握手+传输。这里用阻塞式提交而非丢弃——导出通道满（后端 429
			// 背压时 httpSink 会 sleep 30s）时宁可让 consumeMetadata 等待，
			// 背压经 responses/requests 通道自然回传到拨号侧放慢下载，也绝不
			// 丢弃已下载的 metadata。peer/infohash 事件廉价、可丢，仍走非阻塞。
			a.submitMetadataEvent(ctx, events, pipeline.Event{
				Type:       pipeline.EventMetadataFetched,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "peer_wire",
				InfoHash:   ihHex,
				IP:         response.IP,
				Port:       response.Port,
				Metadata:   metadata,
			}, stats)
		}
	}
}

func (a *Application) emitStats(ctx context.Context, events chan<- pipeline.Event, stats *runtimeStats, downloader *dht.Wire) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var previous statsSnapshot
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			packetStats := a.aggregatePacketStats()
			current := statsSnapshot{
				infohashEventsSent:      stats.infohashEventsSent.Load(),
				infohashEventsDropped:   stats.infohashEventsDropped.Load(),
				infohashEventsDeduped:   stats.infohashEventsDeduped.Load(),
				peerEventsSent:          stats.peerEventsSent.Load(),
				peerEventsDropped:       stats.peerEventsDropped.Load(),
				peerEventsDeduped:       stats.peerEventsDeduped.Load(),
				metadataRequestsQueued:  stats.metadataRequestsQueued.Load(),
				metadataRequestsDeduped: stats.metadataRequestsDeduped.Load(),
				checkBatchesQueued:      stats.checkBatchesQueued.Load(),
				checkBatchesDropped:     stats.checkBatchesDropped.Load(),
				checkBatchesProcessed:   stats.checkBatchesProcessed.Load(),
				activeLookupsQueued:     stats.activeLookupsQueued.Load(),
				activeLookupsDropped:    stats.activeLookupsDropped.Load(),
				activeLookupsSent:       stats.activeLookupsSent.Load(),
				sampleQueriesSent:       stats.sampleQueriesSent.Load(),
				sampleResponses:         stats.sampleResponses.Load(),
				sampleHashesReceived:    stats.sampleHashesReceived.Load(),
				sampleHashesQueued:      stats.sampleHashesQueued.Load(),
				metadataEventsSent:      stats.metadataEventsSent.Load(),
				metadataEventsDropped:   stats.metadataEventsDropped.Load(),
				metadataEventsDeduped:   stats.metadataEventsDeduped.Load(),
				metadataEventsFiltered:  stats.metadataEventsFiltered.Load(),
				metadataLocale:          stats.metadataLocale.snapshot(),
				dhtPacketsReceived:      packetStats.Received,
				dhtPacketsEnqueued:      packetStats.Enqueued,
				dhtPacketsDropped:       packetStats.Dropped,
				dhtPacketsHandled:       packetStats.Handled,
				dhtPacketDecodeErrors:   packetStats.DecodeErrors,
				dhtBytesReceived:        packetStats.BytesReceived,
				dhtBytesSent:            packetStats.BytesSent,
				dhtFollowupsSent:        packetStats.FollowupsSent,
				dhtRoutingNodes:         packetStats.RoutingNodes,
				dhtNodesInserted:        packetStats.NodesInserted,
				dhtNodesRemoved:         packetStats.NodesRemoved,
				dhtRefreshQueries:       packetStats.RefreshQueries,
				wireQueueDropped:        downloader.Stats.QueueDropped.Load(),
				wireDialAttempts:        downloader.Stats.DialAttempts.Load(),
				wireDialOK:              downloader.Stats.DialOK.Load(),
				wireDialFailed:          downloader.Stats.DialFailed.Load(),
				wireHandshakeOK:         downloader.Stats.HandshakeOK.Load(),
				wireHandshakeFailed:     downloader.Stats.HandshakeFailed.Load(),
				wireDownloadOK:          downloader.Stats.DownloadOK.Load(),
				wireDownloadFailed:      downloader.Stats.DownloadFailed.Load(),
				wireBlacklisted:         downloader.Stats.Blacklisted.Load(),
			}
			funnel := downloader.FunnelBySource()
			announce := funnel[dht.PeerSourceAnnounce]
			getPeers := funnel[dht.PeerSourceGetPeers]
			current.wireAnnounceQueued = announce.Queued
			current.wireAnnounceBlacklist = announce.Blacklisted
			current.wireAnnounceInflight = announce.InflightDeduped
			current.wireAnnounceDial = announce.DialAttempts
			current.wireAnnounceDialOK = announce.DialOK
			current.wireAnnounceDownload = announce.DownloadOK
			current.wireGetPeersQueued = getPeers.Queued
			current.wireGetPeersBlacklist = getPeers.Blacklisted
			current.wireGetPeersInflight = getPeers.InflightDeduped
			current.wireGetPeersDial = getPeers.DialAttempts
			current.wireGetPeersDialOK = getPeers.DialOK
			current.wireGetPeersDownload = getPeers.DownloadOK
			blStats := downloader.BlacklistStats()
			current.wireBlacklistSize = int64(blStats.Size)
			current.wireBlacklistMax = int64(blStats.MaxSize)
			current.wireBlacklistRejected = blStats.InsertRejected
			current.wireBlacklistExpired = blStats.ExpiredEvicted
			a.logRuntimeDelta(current, previous)
			previous = current
			if !a.shouldEmitWorkerStats() {
				continue
			}
			workerStats := map[string]uint64{
				"infohash_events_sent":      current.infohashEventsSent,
				"infohash_events_dropped":   current.infohashEventsDropped,
				"infohash_events_deduped":   current.infohashEventsDeduped,
				"peer_events_sent":          current.peerEventsSent,
				"peer_events_dropped":       current.peerEventsDropped,
				"peer_events_deduped":       current.peerEventsDeduped,
				"metadata_requests_queued":  current.metadataRequestsQueued,
				"metadata_requests_deduped": current.metadataRequestsDeduped,
				"check_batches_queued":      current.checkBatchesQueued,
				"check_batches_dropped":     current.checkBatchesDropped,
				"check_batches_processed":   current.checkBatchesProcessed,
				"active_lookups_queued":     current.activeLookupsQueued,
				"active_lookups_dropped":    current.activeLookupsDropped,
				"active_lookups_sent":       current.activeLookupsSent,
				"sample_queries_sent":       current.sampleQueriesSent,
				"sample_responses":          current.sampleResponses,
				"sample_hashes_received":    current.sampleHashesReceived,
				"sample_hashes_queued":      current.sampleHashesQueued,
				"metadata_events_sent":      current.metadataEventsSent,
				"metadata_events_dropped":   current.metadataEventsDropped,
				"metadata_events_deduped":   current.metadataEventsDeduped,
				"metadata_events_filtered":  current.metadataEventsFiltered,
				"dht_packets_received":      current.dhtPacketsReceived,
				"dht_packets_enqueued":      current.dhtPacketsEnqueued,
				"dht_packets_dropped":       current.dhtPacketsDropped,
				"dht_packets_handled":       current.dhtPacketsHandled,
				"dht_packet_decode_errors":  current.dhtPacketDecodeErrors,
				"dht_bytes_received":        current.dhtBytesReceived,
				"dht_bytes_sent":            current.dhtBytesSent,
				"dht_followups_sent":        current.dhtFollowupsSent,
				"dht_routing_nodes":         current.dhtRoutingNodes,
				"dht_nodes_inserted":        current.dhtNodesInserted,
				"dht_nodes_removed":         current.dhtNodesRemoved,
				"dht_refresh_queries":       current.dhtRefreshQueries,
				"wire_queue_dropped":        uint64(current.wireQueueDropped),
				"wire_dial_attempts":        uint64(current.wireDialAttempts),
				"wire_dial_ok":              uint64(current.wireDialOK),
				"wire_dial_failed":          uint64(current.wireDialFailed),
				"wire_handshake_ok":         uint64(current.wireHandshakeOK),
				"wire_handshake_failed":     uint64(current.wireHandshakeFailed),
				"wire_download_ok":          uint64(current.wireDownloadOK),
				"wire_download_failed":      uint64(current.wireDownloadFailed),
				"wire_blacklisted":          uint64(current.wireBlacklisted),
				"wire_announce_queued":      uint64(current.wireAnnounceQueued),
				"wire_announce_blacklisted": uint64(current.wireAnnounceBlacklist),
				"wire_announce_inflight":    uint64(current.wireAnnounceInflight),
				"wire_announce_dial":        uint64(current.wireAnnounceDial),
				"wire_announce_dial_ok":     uint64(current.wireAnnounceDialOK),
				"wire_announce_download":    uint64(current.wireAnnounceDownload),
				"wire_getpeers_queued":      uint64(current.wireGetPeersQueued),
				"wire_getpeers_blacklisted": uint64(current.wireGetPeersBlacklist),
				"wire_getpeers_inflight":    uint64(current.wireGetPeersInflight),
				"wire_getpeers_dial":        uint64(current.wireGetPeersDial),
				"wire_getpeers_dial_ok":     uint64(current.wireGetPeersDialOK),
				"wire_getpeers_download":    uint64(current.wireGetPeersDownload),
				// 黑名单诊断为当前瞬时值（gauge），非窗口增量。
				"wire_blacklist_size":       uint64(current.wireBlacklistSize),
				"wire_blacklist_max":        uint64(current.wireBlacklistMax),
				"wire_blacklist_rejected":   uint64(current.wireBlacklistRejected),
				"wire_blacklist_expired":    uint64(current.wireBlacklistExpired),
			}
			current.metadataLocale.addWorkerStats(workerStats)
			a.submitEvent(events, pipeline.Event{
				Type:       pipeline.EventWorkerStats,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "runtime",
				Stats:      workerStats,
			}, func(delta uint64) uint64 { return delta }, func(delta uint64) uint64 { return delta })
		}
	}
}

func (a *Application) logRuntimeDelta(current, previous statsSnapshot) {
	// DHT UDP 带宽（不含 peer-wire TCP 流量），KB/s
	const interval = 30
	netInKBps := (current.dhtBytesReceived - previous.dhtBytesReceived) / 1024 / interval
	netOutKBps := (current.dhtBytesSent - previous.dhtBytesSent) / 1024 / interval
	localeDelta := current.metadataLocale.subtract(previous.metadataLocale)
	a.logger.Printf(
		"runtime 30s: dht_recv=%d handled=%d dropped=%d decode_err=%d net_in=%dKB/s net_out=%dKB/s nodes=%d node_add=%d node_rm=%d refresh_q=%d lookup_queue=%d lookup_drop=%d lookup_sent=%d follow_sent=%d sample_q=%d sample_resp=%d sample_hash=%d sample_unique=%d peer_sent=%d peer_drop=%d peer_dedup=%d meta_req=%d meta_req_dedup=%d meta_sent=%d meta_drop=%d meta_dedup=%d meta_filtered=%d meta_locale_n=%d meta_han=%d meta_kana=%d meta_hangul=%d meta_zh_proxy=%d check_drop=%d paused=%v wire_q_drop=%d wire_dial=%d wire_conn=%d wire_dial_fail=%d wire_hs=%d wire_hs_fail=%d wire_ok=%d wire_dl_fail=%d wire_bl=%d ann_q=%d ann_bl=%d ann_inflight=%d ann_dial=%d ann_conn=%d ann_ok=%d gp_q=%d gp_bl=%d gp_inflight=%d gp_dial=%d gp_conn=%d gp_ok=%d bl_size=%d bl_max=%d bl_reject=%d bl_expired=%d",
		current.dhtPacketsReceived-previous.dhtPacketsReceived,
		current.dhtPacketsHandled-previous.dhtPacketsHandled,
		current.dhtPacketsDropped-previous.dhtPacketsDropped,
		current.dhtPacketDecodeErrors-previous.dhtPacketDecodeErrors,
		netInKBps,
		netOutKBps,
		current.dhtRoutingNodes,
		current.dhtNodesInserted-previous.dhtNodesInserted,
		current.dhtNodesRemoved-previous.dhtNodesRemoved,
		current.dhtRefreshQueries-previous.dhtRefreshQueries,
		current.activeLookupsQueued-previous.activeLookupsQueued,
		current.activeLookupsDropped-previous.activeLookupsDropped,
		current.activeLookupsSent-previous.activeLookupsSent,
		current.dhtFollowupsSent-previous.dhtFollowupsSent,
		current.sampleQueriesSent-previous.sampleQueriesSent,
		current.sampleResponses-previous.sampleResponses,
		current.sampleHashesReceived-previous.sampleHashesReceived,
		current.sampleHashesQueued-previous.sampleHashesQueued,
		current.peerEventsSent-previous.peerEventsSent,
		current.peerEventsDropped-previous.peerEventsDropped,
		current.peerEventsDeduped-previous.peerEventsDeduped,
		current.metadataRequestsQueued-previous.metadataRequestsQueued,
		current.metadataRequestsDeduped-previous.metadataRequestsDeduped,
		current.metadataEventsSent-previous.metadataEventsSent,
		current.metadataEventsDropped-previous.metadataEventsDropped,
		current.metadataEventsDeduped-previous.metadataEventsDeduped,
		current.metadataEventsFiltered-previous.metadataEventsFiltered,
		localeDelta.classified,
		localeDelta.han,
		localeDelta.kana,
		localeDelta.hangul,
		localeDelta.chineseProxy,
		current.checkBatchesDropped-previous.checkBatchesDropped,
		a.metaPaused.Load(),
		current.wireQueueDropped-previous.wireQueueDropped,
		current.wireDialAttempts-previous.wireDialAttempts,
		current.wireDialOK-previous.wireDialOK,
		current.wireDialFailed-previous.wireDialFailed,
		current.wireHandshakeOK-previous.wireHandshakeOK,
		current.wireHandshakeFailed-previous.wireHandshakeFailed,
		current.wireDownloadOK-previous.wireDownloadOK,
		current.wireDownloadFailed-previous.wireDownloadFailed,
		current.wireBlacklisted-previous.wireBlacklisted,
		current.wireAnnounceQueued-previous.wireAnnounceQueued,
		current.wireAnnounceBlacklist-previous.wireAnnounceBlacklist,
		current.wireAnnounceInflight-previous.wireAnnounceInflight,
		current.wireAnnounceDial-previous.wireAnnounceDial,
		current.wireAnnounceDialOK-previous.wireAnnounceDialOK,
		current.wireAnnounceDownload-previous.wireAnnounceDownload,
		current.wireGetPeersQueued-previous.wireGetPeersQueued,
		current.wireGetPeersBlacklist-previous.wireGetPeersBlacklist,
		current.wireGetPeersInflight-previous.wireGetPeersInflight,
		current.wireGetPeersDial-previous.wireGetPeersDial,
		current.wireGetPeersDialOK-previous.wireGetPeersDialOK,
		current.wireGetPeersDownload-previous.wireGetPeersDownload,
		// 黑名单为瞬时 gauge：size/max 原样打印，reject/expired 打累计增量。
		current.wireBlacklistSize,
		current.wireBlacklistMax,
		current.wireBlacklistRejected-previous.wireBlacklistRejected,
		current.wireBlacklistExpired-previous.wireBlacklistExpired,
	)
}

func buildInfohashSourceKey(ihHex, source, ip string, port int) string {
	b := make([]byte, 0, len(ihHex)+len(source)+len(ip)+16)
	b = append(b, ihHex...)
	b = append(b, '|')
	b = append(b, source...)
	b = append(b, '|')
	b = append(b, ip...)
	b = append(b, '|')
	b = strconv.AppendInt(b, int64(port), 10)
	return string(b)
}

func buildInfohashPeerKey(ihHex, ip string, port int) string {
	b := make([]byte, 0, len(ihHex)+len(ip)+14)
	b = append(b, ihHex...)
	b = append(b, '|')
	b = append(b, ip...)
	b = append(b, '|')
	b = strconv.AppendInt(b, int64(port), 10)
	return string(b)
}

func (a *Application) submitEvent(events chan<- pipeline.Event, event pipeline.Event, onDrop func(uint64) uint64, onSuccess func(uint64) uint64) {
	if !a.shouldExportEvent(event) {
		return
	}

	select {
	case events <- event:
		onSuccess(1)
	default:
		onDrop(1)
	}
}

// submitMetadataEvent 阻塞式提交 metadata 事件，永不丢弃已下载的产物。
// 通道满时等待，直到导出侧腾出空间或进程退出（ctx 取消）。
func (a *Application) submitMetadataEvent(ctx context.Context, events chan<- pipeline.Event, event pipeline.Event, stats *runtimeStats) {
	if !a.shouldExportEvent(event) {
		return
	}

	// 快路径：通道有空位立即入队，不进入 select 的调度开销。
	select {
	case events <- event:
		stats.metadataEventsSent.Add(1)
		return
	default:
	}

	// 慢路径：通道已满（后端背压），阻塞等待而非丢弃。
	select {
	case events <- event:
		stats.metadataEventsSent.Add(1)
	case <-ctx.Done():
		stats.metadataEventsDropped.Add(1)
	}
}

// --- 过滤与拒绝上报 ---

// flushRejectLoop 批量将被过滤的 hash 上报给后端 /api/v1/torrents/reject，
// 让后端 cuckoo filter 记住这些 hash，避免跨进程重启后重复下载。
func (a *Application) flushRejectLoop(ctx context.Context) {
	const batchSize = 512
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	buf := make([]string, 0, batchSize)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		a.reportRejected(buf)
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case h := <-a.rejectQueue:
			buf = append(buf, h)
			if len(buf) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// reportRejected POSTs a batch of filtered infohashes to the backend so they
// are added to the exact rejected-hash store. Only runs when the exporter is
// HTTP (i.e. there is an actual backend to talk to).
func (a *Application) reportRejected(hashes []string) {
	if len(hashes) == 0 || a.cfg.Exporter.Kind != "http" {
		return
	}
	body, _ := json.Marshal(hashes)
	req, err := http.NewRequest(http.MethodPost,
		a.baseURL()+"/api/v1/torrents/reject",
		bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.Exporter.APIKey != "" {
		req.Header.Set("X-API-Key", a.cfg.Exporter.APIKey)
	}
	resp, err := a.apiClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// buildFilterChain constructs the ordered filter chain from configuration.
// A threshold ≤ 0 disables the corresponding rule.
func buildFilterChain(cfg config.FilterConfig) *filter.Chain {
	c := filter.NewChain()
	if cfg.MaxFileCount > 0 {
		c.Add("too_many_files", filter.TooManyFiles(cfg.MaxFileCount))
	}
	if cfg.MaxFileCountNonCN > 0 {
		c.Add("non_chinese_files", filter.NonChineseHighFileCount(cfg.MaxFileCountNonCN))
	}
	if cfg.MaxFileCountNumeric > 0 {
		c.Add("numeric_file_names", filter.NumericOnlyFileNames(cfg.MaxFileCountNumeric))
	}
	return c
}

// --- 辅助函数 ---

func (a *Application) listenAddrs() ([]string, error) {
	if addrs := splitCommaSeparated(a.cfg.ListenAddrs); len(addrs) > 0 {
		return addrs, nil
	}
	count := a.cfg.Discovery.Instances
	if count <= 1 {
		return []string{a.cfg.ListenAddr}, nil
	}
	host, rawPort, err := net.SplitHostPort(a.cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("expand listen address %q: %w", a.cfg.ListenAddr, err)
	}
	startPort, err := strconv.Atoi(rawPort)
	if err != nil || startPort < 1 || startPort+count-1 > 65535 {
		return nil, fmt.Errorf("cannot allocate %d DHT ports from %q", count, a.cfg.ListenAddr)
	}
	addrs := make([]string, count)
	for i := range addrs {
		addrs[i] = net.JoinHostPort(host, strconv.Itoa(startPort+i))
	}
	return addrs, nil
}

func splitCommaSeparated(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func (a *Application) nodeIDPath(idx int) string {
	if a.cfg.Discovery.NodeIDDir != "" {
		return filepath.Join(a.cfg.Discovery.NodeIDDir, "node_"+strconv.Itoa(idx))
	}
	if idx == 0 {
		return a.cfg.Discovery.NodeIDFile
	}
	return ""
}

func (a *Application) aggregatePacketStats() dht.PacketStats {
	var agg dht.PacketStats
	for _, d := range a.dhts {
		ps := d.PacketStats()
		agg.Received += ps.Received
		agg.Enqueued += ps.Enqueued
		agg.Dropped += ps.Dropped
		agg.Handled += ps.Handled
		agg.DecodeErrors += ps.DecodeErrors
		agg.BytesReceived += ps.BytesReceived
		agg.BytesSent += ps.BytesSent
		agg.FollowupsSent += ps.FollowupsSent
		agg.RoutingNodes += ps.RoutingNodes
		agg.NodesInserted += ps.NodesInserted
		agg.NodesRemoved += ps.NodesRemoved
		agg.RefreshQueries += ps.RefreshQueries
	}
	return agg
}

func (a *Application) baseURL() string {
	url := a.cfg.Exporter.HTTPEndpoint
	if idx := strings.Index(url, "/api/"); idx > 0 {
		return url[:idx]
	}
	return url
}

func (a *Application) shouldExportEvent(event pipeline.Event) bool {
	if a.cfg.Exporter.Kind != "http" {
		return true
	}
	if !strings.Contains(a.cfg.Exporter.HTTPEndpoint, "/api/v1/torrents/batch") {
		return true
	}
	return event.Type == pipeline.EventMetadataFetched && event.Metadata != nil
}

func (a *Application) shouldEmitPeerEvents() bool {
	if !a.cfg.Discovery.EmitPeerEvents {
		return false
	}
	if a.cfg.Exporter.Kind != "http" {
		return true
	}
	return !strings.Contains(a.cfg.Exporter.HTTPEndpoint, "/api/v1/torrents/batch")
}

func (a *Application) shouldEmitWorkerStats() bool {
	if a.cfg.Exporter.Kind != "http" {
		return true
	}
	return !strings.Contains(a.cfg.Exporter.HTTPEndpoint, "/api/v1/torrents/batch")
}

func (a *Application) checkWorkerCount() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 4 {
		workers = 4
	}
	if metaWorkers := a.cfg.Metadata.WorkerQueueSize / 128; metaWorkers > workers {
		workers = metaWorkers
	}
	if workers > defaultCheckWorkersCap {
		workers = defaultCheckWorkersCap
	}
	return workers
}

func (a *Application) checkBatchExists(hashes []string) {
	if len(hashes) == 0 {
		return
	}
	// POST to avoid URL-length limits and URL-encoding overhead.
	body, _ := json.Marshal(hashes)
	req, err := http.NewRequest(http.MethodPost,
		a.baseURL()+"/api/v1/torrents/check",
		bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.Exporter.APIKey != "" {
		req.Header.Set("X-API-Key", a.cfg.Exporter.APIKey)
	}
	resp, err := a.apiClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var found []string
	if json.NewDecoder(resp.Body).Decode(&found) == nil {
		for _, h := range found {
			a.remoteKnown.Set(h)
		}
	}
}

// --- metadata 解析（与原 app.go 完全一致）---

func normalizeMetadata(data []byte) (*pipeline.Metadata, error) {
	decoded, err := dht.Decode(data)
	if err != nil {
		return nil, err
	}

	info, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, errUnexpectedMetadata
	}

	metadata := &pipeline.Metadata{}
	if name := firstString(info, "name.utf-8", "name"); name != "" {
		metadata.Name = fixEncoding(name)
	}
	if length, ok := asInt64(info["length"]); ok {
		metadata.Length = length
	}
	if pieceLength, ok := asInt64(info["piece length"]); ok {
		metadata.PieceLength = int(pieceLength)
	}
	if private, ok := asBool(info["private"]); ok {
		metadata.Private = private
	}
	if files, ok := info["files"].([]interface{}); ok {
		metadata.Files = make([]pipeline.MetadataFile, 0, len(files))
		for _, file := range files {
			item, ok := file.(map[string]interface{})
			if !ok {
				continue
			}
			path := pathParts(item)
			if len(path) == 0 {
				continue
			}
			entry := pipeline.MetadataFile{}
			if length, ok := asInt64(item["length"]); ok {
				entry.Length = length
			}
			clean := make([]string, 0, len(path))
			for _, p := range path {
				p = fixEncoding(p)
				if p != "" && !isPaddingFile(p) {
					clean = append(clean, p)
				}
			}
			if len(clean) == 0 {
				continue
			}
			entry.Path = clean
			entry.PathText = filepath.ToSlash(filepath.Join(clean...))
			metadata.Files = append(metadata.Files, entry)
		}
	}

	if len(metadata.Files) == 0 && metadata.Name != "" && metadata.Length > 0 {
		metadata.Files = []pipeline.MetadataFile{{
			Path:     []string{metadata.Name},
			PathText: metadata.Name,
			Length:   metadata.Length,
		}}
	}
	if metadata.Length == 0 && len(metadata.Files) > 0 {
		var total int64
		for _, f := range metadata.Files {
			total += f.Length
		}
		metadata.Length = total
	}
	metadata.FileCount = len(metadata.Files)
	if metadata.FileCount > 1 {
		slices.SortFunc(metadata.Files, func(a, b pipeline.MetadataFile) int {
			return strings.Compare(a.PathText, b.PathText)
		})
	}
	if metadata.FileCount > 0 && metadata.Name == "" {
		metadata.Name = metadata.Files[0].Path[0]
	}
	if metadata.Name == "" || metadata.Length <= 0 {
		return nil, errUnexpectedMetadata
	}
	return metadata, nil
}

func isPaddingFile(name string) bool {
	return len(name) > 10 && strings.HasPrefix(name, "_____padding_file_")
}

func fixEncoding(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || utf8.ValidString(s) {
		return s
	}
	decoded, err := io.ReadAll(transform.NewReader(
		strings.NewReader(s),
		simplifiedchinese.GBK.NewDecoder(),
	))
	if err == nil {
		result := string(decoded)
		if utf8.ValidString(result) {
			return strings.TrimSpace(result)
		}
	}
	return strings.ToValidUTF8(s, "")
}

func firstString(values map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok {
			value = strings.TrimSpace(value)
			if value != "" {
				return value
			}
		}
	}
	return ""
}

func asInt64(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case uint64:
		return int64(typed), true
	case float64:
		return int64(typed), true
	default:
		return 0, false
	}
}

func asBool(value interface{}) (bool, bool) {
	if intValue, ok := asInt64(value); ok {
		return intValue != 0, true
	}
	if boolValue, ok := value.(bool); ok {
		return boolValue, true
	}
	return false, false
}

func pathParts(values map[string]interface{}) []string {
	for _, key := range []string{"path.utf-8", "path"} {
		raw, ok := values[key].([]interface{})
		if !ok {
			continue
		}
		parts := make([]string, 0, len(raw))
		for _, part := range raw {
			if str, ok := part.(string); ok {
				str = strings.TrimSpace(str)
				if str != "" {
					parts = append(parts, str)
				}
			}
		}
		if len(parts) > 0 {
			return parts
		}
	}
	return nil
}

var errUnexpectedMetadata = errors.New("unexpected metadata payload")
