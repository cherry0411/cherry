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
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
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
	metadataEventsSent      uint64
	metadataEventsDropped   uint64
	metadataEventsDeduped   uint64
	metadataEventsFiltered  uint64
	dhtPacketsReceived      uint64
	dhtPacketsEnqueued      uint64
	dhtPacketsDropped       uint64
	dhtPacketsHandled       uint64
	dhtPacketDecodeErrors   uint64
	dhtBytesReceived        uint64
	dhtBytesSent            uint64
	wireDialAttempts        int64
	wireDialOK              int64
	wireHandshakeOK         int64
	wireDownloadOK          int64
	wireBlacklisted         int64
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
		a.submitInfohashEvent(events, ihHex, ip, port, "get_peers", stats, now)
	}
	onGetPeersResponse := func(infoHash string, peer *dht.Peer) {
		now := time.Now().UTC()
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, peer.IP.String(), peer.Port, "get_peers_response", stats, now)
		}
		a.queueMetadataRequest(downloader, ihHex, peer.IP.String(), peer.Port, stats, checkQueue)
	}
	onAnnouncePeer := func(infoHash, ip string, port int) {
		now := time.Now().UTC()
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, ip, port, "announce_peer", stats, now)
		}
		a.queueMetadataRequest(downloader, ihHex, ip, port, stats, checkQueue)
	}

	addrs := a.listenAddrs()
	for i, addr := range addrs {
		dhtCfg := dht.NewCrawlConfig()
		if a.cfg.Discovery.Mode == "standard" {
			dhtCfg = dht.NewStandardConfig()
		}
		dhtCfg.Address = addr
		dhtCfg.PacketWorkerLimit = a.cfg.Discovery.PacketWorkers
		dhtCfg.PacketJobLimit = a.cfg.Discovery.PacketJobs
		dhtCfg.PacketReadWorkers = a.cfg.Discovery.ReadWorkers
		dhtCfg.MaxNodes = a.cfg.Discovery.MaxNodes
		dhtCfg.RefreshNodeNum = a.cfg.Discovery.RefreshNodes
		dhtCfg.NodeIDFile = a.nodeIDPath(i)
		dhtCfg.OnGetPeers = onGetPeers
		dhtCfg.OnGetPeersResponse = onGetPeersResponse
		dhtCfg.OnAnnouncePeer = onAnnouncePeer
		d := dht.New(dhtCfg)
		a.dhts = append(a.dhts, d)
		go d.Run()
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

	<-ctx.Done()
	a.logger.Printf("shutdown")
	return nil
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
	a.logger.Printf("autotune: enabled pause=%dMB resume=%dMB", pauseThreshold/1024/1024, resumeThreshold/1024/1024)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// G11: runtime/metrics avoids STW unlike ReadMemStats.
			samples := []runtimemetrics.Sample{
				{Name: "/memory/classes/heap/objects:bytes"},
				{Name: "/memory/classes/heap/inuse:bytes"},
				{Name: "/cpu/classes/idle:cpu-seconds"},
				{Name: "/cpu/classes/total:cpu-seconds"},
			}
			runtimemetrics.Read(samples)
			heapAlloc := samples[0].Value.Uint64()
			heapInUse := samples[1].Value.Uint64()
			idleCPU := samples[2].Value.Float64()
			totalCPU := samples[3].Value.Float64()
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

func (a *Application) queueMetadataRequest(downloader *dht.Wire, ihHex, ip string, port int, stats *runtimeStats, checkQueue chan<- string) {
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
	if a.remoteKnown.Contains(ihHex) {
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
	downloader.Request(infoHashBytes, ip, port)
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
				metadataEventsSent:      stats.metadataEventsSent.Load(),
				metadataEventsDropped:   stats.metadataEventsDropped.Load(),
				metadataEventsDeduped:   stats.metadataEventsDeduped.Load(),
				metadataEventsFiltered:  stats.metadataEventsFiltered.Load(),
				dhtPacketsReceived:      packetStats.Received,
				dhtPacketsEnqueued:      packetStats.Enqueued,
				dhtPacketsDropped:       packetStats.Dropped,
				dhtPacketsHandled:       packetStats.Handled,
				dhtPacketDecodeErrors:   packetStats.DecodeErrors,
				dhtBytesReceived:        packetStats.BytesReceived,
				dhtBytesSent:            packetStats.BytesSent,
				wireDialAttempts:        downloader.Stats.DialAttempts.Load(),
				wireDialOK:              downloader.Stats.DialOK.Load(),
				wireHandshakeOK:         downloader.Stats.HandshakeOK.Load(),
				wireDownloadOK:          downloader.Stats.DownloadOK.Load(),
				wireBlacklisted:         downloader.Stats.Blacklisted.Load(),
			}
			a.logRuntimeDelta(current, previous)
			previous = current
			if !a.shouldEmitWorkerStats() {
				continue
			}
			a.submitEvent(events, pipeline.Event{
				Type:       pipeline.EventWorkerStats,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "runtime",
				Stats: map[string]uint64{
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
				},
			}, func(delta uint64) uint64 { return delta }, func(delta uint64) uint64 { return delta })
		}
	}
}

func (a *Application) logRuntimeDelta(current, previous statsSnapshot) {
	// DHT UDP 带宽（不含 peer-wire TCP 流量），KB/s
	const interval = 30
	netInKBps := (current.dhtBytesReceived - previous.dhtBytesReceived) / 1024 / interval
	netOutKBps := (current.dhtBytesSent - previous.dhtBytesSent) / 1024 / interval
	a.logger.Printf(
		"runtime 30s: dht_recv=%d handled=%d dropped=%d decode_err=%d net_in=%dKB/s net_out=%dKB/s peer_sent=%d peer_drop=%d peer_dedup=%d meta_req=%d meta_req_dedup=%d meta_sent=%d meta_drop=%d meta_dedup=%d meta_filtered=%d check_drop=%d paused=%v wire_dial=%d wire_conn=%d wire_hs=%d wire_ok=%d wire_bl=%d",
		current.dhtPacketsReceived-previous.dhtPacketsReceived,
		current.dhtPacketsHandled-previous.dhtPacketsHandled,
		current.dhtPacketsDropped-previous.dhtPacketsDropped,
		current.dhtPacketDecodeErrors-previous.dhtPacketDecodeErrors,
		netInKBps,
		netOutKBps,
		current.peerEventsSent-previous.peerEventsSent,
		current.peerEventsDropped-previous.peerEventsDropped,
		current.peerEventsDeduped-previous.peerEventsDeduped,
		current.metadataRequestsQueued-previous.metadataRequestsQueued,
		current.metadataRequestsDeduped-previous.metadataRequestsDeduped,
		current.metadataEventsSent-previous.metadataEventsSent,
		current.metadataEventsDropped-previous.metadataEventsDropped,
		current.metadataEventsDeduped-previous.metadataEventsDeduped,
		current.metadataEventsFiltered-previous.metadataEventsFiltered,
		current.checkBatchesDropped-previous.checkBatchesDropped,
		a.metaPaused.Load(),
		current.wireDialAttempts-previous.wireDialAttempts,
		current.wireDialOK-previous.wireDialOK,
		current.wireHandshakeOK-previous.wireHandshakeOK,
		current.wireDownloadOK-previous.wireDownloadOK,
		current.wireBlacklisted-previous.wireBlacklisted,
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
// are added to the persistent cuckoo filter.  Only runs when the exporter is
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

func (a *Application) listenAddrs() []string {
	if a.cfg.ListenAddrs != "" {
		parts := strings.Split(a.cfg.ListenAddrs, ",")
		addrs := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				addrs = append(addrs, p)
			}
		}
		if len(addrs) > 0 {
			return addrs
		}
	}
	return []string{a.cfg.ListenAddr}
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
			entry := pipeline.MetadataFile{}
			if length, ok := asInt64(item["length"]); ok {
				entry.Length = length
			}
			if path := pathParts(item); len(path) > 0 {
				entry.Path = path
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
			}
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
