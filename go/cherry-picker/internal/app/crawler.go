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
	"sort"
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
	"cherry-picker/internal/pipeline"

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
	dht    *dht.DHT

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
	dhtPacketsReceived      uint64
	dhtPacketsEnqueued      uint64
	dhtPacketsDropped       uint64
	dhtPacketsHandled       uint64
	dhtPacketDecodeErrors   uint64
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
	lruCaps := newLRUCaps(cfg)
	return &Application{
		cfg:    cfg,
		logger: logger,

		infohashSeen:        cache.NewLRU(lruCaps.infohashSeen),
		peerSeen:            cache.NewLRU(lruCaps.peerSeen),
		metadataRequestSeen: cache.NewLRU(lruCaps.metadataRequestSeen),
		metadataResultSeen:  cache.NewLRU(lruCaps.metadataResultSeen),
		remoteKnown:         cache.NewLRU(lruCaps.remoteKnown),

		peerCounts: make(map[string]int, 4096),
		apiClient:  &http.Client{Timeout: 10 * time.Second},
	}
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

	// checkQueue：批量向 API 检查 hash 是否已存在，容量从 4096 增大到 100000
	checkQueue := make(chan string, 100_000)
	go a.runCheckLoop(ctx, checkQueue, stats)

	// peer wire metadata 下载器
	downloader := dht.NewWire(a.cfg.Metadata.BlackListSize, a.cfg.Metadata.RequestQueueSize, a.cfg.Metadata.WorkerQueueSize)
	go a.consumeMetadata(ctx, downloader, events, stats)
	go downloader.Run()
	a.logger.Printf("metadata workers: %d, queue: %d", a.cfg.Metadata.WorkerQueueSize, a.cfg.Metadata.RequestQueueSize)

	// DHT 配置
	dhtConfig := dht.NewCrawlConfig()
	if a.cfg.Discovery.Mode == "standard" {
		dhtConfig = dht.NewStandardConfig()
	}
	dhtConfig.Address = a.cfg.ListenAddr
	dhtConfig.PacketWorkerLimit = a.cfg.Discovery.PacketWorkers
	dhtConfig.PacketJobLimit = a.cfg.Discovery.PacketJobs
	dhtConfig.MaxNodes = a.cfg.Discovery.MaxNodes
	dhtConfig.RefreshNodeNum = a.cfg.Discovery.RefreshNodes

	dhtConfig.OnGetPeers = func(infoHash, ip string, port int) {
		ihHex := hex.EncodeToString([]byte(infoHash))
		a.submitInfohashEvent(events, ihHex, ip, port, "get_peers", stats)
	}
	dhtConfig.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, peer.IP.String(), peer.Port, "get_peers_response", stats)
		}
		a.queueMetadataRequest(downloader, ihHex, peer.IP.String(), peer.Port, stats, checkQueue)
	}
	dhtConfig.OnAnnouncePeer = func(infoHash, ip string, port int) {
		ihHex := hex.EncodeToString([]byte(infoHash))
		if a.cfg.Role != "metadata" {
			a.submitPeerEvent(events, ihHex, ip, port, "announce_peer", stats)
		}
		a.queueMetadataRequest(downloader, ihHex, ip, port, stats, checkQueue)
	}

	a.dht = dht.New(dhtConfig)

	// 后台 goroutines
	go a.emitStats(ctx, events, stats, a.dht)
	go a.flushPeerCountsLoop(ctx)
	go a.pollPendingRequests(ctx)
	if a.cfg.AutoTune {
		go a.autoTuneLoop(ctx)
	}
	debug.SetGCPercent(200)

	go a.dht.Run()
	a.logger.Printf("started: instance=%s udp=%s workers=%d nodes=%d",
		a.cfg.InstanceID, a.cfg.ListenAddr,
		a.cfg.Metadata.WorkerQueueSize, a.cfg.Discovery.MaxNodes)

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

// flushPeerCountsLoop 每60秒将累积的 peer counts 批量上报到 API。
func (a *Application) flushPeerCountsLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
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
				a.dht.GetPeers(h)
			}
		}
	}
}

// autoTuneLoop 每30秒采样内存和队列状态，动态调整 metadata 入队。
//
// 策略：
//   - HeapInuse > 500MB → 暂停 metadata 请求入队，强制 GC，等内存降下来
//   - HeapInuse < 300MB → 恢复入队
func (a *Application) autoTuneLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	pauseThreshold, resumeThreshold := a.autoTuneThresholds()
	controller := &autoTuneController{}
	a.logger.Printf("autotune: enabled pause=%dMB resume=%dMB", pauseThreshold/1024/1024, resumeThreshold/1024/1024)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			heapAlloc := ms.HeapAlloc
			heapInUse := ms.HeapInuse
			paused := a.metaPaused.Load()
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
				a.logger.Printf("autotune: heap_alloc=%dMB heap_inuse=%dMB paused=%v seenSizes=[ih=%d peer=%d metaReq=%d remote=%d]",
					heapAlloc/1024/1024, heapInUse/1024/1024, paused,
					a.infohashSeen.Len(), a.peerSeen.Len(),
					a.metadataRequestSeen.Len(), a.remoteKnown.Len())
			}
		}
	}
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

func newLRUCaps(cfg config.Config) lruCapConfig {
	cpuScale := runtime.GOMAXPROCS(0)
	if cpuScale < 1 {
		cpuScale = 1
	}

	peerCap := clampInt(cpuScale*20_000, 80_000, 200_000)
	infohashCap := clampInt(cpuScale*24_000, 100_000, 240_000)
	metadataRequestCap := clampInt(cpuScale*36_000, 150_000, 320_000)
	metadataResultCap := clampInt(cpuScale*28_000, 120_000, 240_000)
	remoteKnownCap := clampInt(cpuScale*36_000, 150_000, 320_000)

	switch cfg.Role {
	case "discovery":
		metadataRequestCap = 64_000
		metadataResultCap = 64_000
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

	pauseThreshold = 768 * 1024 * 1024
	if a.cfg.Role != "discovery" {
		pauseThreshold = 1024 * 1024 * 1024
	}
	pauseThreshold += uint64(maxInt(a.cfg.EventQueue-16_384, 0)) * 1024
	pauseThreshold += uint64(maxInt(a.cfg.Metadata.RequestQueueSize-16_384, 0)) * 256
	pauseThreshold += uint64(maxInt(a.cfg.Metadata.WorkerQueueSize-256, 0)) * 512 * 1024
	pauseThreshold = clampUint64(pauseThreshold, 768*1024*1024, 3*1024*1024*1024)

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

func (a *Application) submitInfohashEvent(events chan<- pipeline.Event, ihHex, ip string, port int, source string, stats *runtimeStats) {
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
		Timestamp:  time.Now().UTC(),
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   ihHex,
		IP:         ip,
		Port:       port,
	}, stats.infohashEventsDropped.Add, stats.infohashEventsSent.Add)
}

func (a *Application) submitPeerEvent(events chan<- pipeline.Event, ihHex, ip string, port int, source string, stats *runtimeStats) {
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
		Timestamp:  time.Now().UTC(),
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   ihHex,
		IP:         ip,
		Port:       port,
	}, stats.peerEventsDropped.Add, stats.peerEventsSent.Add)
}

func (a *Application) queueMetadataRequest(downloader *dht.Wire, ihHex, ip string, port int, stats *runtimeStats, checkQueue chan<- string) {
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

func (a *Application) consumeMetadata(ctx context.Context, downloader *dht.Wire, events chan<- pipeline.Event, stats *runtimeStats) {
	responses := downloader.Response()
	var ok, fail uint64
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logTicker.C:
			a.logger.Printf("metadata download: ok=%d fail=%d (30s)", ok, fail)
			ok, fail = 0, 0
		case response := <-responses:
			ihHex := hex.EncodeToString(response.InfoHash)
			responseKey := buildInfohashPeerKey(ihHex, response.IP, response.Port)
			if !a.metadataResultSeen.Set(responseKey) {
				stats.metadataEventsDeduped.Add(1)
				fail++
				continue
			}
			metadata, err := normalizeMetadata(response.MetadataInfo)
			if err != nil {
				stats.metadataEventsDeduped.Add(1)
				fail++
				continue
			}
			ok++
			a.submitEvent(events, pipeline.Event{
				Type:       pipeline.EventMetadataFetched,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "peer_wire",
				InfoHash:   ihHex,
				IP:         response.IP,
				Port:       response.Port,
				Metadata:   metadata,
			}, stats.metadataEventsDropped.Add, stats.metadataEventsSent.Add)
		}
	}
}

func (a *Application) emitStats(ctx context.Context, events chan<- pipeline.Event, stats *runtimeStats, node *dht.DHT) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var previous statsSnapshot
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			packetStats := node.PacketStats()
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
				dhtPacketsReceived:      packetStats.Received,
				dhtPacketsEnqueued:      packetStats.Enqueued,
				dhtPacketsDropped:       packetStats.Dropped,
				dhtPacketsHandled:       packetStats.Handled,
				dhtPacketDecodeErrors:   packetStats.DecodeErrors,
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
					"dht_packets_received":      current.dhtPacketsReceived,
					"dht_packets_enqueued":      current.dhtPacketsEnqueued,
					"dht_packets_dropped":       current.dhtPacketsDropped,
					"dht_packets_handled":       current.dhtPacketsHandled,
					"dht_packet_decode_errors":  current.dhtPacketDecodeErrors,
				},
			}, func(delta uint64) uint64 { return delta }, func(delta uint64) uint64 { return delta })
		}
	}
}

func (a *Application) logRuntimeDelta(current, previous statsSnapshot) {
	a.logger.Printf(
		"runtime 30s: dht_recv=%d handled=%d dropped=%d decode_err=%d peer_sent=%d peer_drop=%d peer_dedup=%d meta_req=%d meta_req_dedup=%d meta_sent=%d meta_drop=%d meta_dedup=%d check_drop=%d paused=%v",
		current.dhtPacketsReceived-previous.dhtPacketsReceived,
		current.dhtPacketsHandled-previous.dhtPacketsHandled,
		current.dhtPacketsDropped-previous.dhtPacketsDropped,
		current.dhtPacketDecodeErrors-previous.dhtPacketDecodeErrors,
		current.peerEventsSent-previous.peerEventsSent,
		current.peerEventsDropped-previous.peerEventsDropped,
		current.peerEventsDeduped-previous.peerEventsDeduped,
		current.metadataRequestsQueued-previous.metadataRequestsQueued,
		current.metadataRequestsDeduped-previous.metadataRequestsDeduped,
		current.metadataEventsSent-previous.metadataEventsSent,
		current.metadataEventsDropped-previous.metadataEventsDropped,
		current.metadataEventsDeduped-previous.metadataEventsDeduped,
		current.checkBatchesDropped-previous.checkBatchesDropped,
		a.metaPaused.Load(),
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

// --- 辅助函数 ---

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
	url := a.baseURL() + "/api/v1/torrents/check?hashes=" + strings.Join(hashes, ",")
	resp, err := a.apiClient.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var found []string
	if json.NewDecoder(resp.Body).Decode(&found) == nil {
		for _, h := range found {
			a.remoteKnown.Set(h) // 添加到有界 LRU，超容量时自动淘汰最旧的
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
		sort.Slice(metadata.Files, func(i, j int) bool {
			return metadata.Files[i].PathText < metadata.Files[j].PathText
		})
	}
	if metadata.Name == "" && metadata.FileCount > 0 {
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
