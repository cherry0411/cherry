package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Role        string
	InstanceID  string
	ListenAddr  string
	ListenAddrs string
	EventQueue  int
	BrokerURL   string
	Dedupe      DedupeConfig
	Discovery   DiscoveryConfig
	Metadata    MetadataConfig
	Filter      FilterConfig
	Exporter    ExporterConfig
	Heat        HeatConfig
	AutoTune    bool
	TargetCPU   float64
	// MemLimitMB 显式指定进程内存上限（MB）。0 = 自动探测
	// （Linux 取 min(物理内存, cgroup 限制)，Windows 取物理内存，各留 30% 余量）。
	MemLimitMB int
}

type DedupeConfig struct {
	PeerTTL     time.Duration
	MetadataTTL time.Duration
}

type DiscoveryConfig struct {
	Mode             string
	EmitPeerEvents   bool
	PrimeNodes       string
	Instances        int
	ActiveLookup     bool
	LookupNodes      int
	LookupDHTs       int
	LookupQueue      int
	LookupRate       int
	LookupWorkers    int
	LookupFollowups  int
	LookupSpread     bool
	SampleInfohashes bool
	SampleRate       int
	PacketWorkers    int
	PacketJobs       int
	ReadWorkers      int
	MaxNodes         int
	RefreshNodes     int
	NodeIDFile       string
	NodeIDDir        string
}

type MetadataConfig struct {
	Enabled          bool
	BlackListSize    int
	RequestQueueSize int
	// WorkerQueueSize is the legacy name for the physical wire-worker ceiling.
	// It remains populated for callers that have not migrated to WorkerMax.
	WorkerQueueSize int
	WorkerInitial   int
	WorkerMin       int
	WorkerMax       int
}

// FilterConfig controls which metadata is silently rejected before export.
// A threshold of -1 disables the corresponding rule; 0 uses the built-in default.
type FilterConfig struct {
	// MaxFileCount rejects any torrent whose file count exceeds this value.
	// Default: 10 000.
	MaxFileCount int

	// MaxFileCountNonCN rejects torrents with more files than this threshold
	// when no Chinese characters appear anywhere in the file paths or name.
	// Default: 1 000.
	MaxFileCountNonCN int

	// MaxFileCountNumeric rejects torrents with more files than this threshold
	// when every filename (extension stripped) is purely numeric.
	// Default: 100.
	MaxFileCountNumeric int

	// Durable storage budgets. Full records above either summary threshold are
	// downgraded, never silently dropped. Hash/reject caps are disabled at 0.
	SummaryAboveFiles     int
	SummaryAbovePathBytes int
	MaxFullPathBytes      int
	MaxStoredNameBytes    int
	SummaryMaxExtensions  int
	HashOnlyAboveFiles    int
	RejectAboveFiles      int
}

type ExporterConfig struct {
	Kind         string
	FilePath     string
	HTTPEndpoint string
	// OracleEndpoint optionally separates the experiment uniqueness oracle
	// from the production metadata endpoint. When empty, existence checks keep
	// using HTTPEndpoint for full backward compatibility.
	OracleEndpoint string
	BatchSize      int
	FlushInterval  time.Duration
	HTTPTimeout    time.Duration
	HTTPRetries    int
	RetryBackoff   time.Duration
	WalDir         string // WAL 本地缓冲目录，空 = 不启用
	APIKey         string // X-API-Key 认证头，空 = 不发送
	// OracleAPIKey authenticates only oracle checks/observations. It never
	// replaces APIKey on the production durable metadata connection.
	OracleAPIKey string
	// CrawlerID 是 durable spool/receipt 协议的稳定身份。它必须跨进程重启
	// 保持不变，不能复用默认带 PID 的 InstanceID。
	CrawlerID string

	// SpoolDir 启用崩溃安全的 pre-send durable spool（推荐用于 http 导出）。
	// 非空时，metadata 事件先落 spool 再经 durable 批次协议投递，替代
	// walSink fallback。空 = 沿用旧 httpSink 路径。
	SpoolDir string
	// SpoolMaxBytes 是 spool 总磁盘上限（字节），用于背压；durable
	// spool 启用时 0/负数会归一化为安全的 4 GiB 默认值。
	SpoolMaxBytes int64
}

// HeatConfig is a separate privacy-reduced, durable get_peers activity
// channel. Secret values are never accepted inline: only paths to 0600 files
// are configured. Enabled defaults to false.
type HeatConfig struct {
	Enabled          bool
	Endpoint         string
	CrawlerID        string
	MasterSecretFile string
	HMACSecretFile   string
	SpoolDir         string
	SpoolMaxBytes    int64
	KnownCrawlers    string
	QueueCapacity    int
	BatchSize        int
	FlushInterval    time.Duration
	HTTPTimeout      time.Duration
	RetryBackoff     time.Duration
	// ShadowBloomEnabled measures probable current/previous-hour duplicates.
	// Admission changes only when ShadowBloomDropProbableDuplicates is enabled.
	ShadowBloomEnabled                bool
	ShadowBloomDropProbableDuplicates bool
	ShadowBloomCapacity               int
	// ShadowBloomFalsePositive is the sizing target in parts per million.
	ShadowBloomFalsePositive  int
	ShadowBloomSampleCapacity int
}

func Load() (Config, error) {
	path := strings.TrimSpace(os.Getenv("CHERRY_PICKER_CONFIG"))
	if path != "" {
		cfg, err := loadFromFile(path)
		if err != nil {
			return Config{}, err
		}
		return cfg, nil
	}

	instanceID := getenvDefault("CHERRY_PICKER_INSTANCE_ID", defaultInstanceID())

	cfg := Config{
		Role:        strings.ToLower(getenvDefault("CHERRY_PICKER_ROLE", "combined")),
		InstanceID:  instanceID,
		ListenAddr:  getenvDefault("CHERRY_PICKER_LISTEN_ADDR", ":6881"),
		ListenAddrs: getenvDefault("CHERRY_PICKER_LISTEN_ADDRS", ""),
		EventQueue:  getenvInt("CHERRY_PICKER_EVENT_QUEUE", defaultEventQueue()),
		BrokerURL:   getenvDefault("CHERRY_PICKER_BROKER_URL", ""),
		AutoTune:    getenvBool("CHERRY_PICKER_AUTO_TUNE", true),
		TargetCPU:   float64(getenvInt("CHERRY_PICKER_TARGET_CPU", 80)) / 100.0,
		MemLimitMB:  getenvInt("CHERRY_PICKER_MEM_LIMIT_MB", 0),
		Dedupe: DedupeConfig{
			PeerTTL:     getenvDuration("CHERRY_PICKER_DEDUPE_PEER_TTL", 10*time.Minute),
			MetadataTTL: getenvDuration("CHERRY_PICKER_DEDUPE_METADATA_TTL", 30*time.Minute),
		},
		Discovery: DiscoveryConfig{
			Mode:             strings.ToLower(getenvDefault("CHERRY_PICKER_DHT_MODE", "crawl")),
			EmitPeerEvents:   getenvBool("CHERRY_PICKER_EMIT_PEER_EVENTS", true),
			PrimeNodes:       getenvDefault("CHERRY_PICKER_DHT_PRIME_NODES", ""),
			Instances:        getenvInt("CHERRY_PICKER_DHT_INSTANCES", 1),
			ActiveLookup:     getenvBool("CHERRY_PICKER_DHT_ACTIVE_LOOKUP", true),
			LookupNodes:      getenvInt("CHERRY_PICKER_DHT_LOOKUP_NODES", 32),
			LookupDHTs:       getenvInt("CHERRY_PICKER_DHT_LOOKUP_DHTS", 1),
			LookupQueue:      getenvInt("CHERRY_PICKER_DHT_LOOKUP_QUEUE", 8192),
			LookupRate:       getenvInt("CHERRY_PICKER_DHT_LOOKUP_RATE", 100),
			LookupWorkers:    getenvInt("CHERRY_PICKER_DHT_LOOKUP_WORKERS", 1),
			LookupFollowups:  getenvInt("CHERRY_PICKER_DHT_LOOKUP_FOLLOWUPS", 0),
			LookupSpread:     getenvBool("CHERRY_PICKER_DHT_LOOKUP_SPREAD", false),
			SampleInfohashes: getenvBool("CHERRY_PICKER_DHT_SAMPLE_INFOHASHES", false),
			SampleRate:       getenvInt("CHERRY_PICKER_DHT_SAMPLE_RATE", 20),
			PacketWorkers:    getenvInt("CHERRY_PICKER_DHT_PACKET_WORKERS", defaultPacketWorkers()),
			PacketJobs:       getenvInt("CHERRY_PICKER_DHT_PACKET_JOBS", defaultPacketJobs()),
			ReadWorkers:      getenvInt("CHERRY_PICKER_DHT_READ_WORKERS", 0),
			MaxNodes:         getenvInt("CHERRY_PICKER_DHT_MAX_NODES", defaultMaxNodes()),
			RefreshNodes:     getenvInt("CHERRY_PICKER_DHT_REFRESH_NODES", defaultRefreshNodes()),
			NodeIDFile:       getenvDefault("CHERRY_PICKER_NODE_ID_FILE", ""),
			NodeIDDir:        getenvDefault("CHERRY_PICKER_NODE_ID_DIR", ""),
		},
		Metadata: MetadataConfig{
			Enabled:          getenvBool("CHERRY_PICKER_METADATA_ENABLED", true),
			BlackListSize:    getenvInt("CHERRY_PICKER_METADATA_BLACKLIST", 131072),
			RequestQueueSize: getenvInt("CHERRY_PICKER_METADATA_REQUEST_QUEUE", defaultMetadataRequestQueue()),
			WorkerQueueSize:  getenvInt("CHERRY_PICKER_METADATA_WORKERS", defaultMetadataWorkers()),
			WorkerInitial:    getenvInt("CHERRY_PICKER_METADATA_WORKERS_INITIAL", 0),
			WorkerMin:        getenvInt("CHERRY_PICKER_METADATA_WORKERS_MIN", 0),
			WorkerMax:        getenvInt("CHERRY_PICKER_METADATA_WORKERS_MAX", 0),
		},
		Filter: FilterConfig{
			MaxFileCount:          getenvInt("CHERRY_PICKER_FILTER_MAX_FILES", 0),
			MaxFileCountNonCN:     getenvInt("CHERRY_PICKER_FILTER_MAX_FILES_NON_CN", 0),
			MaxFileCountNumeric:   getenvInt("CHERRY_PICKER_FILTER_MAX_FILES_NUMERIC", 0),
			SummaryAboveFiles:     getenvInt("CHERRY_PICKER_POLICY_SUMMARY_FILES", 0),
			SummaryAbovePathBytes: getenvInt("CHERRY_PICKER_POLICY_SUMMARY_PATH_BYTES", 0),
			MaxFullPathBytes:      getenvInt("CHERRY_PICKER_POLICY_MAX_PATH_BYTES", 0),
			MaxStoredNameBytes:    getenvInt("CHERRY_PICKER_POLICY_MAX_NAME_BYTES", 0),
			SummaryMaxExtensions:  getenvInt("CHERRY_PICKER_POLICY_SUMMARY_EXTENSIONS", 0),
			HashOnlyAboveFiles:    getenvInt("CHERRY_PICKER_POLICY_HASH_ONLY_FILES", 0),
			RejectAboveFiles:      getenvInt("CHERRY_PICKER_POLICY_REJECT_FILES", 0),
		},
		Exporter: ExporterConfig{
			Kind:           strings.ToLower(getenvDefault("CHERRY_PICKER_EXPORTER", "stdout")),
			FilePath:       getenvDefault("CHERRY_PICKER_EXPORTER_FILE", ""),
			HTTPEndpoint:   getenvDefault("CHERRY_PICKER_EXPORTER_URL", ""),
			OracleEndpoint: strings.TrimSpace(os.Getenv("CHERRY_PICKER_ORACLE_URL")),
			BatchSize:      getenvInt("CHERRY_PICKER_EXPORTER_BATCH", 512),
			FlushInterval:  getenvDuration("CHERRY_PICKER_EXPORTER_FLUSH", 500*time.Millisecond),
			HTTPTimeout:    getenvDuration("CHERRY_PICKER_EXPORTER_TIMEOUT", 5*time.Second),
			HTTPRetries:    getenvInt("CHERRY_PICKER_EXPORTER_HTTP_RETRIES", 3),
			RetryBackoff:   getenvDuration("CHERRY_PICKER_EXPORTER_RETRY_BACKOFF", time.Second),
			WalDir:         getenvDefault("CHERRY_PICKER_WAL_DIR", ""),
			APIKey:         getenvDefault("CHERRY_API_KEY", ""),
			OracleAPIKey:   strings.TrimSpace(os.Getenv("CHERRY_PICKER_ORACLE_API_KEY")),
			CrawlerID:      strings.TrimSpace(os.Getenv("CHERRY_PICKER_CRAWLER_ID")),
			SpoolDir:       getenvDefault("CHERRY_PICKER_SPOOL_DIR", ""),
			SpoolMaxBytes:  int64(getenvInt("CHERRY_PICKER_SPOOL_MAX_BYTES", 0)),
		},
		Heat: HeatConfig{
			Enabled:                           getenvBool("CHERRY_PICKER_HEAT_ENABLED", false),
			Endpoint:                          strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_ENDPOINT")),
			CrawlerID:                         strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_CRAWLER_ID")),
			MasterSecretFile:                  strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_MASTER_SECRET_FILE")),
			HMACSecretFile:                    strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_HMAC_SECRET_FILE")),
			SpoolDir:                          strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_SPOOL_DIR")),
			SpoolMaxBytes:                     int64(getenvInt("CHERRY_PICKER_HEAT_SPOOL_MAX_BYTES", 0)),
			KnownCrawlers:                     strings.TrimSpace(os.Getenv("CHERRY_PICKER_HEAT_KNOWN_CRAWLERS")),
			QueueCapacity:                     getenvInt("CHERRY_PICKER_HEAT_QUEUE", 0),
			BatchSize:                         getenvInt("CHERRY_PICKER_HEAT_BATCH", 0),
			FlushInterval:                     getenvDuration("CHERRY_PICKER_HEAT_FLUSH", 0),
			HTTPTimeout:                       getenvDuration("CHERRY_PICKER_HEAT_HTTP_TIMEOUT", 0),
			RetryBackoff:                      getenvDuration("CHERRY_PICKER_HEAT_RETRY_BACKOFF", 0),
			ShadowBloomEnabled:                getenvBool("CHERRY_PICKER_HEAT_SHADOW_BLOOM_ENABLED", false),
			ShadowBloomDropProbableDuplicates: getenvBool("CHERRY_PICKER_HEAT_SHADOW_BLOOM_DROP_PROBABLE_DUPLICATES", false),
			ShadowBloomCapacity:               getenvInt("CHERRY_PICKER_HEAT_SHADOW_BLOOM_CAPACITY", 0),
			ShadowBloomFalsePositive:          getenvInt("CHERRY_PICKER_HEAT_SHADOW_BLOOM_FP_PPM", 0),
			ShadowBloomSampleCapacity:         getenvInt("CHERRY_PICKER_HEAT_SHADOW_SAMPLE_CAPACITY", 0),
		},
	}

	return normalize(cfg), nil
}

func loadFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var raw fileConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode config: expected exactly one JSON value")
		}
		return Config{}, fmt.Errorf("decode trailing config data: %w", err)
	}

	// auto_tune 未在 JSON 中出现时默认开启
	autoTune := true
	if raw.AutoTune != nil {
		autoTune = *raw.AutoTune
	}
	activeLookup := true
	if raw.Discovery.ActiveLookup != nil {
		activeLookup = *raw.Discovery.ActiveLookup
	}

	cfg := Config{
		Role:        strings.ToLower(strings.TrimSpace(raw.Role)),
		InstanceID:  strings.TrimSpace(raw.InstanceID),
		ListenAddr:  strings.TrimSpace(raw.ListenAddr),
		ListenAddrs: strings.TrimSpace(raw.ListenAddrs),
		EventQueue:  raw.EventQueue,
		AutoTune:    autoTune,
		MemLimitMB:  raw.MemLimitMB,
		Dedupe: DedupeConfig{
			PeerTTL:     parseDuration(raw.Dedupe.PeerTTL),
			MetadataTTL: parseDuration(raw.Dedupe.MetadataTTL),
		},
		Discovery: DiscoveryConfig{
			Mode:             strings.ToLower(strings.TrimSpace(raw.Discovery.Mode)),
			EmitPeerEvents:   raw.Discovery.EmitPeerEvents,
			PrimeNodes:       strings.TrimSpace(raw.Discovery.PrimeNodes),
			Instances:        intOrDefault(raw.Discovery.Instances, 1),
			ActiveLookup:     activeLookup,
			LookupNodes:      intOrDefault(raw.Discovery.LookupNodes, 32),
			LookupDHTs:       intOrDefault(raw.Discovery.LookupDHTs, 1),
			LookupQueue:      intOrDefault(raw.Discovery.LookupQueue, 8192),
			LookupRate:       intOrDefault(raw.Discovery.LookupRate, 100),
			LookupWorkers:    intOrDefault(raw.Discovery.LookupWorkers, 1),
			LookupFollowups:  raw.Discovery.LookupFollowups,
			LookupSpread:     raw.Discovery.LookupSpread,
			SampleInfohashes: raw.Discovery.SampleInfohashes,
			SampleRate:       intOrDefault(raw.Discovery.SampleRate, 20),
			PacketWorkers:    intOrDefault(raw.Discovery.PacketWorkers, 512),
			PacketJobs:       intOrDefault(raw.Discovery.PacketJobs, 65536),
			ReadWorkers:      raw.Discovery.ReadWorkers,
			MaxNodes:         intOrDefault(raw.Discovery.MaxNodes, 50000),
			RefreshNodes:     intOrDefault(raw.Discovery.RefreshNodes, 2048),
			NodeIDFile:       strings.TrimSpace(raw.Discovery.NodeIDFile),
			NodeIDDir:        strings.TrimSpace(raw.Discovery.NodeIDDir),
		},
		Metadata: MetadataConfig{
			Enabled:          raw.Metadata.Enabled,
			BlackListSize:    raw.Metadata.BlackListSize,
			RequestQueueSize: intOrDefault(raw.Metadata.RequestQueueSize, defaultMetadataRequestQueue()),
			WorkerQueueSize:  intOrDefault(raw.Metadata.WorkerQueueSize, defaultMetadataWorkers()),
			WorkerInitial:    raw.Metadata.WorkerInitial,
			WorkerMin:        raw.Metadata.WorkerMin,
			WorkerMax:        raw.Metadata.WorkerMax,
		},
		Filter: FilterConfig{
			MaxFileCount:          raw.Filter.MaxFileCount,
			MaxFileCountNonCN:     raw.Filter.MaxFileCountNonCN,
			MaxFileCountNumeric:   raw.Filter.MaxFileCountNumeric,
			SummaryAboveFiles:     raw.Filter.SummaryAboveFiles,
			SummaryAbovePathBytes: raw.Filter.SummaryAbovePathBytes,
			MaxFullPathBytes:      raw.Filter.MaxFullPathBytes,
			MaxStoredNameBytes:    raw.Filter.MaxStoredNameBytes,
			SummaryMaxExtensions:  raw.Filter.SummaryMaxExtensions,
			HashOnlyAboveFiles:    raw.Filter.HashOnlyAboveFiles,
			RejectAboveFiles:      raw.Filter.RejectAboveFiles,
		},
		Exporter: ExporterConfig{
			Kind:           strings.ToLower(strings.TrimSpace(raw.Exporter.Kind)),
			FilePath:       strings.TrimSpace(raw.Exporter.FilePath),
			HTTPEndpoint:   strings.TrimSpace(raw.Exporter.HTTPEndpoint),
			OracleEndpoint: strings.TrimSpace(raw.Exporter.OracleEndpoint),
			BatchSize:      raw.Exporter.BatchSize,
			FlushInterval:  parseDuration(raw.Exporter.FlushInterval),
			HTTPTimeout:    parseDuration(raw.Exporter.HTTPTimeout),
			HTTPRetries:    raw.Exporter.HTTPRetries,
			RetryBackoff:   parseDuration(raw.Exporter.RetryBackoff),
			WalDir:         strings.TrimSpace(raw.Exporter.WalDir),
			APIKey:         strings.TrimSpace(raw.Exporter.APIKey),
			OracleAPIKey:   strings.TrimSpace(raw.Exporter.OracleAPIKey),
			CrawlerID:      strings.TrimSpace(raw.Exporter.CrawlerID),
			SpoolDir:       strings.TrimSpace(raw.Exporter.SpoolDir),
			SpoolMaxBytes:  raw.Exporter.SpoolMaxBytes,
		},
		Heat: HeatConfig{
			Enabled:                           raw.Heat.Enabled,
			Endpoint:                          strings.TrimSpace(raw.Heat.Endpoint),
			CrawlerID:                         strings.TrimSpace(raw.Heat.CrawlerID),
			MasterSecretFile:                  strings.TrimSpace(raw.Heat.MasterSecretFile),
			HMACSecretFile:                    strings.TrimSpace(raw.Heat.HMACSecretFile),
			SpoolDir:                          strings.TrimSpace(raw.Heat.SpoolDir),
			SpoolMaxBytes:                     raw.Heat.SpoolMaxBytes,
			KnownCrawlers:                     strings.TrimSpace(raw.Heat.KnownCrawlers),
			QueueCapacity:                     raw.Heat.QueueCapacity,
			BatchSize:                         raw.Heat.BatchSize,
			FlushInterval:                     parseDuration(raw.Heat.FlushInterval),
			HTTPTimeout:                       parseDuration(raw.Heat.HTTPTimeout),
			RetryBackoff:                      parseDuration(raw.Heat.RetryBackoff),
			ShadowBloomEnabled:                raw.Heat.ShadowBloomEnabled,
			ShadowBloomDropProbableDuplicates: raw.Heat.ShadowBloomDropProbableDuplicates,
			ShadowBloomCapacity:               raw.Heat.ShadowBloomCapacity,
			ShadowBloomFalsePositive:          raw.Heat.ShadowBloomFalsePositive,
			ShadowBloomSampleCapacity:         raw.Heat.ShadowBloomSampleCapacity,
		},
	}

	return normalize(cfg), nil
}

func normalize(cfg Config) Config {
	if cfg.Role == "" {
		cfg.Role = "combined"
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = defaultInstanceID()
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":6881"
	}
	if cfg.EventQueue <= 0 {
		cfg.EventQueue = defaultEventQueue()
	}
	if cfg.Dedupe.PeerTTL <= 0 {
		cfg.Dedupe.PeerTTL = 10 * time.Minute
	}
	if cfg.Dedupe.MetadataTTL <= 0 {
		cfg.Dedupe.MetadataTTL = 30 * time.Minute
	}
	if cfg.Discovery.Mode == "" {
		cfg.Discovery.Mode = "crawl"
	}
	if cfg.Discovery.Instances <= 0 {
		cfg.Discovery.Instances = 1
	}
	if cfg.Discovery.LookupNodes <= 0 {
		cfg.Discovery.LookupNodes = 32
	}
	if cfg.Discovery.LookupDHTs <= 0 {
		cfg.Discovery.LookupDHTs = 1
	}
	if cfg.Discovery.LookupQueue <= 0 {
		cfg.Discovery.LookupQueue = 8192
	}
	if cfg.Discovery.LookupRate <= 0 {
		cfg.Discovery.LookupRate = 100
	}
	if cfg.Discovery.LookupWorkers <= 0 {
		cfg.Discovery.LookupWorkers = 1
	}
	if cfg.Discovery.LookupFollowups < 0 {
		cfg.Discovery.LookupFollowups = 0
	}
	if cfg.Discovery.LookupFollowups > 8 {
		cfg.Discovery.LookupFollowups = 8
	}
	if cfg.Discovery.SampleRate <= 0 {
		cfg.Discovery.SampleRate = 20
	}
	if cfg.Discovery.PacketWorkers <= 0 {
		cfg.Discovery.PacketWorkers = defaultPacketWorkers()
	}
	if cfg.Discovery.PacketJobs <= 0 {
		cfg.Discovery.PacketJobs = defaultPacketJobs()
	}
	if cfg.Discovery.MaxNodes <= 0 {
		cfg.Discovery.MaxNodes = defaultMaxNodes()
	}
	if cfg.Discovery.RefreshNodes <= 0 {
		cfg.Discovery.RefreshNodes = defaultRefreshNodes()
	}
	if cfg.Metadata.BlackListSize <= 0 {
		cfg.Metadata.BlackListSize = 131072
	}
	if cfg.Metadata.RequestQueueSize <= 0 {
		cfg.Metadata.RequestQueueSize = defaultMetadataRequestQueue()
	}
	// worker_queue_size is the legacy spelling of worker_max. A new explicit
	// worker_max wins; otherwise existing production configurations retain the
	// exact old ceiling and initial active-worker count.
	if cfg.Metadata.WorkerMax <= 0 {
		cfg.Metadata.WorkerMax = cfg.Metadata.WorkerQueueSize
	}
	if cfg.Metadata.WorkerMax <= 0 {
		cfg.Metadata.WorkerMax = defaultMetadataWorkers()
	}
	cfg.Metadata.WorkerQueueSize = cfg.Metadata.WorkerMax
	if cfg.Metadata.WorkerMin <= 0 {
		cfg.Metadata.WorkerMin = defaultMetadataWorkerMin(cfg.Metadata.WorkerMax)
	}
	if cfg.Metadata.WorkerMin > cfg.Metadata.WorkerMax {
		cfg.Metadata.WorkerMin = cfg.Metadata.WorkerMax
	}
	if cfg.Metadata.WorkerInitial <= 0 {
		cfg.Metadata.WorkerInitial = cfg.Metadata.WorkerMax
	}
	if cfg.Metadata.WorkerInitial < cfg.Metadata.WorkerMin {
		cfg.Metadata.WorkerInitial = cfg.Metadata.WorkerMin
	}
	if cfg.Metadata.WorkerInitial > cfg.Metadata.WorkerMax {
		cfg.Metadata.WorkerInitial = cfg.Metadata.WorkerMax
	}
	normalizeFilterConfig(&cfg.Filter)

	if cfg.Exporter.Kind == "" {
		cfg.Exporter.Kind = "stdout"
	}
	if cfg.Exporter.BatchSize <= 0 {
		cfg.Exporter.BatchSize = 512
	}
	if cfg.Exporter.FlushInterval <= 0 {
		cfg.Exporter.FlushInterval = 500 * time.Millisecond
	}
	if cfg.Exporter.HTTPTimeout <= 0 {
		cfg.Exporter.HTTPTimeout = 5 * time.Second
	}
	if cfg.Exporter.HTTPRetries < 0 {
		cfg.Exporter.HTTPRetries = 0
	}
	if cfg.Exporter.RetryBackoff <= 0 {
		cfg.Exporter.RetryBackoff = time.Second
	}
	if cfg.Exporter.SpoolDir != "" && cfg.Exporter.SpoolMaxBytes <= 0 {
		// Bound local outage buffering so a disconnected storage service cannot
		// consume the crawler's entire disk. Operators can raise this explicitly.
		cfg.Exporter.SpoolMaxBytes = 4 << 30
	}
	if cfg.Heat.QueueCapacity <= 0 {
		cfg.Heat.QueueCapacity = 65_536
	}
	if cfg.Heat.BatchSize <= 0 {
		cfg.Heat.BatchSize = 4_096
	}
	if cfg.Heat.BatchSize > cfg.Heat.QueueCapacity {
		cfg.Heat.BatchSize = cfg.Heat.QueueCapacity
	}
	if cfg.Heat.FlushInterval <= 0 {
		cfg.Heat.FlushInterval = 25 * time.Millisecond
	}
	if cfg.Heat.HTTPTimeout <= 0 {
		cfg.Heat.HTTPTimeout = 10 * time.Second
	}
	if cfg.Heat.RetryBackoff <= 0 {
		cfg.Heat.RetryBackoff = time.Second
	}
	if cfg.Heat.SpoolMaxBytes <= 0 {
		cfg.Heat.SpoolMaxBytes = 512 << 20
	}
	if cfg.Heat.ShadowBloomCapacity <= 0 {
		cfg.Heat.ShadowBloomCapacity = 1_000_000
	}
	if cfg.Heat.ShadowBloomFalsePositive <= 0 {
		cfg.Heat.ShadowBloomFalsePositive = 1_000
	}
	if cfg.Heat.ShadowBloomSampleCapacity <= 0 {
		cfg.Heat.ShadowBloomSampleCapacity = 4_096
	}
	// The hard-filter flag is explicit and sufficient on its own. Enabling the
	// underlying bounded Bloom avoids a silently ineffective configuration.
	if cfg.Heat.ShadowBloomDropProbableDuplicates {
		cfg.Heat.ShadowBloomEnabled = true
	}

	switch cfg.Role {
	case "discovery":
		cfg.Discovery.EmitPeerEvents = true
		cfg.Metadata.Enabled = false
	case "metadata":
		cfg.Discovery.EmitPeerEvents = false
		cfg.Metadata.Enabled = true
	default:
		cfg.Discovery.EmitPeerEvents = true
		cfg.Metadata.Enabled = true
	}

	if cfg.Exporter.Kind == "file" && cfg.Exporter.FilePath == "" {
		cfg.Exporter.FilePath = "events.jsonl"
	}

	return cfg
}

type fileConfig struct {
	Role        string              `json:"role"`
	InstanceID  string              `json:"instance_id"`
	ListenAddr  string              `json:"listen_addr"`
	ListenAddrs string              `json:"listen_addrs"`
	EventQueue  int                 `json:"event_queue"`
	AutoTune    *bool               `json:"auto_tune"`
	MemLimitMB  int                 `json:"mem_limit_mb"`
	Dedupe      fileDedupeConfig    `json:"dedupe"`
	Discovery   fileDiscoveryConfig `json:"discovery"`
	Metadata    fileMetadataConfig  `json:"metadata"`
	Filter      fileFilterConfig    `json:"filter"`
	Exporter    fileExporterConfig  `json:"exporter"`
	Heat        fileHeatConfig      `json:"heat"`
}

type fileFilterConfig struct {
	MaxFileCount          int `json:"max_file_count"`
	MaxFileCountNonCN     int `json:"max_file_count_non_cn"`
	MaxFileCountNumeric   int `json:"max_file_count_numeric"`
	SummaryAboveFiles     int `json:"summary_above_files"`
	SummaryAbovePathBytes int `json:"summary_above_path_bytes"`
	MaxFullPathBytes      int `json:"max_full_path_bytes"`
	MaxStoredNameBytes    int `json:"max_stored_name_bytes"`
	SummaryMaxExtensions  int `json:"summary_max_extensions"`
	HashOnlyAboveFiles    int `json:"hash_only_above_files"`
	RejectAboveFiles      int `json:"reject_above_files"`
}

type fileDedupeConfig struct {
	PeerTTL     string `json:"peer_ttl"`
	MetadataTTL string `json:"metadata_ttl"`
}

type fileDiscoveryConfig struct {
	Mode             string `json:"mode"`
	EmitPeerEvents   bool   `json:"emit_peer_events"`
	PrimeNodes       string `json:"prime_nodes"`
	Instances        int    `json:"instances"`
	ActiveLookup     *bool  `json:"active_lookup"`
	LookupNodes      int    `json:"lookup_nodes"`
	LookupDHTs       int    `json:"lookup_dhts"`
	LookupQueue      int    `json:"lookup_queue"`
	LookupRate       int    `json:"lookup_rate"`
	LookupWorkers    int    `json:"lookup_workers"`
	LookupFollowups  int    `json:"lookup_followups"`
	LookupSpread     bool   `json:"lookup_spread"`
	SampleInfohashes bool   `json:"sample_infohashes"`
	SampleRate       int    `json:"sample_rate"`
	PacketWorkers    int    `json:"packet_workers"`
	PacketJobs       int    `json:"packet_jobs"`
	ReadWorkers      int    `json:"read_workers"`
	MaxNodes         int    `json:"max_nodes"`
	RefreshNodes     int    `json:"refresh_nodes"`
	NodeIDFile       string `json:"node_id_file"`
	NodeIDDir        string `json:"node_id_dir"`
}

type fileMetadataConfig struct {
	Enabled          bool `json:"enabled"`
	BlackListSize    int  `json:"black_list_size"`
	RequestQueueSize int  `json:"request_queue_size"`
	WorkerQueueSize  int  `json:"worker_queue_size"`
	WorkerInitial    int  `json:"worker_initial"`
	WorkerMin        int  `json:"worker_min"`
	WorkerMax        int  `json:"worker_max"`
}

type fileExporterConfig struct {
	Kind           string `json:"kind"`
	FilePath       string `json:"file_path"`
	HTTPEndpoint   string `json:"http_endpoint"`
	OracleEndpoint string `json:"oracle_endpoint"`
	BatchSize      int    `json:"batch_size"`
	FlushInterval  string `json:"flush_interval"`
	HTTPTimeout    string `json:"http_timeout"`
	HTTPRetries    int    `json:"http_retries"`
	RetryBackoff   string `json:"retry_backoff"`
	WalDir         string `json:"wal_dir"`
	APIKey         string `json:"api_key"`
	OracleAPIKey   string `json:"oracle_api_key"`
	CrawlerID      string `json:"crawler_id"`
	SpoolDir       string `json:"spool_dir"`
	SpoolMaxBytes  int64  `json:"spool_max_bytes"`
}

type fileHeatConfig struct {
	Enabled                           bool   `json:"enabled"`
	Endpoint                          string `json:"endpoint"`
	CrawlerID                         string `json:"crawler_id"`
	MasterSecretFile                  string `json:"master_secret_file"`
	HMACSecretFile                    string `json:"hmac_secret_file"`
	SpoolDir                          string `json:"spool_dir"`
	SpoolMaxBytes                     int64  `json:"spool_max_bytes"`
	KnownCrawlers                     string `json:"known_crawlers"`
	QueueCapacity                     int    `json:"queue_capacity"`
	BatchSize                         int    `json:"batch_size"`
	FlushInterval                     string `json:"flush_interval"`
	HTTPTimeout                       string `json:"http_timeout"`
	RetryBackoff                      string `json:"retry_backoff"`
	ShadowBloomEnabled                bool   `json:"shadow_bloom_enabled"`
	ShadowBloomDropProbableDuplicates bool   `json:"shadow_bloom_drop_probable_duplicates"`
	ShadowBloomCapacity               int    `json:"shadow_bloom_capacity"`
	ShadowBloomFalsePositive          int    `json:"shadow_bloom_fp_ppm"`
	ShadowBloomSampleCapacity         int    `json:"shadow_bloom_sample_capacity"`
}

// normalizeFilterConfig applies built-in defaults for any filter threshold that
// is zero (unset). A negative value disables the corresponding rule.
func normalizeFilterConfig(f *FilterConfig) {
	if f.MaxFileCount == 0 {
		f.MaxFileCount = 10_000
	}
	if f.MaxFileCountNonCN == 0 {
		f.MaxFileCountNonCN = 1_000
	}
	if f.MaxFileCountNumeric == 0 {
		f.MaxFileCountNumeric = 100
	}
	if f.SummaryAboveFiles <= 0 {
		f.SummaryAboveFiles = 2_000
	}
	if f.SummaryAbovePathBytes <= 0 {
		f.SummaryAbovePathBytes = 512 << 10
	}
	if f.MaxFullPathBytes <= 0 {
		f.MaxFullPathBytes = 4 << 10
	}
	if f.MaxStoredNameBytes <= 0 {
		f.MaxStoredNameBytes = 1 << 10
	}
	if f.SummaryMaxExtensions <= 0 {
		f.SummaryMaxExtensions = 32
	}
	if f.HashOnlyAboveFiles < 0 {
		f.HashOnlyAboveFiles = 0
	}
	if f.RejectAboveFiles < 0 {
		f.RejectAboveFiles = 0
	}
}

func intOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func cpuScale() int {
	cpus := runtime.NumCPU()
	if cpus < 1 {
		return 1
	}
	return cpus
}

func defaultEventQueue() int {
	value := cpuScale() * 2048
	if value < 4096 {
		return 4096
	}
	if value > 16384 {
		return 16384
	}
	return value
}

func defaultPacketWorkers() int {
	value := cpuScale() * 4
	if value < 4 {
		return 4
	}
	if value > 32 {
		return 32
	}
	return value
}

func defaultPacketJobs() int {
	value := defaultPacketWorkers() * 512
	if value < 4096 {
		return 4096
	}
	if value > 16384 {
		return 16384
	}
	return value
}

func defaultMaxNodes() int {
	value := cpuScale() * 2500
	if value < 5000 {
		return 5000
	}
	if value > 20000 {
		return 20000
	}
	return value
}

func defaultRefreshNodes() int {
	value := cpuScale() * 128
	if value < 256 {
		return 256
	}
	if value > 1024 {
		return 1024
	}
	return value
}

func defaultMetadataWorkers() int {
	// wire worker 是 I/O-bound（绝大部分时间阻塞在拨号超时/网络读），
	// 上限可远超核数；实际并发由 tuneWireWorkers 按拨号成功率和 CPU
	// 利用率动态收缩，这里只定天花板。
	value := cpuScale() * 512
	if value < 1024 {
		return 1024
	}
	if value > 4096 {
		return 4096
	}
	return value
}

func defaultMetadataWorkerMin(maxWorkers int) int {
	if maxWorkers < 64 {
		return 1
	}
	value := maxWorkers / 8
	if value < 64 {
		return 64
	}
	if value > maxWorkers {
		return maxWorkers
	}
	return value
}

func defaultMetadataRequestQueue() int {
	value := defaultMetadataWorkers() * 32
	if value < 8192 {
		return 8192
	}
	if value > 32768 {
		return 32768
	}
	return value
}

func parseDuration(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return parsed
}

func defaultInstanceID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func getenvDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
