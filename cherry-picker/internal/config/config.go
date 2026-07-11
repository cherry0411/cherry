package config

import (
	"encoding/json"
	"fmt"
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
	Mode           string
	EmitPeerEvents bool
	PacketWorkers  int
	PacketJobs     int
	MaxNodes       int
	RefreshNodes   int
	NodeIDFile     string
	NodeIDDir      string
}

type MetadataConfig struct {
	Enabled          bool
	BlackListSize    int
	RequestQueueSize int
	WorkerQueueSize  int
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
}

type ExporterConfig struct {
	Kind          string
	FilePath      string
	HTTPEndpoint  string
	BatchSize     int
	FlushInterval time.Duration
	HTTPTimeout   time.Duration
	HTTPRetries   int
	RetryBackoff  time.Duration
	WalDir        string // WAL 本地缓冲目录，空 = 不启用
	APIKey        string // X-API-Key 认证头，空 = 不发送
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
			Mode:           strings.ToLower(getenvDefault("CHERRY_PICKER_DHT_MODE", "crawl")),
			EmitPeerEvents: getenvBool("CHERRY_PICKER_EMIT_PEER_EVENTS", true),
			PacketWorkers:  getenvInt("CHERRY_PICKER_DHT_PACKET_WORKERS", defaultPacketWorkers()),
			PacketJobs:     getenvInt("CHERRY_PICKER_DHT_PACKET_JOBS", defaultPacketJobs()),
			MaxNodes:       getenvInt("CHERRY_PICKER_DHT_MAX_NODES", defaultMaxNodes()),
			RefreshNodes:   getenvInt("CHERRY_PICKER_DHT_REFRESH_NODES", defaultRefreshNodes()),
			NodeIDFile:     getenvDefault("CHERRY_PICKER_NODE_ID_FILE", ""),
			NodeIDDir:      getenvDefault("CHERRY_PICKER_NODE_ID_DIR", ""),
		},
		Metadata: MetadataConfig{
			Enabled:          getenvBool("CHERRY_PICKER_METADATA_ENABLED", true),
			BlackListSize:    getenvInt("CHERRY_PICKER_METADATA_BLACKLIST", 131072),
			RequestQueueSize: getenvInt("CHERRY_PICKER_METADATA_REQUEST_QUEUE", defaultMetadataRequestQueue()),
			WorkerQueueSize:  getenvInt("CHERRY_PICKER_METADATA_WORKERS", defaultMetadataWorkers()),
		},
		Filter: FilterConfig{
			MaxFileCount:        getenvInt("CHERRY_PICKER_FILTER_MAX_FILES", 0),
			MaxFileCountNonCN:   getenvInt("CHERRY_PICKER_FILTER_MAX_FILES_NON_CN", 0),
			MaxFileCountNumeric: getenvInt("CHERRY_PICKER_FILTER_MAX_FILES_NUMERIC", 0),
		},
		Exporter: ExporterConfig{
			Kind:          strings.ToLower(getenvDefault("CHERRY_PICKER_EXPORTER", "stdout")),
			FilePath:      getenvDefault("CHERRY_PICKER_EXPORTER_FILE", ""),
			HTTPEndpoint:  getenvDefault("CHERRY_PICKER_EXPORTER_URL", ""),
			BatchSize:     getenvInt("CHERRY_PICKER_EXPORTER_BATCH", 512),
			FlushInterval: getenvDuration("CHERRY_PICKER_EXPORTER_FLUSH", 500*time.Millisecond),
			HTTPTimeout:   getenvDuration("CHERRY_PICKER_EXPORTER_TIMEOUT", 5*time.Second),
			HTTPRetries:   getenvInt("CHERRY_PICKER_EXPORTER_HTTP_RETRIES", 3),
			RetryBackoff:  getenvDuration("CHERRY_PICKER_EXPORTER_RETRY_BACKOFF", time.Second),
			WalDir:        getenvDefault("CHERRY_PICKER_WAL_DIR", ""),
			APIKey:        getenvDefault("CHERRY_API_KEY", ""),
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
	if err := json.Unmarshal(data, &raw); err != nil {
		return Config{}, err
	}

	// auto_tune 未在 JSON 中出现时默认开启
	autoTune := true
	if raw.AutoTune != nil {
		autoTune = *raw.AutoTune
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
			Mode:           strings.ToLower(strings.TrimSpace(raw.Discovery.Mode)),
			EmitPeerEvents: raw.Discovery.EmitPeerEvents,
			PacketWorkers:  intOrDefault(raw.Discovery.PacketWorkers, 512),
			PacketJobs:     intOrDefault(raw.Discovery.PacketJobs, 65536),
			MaxNodes:       intOrDefault(raw.Discovery.MaxNodes, 50000),
			RefreshNodes:   intOrDefault(raw.Discovery.RefreshNodes, 2048),
			NodeIDFile:     strings.TrimSpace(raw.Discovery.NodeIDFile),
			NodeIDDir:      strings.TrimSpace(raw.Discovery.NodeIDDir),
		},
		Metadata: MetadataConfig{
			Enabled:          raw.Metadata.Enabled,
			BlackListSize:    raw.Metadata.BlackListSize,
			RequestQueueSize: intOrDefault(raw.Metadata.RequestQueueSize, defaultMetadataRequestQueue()),
			WorkerQueueSize:  intOrDefault(raw.Metadata.WorkerQueueSize, defaultMetadataWorkers()),
		},
		Filter: FilterConfig{
			MaxFileCount:        raw.Filter.MaxFileCount,
			MaxFileCountNonCN:   raw.Filter.MaxFileCountNonCN,
			MaxFileCountNumeric: raw.Filter.MaxFileCountNumeric,
		},
		Exporter: ExporterConfig{
			Kind:          strings.ToLower(strings.TrimSpace(raw.Exporter.Kind)),
			FilePath:      strings.TrimSpace(raw.Exporter.FilePath),
			HTTPEndpoint:  strings.TrimSpace(raw.Exporter.HTTPEndpoint),
			BatchSize:     raw.Exporter.BatchSize,
			FlushInterval: parseDuration(raw.Exporter.FlushInterval),
			HTTPTimeout:   parseDuration(raw.Exporter.HTTPTimeout),
			HTTPRetries:   raw.Exporter.HTTPRetries,
			RetryBackoff:  parseDuration(raw.Exporter.RetryBackoff),
			WalDir:        strings.TrimSpace(raw.Exporter.WalDir),
			APIKey:        strings.TrimSpace(raw.Exporter.APIKey),
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
	if cfg.Metadata.WorkerQueueSize <= 0 {
		cfg.Metadata.WorkerQueueSize = defaultMetadataWorkers()
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
}

type fileFilterConfig struct {
	MaxFileCount        int `json:"max_file_count"`
	MaxFileCountNonCN   int `json:"max_file_count_non_cn"`
	MaxFileCountNumeric int `json:"max_file_count_numeric"`
}

type fileDedupeConfig struct {
	PeerTTL     string `json:"peer_ttl"`
	MetadataTTL string `json:"metadata_ttl"`
}

type fileDiscoveryConfig struct {
	Mode           string `json:"mode"`
	EmitPeerEvents bool   `json:"emit_peer_events"`
	PacketWorkers  int    `json:"packet_workers"`
	PacketJobs     int    `json:"packet_jobs"`
	MaxNodes       int    `json:"max_nodes"`
	RefreshNodes   int    `json:"refresh_nodes"`
	NodeIDFile     string `json:"node_id_file"`
	NodeIDDir      string `json:"node_id_dir"`
}

type fileMetadataConfig struct {
	Enabled          bool `json:"enabled"`
	BlackListSize    int  `json:"black_list_size"`
	RequestQueueSize int  `json:"request_queue_size"`
	WorkerQueueSize  int  `json:"worker_queue_size"`
}

type fileExporterConfig struct {
	Kind          string `json:"kind"`
	FilePath      string `json:"file_path"`
	HTTPEndpoint  string `json:"http_endpoint"`
	BatchSize     int    `json:"batch_size"`
	FlushInterval string `json:"flush_interval"`
	HTTPTimeout   string `json:"http_timeout"`
	HTTPRetries   int    `json:"http_retries"`
	RetryBackoff  string `json:"retry_backoff"`
	WalDir        string `json:"wal_dir"`
	APIKey        string `json:"api_key"`
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
