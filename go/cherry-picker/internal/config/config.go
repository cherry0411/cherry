package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Role          string
	InstanceID    string
	ListenAddr    string
	EventQueue    int
	BrokerURL     string
	Dedupe        DedupeConfig
	Discovery     DiscoveryConfig
	Metadata      MetadataConfig
	Exporter      ExporterConfig
	AutoTune      bool
	TargetCPU     float64
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
}

type MetadataConfig struct {
	Enabled          bool
	BlackListSize    int
	RequestQueueSize int
	WorkerQueueSize  int
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
		Role:       strings.ToLower(getenvDefault("CHERRY_PICKER_ROLE", "combined")),
		InstanceID: instanceID,
		ListenAddr: getenvDefault("CHERRY_PICKER_LISTEN_ADDR", ":6881"),
		EventQueue: getenvInt("CHERRY_PICKER_EVENT_QUEUE", 4096),
		BrokerURL:  getenvDefault("CHERRY_PICKER_BROKER_URL", ""),
		AutoTune:   getenvBool("CHERRY_PICKER_AUTO_TUNE", false),
		TargetCPU:  float64(getenvInt("CHERRY_PICKER_TARGET_CPU", 80)) / 100.0,
		Dedupe: DedupeConfig{
			PeerTTL:     getenvDuration("CHERRY_PICKER_DEDUPE_PEER_TTL", 10*time.Minute),
			MetadataTTL: getenvDuration("CHERRY_PICKER_DEDUPE_METADATA_TTL", 30*time.Minute),
		},
		Discovery: DiscoveryConfig{
			Mode:           strings.ToLower(getenvDefault("CHERRY_PICKER_DHT_MODE", "crawl")),
			EmitPeerEvents: getenvBool("CHERRY_PICKER_EMIT_PEER_EVENTS", true),
			PacketWorkers:  getenvInt("CHERRY_PICKER_DHT_PACKET_WORKERS", 512),
			PacketJobs:     getenvInt("CHERRY_PICKER_DHT_PACKET_JOBS", 65536),
			MaxNodes:       getenvInt("CHERRY_PICKER_DHT_MAX_NODES", 50000),
			RefreshNodes:   getenvInt("CHERRY_PICKER_DHT_REFRESH_NODES", 2048),
		},
		Metadata: MetadataConfig{
			Enabled:          getenvBool("CHERRY_PICKER_METADATA_ENABLED", true),
			BlackListSize:    getenvInt("CHERRY_PICKER_METADATA_BLACKLIST", 65536),
			RequestQueueSize: getenvInt("CHERRY_PICKER_METADATA_REQUEST_QUEUE", 65536),
			WorkerQueueSize:  getenvInt("CHERRY_PICKER_METADATA_WORKERS", 512),
		},
		Exporter: ExporterConfig{
			Kind:          strings.ToLower(getenvDefault("CHERRY_PICKER_EXPORTER", "stdout")),
			FilePath:      getenvDefault("CHERRY_PICKER_EXPORTER_FILE", ""),
			HTTPEndpoint:  getenvDefault("CHERRY_PICKER_EXPORTER_URL", ""),
			BatchSize:     getenvInt("CHERRY_PICKER_EXPORTER_BATCH", 128),
			FlushInterval: getenvDuration("CHERRY_PICKER_EXPORTER_FLUSH", 2*time.Second),
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

	cfg := Config{
		Role:       strings.ToLower(strings.TrimSpace(raw.Role)),
		InstanceID: strings.TrimSpace(raw.InstanceID),
		ListenAddr: strings.TrimSpace(raw.ListenAddr),
		EventQueue: raw.EventQueue,
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
		},
		Metadata: MetadataConfig{
			Enabled:          raw.Metadata.Enabled,
			BlackListSize:    raw.Metadata.BlackListSize,
			RequestQueueSize: raw.Metadata.RequestQueueSize,
			WorkerQueueSize:  raw.Metadata.WorkerQueueSize,
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
		cfg.EventQueue = 4096
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
		cfg.Discovery.PacketWorkers = 512
	}
	if cfg.Discovery.PacketJobs <= 0 {
		cfg.Discovery.PacketJobs = 65536
	}
	if cfg.Discovery.MaxNodes <= 0 {
		cfg.Discovery.MaxNodes = 50000
	}
	if cfg.Discovery.RefreshNodes <= 0 {
		cfg.Discovery.RefreshNodes = 2048
	}
	if cfg.Metadata.BlackListSize <= 0 {
		cfg.Metadata.BlackListSize = 65536
	}
	if cfg.Metadata.RequestQueueSize <= 0 {
		cfg.Metadata.RequestQueueSize = 65536
	}
	if cfg.Metadata.WorkerQueueSize <= 0 {
		cfg.Metadata.WorkerQueueSize = 512
	}
	if cfg.Exporter.Kind == "" {
		cfg.Exporter.Kind = "stdout"
	}
	if cfg.Exporter.BatchSize <= 0 {
		cfg.Exporter.BatchSize = 128
	}
	if cfg.Exporter.FlushInterval <= 0 {
		cfg.Exporter.FlushInterval = 2 * time.Second
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
	Role       string              `json:"role"`
	InstanceID string              `json:"instance_id"`
	ListenAddr string              `json:"listen_addr"`
	EventQueue int                 `json:"event_queue"`
	Dedupe     fileDedupeConfig    `json:"dedupe"`
	Discovery  fileDiscoveryConfig `json:"discovery"`
	Metadata   fileMetadataConfig  `json:"metadata"`
	Exporter   fileExporterConfig  `json:"exporter"`
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

func intOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
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
