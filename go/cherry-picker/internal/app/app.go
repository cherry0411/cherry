package app

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"cherry-picker/internal/config"
	dht "cherry-picker/internal/dht"
	"cherry-picker/internal/export"
	"cherry-picker/internal/pipeline"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

type Application struct {
	cfg    config.Config
	logger *log.Logger
}

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
}

type roleBehavior struct {
	emitPeerEvents bool
	fetchMetadata  bool
}

func New(cfg config.Config, logger *log.Logger) *Application {
	return &Application{cfg: cfg, logger: logger}
}

func (a *Application) Run(ctx context.Context) error {
	events := make(chan pipeline.Event, a.cfg.EventQueue)
	sink, err := export.NewSink(a.cfg.Exporter)
	if err != nil {
		return err
	}

	exporter := export.NewBatchExporter(a.logger, sink, a.cfg.Exporter.BatchSize, a.cfg.Exporter.FlushInterval, events)
	go func() {
		if err := exporter.Run(ctx); err != nil {
			a.logger.Printf("exporter stopped with error: %v", err)
		}
	}()

	stats := &runtimeStats{}
	behavior := a.roleBehavior()
	infohashSeen := newSeenSet(a.cfg.Dedupe.PeerTTL)
	peerSeen := newSeenSet(a.cfg.Dedupe.PeerTTL)

	var downloader *dht.Wire
	metadataRequestSeen := newSeenSet(a.cfg.Dedupe.MetadataTTL)
	metadataResultSeen := newSeenSet(a.cfg.Dedupe.MetadataTTL)
	if behavior.fetchMetadata {
		downloader = dht.NewWire(a.cfg.Metadata.BlackListSize, a.cfg.Metadata.RequestQueueSize, a.cfg.Metadata.WorkerQueueSize)
		go a.consumeMetadata(ctx, downloader, events, stats, metadataResultSeen)
		go downloader.Run()
		a.logger.Printf("metadata workers enabled: blacklist=%d queue=%d workers=%d", a.cfg.Metadata.BlackListSize, a.cfg.Metadata.RequestQueueSize, a.cfg.Metadata.WorkerQueueSize)
	}

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
		infoHashHex := hex.EncodeToString([]byte(infoHash))
		a.submitInfohashEvent(events, infoHashHex, ip, port, "get_peers", infohashSeen, stats)
	}
	dhtConfig.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		infoHashHex := hex.EncodeToString([]byte(infoHash))
		if behavior.emitPeerEvents {
			a.submitPeerEvent(events, infoHashHex, peer.IP.String(), peer.Port, "get_peers_response", peerSeen, stats)
		}
		if downloader != nil {
			a.queueMetadataRequest(downloader, infoHashHex, peer.IP.String(), peer.Port, metadataRequestSeen, stats)
		}
	}
	dhtConfig.OnAnnouncePeer = func(infoHash, ip string, port int) {
		infoHashHex := hex.EncodeToString([]byte(infoHash))

		if behavior.emitPeerEvents {
			a.submitPeerEvent(events, infoHashHex, ip, port, "announce_peer", peerSeen, stats)
		}

		if downloader != nil {
			a.queueMetadataRequest(downloader, infoHashHex, ip, port, metadataRequestSeen, stats)
		}
	}

	node := dht.New(dhtConfig)
	go a.emitStats(ctx, events, stats, node)
	go node.Run()
	a.logger.Printf("crawler started: role=%s instance=%s listen=%s mode=%s", a.cfg.Role, a.cfg.InstanceID, a.cfg.ListenAddr, a.cfg.Discovery.Mode)

	<-ctx.Done()
	a.logger.Printf("shutdown requested")
	return nil
}

func (a *Application) submitInfohashEvent(events chan<- pipeline.Event, infoHashHex, ip string, port int, source string, seen *seenSet, stats *runtimeStats) {
	if seen.Seen(strings.Join([]string{infoHashHex, source, ip, strconv.Itoa(port)}, "|")) {
		stats.infohashEventsDeduped.Add(1)
		return
	}

	a.submitEvent(events, pipeline.Event{
		Type:       pipeline.EventPeerDiscovered,
		Timestamp:  time.Now().UTC(),
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   infoHashHex,
		IP:         ip,
		Port:       port,
	}, stats.infohashEventsDropped.Add, stats.infohashEventsSent.Add)
}

func (a *Application) submitPeerEvent(events chan<- pipeline.Event, infoHashHex, ip string, port int, source string, seen *seenSet, stats *runtimeStats) {
	peerKey := strings.Join([]string{infoHashHex, ip, strconv.Itoa(port)}, "|")
	if seen.Seen(peerKey) {
		stats.peerEventsDeduped.Add(1)
		return
	}

	a.submitEvent(events, pipeline.Event{
		Type:       pipeline.EventPeerDiscovered,
		Timestamp:  time.Now().UTC(),
		InstanceID: a.cfg.InstanceID,
		Source:     source,
		InfoHash:   infoHashHex,
		IP:         ip,
		Port:       port,
	}, stats.peerEventsDropped.Add, stats.peerEventsSent.Add)
}

func (a *Application) queueMetadataRequest(downloader *dht.Wire, infoHashHex, ip string, port int, seen *seenSet, stats *runtimeStats) {
	requestKey := strings.Join([]string{infoHashHex, ip, strconv.Itoa(port)}, "|")
	if seen.Seen(requestKey) {
		stats.metadataRequestsDeduped.Add(1)
		return
	}

	infoHashBytes, err := hex.DecodeString(infoHashHex)
	if err != nil {
		return
	}

	stats.metadataRequestsQueued.Add(1)
	downloader.Request(infoHashBytes, ip, port)
}

func (a *Application) roleBehavior() roleBehavior {
	switch a.cfg.Role {
	case "discovery":
		return roleBehavior{emitPeerEvents: true}
	case "metadata":
		return roleBehavior{fetchMetadata: true}
	default:
		return roleBehavior{emitPeerEvents: true, fetchMetadata: true}
	}
}

func (a *Application) consumeMetadata(ctx context.Context, downloader *dht.Wire, events chan<- pipeline.Event, stats *runtimeStats, seen *seenSet) {
	responses := downloader.Response()
	for {
		select {
		case <-ctx.Done():
			return
		case response := <-responses:
			infoHashHex := hex.EncodeToString(response.InfoHash)
			responseKey := strings.Join([]string{infoHashHex, response.IP, strconv.Itoa(response.Port)}, "|")
			if seen.Seen(responseKey) {
				stats.metadataEventsDeduped.Add(1)
				continue
			}
			metadata, err := normalizeMetadata(response.MetadataInfo)
			event := pipeline.Event{
				Type:       pipeline.EventMetadataFetched,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "peer_wire",
				InfoHash:   infoHashHex,
				IP:         response.IP,
				Port:       response.Port,
				Metadata:   metadata,
			}
			if err != nil {
				event.Error = err.Error()
			}
			a.submitEvent(events, event, stats.metadataEventsDropped.Add, stats.metadataEventsSent.Add)
		}
	}
}

func (a *Application) emitStats(ctx context.Context, events chan<- pipeline.Event, stats *runtimeStats, node *dht.DHT) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			packetStats := node.PacketStats()
			a.submitEvent(events, pipeline.Event{
				Type:       pipeline.EventWorkerStats,
				Timestamp:  time.Now().UTC(),
				InstanceID: a.cfg.InstanceID,
				Source:     "runtime",
				Stats: map[string]uint64{
					"infohash_events_sent":      stats.infohashEventsSent.Load(),
					"infohash_events_dropped":   stats.infohashEventsDropped.Load(),
					"infohash_events_deduped":   stats.infohashEventsDeduped.Load(),
					"peer_events_sent":          stats.peerEventsSent.Load(),
					"peer_events_dropped":       stats.peerEventsDropped.Load(),
					"peer_events_deduped":       stats.peerEventsDeduped.Load(),
					"metadata_requests_queued":  stats.metadataRequestsQueued.Load(),
					"metadata_requests_deduped": stats.metadataRequestsDeduped.Load(),
					"metadata_events_sent":      stats.metadataEventsSent.Load(),
					"metadata_events_dropped":   stats.metadataEventsDropped.Load(),
					"metadata_events_deduped":   stats.metadataEventsDeduped.Load(),
					"dht_packets_received":      packetStats.Received,
					"dht_packets_enqueued":      packetStats.Enqueued,
					"dht_packets_dropped":       packetStats.Dropped,
					"dht_packets_handled":       packetStats.Handled,
					"dht_packet_decode_errors":  packetStats.DecodeErrors,
				},
			}, func(delta uint64) uint64 { return delta }, func(delta uint64) uint64 { return delta })
		}
	}
}

func (a *Application) submitEvent(events chan<- pipeline.Event, event pipeline.Event, onDrop func(uint64) uint64, onSuccess func(uint64) uint64) {
	select {
	case events <- event:
		onSuccess(1)
	default:
		onDrop(1)
	}
}

type seenSet struct {
	mu      sync.Mutex
	items   map[string]time.Time
	ttl     time.Duration
	cleanup time.Time
}

func newSeenSet(ttl time.Duration) *seenSet {
	return &seenSet{items: make(map[string]time.Time), ttl: ttl}
}

func (s *seenSet) Seen(key string) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cleanup.IsZero() || now.Sub(s.cleanup) > s.ttl {
		for existingKey, seenAt := range s.items {
			if now.Sub(seenAt) > s.ttl {
				delete(s.items, existingKey)
			}
		}
		s.cleanup = now
	}

	if seenAt, ok := s.items[key]; ok && now.Sub(seenAt) <= s.ttl {
		return true
	}

	s.items[key] = now
	return false
}

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
					if p != "" && !isPaddingFile(p) { clean = append(clean, p) }
				}
				if len(clean) == 0 { continue }
				entry.Path = clean
				entry.PathText = filepath.ToSlash(filepath.Join(clean...))
			}
			metadata.Files = append(metadata.Files, entry)
		}
	}

	// Single-file torrent: synthesize a file entry from name + length
	if len(metadata.Files) == 0 && metadata.Name != "" && metadata.Length > 0 {
		metadata.Files = []pipeline.MetadataFile{{
			Path:     []string{metadata.Name},
			PathText: metadata.Name,
			Length:   metadata.Length,
		}}
	}
	if metadata.Length == 0 && len(metadata.Files) > 0 {
		var total int64
		for _, file := range metadata.Files {
			total += file.Length
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
	// Try GBK → UTF-8 (most common legacy encoding in Chinese torrents)
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
	// Fallback: strip invalid UTF-8 bytes
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
