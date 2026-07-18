package app

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cherry-picker/internal/config"
	dht "cherry-picker/internal/dht"
	"cherry-picker/internal/pipeline"
)

func testLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func defaultTestConfig() config.Config {
	return config.Config{
		InstanceID: "test",
		Exporter: config.ExporterConfig{
			Kind: "stdout",
		},
		Discovery: config.DiscoveryConfig{
			EmitPeerEvents: true,
		},
	}
}

func TestSubmitInfohashEventKeepsDistinctSources(t *testing.T) {
	app := New(defaultTestConfig(), testLogger())
	events := make(chan pipeline.Event, 4)
	stats := &runtimeStats{}

	// 不同 source → 应该各发送一次
	app.submitInfohashEvent(events, "abc123", "1.1.1.1", 6881, "get_peers", stats, time.Now())
	app.submitInfohashEvent(events, "abc123", "1.1.1.1", 6881, "get_peers_response", stats, time.Now())

	if got := len(events); got != 2 {
		t.Fatalf("len(events) = %d, want 2", got)
	}
	if got := stats.infohashEventsDeduped.Load(); got != 0 {
		t.Fatalf("infohash deduped = %d, want 0", got)
	}
}

func TestSubmitInfohashEventDeduplicates(t *testing.T) {
	app := New(defaultTestConfig(), testLogger())
	events := make(chan pipeline.Event, 4)
	stats := &runtimeStats{}

	// 完全相同的事件 → 第二次应该被去重
	app.submitInfohashEvent(events, "abc123", "1.1.1.1", 6881, "get_peers", stats, time.Now())
	app.submitInfohashEvent(events, "abc123", "1.1.1.1", 6881, "get_peers", stats, time.Now())

	if got := len(events); got != 1 {
		t.Fatalf("len(events) = %d, want 1", got)
	}
	if got := stats.infohashEventsDeduped.Load(); got != 1 {
		t.Fatalf("infohash deduped = %d, want 1", got)
	}
}

func TestQueueActiveLookupDropRemainsRetryable(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Metadata.Enabled = true
	cfg.Discovery.ActiveLookup = true
	application := New(cfg, testLogger())
	queue := make(chan string, 1)
	stats := &runtimeStats{}

	if !application.queueActiveLookup(queue, "first", "live|", stats) {
		t.Fatal("first lookup was not admitted")
	}
	if application.queueActiveLookup(queue, "retry", "live|", stats) {
		t.Fatal("lookup was admitted into a full queue")
	}
	if application.infohashSeen.Contains("live|retry") {
		t.Fatal("dropped lookup poisoned the seen cache")
	}
	<-queue
	if !application.queueActiveLookup(queue, "retry", "live|", stats) {
		t.Fatal("dropped lookup was not retryable after capacity returned")
	}
	if got := stats.activeLookupsDropped.Load(); got != 1 {
		t.Fatalf("activeLookupsDropped = %d, want 1", got)
	}
}

func TestQueueMetadataRequestFullWireQueueRemainsRetryable(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Metadata.Enabled = true
	application := New(cfg, testLogger())
	downloader := dht.NewWire(64, 1, 1)
	stats := &runtimeStats{}
	checkQueue := make(chan string, 2)
	infoHash := "0123456789012345678901234567890123456789"
	requestKey := buildInfohashPeerKey(infoHash, "127.0.0.1", 6882)

	if !downloader.RequestFromSource(make([]byte, 20), "127.0.0.1", 6881, dht.PeerSourceUnknown) {
		t.Fatal("failed to fill wire queue")
	}
	application.queueMetadataRequest(downloader, infoHash, "127.0.0.1", 6882, dht.PeerSourceGetPeers, stats, checkQueue)

	if application.metadataRequestSeen.Contains(requestKey) {
		t.Fatal("wire queue drop poisoned metadata request reservation")
	}
	if got := stats.metadataRequestsQueued.Load(); got != 1 {
		t.Fatalf("metadataRequestsQueued = %d, want 1 legacy admission attempt", got)
	}
	if got := stats.metadataReqAdmitted.Load(); got != 0 {
		t.Fatalf("metadataReqAdmitted = %d, want 0 for rejected admission", got)
	}
	if got := downloader.Stats.QueueDropped.Load(); got != 1 {
		t.Fatalf("wire QueueDropped = %d, want 1", got)
	}

	retryDownloader := dht.NewWire(64, 1, 1)
	application.queueMetadataRequest(retryDownloader, infoHash, "127.0.0.1", 6882, dht.PeerSourceGetPeers, stats, checkQueue)
	if !application.metadataRequestSeen.Contains(requestKey) {
		t.Fatal("request rejected by a full queue was not retryable")
	}
	if got := stats.metadataRequestsQueued.Load(); got != 2 {
		t.Fatalf("metadataRequestsQueued after retry = %d, want 2 legacy attempts", got)
	}
	if got := stats.metadataReqAdmitted.Load(); got != 1 {
		t.Fatalf("metadataReqAdmitted after retry = %d, want 1", got)
	}
}

func TestQueueMetadataRequestPauseRemainsRetryable(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Metadata.Enabled = true
	application := New(cfg, testLogger())
	downloader := dht.NewWire(64, 1, 1)
	stats := &runtimeStats{}
	checkQueue := make(chan string, 2)
	infoHash := "0123456789012345678901234567890123456789"
	requestKey := buildInfohashPeerKey(infoHash, "127.0.0.1", 6881)

	application.metaPaused.Store(true)
	application.queueMetadataRequest(downloader, infoHash, "127.0.0.1", 6881, dht.PeerSourceAnnounce, stats, checkQueue)
	if application.metadataRequestSeen.Contains(requestKey) {
		t.Fatal("paused request poisoned metadata request reservation")
	}
	if got := stats.metadataRequestsQueued.Load(); got != 0 {
		t.Fatalf("metadataRequestsQueued while paused = %d, want 0", got)
	}
	if got := stats.metadataReqAdmitted.Load(); got != 0 {
		t.Fatalf("metadataReqAdmitted while paused = %d, want 0", got)
	}

	application.metaPaused.Store(false)
	application.queueMetadataRequest(downloader, infoHash, "127.0.0.1", 6881, dht.PeerSourceAnnounce, stats, checkQueue)
	if !application.metadataRequestSeen.Contains(requestKey) {
		t.Fatal("retry after pause did not retain an admitted reservation")
	}
	if got := stats.metadataRequestsQueued.Load(); got != 1 {
		t.Fatalf("metadataRequestsQueued after retry = %d, want 1", got)
	}
	if got := stats.metadataReqAdmitted.Load(); got != 1 {
		t.Fatalf("metadataReqAdmitted after retry = %d, want 1", got)
	}
}

func TestNormalizeMetadataPrefersUTF8AndAggregatesFiles(t *testing.T) {
	data := []byte(dht.Encode(map[string]interface{}{
		"name":         "fallback-name",
		"name.utf-8":   "release-name",
		"piece length": 16384,
		"private":      1,
		"files": []interface{}{
			map[string]interface{}{
				"length":     4,
				"path":       []interface{}{"ignored", "old.txt"},
				"path.utf-8": []interface{}{"release-name", "file-a.txt"},
			},
			map[string]interface{}{
				"length": 7,
				"path":   []interface{}{"release-name", "file-b.txt"},
			},
		},
	}))

	metadata, err := normalizeMetadata(data)
	if err != nil {
		t.Fatalf("normalizeMetadata() error = %v", err)
	}
	if metadata.Name != "release-name" {
		t.Fatalf("Name = %q, want release-name", metadata.Name)
	}
	if metadata.Length != 11 {
		t.Fatalf("Length = %d, want 11", metadata.Length)
	}
	if metadata.FileCount != 2 {
		t.Fatalf("FileCount = %d, want 2", metadata.FileCount)
	}
	if !metadata.Private {
		t.Fatal("Private = false, want true")
	}
	if metadata.Files[0].PathText != "release-name/file-a.txt" {
		t.Fatalf("first path = %q, want sorted utf-8 path", metadata.Files[0].PathText)
	}
}

func TestNormalizeMetadataSingleFile(t *testing.T) {
	data := []byte(dht.Encode(map[string]interface{}{
		"name":         "ubuntu-24.04-desktop-amd64.iso",
		"length":       4865957888,
		"piece length": 262144,
		"pieces":       "fake-hash-data",
	}))

	metadata, err := normalizeMetadata(data)
	if err != nil {
		t.Fatalf("normalizeMetadata() error = %v", err)
	}
	if metadata.FileCount != 1 {
		t.Fatalf("FileCount = %d, want 1", metadata.FileCount)
	}
	if len(metadata.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(metadata.Files))
	}
	if metadata.Files[0].PathText != "ubuntu-24.04-desktop-amd64.iso" {
		t.Fatalf("PathText = %q", metadata.Files[0].PathText)
	}
	if metadata.Files[0].Length != 4865957888 {
		t.Fatalf("File.Length = %d, want 4865957888", metadata.Files[0].Length)
	}
}

func TestNormalizeMetadataSkipsFilesWithoutUsablePaths(t *testing.T) {
	data := []byte(dht.Encode(map[string]interface{}{
		"files": []interface{}{
			map[string]interface{}{"length": 99},
			map[string]interface{}{
				"length": 7,
				"path":   []interface{}{"release-name", "file.txt"},
			},
		},
	}))

	metadata, err := normalizeMetadata(data)
	if err != nil {
		t.Fatalf("normalizeMetadata() error = %v", err)
	}
	if metadata.Name != "release-name" {
		t.Fatalf("Name = %q, want release-name", metadata.Name)
	}
	if metadata.Length != 7 {
		t.Fatalf("Length = %d, want 7", metadata.Length)
	}
	if metadata.FileCount != 1 {
		t.Fatalf("FileCount = %d, want 1", metadata.FileCount)
	}
}

func TestNormalizeMetadataRejectsOnlyFilesWithoutPaths(t *testing.T) {
	data := []byte(dht.Encode(map[string]interface{}{
		"files": []interface{}{
			map[string]interface{}{"length": 99},
		},
	}))

	if _, err := normalizeMetadata(data); err == nil {
		t.Fatal("normalizeMetadata() error = nil, want malformed metadata error")
	}
}

func TestAutoTuneControllerRequiresSustainedPressure(t *testing.T) {
	controller := &autoTuneController{}
	now := time.Unix(1_700_000_000, 0)
	pauseThreshold := uint64(100)
	resumeThreshold := uint64(70)

	for i := 0; i < autoTunePauseSamples-1; i++ {
		if action := controller.nextAction(now.Add(time.Duration(i)*time.Second), false, 101, pauseThreshold, resumeThreshold); action != autoTuneNoop {
			t.Fatalf("sample %d action = %v, want noop", i, action)
		}
	}
	if action := controller.nextAction(now.Add(autoTunePauseSamples*time.Second), false, 101, pauseThreshold, resumeThreshold); action != autoTunePause {
		t.Fatalf("pause action = %v, want pause", action)
	}
}

func TestAutoTuneControllerWaitsBeforeResume(t *testing.T) {
	controller := &autoTuneController{pausedAt: time.Unix(1_700_000_000, 0)}
	now := controller.pausedAt
	pauseThreshold := uint64(100)
	resumeThreshold := uint64(70)

	if action := controller.nextAction(now.Add(10*time.Second), true, 60, pauseThreshold, resumeThreshold); action != autoTuneNoop {
		t.Fatalf("early resume action = %v, want noop", action)
	}
	if action := controller.nextAction(now.Add(autoTuneMinPause), true, 60, pauseThreshold, resumeThreshold); action != autoTuneResume {
		t.Fatalf("resume action = %v, want resume", action)
	}
}

func TestDurableMetadataEndpointUpgradesLegacyBatchURL(t *testing.T) {
	tests := map[string]string{
		"https://storage.example/api/v1/torrents/batch":         "https://storage.example/api/v1/torrents/batch/durable",
		"https://storage.example/api/v1/torrents/batch/":        "https://storage.example/api/v1/torrents/batch/durable",
		"https://storage.example/api/v1/torrents/batch/durable": "https://storage.example/api/v1/torrents/batch/durable",
		"https://storage.example/custom-ingest":                 "https://storage.example/custom-ingest",
	}
	for input, want := range tests {
		if got := durableMetadataEndpoint(input); got != want {
			t.Errorf("durableMetadataEndpoint(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestCheckBatchUsesIndependentOracleEndpointAndKey(t *testing.T) {
	requests := make(chan *http.Request, 1)
	oracle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(r.Context())
		_ = json.NewEncoder(w).Encode([]string{"0123456789abcdef0123456789abcdef01234567"})
	}))
	defer oracle.Close()
	cfg := defaultTestConfig()
	cfg.Exporter.Kind = "http"
	cfg.Exporter.HTTPEndpoint = "https://storage.invalid/api/v1/torrents/batch"
	cfg.Exporter.APIKey = "storage-secret"
	cfg.Exporter.OracleEndpoint = oracle.URL + "/api/v1/oracle/observations"
	cfg.Exporter.OracleAPIKey = "oracle-secret"
	application := New(cfg, testLogger())
	application.checkBatchExists([]string{"0123456789abcdef0123456789abcdef01234567"})
	request := <-requests
	if request.URL.Path != "/api/v1/torrents/check" || request.Header.Get("X-API-Key") != "oracle-secret" {
		t.Fatalf("oracle request path=%s key=%q", request.URL.Path, request.Header.Get("X-API-Key"))
	}
	if !application.remoteKnown.Contains("0123456789abcdef0123456789abcdef01234567") {
		t.Fatal("oracle check result was not applied")
	}
}

func TestCheckBatchFallsBackToLegacyExporterEndpointAndKey(t *testing.T) {
	requests := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(r.Context())
		_ = json.NewEncoder(w).Encode([]string{})
	}))
	defer server.Close()
	cfg := defaultTestConfig()
	cfg.Exporter.HTTPEndpoint = server.URL + "/api/v1/torrents/batch"
	cfg.Exporter.APIKey = "legacy-secret"
	application := New(cfg, testLogger())
	application.checkBatchExists([]string{"0123456789abcdef0123456789abcdef01234567"})
	request := <-requests
	if request.URL.Path != "/api/v1/torrents/check" || request.Header.Get("X-API-Key") != "legacy-secret" {
		t.Fatalf("legacy request path=%s key=%q", request.URL.Path, request.Header.Get("X-API-Key"))
	}
}

func TestBuildStoragePolicyRejectsUnwritableConfiguredBudget(t *testing.T) {
	_, err := buildStoragePolicy(config.FilterConfig{SummaryAboveFiles: 10_001})
	if err == nil {
		t.Fatal("expected policy budget above the closed wire schema to fail at startup")
	}
}
