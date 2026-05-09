package app

import (
	"io"
	"log"
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
