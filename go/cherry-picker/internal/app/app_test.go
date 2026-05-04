package app

import (
	"testing"
	"time"

	dht "cherry-picker/internal/dht"
	"cherry-picker/internal/pipeline"
)

func TestSubmitInfohashEventKeepsDistinctSources(t *testing.T) {
	application := &Application{}
	events := make(chan pipeline.Event, 4)
	seen := newSeenSet(time.Minute)
	stats := &runtimeStats{}

	application.submitInfohashEvent(events, "abc", "1.1.1.1", 6881, "get_peers", seen, stats)
	application.submitInfohashEvent(events, "abc", "1.1.1.1", 6881, "get_peers_response", seen, stats)

	if got := len(events); got != 2 {
		t.Fatalf("len(events) = %d, want 2", got)
	}
	if got := stats.infohashEventsDeduped.Load(); got != 0 {
		t.Fatalf("infohash deduped = %d, want 0", got)
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
