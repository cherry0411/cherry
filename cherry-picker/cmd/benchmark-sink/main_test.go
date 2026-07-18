package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testHash = "0123456789abcdef0123456789abcdef01234567"
const secondTestHash = "89abcdef0123456789abcdef0123456789abcdef"

func TestStorePersistsGlobalUniqueness(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashes.bin")
	s, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := parseHash(testHash)
	if !ok {
		t.Fatal("test hash did not parse")
	}
	accepted, duplicates, err := s.add(recordMetadata, []hashKey{key, key})
	if err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || duplicates != 1 {
		t.Fatalf("accepted=%d duplicates=%d", accepted, duplicates)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.close()
	accepted, duplicates, err = reloaded.add(recordMetadata, []hashKey{key})
	if err != nil {
		t.Fatal(err)
	}
	if accepted != 0 || duplicates != 1 {
		t.Fatalf("after reload accepted=%d duplicates=%d", accepted, duplicates)
	}
}

func TestBatchAndCheckEndpoints(t *testing.T) {
	s, err := openStore(filepath.Join(t.TempDir(), "hashes.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	body := `{"events":[{"type":"metadata_fetched","instance_id":"run-1","info_hash":"` + testHash + `","metadata":{"name":"ignored"}}]}`
	recorder := httptest.NewRecorder()
	s.handleBatch(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/torrents/batch", strings.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("batch status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var result struct {
		Accepted int `json:"accepted"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 1 {
		t.Fatalf("accepted=%d", result.Accepted)
	}

	recorder = httptest.NewRecorder()
	s.handleCheck(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/torrents/check", strings.NewReader(`["`+testHash+`"]`)))
	if !strings.Contains(recorder.Body.String(), testHash) {
		t.Fatalf("check response=%s", recorder.Body.String())
	}
	if got := s.checkHashes.Load(); got != 1 {
		t.Fatalf("check hashes=%d", got)
	}
	if got := s.checkFound.Load(); got != 1 {
		t.Fatalf("check found=%d", got)
	}
}

func TestStoreUsesReadOnlyBaselineAndWritableOverlay(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.bin")
	baseline, err := openStore(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	baselineKey, ok := parseHash(testHash)
	if !ok {
		t.Fatal("baseline hash did not parse")
	}
	if accepted, _, err := baseline.add(recordMetadata, []hashKey{baselineKey}); err != nil || accepted != 1 {
		t.Fatalf("seed baseline accepted=%d err=%v", accepted, err)
	}
	if err := baseline.close(); err != nil {
		t.Fatal(err)
	}

	overlayPath := filepath.Join(dir, "block-1.bin")
	overlay, err := openStoreWithBaseline(overlayPath, baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.close()
	if overlay.baselineMetadata != 1 || len(overlay.metadata) != 1 {
		t.Fatalf("baseline=%d total=%d", overlay.baselineMetadata, len(overlay.metadata))
	}
	if accepted, duplicates, err := overlay.add(recordMetadata, []hashKey{baselineKey}); err != nil || accepted != 0 || duplicates != 1 {
		t.Fatalf("baseline duplicate accepted=%d duplicates=%d err=%v", accepted, duplicates, err)
	}
	overlayKey, ok := parseHash(secondTestHash)
	if !ok {
		t.Fatal("overlay hash did not parse")
	}
	if accepted, duplicates, err := overlay.add(recordMetadata, []hashKey{overlayKey}); err != nil || accepted != 1 || duplicates != 0 {
		t.Fatalf("overlay accepted=%d duplicates=%d err=%v", accepted, duplicates, err)
	}

	recorder := httptest.NewRecorder()
	overlay.handleStats(recorder, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var stats map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats["baseline_metadata_unique"] != float64(1) || stats["overlay_metadata_unique"] != float64(1) || stats["metadata_unique"] != float64(2) {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	info, err := os.Stat(overlayPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != recordSize {
		t.Fatalf("overlay bytes=%d want=%d", info.Size(), recordSize)
	}
	baselineInfo, err := os.Stat(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if baselineInfo.Size() != recordSize {
		t.Fatalf("baseline mutated: bytes=%d want=%d", baselineInfo.Size(), recordSize)
	}
}

func TestStoreRejectsBaselineAsOverlay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oracle.bin")
	if _, err := openStoreWithBaseline(path, path); err == nil {
		t.Fatal("same baseline and overlay path should fail")
	}
}
