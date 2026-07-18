package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

const testHash = "0123456789abcdef0123456789abcdef01234567"

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
