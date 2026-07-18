package export

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cherry-picker/internal/spool"
)

func TestOracleObserverSendsOnlyHashAndTypedAction(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/oracle/observations" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.Header.Get("X-API-Key") != "oracle-secret" {
			t.Errorf("oracle key=%q", r.Header.Get("X-API-Key"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	observer, err := NewOracleObserver(OracleObserverOptions{
		Logger: ioLogger(), Endpoint: server.URL, APIKey: "oracle-secret",
		BatchSize: 4, FlushDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := spool.Record{
		InfoHash:   "0123456789abcdef0123456789abcdef01234567",
		Encoding:   spool.EncodingNormalized,
		Normalized: &spool.NormalizedMetadata{Name: "must-not-leak", Files: []spool.File{{Path: "secret/file", Length: 1}}},
	}
	if !observer.Submit(record) {
		t.Fatal("submit failed")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := observer.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(got)
	if string(encoded) != `{"observations":[{"action":"full","info_hash":"0123456789abcdef0123456789abcdef01234567"}]}` &&
		string(encoded) != `{"observations":[{"info_hash":"0123456789abcdef0123456789abcdef01234567","action":"full"}]}` {
		t.Fatalf("unexpected closed projection: %s", encoded)
	}
	if snapshot := observer.Snapshot(); snapshot.Sent != 1 || snapshot.Dropped != 0 || snapshot.HTTPFailures != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestOracleObserverFailureIsVisibleAndProductionSubmitNeverBlocks(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	observer, err := NewOracleObserver(OracleObserverOptions{
		Logger: ioLogger(), Endpoint: server.URL, Capacity: 1, BatchSize: 1,
		RetryBackoff: time.Hour, HTTPTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := spool.Record{InfoHash: "0123456789abcdef0123456789abcdef01234567", Encoding: spool.EncodingSummary}
	start := time.Now()
	if !observer.Submit(record) {
		t.Fatal("first submit failed")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("observer submit blocked production")
	}
	deadline := time.Now().Add(time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if requests.Load() == 0 {
		t.Fatal("observer never attempted HTTP")
	}
	observer.Submit(record)
	observer.Submit(record) // bounded queue must expose at least one drop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := observer.Close(ctx); err == nil {
		t.Fatal("close unexpectedly drained unavailable oracle")
	}
	snapshot := observer.Snapshot()
	if snapshot.HTTPFailures == 0 || snapshot.Dropped == 0 {
		t.Fatalf("failure evidence hidden: %+v", snapshot)
	}
}

func TestOracleObservationActionMappingIsClosed(t *testing.T) {
	tests := map[spool.Encoding]string{
		spool.EncodingNormalized: "full",
		spool.EncodingSummary:    "summary",
		spool.EncodingHashOnly:   "hash_only",
		spool.EncodingReject:     "reject",
	}
	for encoding, want := range tests {
		got, ok := oracleObservationFromRecord(spool.Record{
			InfoHash: "0123456789abcdef0123456789abcdef01234567", Encoding: encoding,
		})
		if !ok || got.Action != want {
			t.Fatalf("encoding=%s observation=%+v ok=%v", encoding, got, ok)
		}
	}
	if _, ok := oracleObservationFromRecord(spool.Record{InfoHash: "0123456789abcdef0123456789abcdef01234567", Encoding: "raw"}); ok {
		t.Fatal("unknown/raw encoding entered oracle protocol")
	}
}

func ioLogger() *log.Logger { return log.New(io.Discard, "", 0) }
