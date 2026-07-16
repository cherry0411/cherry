package export

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cherry-picker/internal/pipeline"
)

func TestHTTPSinkRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.ReadAll(r.Body)
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := &httpSink{
		client:       &http.Client{Timeout: time.Second},
		url:          server.URL,
		retries:      3,
		retryBackoff: time.Millisecond,
	}

	err := sink.WriteBatch(context.Background(), []pipeline.Event{{Type: pipeline.EventPeerDiscovered}})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
}

type retryOnceSink struct {
	attempts  atomic.Int32
	delivered atomic.Int32
}

func (s *retryOnceSink) WriteBatch(_ context.Context, batch []pipeline.Event) error {
	if s.attempts.Add(1) == 1 {
		return errors.New("temporary failure")
	}
	s.delivered.Add(int32(len(batch)))
	return nil
}

func (s *retryOnceSink) Close(context.Context) error { return nil }

func TestBatchExporterRetainsBatchAfterSinkFailure(t *testing.T) {
	events := make(chan pipeline.Event, 1)
	sink := &retryOnceSink{}
	exporter := NewBatchExporter(log.New(io.Discard, "", 0), sink, 1, 5*time.Millisecond, events)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- exporter.Run(ctx) }()

	events <- pipeline.Event{Type: pipeline.EventMetadataFetched, InfoHash: "hash"}
	deadline := time.Now().Add(time.Second)
	for sink.delivered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := sink.attempts.Load(); got < 2 {
		t.Fatalf("attempts = %d, want at least 2", got)
	}
	if got := sink.delivered.Load(); got != 1 {
		t.Fatalf("delivered = %d, want 1", got)
	}
}

type recordingSink struct {
	delivered atomic.Int32
}

func (s *recordingSink) WriteBatch(_ context.Context, batch []pipeline.Event) error {
	s.delivered.Add(int32(len(batch)))
	return nil
}

func (s *recordingSink) Close(context.Context) error { return nil }

func TestWALReplaysEntryLargerThanScannerLimit(t *testing.T) {
	dir := t.TempDir()
	inner := &recordingSink{}
	w := &walSink{inner: inner, walDir: dir, done: make(chan struct{})}
	largePath := strings.Repeat("x", 9<<20)
	batch := []pipeline.Event{{
		Type: pipeline.EventMetadataFetched,
		Metadata: &pipeline.Metadata{Files: []pipeline.MetadataFile{{
			PathText: largePath,
		}}},
	}}
	if err := w.appendToWAL(batch); err != nil {
		t.Fatal(err)
	}
	w.replayAll()

	if got := inner.delivered.Load(); got != 1 {
		t.Fatalf("delivered = %d, want 1", got)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("WAL files remain after successful replay: %v", files)
	}
}

type failingSink struct{}

func (failingSink) WriteBatch(context.Context, []pipeline.Event) error {
	return errors.New("upstream unavailable")
}
func (failingSink) Close(context.Context) error { return nil }

func TestWALDoesNotAcknowledgeFallbackWriteFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "removed")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	w := &walSink{inner: failingSink{}, walDir: dir, done: make(chan struct{})}
	if err := w.WriteBatch(context.Background(), []pipeline.Event{{Type: pipeline.EventMetadataFetched}}); err == nil {
		t.Fatal("WriteBatch returned nil when both upstream and WAL failed")
	}
}
