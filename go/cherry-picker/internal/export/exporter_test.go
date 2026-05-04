package export

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
