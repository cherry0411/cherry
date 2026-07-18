package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cherry-picker/internal/spool"
)

const testAPIKey = "test-secret"

func newTestExporter(t *testing.T, url string) (*SpoolExporter, *spool.Spool) {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	exp, err := NewSpoolExporter(SpoolExporterOptions{
		Logger:       log.New(io.Discard, "", 0),
		Spool:        sp,
		URL:          url,
		APIKey:       testAPIKey,
		BatchSize:    4,
		RetryBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		sp.Close()
		t.Fatalf("new exporter: %v", err)
	}
	return exp, sp
}

func testRecord(name string) spool.Record {
	return spool.Record{
		InfoHash: "aabbccddeeff00112233445566778899aabbccdd",
		Encoding: spool.EncodingNormalized,
		Normalized: &spool.NormalizedMetadata{
			Name: name, TotalLength: 1,
			Files: []spool.File{{Path: name, Length: 1}},
		},
	}
}

func appendTestRecords(t *testing.T, sp *spool.Spool, count int) {
	t.Helper()
	records := make([]spool.Record, count)
	for i := range records {
		records[i] = testRecord("file")
	}
	if _, err := sp.AppendBatchDurable(records); err != nil {
		t.Fatalf("append durable records: %v", err)
	}
}

type rawBatchRequest struct {
	SchemaVersion int             `json:"schema_version"`
	CrawlerID     string          `json:"crawler_id"`
	Epoch         uint64          `json:"epoch"`
	StartSequence uint64          `json:"start_sequence"`
	EndSequence   uint64          `json:"end_sequence"`
	PayloadSHA256 string          `json:"payload_sha256"`
	Events        json.RawMessage `json:"events"`
}

func TestDeliverHashesExactEventsBytesAndCommits(t *testing.T) {
	var got rawBatchRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != testAPIKey {
			t.Errorf("missing API key")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(got.Events)
		want := hex.EncodeToString(sum[:])
		if got.PayloadSHA256 != want {
			t.Errorf("checksum=%s want exact raw events checksum=%s", got.PayloadSHA256, want)
		}
		writeACK(w, got, 2, 0)
	}))
	defer srv.Close()

	exp, sp := newTestExporter(t, srv.URL)
	defer sp.Close()
	appendTestRecords(t, sp, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runDeliverUntilDrained(t, exp, sp, ctx)
	if got.SchemaVersion != durableProtocolSchemaVersion || got.CrawlerID != "crawler-test" ||
		got.StartSequence != 1 || got.EndSequence != 2 {
		t.Fatalf("schema=%d identity=[%s %d-%d], want schema=2 [crawler-test 1-2]",
			got.SchemaVersion, got.CrawlerID, got.StartSequence, got.EndSequence)
	}
	_, acked, _, _, err := sp.CursorPosition()
	if err != nil || acked != 2 {
		t.Fatalf("cursor acked=%d err=%v, want 2", acked, err)
	}
}

func TestDeliverRetriesSameBatchAfter429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rawBatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeACK(w, req, 1, 0)
	}))
	defer srv.Close()
	exp, sp := newTestExporter(t, srv.URL)
	defer sp.Close()
	appendTestRecords(t, sp, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runDeliverUntilDrained(t, exp, sp, ctx)
	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want exactly 3", calls.Load())
	}
}

func TestDeliverStopsOn409WithoutAdvancing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(DurableBatchResponse{ExpectedStart: 99, Error: "gap"})
	}))
	defer srv.Close()
	exp, sp := newTestExporter(t, srv.URL)
	defer sp.Close()
	appendTestRecords(t, sp, 1)

	err := exp.Deliver(context.Background())
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Deliver err=%v, want ErrConflict", err)
	}
	_, acked, _, _, cursorErr := sp.CursorPosition()
	if cursorErr != nil || acked != 0 {
		t.Fatalf("acked=%d err=%v, want 0", acked, cursorErr)
	}
}

func TestDeliverRejectsInvalidACKs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*DurableBatchResponse)
	}{
		{"crawler", func(a *DurableBatchResponse) { a.CrawlerID = "other" }},
		{"checksum", func(a *DurableBatchResponse) { a.PayloadSHA256 = "deadbeef" }},
		{"not_committed", func(a *DurableBatchResponse) { a.Committed = false }},
		{"counts", func(a *DurableBatchResponse) { a.Accepted = 0 }},
		{"errors", func(a *DurableBatchResponse) { a.Errors = 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req rawBatchRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				ack := validACK(req, 1, 0)
				tt.mutate(&ack)
				_ = json.NewEncoder(w).Encode(ack)
			}))
			defer srv.Close()
			exp, sp := newTestExporter(t, srv.URL)
			defer sp.Close()
			appendTestRecords(t, sp, 1)
			err := exp.Deliver(context.Background())
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("Deliver err=%v, want ErrProtocol", err)
			}
			_, acked, _, _, _ := sp.CursorPosition()
			if acked != 0 {
				t.Fatalf("acked=%d, want 0", acked)
			}
		})
	}
}

func TestNewSpoolExporterRequiresAPIKey(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	if _, err := NewSpoolExporter(SpoolExporterOptions{Spool: sp, URL: "http://localhost"}); err == nil {
		t.Fatal("expected missing API key to fail closed")
	}
}

func TestNewSpoolExporterClampsBatchToProtocolLimit(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	exporter, err := NewSpoolExporter(SpoolExporterOptions{
		Spool: sp, URL: "http://localhost", APIKey: testAPIKey,
		BatchSize: durableProtocolMaxEvents + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exporter.batchSize != durableProtocolMaxEvents {
		t.Fatalf("batch size=%d, want protocol cap %d", exporter.batchSize, durableProtocolMaxEvents)
	}
}

func TestDurableEventUsesBodiesOnlyForSearchableMetadata(t *testing.T) {
	records := []spool.Record{
		testRecord("full.mkv"),
		{
			InfoHash: "11223344556677889900aabbccddeeff00112233",
			Encoding: spool.EncodingSummary,
			Summary: &spool.SummaryMetadata{
				Name: "large", TotalLength: 2, FileCount: 2,
				RepresentativeFiles: []spool.File{{Path: "sample.mp4", Length: 1}},
				Extensions:          []spool.ExtensionSummary{{Extension: "mp4", Files: 2, Bytes: 2}},
			},
		},
		{
			InfoHash:     "223344556677889900aabbccddeeff0011223344",
			Encoding:     spool.EncodingHashOnly,
			DecisionCode: spool.DecisionInvalidMetadata,
		},
		{
			InfoHash:     "3344556677889900aabbccddeeff001122334455",
			Encoding:     spool.EncodingReject,
			DecisionCode: spool.DecisionRejectFileCap,
		},
	}

	for index, record := range records {
		event := durableEventFromRecord(record)
		bodies := 0
		for _, present := range []bool{event.Normalized != nil, event.Summary != nil} {
			if present {
				bodies++
			}
		}
		wantBodies := 0
		if record.Encoding == spool.EncodingNormalized || record.Encoding == spool.EncodingSummary {
			wantBodies = 1
		}
		if event.Encoding != record.Encoding || event.DecisionCode != record.DecisionCode || bodies != wantBodies {
			t.Fatalf("event %d encoding=%q code=%d bodies=%d", index, event.Encoding, event.DecisionCode, bodies)
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"policy_id", "region", "piece_length", "reason", `"hash_only":`, `"reject":`} {
			if strings.Contains(string(encoded), forbidden) {
				t.Fatalf("event %d contains deleted field %q: %s", index, forbidden, encoded)
			}
		}
	}
}

func validACK(req rawBatchRequest, accepted, duplicates int) DurableBatchResponse {
	return DurableBatchResponse{
		CrawlerID: req.CrawlerID, Epoch: req.Epoch,
		StartSequence: req.StartSequence, EndSequence: req.EndSequence,
		PayloadSHA256: req.PayloadSHA256,
		Accepted:      accepted, Duplicates: duplicates, Committed: true,
	}
}

func writeACK(w http.ResponseWriter, req rawBatchRequest, accepted, duplicates int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(validACK(req, accepted, duplicates))
}

func runDeliverUntilDrained(t *testing.T, exp *SpoolExporter, sp *spool.Spool, ctx context.Context) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- exp.Deliver(ctx) }()
	for {
		_, acked, next, _, err := sp.CursorPosition()
		if err != nil {
			t.Fatalf("cursor: %v", err)
		}
		if next > 1 && acked == next-1 {
			return
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Deliver stopped early: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for drain: %v", ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
