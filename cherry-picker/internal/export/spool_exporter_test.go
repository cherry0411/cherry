package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestExporterReplaysPersistedPartialTailAfterCommitConnectionLossAndRestart(t *testing.T) {
	type received struct {
		req  rawBatchRequest
		body []byte
	}
	var (
		mu       sync.Mutex
		receipts []received
		last     rawBatchRequest
		first    = make(chan struct{}, 1)
	)
	eventCount := func(raw json.RawMessage) int {
		var events []json.RawMessage
		if err := json.Unmarshal(raw, &events); err != nil {
			t.Errorf("decode events: %v", err)
		}
		return len(events)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		var req rawBatchRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}

		mu.Lock()
		receipts = append(receipts, received{req: req, body: append([]byte(nil), body...)})
		call := len(receipts)
		if call == 1 {
			last = req // The authority commits before the connection is lost.
		}
		current := last
		mu.Unlock()

		if call == 1 {
			if req.StartSequence != 1 || req.EndSequence != 56 {
				t.Errorf("first range=%d..%d, want 1..56", req.StartSequence, req.EndSequence)
			}
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server response does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack committed response: %v", err)
				return
			}
			_ = conn.Close() // ACK is lost after the server-side commit.
			first <- struct{}{}
			return
		}

		if req.StartSequence == current.StartSequence && req.EndSequence == current.EndSequence &&
			req.PayloadSHA256 == current.PayloadSHA256 {
			writeACK(w, req, eventCount(req.Events), 0)
			return
		}
		if req.StartSequence != current.EndSequence+1 {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(DurableBatchResponse{ExpectedStart: current.EndSequence + 1})
			return
		}
		mu.Lock()
		last = req
		mu.Unlock()
		writeACK(w, req, eventCount(req.Events), 0)
	}))
	defer srv.Close()

	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 56)
	exp1, err := NewSpoolExporter(SpoolExporterOptions{
		Logger: log.New(io.Discard, "", 0), Spool: sp1, URL: srv.URL, APIKey: testAPIKey,
		BatchSize: 56, RetryBackoff: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- exp1.Deliver(ctx1) }()
	select {
	case <-first:
		cancel1()
	case <-time.After(2 * time.Second):
		t.Fatal("first committed request was not observed")
	}
	if err := <-done1; err != nil {
		t.Fatalf("first exporter stop: %v", err)
	}
	_, acked, _, _, err := sp1.CursorPosition()
	if err != nil || acked != 0 {
		t.Fatalf("local ACK advanced after lost response: acked=%d err=%v", acked, err)
	}
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}

	sp2, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatalf("reopen spool: %v", err)
	}
	defer sp2.Close()
	// Producer progress and a larger exporter batch must not widen the batch
	// which may already have committed remotely.
	appendTestRecords(t, sp2, 456)
	exp2, err := NewSpoolExporter(SpoolExporterOptions{
		Logger: log.New(io.Discard, "", 0), Spool: sp2, URL: srv.URL, APIKey: testAPIKey,
		BatchSize: 512, RetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	runDeliverUntilDrained(t, exp2, sp2, ctx2)

	mu.Lock()
	got := append([]received(nil), receipts...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("requests=%d, want lost-send, exact replay, suffix", len(got))
	}
	if got[1].req.StartSequence != 1 || got[1].req.EndSequence != 56 ||
		!bytes.Equal(got[0].body, got[1].body) {
		t.Fatalf("restart did not replay identical 1..56 body: first=%d..%d second=%d..%d",
			got[0].req.StartSequence, got[0].req.EndSequence,
			got[1].req.StartSequence, got[1].req.EndSequence)
	}
	if got[2].req.StartSequence != 57 || got[2].req.EndSequence != 512 {
		t.Fatalf("suffix range=%d..%d, want 57..512", got[2].req.StartSequence, got[2].req.EndSequence)
	}
}

func TestExportCheckpointSurvivesPreSendCrashProducerAppendAndBatchSizeChange(t *testing.T) {
	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 2)
	b1, err := sp1.NextBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := prepareDurableBatch(b1)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp1.EnsureExportCheckpoint(b1, durableProtocolSchemaVersion, p1.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 3)
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}

	sp2, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	b2, err := sp2.NextBatch(512)
	if err != nil {
		t.Fatal(err)
	}
	if b2.StartSeq != 1 || b2.EndSeq != 2 || len(b2.Records) != 2 {
		t.Fatalf("replayed range=%d..%d/%d, want 1..2/2", b2.StartSeq, b2.EndSeq, len(b2.Records))
	}
	p2, err := prepareDurableBatch(b2)
	if err != nil {
		t.Fatal(err)
	}
	if p2.payloadSHA256 != p1.payloadSHA256 {
		t.Fatalf("replayed digest=%s want %s", p2.payloadSHA256, p1.payloadSHA256)
	}
	if err := sp2.EnsureExportCheckpoint(b2, durableProtocolSchemaVersion, p2.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	if err := sp2.CommitBatch(b2); err != nil {
		t.Fatal(err)
	}
	if err := sp2.ClearExportCheckpoint(b2); err != nil {
		t.Fatal(err)
	}
	b3, err := sp2.NextBatch(512)
	if err != nil {
		t.Fatal(err)
	}
	if b3.StartSeq != 3 || b3.EndSeq != 5 {
		t.Fatalf("post-replay suffix=%d..%d, want 3..5", b3.StartSeq, b3.EndSeq)
	}
}

func TestInterruptedCheckpointTempIsNeverTreatedAsSendBarrier(t *testing.T) {
	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 2)
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "export-inflight.json.tmp")
	if err := os.WriteFile(tmp, []byte("interrupted-write"), 0o600); err != nil {
		t.Fatal(err)
	}

	sp2, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatalf("orphan temp must not imply a request was sent: %v", err)
	}
	defer sp2.Close()
	b, err := sp2.NextBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	p, err := prepareDurableBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp2.EnsureExportCheckpoint(b, durableProtocolSchemaVersion, p.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "export-inflight.json")); err != nil {
		t.Fatalf("durable checkpoint was not published: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("orphan temp was not replaced: %v", err)
	}
}

func TestCommittedCursorRecoversBeforeCheckpointCleanup(t *testing.T) {
	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 2)
	b, err := sp1.NextBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	p, err := prepareDurableBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp1.EnsureExportCheckpoint(b, durableProtocolSchemaVersion, p.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	if err := sp1.CommitBatch(b); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after the cursor fsync and before checkpoint deletion.
	if _, err := os.Stat(filepath.Join(dir, "export-inflight.json")); err != nil {
		t.Fatalf("checkpoint missing before simulated crash: %v", err)
	}
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}

	sp2, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	if _, err := os.Stat(filepath.Join(dir, "export-inflight.json")); !os.IsNotExist(err) {
		t.Fatalf("stale committed checkpoint was not removed: %v", err)
	}
	_, acked, _, _, err := sp2.CursorPosition()
	if err != nil || acked != 2 {
		t.Fatalf("recovered ack=%d err=%v, want 2", acked, err)
	}
}

func TestCorruptExportCheckpointFailsClosedWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 1)
	b, err := sp1.NextBatch(1)
	if err != nil {
		t.Fatal(err)
	}
	p, err := prepareDurableBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp1.EnsureExportCheckpoint(b, durableProtocolSchemaVersion, p.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "export-inflight.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append(append([]byte(nil), before...), byte('x'))
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1}); err == nil {
		reopened.Close()
		t.Fatal("corrupt checkpoint unexpectedly opened")
	} else if !errors.Is(err, spool.ErrCorruption) {
		t.Fatalf("open error=%v, want ErrCorruption", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed Open mutated corrupt checkpoint")
	}
}

func TestReplayDigestMismatchPoisonsBeforeNetwork(t *testing.T) {
	dir := t.TempDir()
	sp1, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, sp1, 1)
	b1, _ := sp1.NextBatch(1)
	p1, _ := prepareDurableBatch(b1)
	if err := sp1.EnsureExportCheckpoint(b1, durableProtocolSchemaVersion, p1.payloadSHA256); err != nil {
		t.Fatal(err)
	}
	if err := sp1.Close(); err != nil {
		t.Fatal(err)
	}

	sp2, err := spool.Open(spool.Options{Dir: dir, CrawlerID: "crawler-test", SyncEveryN: 1})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := sp2.NextBatch(512)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp2.EnsureExportCheckpoint(b2, durableProtocolSchemaVersion, strings.Repeat("0", 64)); !errors.Is(err, spool.ErrPoisoned) {
		t.Fatalf("digest mismatch error=%v, want ErrPoisoned", err)
	}
	_ = sp2.Close()
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
