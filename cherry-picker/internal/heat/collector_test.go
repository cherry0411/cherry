package heat

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type capturedDelivery struct {
	body   []byte
	header http.Header
}

func TestCollectorResponseLossReplaysIdenticalBodyAndReceipt(t *testing.T) {
	var mu sync.Mutex
	var deliveries []capturedDelivery
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		deliveries = append(deliveries, capturedDelivery{body: body, header: r.Header.Clone()})
		attempt := len(deliveries)
		mu.Unlock()
		if attempt == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("test server cannot simulate response loss")
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		writeAcceptedACK(t, w, r, body)
	}))
	defer server.Close()

	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "jp-crawler-01", SpoolDir: t.TempDir(),
		SpoolMaxBytes: 1 << 20, QueueCapacity: 32, BatchSize: 8,
		FlushDelay: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(bytes.Repeat([]byte{0x42}, 20))
	if !collector.Observe(hash, "8.8.8.8", time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)) {
		t.Fatal("observation not admitted")
	}
	waitFor(t, 3*time.Second, func() bool { return collector.Snapshot().Exported == 1 })
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(closeCtx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) != 2 {
		t.Fatalf("deliveries=%d want=2", len(deliveries))
	}
	if !bytes.Equal(deliveries[0].body, deliveries[1].body) {
		t.Fatal("response-loss replay changed CHHT body")
	}
	for _, delivery := range deliveries {
		if delivery.header.Get("X-API-Key") != "" || bytes.Contains(delivery.body, testMasterSecret) {
			t.Fatal("raw HMAC secret left the crawler process")
		}
	}
	for _, name := range []string{
		"X-CHHT-Crawler", "X-CHHT-Epoch", "X-CHHT-Sequence", "X-CHHT-End-Sequence",
		"X-CHHT-Payload-SHA256", "X-CHHT-Signature",
	} {
		if got, want := deliveries[1].header.Get(name), deliveries[0].header.Get(name); got == "" || got != want {
			t.Errorf("replayed %s=%q want exact %q", name, got, want)
		}
	}
	if _, err := strconv.ParseUint(deliveries[0].header.Get("X-CHHT-Epoch"), 10, 64); err != nil {
		t.Fatalf("epoch is not uint64 decimal: %v", err)
	}
	if _, err := DecodeWire(deliveries[0].body); err != nil {
		t.Fatalf("invalid grouped body: %v", err)
	}
}

func TestCollectorRejectsArbitrary2xxAndMismatchedHTTP200WithoutDeletingSpool(t *testing.T) {
	var attempts atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if attempt == 2 {
			response, err := responseReceipt(r, body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response["payloadSha256"] = strings.Repeat("0", 64)
			_ = json.NewEncoder(w).Encode(response)
			return
		}
		writeAcceptedACK(t, w, r, body)
	}))
	defer server.Close()

	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "crawler", SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20,
		QueueCapacity: 8, BatchSize: 8, FlushDelay: time.Millisecond, RetryBackoff: time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(bytes.Repeat([]byte{0x31}, 20))
	collector.Observe(hash, "8.8.8.8", time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC))
	waitFor(t, 3*time.Second, func() bool { return collector.Snapshot().Exported == 1 })
	snapshot := collector.Snapshot()
	if attempts.Load() < 3 || snapshot.ExportRetries < 2 || snapshot.SpoolRecords != 0 {
		t.Fatalf("mismatched 200 was not fail-closed: attempts=%d snapshot=%+v", attempts.Load(), snapshot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRejectsMismatchedDayClosedReceiptWithoutAdvancing(t *testing.T) {
	var attempts atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if attempts.Add(1) == 1 {
			response, err := responseReceipt(r, body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			delete(response, "received")
			delete(response, "inserted")
			delete(response, "replay")
			response["code"] = "day_closed"
			response["crawler"] = "wrong-crawler"
			response["error"] = "CHHT UTC day is closed"
			w.WriteHeader(http.StatusGone)
			_ = json.NewEncoder(w).Encode(response)
			return
		}
		writeAcceptedACK(t, w, r, body)
	}))
	defer server.Close()

	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "crawler", SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20,
		QueueCapacity: 8, BatchSize: 8, FlushDelay: time.Millisecond, RetryBackoff: time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	collector.Observe(string(bytes.Repeat([]byte{0x33}, 20)), "8.8.8.8",
		time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC))
	waitFor(t, 3*time.Second, func() bool { return collector.Snapshot().Exported == 1 })
	snapshot := collector.Snapshot()
	if attempts.Load() < 2 || snapshot.ExportRetries == 0 || snapshot.ClosedDayRejectedRecords != 0 {
		t.Fatalf("mismatched 410 did not fail closed: attempts=%d snapshot=%+v", attempts.Load(), snapshot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorNeverFollowsEndpointRedirects(t *testing.T) {
	var redirectedRequests atomic.Uint64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	collector, err := New(Options{
		Endpoint: redirector.URL, CrawlerID: "crawler", SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20,
		QueueCapacity: 8, BatchSize: 8, FlushDelay: time.Millisecond, RetryBackoff: time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	collector.Observe(string(bytes.Repeat([]byte{0x34}, 20)), "8.8.8.8",
		time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC))
	waitFor(t, time.Second, func() bool {
		snapshot := collector.Snapshot()
		return snapshot.Durable == 1 && snapshot.ExportRetries > 0
	})
	if redirectedRequests.Load() != 0 || collector.Snapshot().SpoolRecords != 1 {
		t.Fatalf("redirect escaped configured endpoint: requests=%d snapshot=%+v",
			redirectedRequests.Load(), collector.Snapshot())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorAdvancesOnlyStrictDayClosedNegativeReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		response, err := responseReceipt(r, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		delete(response, "received")
		delete(response, "inserted")
		delete(response, "replay")
		response["code"] = "day_closed"
		response["error"] = "CHHT UTC day is closed"
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "crawler", SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20,
		QueueCapacity: 8, BatchSize: 8, FlushDelay: time.Millisecond, RetryBackoff: time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(bytes.Repeat([]byte{0x32}, 20))
	collector.Observe(hash, "8.8.8.8", time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC))
	waitFor(t, 3*time.Second, func() bool {
		return collector.Snapshot().ClosedDayRejectedRecords == 1
	})
	snapshot := collector.Snapshot()
	if snapshot.ClosedDayRejectedBatches != 1 || snapshot.Exported != 0 || snapshot.SpoolRecords != 0 {
		t.Fatalf("unexpected negative-receipt accounting: %+v", snapshot)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSpoolRestartReconstructsIdenticalBodyAndReceipt(t *testing.T) {
	dir := t.TempDir()
	sp, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20, segmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	rows := []Observation{
		spoolObservation(2), spoolObservation(0), spoolObservation(1), spoolObservation(1),
	}
	if err := sp.appendDurable(rows); err != nil {
		t.Fatal(err)
	}
	before, err := sp.readBatch(8)
	if err != nil {
		t.Fatal(err)
	}
	beforeWire, err := BuildWireBatch(
		before.Observations[0].Day, before.Observations[0].Hour, before.Observations)
	if err != nil {
		t.Fatal(err)
	}
	beforeBody, err := EncodeWire(beforeWire)
	if err != nil {
		t.Fatal(err)
	}
	beforeReceipt := buildDeliveryReceipt("jp-crawler-01", string(testMasterSecret), before, beforeBody)
	if err := sp.close(); err != nil {
		t.Fatal(err)
	}

	sp, err = openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20, segmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	after, err := sp.readBatch(8)
	if err != nil {
		t.Fatal(err)
	}
	afterWire, err := BuildWireBatch(
		after.Observations[0].Day, after.Observations[0].Hour, after.Observations)
	if err != nil {
		t.Fatal(err)
	}
	afterBody, err := EncodeWire(afterWire)
	if err != nil {
		t.Fatal(err)
	}
	afterReceipt := buildDeliveryReceipt("jp-crawler-01", string(testMasterSecret), after, afterBody)

	if before.Epoch != after.Epoch || before.StartSequence != after.StartSequence ||
		before.EndSequence != after.EndSequence {
		t.Fatalf("receipt range changed across restart: before=%+v after=%+v", before, after)
	}
	if !bytes.Equal(beforeBody, afterBody) || beforeReceipt != afterReceipt {
		t.Fatalf("delivery changed across restart:\nbefore=%+v\nafter=%+v", beforeReceipt, afterReceipt)
	}
}

func TestCollectorNeverPersistsRawIPAndExposesEndpointFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	dir := t.TempDir()
	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "crawler", SpoolDir: dir, SpoolMaxBytes: 1 << 20,
		QueueCapacity: 32, BatchSize: 8, FlushDelay: time.Millisecond, RetryBackoff: 10 * time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(bytes.Repeat([]byte{0x7f}, 20))
	if !collector.Observe(hash, "8.8.8.8", time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)) {
		t.Fatal("observation not admitted")
	}
	waitFor(t, time.Second, func() bool {
		snapshot := collector.Snapshot()
		return snapshot.Durable == 1 && snapshot.ExportRetries > 0
	})
	paths, err := filepath.Glob(filepath.Join(dir, "heat-*.spool"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("spool paths=%v err=%v", paths, err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("8.8.8.8")) {
			t.Fatal("raw IP leaked to spool")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorQueueAndCapacityDropsAreExplicitAndNonBlocking(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "crawler", SpoolDir: t.TempDir(), SpoolMaxBytes: 512,
		QueueCapacity: 4, BatchSize: 4, FlushDelay: time.Millisecond, RetryBackoff: 100 * time.Millisecond,
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	active := collector.activeDay.Load()
	if !collector.advanceTo(active + 1) {
		t.Fatal("could not establish clean test day")
	}
	start := time.Now()
	for idx := 0; idx < 20_000; idx++ {
		var hash [20]byte
		hash[0] = byte(idx >> 8)
		hash[1] = byte(idx)
		collector.Observe(string(hash[:]), "8.8.8.8", time.Unix(int64(active+1)*86_400+3600, 0).UTC())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("hot-path admission blocked for %v", elapsed)
	}
	waitFor(t, time.Second, func() bool { return collector.Snapshot().QueueDropped > 0 })
	snapshot := collector.Snapshot()
	if snapshot.QueueCapacity != 4 || snapshot.QueueDropped == 0 {
		t.Fatalf("drop metrics=%+v", snapshot)
	}
	collector.completion.mu.Lock()
	dirty := collector.completion.data.Days[dayKey(active+1)].Dirty
	collector.completion.mu.Unlock()
	if !dirty {
		t.Fatal("queue drop did not poison the UTC completion state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = collector.Close(ctx)
}

func TestCompletionGoldenSignature(t *testing.T) {
	request := completionRequest{
		Crawler: "jp-crawler-01", Day: "2026-07-20", Epoch: 72_623_859_790_382_856,
		StartSequence: 100, NextSequence: 104, Clean: true,
	}
	const want = "db60e250cfc7b952ab946e3ff9f36615fb8915d9d9420f38aee383ed941ec4ae"
	if got := signCompletion(request, string(testMasterSecret)); got != want {
		t.Fatalf("completion signature=%s want=%s", got, want)
	}
}

func TestCollectorCompletesOnlyFullLosslessDayAfterCrossDaySpoolDrain(t *testing.T) {
	var completions atomic.Uint64
	var captured http.Header
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/completions") {
			mu.Lock()
			captured = r.Header.Clone()
			mu.Unlock()
			completions.Add(1)
			writeCompletionACK(t, w, r, false)
			return
		}
		body, _ := io.ReadAll(r.Body)
		writeAcceptedACK(t, w, r, body)
	}))
	defer server.Close()
	day := uint32(20_654)
	now := time.Unix(int64(day)*86_400+3600, 0).UTC()
	collector, err := New(Options{
		Endpoint: server.URL + "/api/v1/heat/batches", CrawlerID: "jp-crawler-01",
		SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20, QueueCapacity: 16, BatchSize: 8,
		FlushDelay: time.Millisecond, RetryBackoff: time.Millisecond, Now: func() time.Time { return now },
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := string(bytes.Repeat([]byte{0x51}, 20))
	if !collector.Observe(hash, "8.8.8.8", now) {
		t.Fatal("initial observation not admitted")
	}
	waitFor(t, time.Second, func() bool { return collector.Snapshot().Exported == 1 })
	if !collector.advanceTo(day + 1) {
		t.Fatal("first boundary failed")
	}
	if !collector.Observe(hash, "8.8.4.4", now.Add(24*time.Hour)) {
		t.Fatal("full-day observation not admitted")
	}
	waitFor(t, time.Second, func() bool { return collector.Snapshot().Exported == 2 })
	if !collector.advanceTo(day + 2) {
		t.Fatal("second boundary failed")
	}
	waitFor(t, time.Second, func() bool { return completions.Load() == 1 })
	mu.Lock()
	header := captured
	mu.Unlock()
	if got, want := header.Get("X-CHHT-Day"), formatObservationDay(day+1); got != want {
		t.Fatalf("completed day=%q want=%q", got, want)
	}
	if header.Get("X-CHHT-Clean") != "1" || header.Get("X-CHHT-Start-Sequence") == "" ||
		header.Get("X-CHHT-Next-Sequence") == "" {
		t.Fatalf("incomplete completion identity: %v", header)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionResponseLossReplaysIdenticalIdentity(t *testing.T) {
	var mu sync.Mutex
	var deliveries []http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/completions") {
			http.Error(w, "unexpected batch", http.StatusBadRequest)
			return
		}
		mu.Lock()
		deliveries = append(deliveries, r.Header.Clone())
		attempt := len(deliveries)
		mu.Unlock()
		if attempt == 1 {
			hijacker := w.(http.Hijacker)
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		writeCompletionACK(t, w, r, true)
	}))
	defer server.Close()
	day := uint32(20_654)
	now := time.Unix(int64(day)*86_400+3600, 0).UTC()
	collector, err := New(Options{
		Endpoint: server.URL + "/api/v1/heat/batches", CrawlerID: "jp-crawler-01",
		SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20, QueueCapacity: 8, BatchSize: 8,
		RetryBackoff: time.Millisecond, Now: func() time.Time { return now },
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !collector.advanceTo(day+1) || !collector.advanceTo(day+2) {
		t.Fatal("completion boundary failed")
	}
	waitFor(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveries) == 2
	})
	mu.Lock()
	first, second := deliveries[0], deliveries[1]
	mu.Unlock()
	for _, name := range []string{
		"X-CHHT-Crawler", "X-CHHT-Day", "X-CHHT-Epoch", "X-CHHT-Start-Sequence",
		"X-CHHT-Next-Sequence", "X-CHHT-Clean", "X-CHHT-Signature",
	} {
		if first.Get(name) == "" || first.Get(name) != second.Get(name) {
			t.Fatalf("completion replay changed %s: %q != %q", name, first.Get(name), second.Get(name))
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTooEarlyBatchNeverAdvancesSpoolUntilAuthenticatedACK(t *testing.T) {
	var allow atomic.Bool
	var attempts atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		attempts.Add(1)
		if !allow.Load() {
			w.WriteHeader(http.StatusTooEarly)
			_, _ = w.Write([]byte(`{"code":"clock_skew","retryable":true}`))
			return
		}
		writeAcceptedACK(t, w, r, body)
	}))
	defer server.Close()
	day := uint32(20_654)
	now := time.Unix(int64(day)*86_400+3600, 0).UTC()
	collector, err := New(Options{
		Endpoint: server.URL + "/api/v1/heat/batches", CrawlerID: "jp-crawler-01",
		SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20, QueueCapacity: 8, BatchSize: 8,
		RetryBackoff: 5 * time.Millisecond, Now: func() time.Time { return now },
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !collector.Observe(string(bytes.Repeat([]byte{0x61}, 20)), "8.8.8.8", now) {
		t.Fatal("observation not admitted")
	}
	waitFor(t, time.Second, func() bool { return attempts.Load() >= 2 })
	blocked := collector.Snapshot()
	if blocked.SpoolRecords != 1 || blocked.Exported != 0 {
		t.Fatalf("425 advanced durable head: %+v", blocked)
	}
	allow.Store(true)
	waitFor(t, time.Second, func() bool { return collector.Snapshot().Exported == 1 })
	if got := collector.Snapshot().SpoolRecords; got != 0 {
		t.Fatalf("ACK did not advance spool: records=%d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTooEarlyCompletionRemainsPendingUntilStorageDayCloses(t *testing.T) {
	var allow atomic.Bool
	var attempts atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/completions") {
			http.Error(w, "unexpected batch", http.StatusBadRequest)
			return
		}
		attempts.Add(1)
		if !allow.Load() {
			w.WriteHeader(http.StatusTooEarly)
			_, _ = w.Write([]byte(`{"code":"clock_skew","retryable":true}`))
			return
		}
		writeCompletionACK(t, w, r, false)
	}))
	defer server.Close()
	day := uint32(20_654)
	now := time.Unix(int64(day)*86_400+3600, 0).UTC()
	collector, err := New(Options{
		Endpoint: server.URL + "/api/v1/heat/batches", CrawlerID: "jp-crawler-01",
		SpoolDir: t.TempDir(), SpoolMaxBytes: 1 << 20, QueueCapacity: 8, BatchSize: 8,
		RetryBackoff: 5 * time.Millisecond, Now: func() time.Time { return now },
		MasterSecret: testMasterSecret, HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The startup day is conservatively dirty. The following zero-event day
	// is nevertheless a valid closure and must remain pending across 425s.
	if !collector.advanceTo(day+1) || !collector.advanceTo(day+2) {
		t.Fatal("completion boundary failed")
	}
	waitFor(t, time.Second, func() bool { return attempts.Load() >= 2 })
	_, _, cursor, err := collector.spool.sequenceState()
	if err != nil {
		t.Fatal(err)
	}
	if pending := collector.completion.ready(cursor); len(pending) != 1 {
		t.Fatalf("425 consumed completion: pending=%d", len(pending))
	}
	allow.Store(true)
	waitFor(t, time.Second, func() bool { return len(collector.completion.ready(cursor)) == 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionStateCrashAndGracefulRestartBothPoisonActiveDay(t *testing.T) {
	dir := t.TempDir()
	spool, err := openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	epoch, next, _, _ := spool.sequenceState()
	tracker, err := openCompletionTracker(dir, "crawler", epoch, 20_654, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.closeThrough(20_654, 20_655, next, true); err != nil {
		t.Fatal(err)
	}
	// Simulate an abnormal process end: release only the spool resources and
	// leave completion.running=true.
	if err := spool.close(); err != nil {
		t.Fatal(err)
	}
	spool, err = openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	epoch, next, _, _ = spool.sequenceState()
	crashRecovered, err := openCompletionTracker(dir, "crawler", epoch, 20_655, next)
	if err != nil {
		t.Fatal(err)
	}
	crashRecovered.mu.Lock()
	crashDirty := crashRecovered.data.Days[dayKey(20_655)].Dirty
	crashRecovered.mu.Unlock()
	if !crashDirty {
		t.Fatal("abnormal restart left active day clean")
	}
	if err := crashRecovered.closeThrough(20_655, 20_656, next, true); err != nil {
		t.Fatal(err)
	}
	if err := crashRecovered.stopDirty(); err != nil {
		t.Fatal(err)
	}
	if err := spool.close(); err != nil {
		t.Fatal(err)
	}
	spool, err = openHeatSpool(spoolOptions{dir: dir, maxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	epoch, next, _, _ = spool.sequenceState()
	gracefulRecovered, err := openCompletionTracker(dir, "crawler", epoch, 20_656, next)
	if err != nil {
		t.Fatal(err)
	}
	gracefulRecovered.mu.Lock()
	gracefulDirty := gracefulRecovered.data.Days[dayKey(20_656)].Dirty
	gracefulRecovered.mu.Unlock()
	if !gracefulDirty {
		t.Fatal("graceful same-day restart left active day clean")
	}
}

func TestNewRejectsShortSigningSecretAndInsecureRemoteEndpoint(t *testing.T) {
	base := Options{
		Endpoint: "http://127.0.0.1/heat", CrawlerID: "crawler", SpoolDir: t.TempDir(),
		MasterSecret: testMasterSecret, HMACSecret: []byte("short"), LocalAddresses: []netip.Addr{},
	}
	if collector, err := New(base); err == nil {
		collector.Close(context.Background())
		t.Fatal("short HMAC secret accepted")
	}
	base.HMACSecret = testMasterSecret
	base.Endpoint = "http://storage.example/heat"
	if collector, err := New(base); err == nil {
		collector.Close(context.Background())
		t.Fatal("insecure remote endpoint accepted")
	}
	base.Endpoint = "http://127.0.0.1/heat"
	base.CrawlerID = strings.Repeat("x", 65)
	if collector, err := New(base); err == nil {
		collector.Close(context.Background())
		t.Fatal("crawler ID longer than backend's 64-byte limit accepted")
	}
}

func TestGoldenDeliveryVector(t *testing.T) {
	var a, b [20]byte
	for idx := range a {
		a[idx] = byte(idx)
		b[idx] = byte(0xff - idx)
	}
	rows := []Observation{
		{Day: 20_654, Hour: 12, InfoHash: b, Actor: 42},
		{Day: 20_654, Hour: 12, InfoHash: a, Actor: 2},
		{Day: 20_654, Hour: 12, InfoHash: a, Actor: 1},
		{Day: 20_654, Hour: 12, InfoHash: a, Actor: 1},
	}
	wireBatch, err := BuildWireBatch(20_654, 12, rows)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeWire(wireBatch)
	if err != nil {
		t.Fatal(err)
	}
	receipt := buildDeliveryReceipt("jp-crawler-01", string(testMasterSecret), spoolBatch{
		Epoch: 0x0102030405060708, StartSequence: 100, EndSequence: 103,
	}, payload)
	const wantPayload = "4348485402000050ae0c02000102030405060708090a0b0c0d0e0f101112130200000000000000010000000000000002fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedec01000000000000002a"
	const wantDigest = "f9677f7b639d9e92f4a4d1710ca5aec8da298618ba8c5c27ae0274bb549f1369"
	const wantSignature = "f412c893652e24aa3999b4d42db2417764b17fc737a45675a502c0c8c8700a7e"
	if got := hex.EncodeToString(payload); got != wantPayload ||
		receipt.PayloadSHA256 != wantDigest || receipt.Signature != wantSignature {
		t.Fatalf("golden vector drift:\npayload=%s\ndigest=%s\nsignature=%s", got, receipt.PayloadSHA256, receipt.Signature)
	}
	var fixture struct {
		PayloadHex string `json:"payload_hex"`
		Digest     string `json:"payload_sha256_lower_hex"`
		Signature  string `json:"signature_hmac_sha256_lower_hex"`
	}
	fixtureBytes, err := os.ReadFile(filepath.Join("testdata", "chht_v2_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.PayloadHex != wantPayload || fixture.Digest != wantDigest || fixture.Signature != wantSignature {
		t.Fatalf("interop JSON fixture drifted: %+v", fixture)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

type testReporter interface {
	Helper()
	Errorf(string, ...any)
}

func writeAcceptedACK(t testReporter, w http.ResponseWriter, r *http.Request, body []byte) {
	t.Helper()
	response, err := responseReceipt(r, body)
	if err != nil {
		t.Errorf("construct ACK: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Errorf("encode ACK: %v", err)
	}
}

func writeCompletionACK(t testReporter, w http.ResponseWriter, r *http.Request, replay bool) {
	t.Helper()
	epoch, err := strconv.ParseUint(r.Header.Get("X-CHHT-Epoch"), 10, 64)
	if err != nil {
		t.Errorf("parse completion epoch: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	start, err := strconv.ParseUint(r.Header.Get("X-CHHT-Start-Sequence"), 10, 64)
	if err != nil {
		t.Errorf("parse completion start: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	next, err := strconv.ParseUint(r.Header.Get("X-CHHT-Next-Sequence"), 10, 64)
	if err != nil {
		t.Errorf("parse completion next: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"crawler": r.Header.Get("X-CHHT-Crawler"), "day": r.Header.Get("X-CHHT-Day"),
		"epoch": epoch, "startSequence": start, "nextSequence": next,
		"clean": true, "replay": replay, "code": nil, "error": nil,
	}); err != nil {
		t.Errorf("encode completion ACK: %v", err)
	}
}

func responseReceipt(r *http.Request, body []byte) (map[string]any, error) {
	wire, err := DecodeWire(body)
	if err != nil {
		return nil, err
	}
	epoch, err := strconv.ParseUint(r.Header.Get("X-CHHT-Epoch"), 10, 64)
	if err != nil {
		return nil, err
	}
	start, err := strconv.ParseUint(r.Header.Get("X-CHHT-Sequence"), 10, 64)
	if err != nil {
		return nil, err
	}
	end, err := strconv.ParseUint(r.Header.Get("X-CHHT-End-Sequence"), 10, 64)
	if err != nil || end == ^uint64(0) {
		return nil, errors.New("invalid end sequence")
	}
	received := 0
	for _, group := range wire.Groups {
		received += len(group.Actors)
	}
	return map[string]any{
		"code": nil, "crawler": r.Header.Get("X-CHHT-Crawler"),
		"day":   time.Unix(int64(wire.Day)*86_400, 0).UTC().Format(time.DateOnly),
		"epoch": epoch, "startSequence": start, "endSequence": end,
		"payloadSha256": r.Header.Get("X-CHHT-Payload-SHA256"),
		"received":      received, "inserted": received, "nextSequence": end + 1,
		"replay": false, "error": nil,
	}, nil
}

func BenchmarkCollectorObserveAdmission(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		writeAcceptedACK(b, w, r, body)
	}))
	defer server.Close()
	collector, err := New(Options{
		Endpoint: server.URL, CrawlerID: "benchmark", SpoolDir: b.TempDir(), SpoolMaxBytes: 64 << 20,
		QueueCapacity: 1 << 20, BatchSize: 4096, MasterSecret: testMasterSecret,
		HMACSecret: testMasterSecret, LocalAddresses: []netip.Addr{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer collector.Close(context.Background())
	hash := string(bytes.Repeat([]byte{1}, 20))
	now := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector.Observe(hash, "8.8.8.8", now)
	}
}
