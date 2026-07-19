package export

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cherry-picker/internal/spool"
)

func TestDurableIngestorSubmitReturnsAfterDurability(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler", SyncEveryN: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	w, err := NewDurableIngestor(DurableIngestorOptions{Spool: sp, BatchSize: 64, MaxDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	const count = 32
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- w.Submit(context.Background(), testRecord("grouped"))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, acked, next, durable, err := sp.CursorPosition()
	if err != nil {
		t.Fatal(err)
	}
	if acked != 0 || next != count+1 || durable != count {
		t.Fatalf("cursor acked=%d next=%d durable=%d", acked, next, durable)
	}
}

func TestDurableIngestorCancelledBeforeAdmission(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	w, err := NewDurableIngestor(DurableIngestorOptions{Spool: sp})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Submit(ctx, testRecord("cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit err=%v, want context.Canceled", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, next, durable, _ := sp.CursorPosition()
	if next != 1 || durable != 0 {
		t.Fatalf("cancelled record was appended: next=%d durable=%d", next, durable)
	}
}

func TestDurableIngestorAbortReleasesAdmittedSubmit(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	w, err := NewDurableIngestor(DurableIngestorOptions{
		Spool: sp, BatchSize: 128, MaxDelay: time.Second, CapacityRetry: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := testRecord("large")
	done := make(chan error, 1)
	go func() { done <- w.Submit(context.Background(), record) }()

	time.Sleep(20 * time.Millisecond)
	w.Abort(ErrConflict)
	select {
	case err := <-done:
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("Submit err=%v, want ErrConflict", err)
		}
	case <-time.After(time.Second):
		t.Fatal("capacity-blocked Submit did not unblock after Abort")
	}
	if err := w.Close(); !errors.Is(err, ErrConflict) {
		t.Fatalf("Close err=%v, want ErrConflict", err)
	}
}

func TestDurableIngestorWakeRechecksWithoutFlushingEarly(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	w, err := NewDurableIngestor(DurableIngestorOptions{
		Spool: sp, BatchSize: 4, MaxDelay: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 4)
	submit := func(name string) {
		go func() { results <- w.Submit(context.Background(), testRecord(name)) }()
	}
	submit("first")
	waitForIngestor(t, time.Second, func() bool { return w.Snapshot().Pending == 1 })
	submit("second")
	waitForIngestor(t, time.Second, func() bool { return w.Snapshot().Pending == 2 })

	// A second admission wakes the writer, but the oldest request still owns
	// the original deadline. Neither request may be flushed just because the
	// wake channel became readable.
	select {
	case err := <-results:
		t.Fatalf("Submit completed before batch/deadline: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if snapshot := w.Snapshot(); snapshot.Groups != 0 || snapshot.SuccessfulAppends != 0 {
		t.Fatalf("wake caused an early flush: %+v", snapshot)
	}

	submit("third")
	submit("fourth")
	for i := 0; i < 4; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("full group did not flush")
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := w.Snapshot()
	if snapshot.Groups != 1 || snapshot.RecordsGrouped != 4 || snapshot.LastGroupSize != 4 ||
		snapshot.MaxGroupSize != 4 || snapshot.BatchFullGroups != 1 || snapshot.TimerGroups != 0 ||
		snapshot.ShutdownGroups != 0 || snapshot.SuccessfulAppends != 1 {
		t.Fatalf("unexpected full-group snapshot: %+v", snapshot)
	}
	if snapshot.PendingPeak < 4 || snapshot.AppendAttempts != 1 || snapshot.AppendFsyncLast <= 0 ||
		snapshot.AppendFsyncTotal < snapshot.AppendFsyncLast || snapshot.AppendFsyncMax < snapshot.AppendFsyncLast {
		t.Fatalf("missing queue/append observability: %+v", snapshot)
	}
}

func TestDurableIngestorFlushesAtOldestRequestDeadline(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	const maxDelay = 100 * time.Millisecond
	w, err := NewDurableIngestor(DurableIngestorOptions{
		Spool: sp, BatchSize: 8, MaxDelay: maxDelay,
	})
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- w.Submit(context.Background(), testRecord("timer")) }()
	waitForIngestor(t, time.Second, func() bool { return w.Snapshot().Pending == 1 })
	select {
	case err := <-done:
		t.Fatalf("Submit completed well before MaxDelay: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timer group did not flush")
	}
	if elapsed := time.Since(started); elapsed < maxDelay/2 {
		t.Fatalf("timer group flushed too early: %v", elapsed)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := w.Snapshot()
	if snapshot.Groups != 1 || snapshot.TimerGroups != 1 || snapshot.LastGroupSize != 1 ||
		snapshot.SuccessfulAppends != 1 || snapshot.Pending != 0 {
		t.Fatalf("unexpected timer snapshot: %+v", snapshot)
	}
}

func TestDurableIngestorCloseFlushesAdmittedRequests(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "crawler"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	w, err := NewDurableIngestor(DurableIngestorOptions{
		Spool: sp, BatchSize: 8, MaxDelay: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- w.Submit(context.Background(), testRecord("shutdown")) }()
	}
	waitForIngestor(t, time.Second, func() bool { return w.Snapshot().Pending == 2 })
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not release admitted Submit")
		}
	}
	snapshot := w.Snapshot()
	if snapshot.Groups != 1 || snapshot.RecordsGrouped != 2 || snapshot.ShutdownGroups != 1 ||
		snapshot.BatchFullGroups != 0 || snapshot.TimerGroups != 0 || snapshot.SuccessfulAppends != 1 ||
		snapshot.Pending != 0 {
		t.Fatalf("unexpected shutdown snapshot: %+v", snapshot)
	}
	_, _, next, durable, err := sp.CursorPosition()
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 || durable != 2 {
		t.Fatalf("Close durability next=%d durable=%d, want 3/2", next, durable)
	}
}

func waitForIngestor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("timed out waiting for ingestor state")
	}
}
