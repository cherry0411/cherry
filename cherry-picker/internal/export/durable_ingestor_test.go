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
