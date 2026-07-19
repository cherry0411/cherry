package export

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cherry-picker/internal/spool"
)

// DurableIngestor turns concurrent metadata submissions into short group
// commits. A successful Submit means the record is on stable storage; it does
// not mean the backend has received it yet.
//
// Requests are admitted under a mutex and, once admitted, always receive a
// durability result even if their caller's context is subsequently cancelled.
// This keeps cancellation from creating an ambiguous "maybe durable" result.
type DurableIngestor struct {
	spool      *spool.Spool
	batchSize  int
	maxDelay   time.Duration
	retryDelay time.Duration

	mu       sync.Mutex
	pending  []*durableRequest
	stats    DurableIngestorSnapshot
	closed   bool
	fatalErr error
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

type durableRequest struct {
	record     spool.Record
	result     chan error
	admittedAt time.Time
}

// DurableIngestorSnapshot exposes the actual group-commit shape and cost. The
// append duration includes record encoding, writes and every fsync performed by
// Spool.AppendBatchDurable; successful calls are the definitive durability
// boundary observed by Submit.
type DurableIngestorSnapshot struct {
	Pending           int
	PendingPeak       int
	Groups            uint64
	RecordsGrouped    uint64
	LastGroupSize     int
	MaxGroupSize      int
	BatchFullGroups   uint64
	TimerGroups       uint64
	ShutdownGroups    uint64
	AppendAttempts    uint64
	SuccessfulAppends uint64
	CapacityRetries   uint64
	AppendErrors      uint64
	AppendFsyncTotal  time.Duration
	AppendFsyncLast   time.Duration
	AppendFsyncMax    time.Duration
}

type durableFlushReason uint8

const (
	durableFlushBatchFull durableFlushReason = iota
	durableFlushTimer
	durableFlushShutdown
)

type DurableIngestorOptions struct {
	Spool         *spool.Spool
	BatchSize     int
	MaxDelay      time.Duration
	CapacityRetry time.Duration
}

func NewDurableIngestor(opts DurableIngestorOptions) (*DurableIngestor, error) {
	if opts.Spool == nil {
		return nil, errors.New("export: durable ingestor requires a spool")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 128
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 25 * time.Millisecond
	}
	if opts.CapacityRetry <= 0 {
		opts.CapacityRetry = 100 * time.Millisecond
	}
	w := &DurableIngestor{
		spool:      opts.Spool,
		batchSize:  opts.BatchSize,
		maxDelay:   opts.MaxDelay,
		retryDelay: opts.CapacityRetry,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// Submit admits one typed, zero-raw record and waits for its group fsync. The
// context can cancel admission, but after admission the call waits for the
// definitive durable/fatal result to avoid ambiguity.
func (w *DurableIngestor) Submit(ctx context.Context, record spool.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	req := &durableRequest{record: record, result: make(chan error, 1), admittedAt: time.Now()}

	w.mu.Lock()
	if w.closed {
		err := w.fatalErr
		if err == nil {
			err = errors.New("export: durable ingestor closed")
		}
		w.mu.Unlock()
		return err
	}
	w.pending = append(w.pending, req)
	if len(w.pending) > w.stats.PendingPeak {
		w.stats.PendingPeak = len(w.pending)
	}
	w.signalLocked()
	w.mu.Unlock()

	return <-req.result
}

// Close stops admission, durably flushes every already-admitted request, and
// waits for the writer. The backend delivery loop should remain alive until
// this returns so it can free spool capacity during graceful shutdown.
func (w *DurableIngestor) Close() error {
	w.mu.Lock()
	w.closed = true
	w.stopOnce.Do(func() { close(w.stop) })
	w.signalLocked()
	w.mu.Unlock()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fatalErr
}

// Abort is used for a terminal delivery conflict or another condition that
// makes forward progress impossible. Already durable spool records remain for
// operator recovery; admitted but not-yet-durable requests receive cause.
func (w *DurableIngestor) Abort(cause error) {
	if cause == nil {
		cause = errors.New("export: durable ingestor aborted")
	}
	w.mu.Lock()
	if w.fatalErr == nil {
		w.fatalErr = cause
	}
	w.closed = true
	w.stopOnce.Do(func() { close(w.stop) })
	w.signalLocked()
	w.mu.Unlock()
}

func (w *DurableIngestor) signalLocked() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Snapshot returns a concurrency-safe point-in-time copy of the group writer
// statistics. Counters are process-lifetime values.
func (w *DurableIngestor) Snapshot() DurableIngestorSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	snapshot := w.stats
	snapshot.Pending = len(w.pending)
	return snapshot
}

func (w *DurableIngestor) run() {
	defer close(w.done)
	stopping := false
	for {
		reason := durableFlushTimer
		for {
			pending, oldest := w.pendingState()
			if stopping {
				if pending == 0 {
					return
				}
				reason = durableFlushShutdown
				break
			}
			if pending >= w.batchSize {
				reason = durableFlushBatchFull
				break
			}
			if pending == 0 {
				select {
				case <-w.wake:
				case <-w.stop:
					stopping = true
				}
				continue
			}

			remaining := time.Until(oldest.Add(w.maxDelay))
			if remaining <= 0 {
				reason = durableFlushTimer
				break
			}
			timer := time.NewTimer(remaining)
			select {
			case <-timer.C:
				reason = durableFlushTimer
				break
			case <-w.wake:
				stopAndDrainTimer(timer)
				// A wake only announces changed state. Re-check the pending
				// count and the original oldest-request deadline; it must not
				// turn every concurrent Submit into an early one-record fsync.
				continue
			case <-w.stop:
				stopAndDrainTimer(timer)
				stopping = true
				continue
			}
			break
		}

		batch := w.takeBatch()
		if len(batch) == 0 {
			if stopping {
				return
			}
			continue
		}

		records := make([]spool.Record, len(batch))
		for i := range batch {
			records[i] = batch[i].record
		}
		w.recordGroup(reason, len(batch))
		for {
			if fatal := w.currentFatal(); fatal != nil {
				completeDurableRequests(batch, fatal)
				w.failPending(fatal)
				return
			}
			started := time.Now()
			_, err := w.spool.AppendBatchDurable(records)
			w.recordAppend(time.Since(started), err)
			if err == nil {
				completeDurableRequests(batch, nil)
				break
			}
			if errors.Is(err, spool.ErrAtCapacity) {
				if stopping {
					time.Sleep(w.retryDelay)
					continue
				}
				timer := time.NewTimer(w.retryDelay)
				select {
				case <-timer.C:
				case <-w.stop:
					stopAndDrainTimer(timer)
					stopping = true
				}
				continue
			}

			fatal := fmt.Errorf("export: durable append failed: %w", err)
			completeDurableRequests(batch, fatal)
			w.failPending(fatal)
			return
		}
	}
}

func (w *DurableIngestor) pendingState() (int, time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return 0, time.Time{}
	}
	return len(w.pending), w.pending[0].admittedAt
}

func (w *DurableIngestor) recordGroup(reason durableFlushReason, size int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stats.Groups++
	w.stats.RecordsGrouped += uint64(size)
	w.stats.LastGroupSize = size
	if size > w.stats.MaxGroupSize {
		w.stats.MaxGroupSize = size
	}
	switch reason {
	case durableFlushBatchFull:
		w.stats.BatchFullGroups++
	case durableFlushTimer:
		w.stats.TimerGroups++
	case durableFlushShutdown:
		w.stats.ShutdownGroups++
	}
}

func (w *DurableIngestor) recordAppend(elapsed time.Duration, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stats.AppendAttempts++
	if err == nil {
		w.stats.SuccessfulAppends++
		w.stats.AppendFsyncTotal += elapsed
		w.stats.AppendFsyncLast = elapsed
		if elapsed > w.stats.AppendFsyncMax {
			w.stats.AppendFsyncMax = elapsed
		}
		return
	}
	if errors.Is(err, spool.ErrAtCapacity) {
		w.stats.CapacityRetries++
		return
	}
	w.stats.AppendErrors++
}

func (w *DurableIngestor) takeBatch() []*durableRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.pending)
	if n > w.batchSize {
		n = w.batchSize
	}
	if n == 0 {
		return nil
	}
	batch := append([]*durableRequest(nil), w.pending[:n]...)
	copy(w.pending, w.pending[n:])
	for i := len(w.pending) - n; i < len(w.pending); i++ {
		if i >= 0 {
			w.pending[i] = nil
		}
	}
	w.pending = w.pending[:len(w.pending)-n]
	if len(w.pending) > 0 {
		w.signalLocked()
	}
	return batch
}

func (w *DurableIngestor) failPending(err error) {
	w.mu.Lock()
	w.closed = true
	if w.fatalErr == nil {
		w.fatalErr = err
	}
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()
	completeDurableRequests(pending, err)
}

func (w *DurableIngestor) currentFatal() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fatalErr
}

func completeDurableRequests(requests []*durableRequest, err error) {
	for _, req := range requests {
		req.result <- err
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
