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
	closed   bool
	fatalErr error
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

type durableRequest struct {
	record spool.Record
	result chan error
}

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
	req := &durableRequest{record: record, result: make(chan error, 1)}

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

func (w *DurableIngestor) run() {
	defer close(w.done)
	stopping := false
	for {
		if !stopping {
			select {
			case <-w.wake:
			case <-w.stop:
				stopping = true
			}
		}

		if !stopping && w.pendingCount() < w.batchSize {
			timer := time.NewTimer(w.maxDelay)
			select {
			case <-timer.C:
			case <-w.wake:
				stopAndDrainTimer(timer)
			case <-w.stop:
				stopAndDrainTimer(timer)
				stopping = true
			}
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
		for {
			if fatal := w.currentFatal(); fatal != nil {
				completeDurableRequests(batch, fatal)
				w.failPending(fatal)
				return
			}
			_, err := w.spool.AppendBatchDurable(records)
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

func (w *DurableIngestor) pendingCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
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
