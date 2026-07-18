package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cherry-picker/internal/spool"
)

const (
	defaultOracleObservationCapacity = 16_384
	defaultOracleObservationBatch    = 512
	defaultOracleObservationDelay    = 250 * time.Millisecond
)

// OracleObservation is deliberately a closed, metadata-free projection. It
// is the only body sent from the production durable path to an experiment
// oracle: no title, file path, piece hash, raw bencode, or spool identity can
// be represented by this type.
type OracleObservation struct {
	InfoHash string `json:"info_hash"`
	Action   string `json:"action"`
}

type oracleObservationRequest struct {
	Observations []OracleObservation `json:"observations"`
}

// OracleObserverSnapshot is safe to sample from the 30-second runtime loop.
// A benchmark window is invalid if Dropped or HTTPFailures advances: both
// conditions can change the attribution time or lose evidence entirely.
type OracleObserverSnapshot struct {
	Queued       uint64
	Sent         uint64
	Dropped      uint64
	HTTPFailures uint64
	Depth        int
	Capacity     int
}

type OracleObserverOptions struct {
	Logger       *log.Logger
	Endpoint     string
	APIKey       string
	Capacity     int
	BatchSize    int
	FlushDelay   time.Duration
	RetryBackoff time.Duration
	HTTPTimeout  time.Duration
}

// OracleObserver asynchronously mirrors only hash+typed retention action to a
// frozen benchmark oracle. Production durability never waits for this channel
// or its HTTP connection.
type OracleObserver struct {
	logger       *log.Logger
	endpoint     string
	apiKey       string
	client       *http.Client
	batchSize    int
	flushDelay   time.Duration
	retryBackoff time.Duration

	mu     sync.RWMutex
	closed bool
	queue  chan OracleObservation
	cancel context.CancelFunc
	done   chan struct{}

	queued       atomic.Uint64
	sent         atomic.Uint64
	dropped      atomic.Uint64
	httpFailures atomic.Uint64
	inflight     atomic.Int64
}

func NewOracleObserver(opts OracleObserverOptions) (*OracleObserver, error) {
	endpoint := oracleObservationEndpoint(opts.Endpoint)
	if endpoint == "" {
		return nil, errors.New("export: oracle observer requires an endpoint")
	}
	if opts.Capacity <= 0 {
		opts.Capacity = defaultOracleObservationCapacity
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultOracleObservationBatch
	}
	if opts.BatchSize > opts.Capacity {
		opts.BatchSize = opts.Capacity
	}
	if opts.FlushDelay <= 0 {
		opts.FlushDelay = defaultOracleObservationDelay
	}
	if opts.RetryBackoff <= 0 {
		opts.RetryBackoff = time.Second
	}
	if opts.HTTPTimeout <= 0 {
		opts.HTTPTimeout = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	o := &OracleObserver{
		logger:       opts.Logger,
		endpoint:     endpoint,
		apiKey:       strings.TrimSpace(opts.APIKey),
		client:       &http.Client{Timeout: opts.HTTPTimeout},
		batchSize:    opts.BatchSize,
		flushDelay:   opts.FlushDelay,
		retryBackoff: opts.RetryBackoff,
		queue:        make(chan OracleObservation, opts.Capacity),
		cancel:       cancel,
		done:         make(chan struct{}),
	}
	go o.run(ctx)
	return o, nil
}

// Submit records an observation after the caller's durable fsync succeeds.
// It never blocks. A full/closed channel increments Dropped, which is a hard
// experiment invalidation signal rather than a silent loss of evidence.
func (o *OracleObserver) Submit(record spool.Record) bool {
	observation, ok := oracleObservationFromRecord(record)
	if !ok {
		o.dropped.Add(1)
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		o.dropped.Add(1)
		return false
	}
	select {
	case o.queue <- observation:
		o.queued.Add(1)
		return true
	default:
		o.dropped.Add(1)
		return false
	}
}

func (o *OracleObserver) Snapshot() OracleObserverSnapshot {
	return OracleObserverSnapshot{
		Queued:       o.queued.Load(),
		Sent:         o.sent.Load(),
		Dropped:      o.dropped.Load(),
		HTTPFailures: o.httpFailures.Load(),
		Depth:        len(o.queue) + int(o.inflight.Load()),
		Capacity:     cap(o.queue),
	}
}

// Close stops admission and attempts to drain the bounded queue. If ctx
// expires, the unsent remainder becomes explicit Dropped evidence.
func (o *OracleObserver) Close(ctx context.Context) error {
	o.mu.Lock()
	if !o.closed {
		o.closed = true
		close(o.queue)
	}
	o.mu.Unlock()
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		o.cancel()
		<-o.done
		return ctx.Err()
	}
}

func (o *OracleObserver) run(ctx context.Context) {
	defer close(o.done)
	batch := make([]OracleObservation, 0, o.batchSize)
	for {
		first, ok := <-o.queue
		if !ok {
			return
		}
		batch = append(batch[:0], first)
		o.inflight.Store(1)
		timer := time.NewTimer(o.flushDelay)
	collect:
		for len(batch) < o.batchSize {
			select {
			case observation, open := <-o.queue:
				if !open {
					stopAndDrainTimer(timer)
					o.deliverUntilDone(ctx, batch)
					o.inflight.Store(0)
					return
				}
				batch = append(batch, observation)
				o.inflight.Store(int64(len(batch)))
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopAndDrainTimer(timer)
				o.dropBatchAndQueue(batch)
				o.inflight.Store(0)
				return
			}
		}
		stopAndDrainTimer(timer)
		delivered := o.deliverUntilDone(ctx, batch)
		o.inflight.Store(0)
		if !delivered {
			o.dropQueue()
			return
		}
	}
}

func (o *OracleObserver) deliverUntilDone(ctx context.Context, batch []OracleObservation) bool {
	for {
		if err := o.send(ctx, batch); err == nil {
			o.sent.Add(uint64(len(batch)))
			return true
		} else {
			o.httpFailures.Add(1)
			o.logger.Printf("oracle observation delivery failed (experiment window invalid until restart): %v", err)
		}
		timer := time.NewTimer(o.retryBackoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			o.dropped.Add(uint64(len(batch)))
			return false
		}
	}
}

func (o *OracleObserver) send(ctx context.Context, batch []OracleObservation) error {
	body, err := json.Marshal(oracleObservationRequest{Observations: batch})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if o.apiKey != "" {
		req.Header.Set("X-API-Key", o.apiKey)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("oracle observations: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (o *OracleObserver) dropBatchAndQueue(batch []OracleObservation) {
	o.dropped.Add(uint64(len(batch)))
	o.dropQueue()
}

func (o *OracleObserver) dropQueue() {
	for range o.queue {
		o.dropped.Add(1)
	}
}

func oracleObservationFromRecord(record spool.Record) (OracleObservation, bool) {
	action := ""
	switch record.Encoding {
	case spool.EncodingNormalized:
		action = "full"
	case spool.EncodingSummary:
		action = "summary"
	case spool.EncodingHashOnly:
		action = "hash_only"
	case spool.EncodingReject:
		action = "reject"
	default:
		return OracleObservation{}, false
	}
	if len(record.InfoHash) != 40 {
		return OracleObservation{}, false
	}
	return OracleObservation{InfoHash: record.InfoHash, Action: action}, true
}

func oracleObservationEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return ""
	}
	if strings.HasSuffix(endpoint, "/api/v1/oracle/observations") {
		return endpoint
	}
	if idx := strings.Index(endpoint, "/api/"); idx > 0 {
		endpoint = endpoint[:idx]
	}
	return endpoint + "/api/v1/oracle/observations"
}
