// Package heat collects privacy-reduced activity evidence from validated
// inbound get_peers requests. It is deliberately independent from metadata
// durability and frozen experiment-oracle channels.
package heat

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultQueueCapacity = 65_536
	defaultBatchSize     = 4_096
	defaultFlushDelay    = 25 * time.Millisecond
	defaultRetryBackoff  = time.Second
	defaultHTTPTimeout   = 10 * time.Second
)

type Options struct {
	Endpoint         string
	CrawlerID        string
	SpoolDir         string
	SpoolMaxBytes    int64
	KnownCrawlers    string
	QueueCapacity    int
	BatchSize        int
	FlushDelay       time.Duration
	RetryBackoff     time.Duration
	HTTPTimeout      time.Duration
	MasterSecret     []byte
	MasterSecretFile string
	HMACSecret       []byte
	HMACSecretFile   string
	LocalAddresses   []netip.Addr
	HTTPClient       *http.Client
	Now              func() time.Time
}

type Snapshot struct {
	Observed                 uint64
	Filtered                 uint64
	Queued                   uint64
	QueueDropped             uint64
	BatchDuplicates          uint64
	Durable                  uint64
	LostBeforeDurable        uint64
	SpoolRetries             uint64
	SpoolFatalErrors         uint64
	Exported                 uint64
	ExportBatches            uint64
	ExportRetries            uint64
	ExportPermanentFailures  uint64
	ClosedDayRejectedRecords uint64
	ClosedDayRejectedBatches uint64
	QueueDepth               int
	QueueCapacity            int
	SpoolBytes               int64
	SpoolMaxBytes            int64
	SpoolRecords             uint64
}

type Collector struct {
	identity           *actorIdentity
	spool              *heatSpool
	completion         *completionTracker
	endpoint           string
	completionEndpoint string
	crawlerID          string
	hmacSecret         string
	client             *http.Client
	batchSize          int
	flushDelay         time.Duration
	retryDelay         time.Duration
	now                func() time.Time

	queue        chan writerItem
	wake         chan struct{}
	errors       chan error
	ctx          context.Context
	cancel       context.CancelFunc
	boundaryStop chan struct{}
	boundaryDone chan struct{}

	closeMu      sync.RWMutex
	closed       bool
	failed       atomic.Bool
	activeDay    atomic.Uint32
	writerDone   chan struct{}
	exporterDone chan struct{}

	observed                 atomic.Uint64
	filtered                 atomic.Uint64
	queued                   atomic.Uint64
	queueDropped             atomic.Uint64
	batchDuplicates          atomic.Uint64
	durable                  atomic.Uint64
	lostBeforeDurable        atomic.Uint64
	spoolRetries             atomic.Uint64
	spoolFatalErrors         atomic.Uint64
	exported                 atomic.Uint64
	exportBatches            atomic.Uint64
	exportRetries            atomic.Uint64
	exportPermanentFailures  atomic.Uint64
	closedDayRejectedRecords atomic.Uint64
	closedDayRejectedBatches atomic.Uint64
}

type writerItem struct {
	observation Observation
	barrier     chan bool
}

func New(opts Options) (*Collector, error) {
	master, err := resolveSecret(opts.MasterSecret, opts.MasterSecretFile, "master secret")
	if err != nil {
		return nil, err
	}
	defer clear(master)
	hmacSecret, err := resolveSecret(opts.HMACSecret, opts.HMACSecretFile, "HMAC secret")
	if err != nil {
		return nil, err
	}
	defer clear(hmacSecret)
	if len(hmacSecret) < 32 {
		return nil, errors.New("heat: HMAC secret must contain at least 32 raw bytes")
	}
	endpoint, err := validateEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}
	if err := validateCrawlerID(opts.CrawlerID); err != nil {
		return nil, err
	}
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = defaultQueueCapacity
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.BatchSize > opts.QueueCapacity {
		opts.BatchSize = opts.QueueCapacity
	}
	if opts.FlushDelay <= 0 {
		opts.FlushDelay = defaultFlushDelay
	}
	if opts.RetryBackoff <= 0 {
		opts.RetryBackoff = defaultRetryBackoff
	}
	if opts.HTTPTimeout <= 0 {
		opts.HTTPTimeout = defaultHTTPTimeout
	}
	local := opts.LocalAddresses
	if local == nil {
		local, err = localInterfaceAddresses()
		if err != nil {
			return nil, fmt.Errorf("heat: enumerate local addresses: %w", err)
		}
	}
	identity, err := newActorIdentity(master, opts.KnownCrawlers, local)
	if err != nil {
		return nil, err
	}
	sp, err := openHeatSpool(spoolOptions{dir: opts.SpoolDir, maxBytes: opts.SpoolMaxBytes})
	if err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	epoch, nextSequence, _, err := sp.sequenceState()
	if err != nil {
		_ = sp.close()
		return nil, err
	}
	today, ok := utcDay(now())
	if !ok || today == 0 {
		_ = sp.close()
		return nil, errors.New("heat: current UTC day is outside supported range")
	}
	completion, err := openCompletionTracker(opts.SpoolDir, opts.CrawlerID, epoch, today, nextSequence)
	if err != nil {
		_ = sp.close()
		return nil, err
	}
	completionEndpoint, err := deriveCompletionEndpoint(endpoint)
	if err != nil {
		_ = sp.close()
		return nil, err
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: opts.HTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConns: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
			},
		}
	} else {
		clone := *client
		client = &clone
	}
	// Never forward pseudonymous activity or signed receipts to a host selected
	// by an HTTP redirect. Endpoint changes are explicit configuration changes.
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{
		identity: identity, spool: sp, completion: completion, endpoint: endpoint,
		completionEndpoint: completionEndpoint, crawlerID: opts.CrawlerID,
		hmacSecret: string(hmacSecret), client: client, batchSize: opts.BatchSize,
		flushDelay: opts.FlushDelay, retryDelay: opts.RetryBackoff, now: now,
		queue: make(chan writerItem, opts.QueueCapacity), wake: make(chan struct{}, 1),
		errors: make(chan error, 32), ctx: ctx, cancel: cancel,
		boundaryStop: make(chan struct{}), boundaryDone: make(chan struct{}),
		writerDone: make(chan struct{}), exporterDone: make(chan struct{}),
	}
	c.activeDay.Store(today)
	go c.runWriter()
	go c.runExporter()
	go c.runBoundary()
	return c, nil
}

// Observe is the get_peers hot-path boundary. A true result means admission to
// the bounded in-memory writer queue, not durable acknowledgement. A false
// result is always reflected by Filtered or QueueDropped.
func (c *Collector) Observe(rawInfoHash, sourceIP string, now time.Time) bool {
	c.observed.Add(1)
	obs, ok := c.identity.observation(rawInfoHash, sourceIP, now)
	if !ok {
		c.filtered.Add(1)
		return false
	}
	for {
		active := c.activeDay.Load()
		if obs.Day < active {
			// Callers must provide the callback instant. A timestamp from an
			// already closed UTC day is invalid input, not activity evidence.
			c.filtered.Add(1)
			return false
		}
		if obs.Day > active {
			if !c.advanceTo(obs.Day) {
				c.queueDropped.Add(1)
				c.completion.markDirty(active)
				return false
			}
			continue
		}
		c.closeMu.RLock()
		if c.closed || c.failed.Load() {
			c.closeMu.RUnlock()
			c.queueDropped.Add(1)
			c.completion.markDirty(obs.Day)
			return false
		}
		if c.activeDay.Load() != obs.Day {
			c.closeMu.RUnlock()
			continue
		}
		select {
		case c.queue <- writerItem{observation: obs}:
			c.queued.Add(1)
			c.closeMu.RUnlock()
			return true
		default:
			c.queueDropped.Add(1)
			c.completion.markDirty(obs.Day)
			c.closeMu.RUnlock()
			return false
		}
	}
}

func (c *Collector) Snapshot() Snapshot {
	spoolBytes, spoolMax, spoolRecords := c.spool.snapshot()
	return Snapshot{
		Observed: c.observed.Load(), Filtered: c.filtered.Load(), Queued: c.queued.Load(),
		QueueDropped: c.queueDropped.Load(), BatchDuplicates: c.batchDuplicates.Load(),
		Durable: c.durable.Load(), LostBeforeDurable: c.lostBeforeDurable.Load(),
		SpoolRetries: c.spoolRetries.Load(), SpoolFatalErrors: c.spoolFatalErrors.Load(),
		Exported: c.exported.Load(), ExportBatches: c.exportBatches.Load(),
		ExportRetries: c.exportRetries.Load(), ExportPermanentFailures: c.exportPermanentFailures.Load(),
		ClosedDayRejectedRecords: c.closedDayRejectedRecords.Load(),
		ClosedDayRejectedBatches: c.closedDayRejectedBatches.Load(),
		QueueDepth:               len(c.queue), QueueCapacity: cap(c.queue), SpoolBytes: spoolBytes,
		SpoolMaxBytes: spoolMax, SpoolRecords: spoolRecords,
	}
}

// Errors reports operational faults without coupling heat delivery to either
// metadata durability or the frozen oracle. Counters remain the authoritative
// signal if this bounded diagnostic channel itself fills.
func (c *Collector) Errors() <-chan error { return c.errors }

func (c *Collector) Close(ctx context.Context) error {
	var stateErr error
	c.closeMu.Lock()
	if !c.closed {
		c.closed = true
		close(c.boundaryStop)
		close(c.queue)
		stateErr = c.completion.stopDirty()
	}
	c.closeMu.Unlock()
	<-c.boundaryDone

	select {
	case <-c.writerDone:
	case <-ctx.Done():
		c.cancel()
		<-c.writerDone
		c.cancel()
		<-c.exporterDone
		return errors.Join(ctx.Err(), stateErr, c.spool.close())
	}
	c.cancel()
	<-c.exporterDone
	return errors.Join(stateErr, c.spool.close())
}

func (c *Collector) runWriter() {
	defer close(c.writerDone)
	batch := make([]Observation, 0, c.batchSize)
	var timer *time.Timer
	var timerC <-chan time.Time
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		ok := c.persistBatch(batch)
		batch = batch[:0]
		return ok
	}
	for {
		select {
		case item, open := <-c.queue:
			if !open {
				if timer != nil {
					stopTimer(timer)
				}
				flush()
				return
			}
			if item.barrier != nil {
				if timer != nil {
					stopTimer(timer)
					timerC = nil
				}
				item.barrier <- flush()
				continue
			}
			batch = append(batch, item.observation)
			if len(batch) == 1 {
				if timer == nil {
					timer = time.NewTimer(c.flushDelay)
				} else {
					timer.Reset(c.flushDelay)
				}
				timerC = timer.C
			}
			if len(batch) < c.batchSize {
				continue
			}
			if timer != nil {
				stopTimer(timer)
				timerC = nil
			}
			if !flush() {
				c.drainLostQueue()
				return
			}
		case <-timerC:
			timerC = nil
			if !flush() {
				c.drainLostQueue()
				return
			}
		case <-c.ctx.Done():
			if timer != nil {
				stopTimer(timer)
			}
			c.markLost(batch)
			c.drainLostQueue()
			return
		}
	}
}

func (c *Collector) persistBatch(batch []Observation) bool {
	unique := make([]Observation, 0, len(batch))
	seen := make(map[Observation]struct{}, len(batch))
	for _, observation := range batch {
		if _, duplicate := seen[observation]; duplicate {
			c.batchDuplicates.Add(1)
			continue
		}
		seen[observation] = struct{}{}
		unique = append(unique, observation)
	}
	for {
		if err := c.spool.appendDurable(unique); err == nil {
			c.durable.Add(uint64(len(unique)))
			select {
			case c.wake <- struct{}{}:
			default:
			}
			return true
		} else {
			c.spoolRetries.Add(1)
			if errors.Is(err, ErrCorruptSpool) {
				c.spoolFatalErrors.Add(1)
				c.failed.Store(true)
				c.markLost(unique)
				c.report(err)
				return false
			}
			c.report(err)
		}
		timer := time.NewTimer(c.retryDelay)
		select {
		case <-timer.C:
		case <-c.ctx.Done():
			stopTimer(timer)
			c.markLost(unique)
			return false
		}
	}
}

func (c *Collector) drainLostQueue() {
	for item := range c.queue {
		if item.barrier != nil {
			item.barrier <- false
			continue
		}
		c.markLost([]Observation{item.observation})
	}
}

func (c *Collector) markLost(observations []Observation) {
	c.lostBeforeDurable.Add(uint64(len(observations)))
	for _, observation := range observations {
		c.completion.markDirty(observation.Day)
	}
}

func (c *Collector) runBoundary() {
	defer close(c.boundaryDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.boundaryStop:
			return
		case <-ticker.C:
			day, ok := utcDay(c.now())
			if ok && day > c.activeDay.Load() {
				c.advanceTo(day)
			}
		}
	}
}

// advanceTo is the once-per-UTC-day ordering barrier. Holding closeMu excludes
// all Observe admissions; the writer barrier then proves that every earlier
// admitted observation reached the durable spool before its end sequence was
// recorded. The exporter may finish later and is proven by the spool cursor.
func (c *Collector) advanceTo(nextDay uint32) bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	current := c.activeDay.Load()
	if nextDay <= current {
		return true
	}
	if c.closed || c.failed.Load() {
		return false
	}
	barrier := make(chan bool, 1)
	select {
	case c.queue <- writerItem{barrier: barrier}:
	case <-c.writerDone:
		c.failed.Store(true)
		_ = c.completion.markDirtyDurable(current)
		return false
	}
	var clean bool
	select {
	case clean = <-barrier:
	case <-c.writerDone:
		c.failed.Store(true)
		_ = c.completion.markDirtyDurable(current)
		return false
	}
	_, nextSequence, _, err := c.spool.sequenceState()
	if err != nil {
		c.failed.Store(true)
		_ = c.completion.markDirtyDurable(current)
		c.report(err)
		return false
	}
	if err := c.completion.closeThrough(current, nextDay, nextSequence, clean); err != nil {
		c.failed.Store(true)
		c.report(err)
		return false
	}
	c.activeDay.Store(nextDay)
	if !clean {
		c.failed.Store(true)
		if err := c.completion.markDirtyDurable(nextDay); err != nil {
			c.report(err)
		}
		c.report(errors.New("heat: UTC writer boundary failed; new day forced partial"))
		return false
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return true
}

func (c *Collector) runExporter() {
	defer close(c.exporterDone)
	for {
		if !c.exportReadyCompletions() {
			return
		}
		batch, err := c.spool.readBatch(c.batchSize)
		if err != nil {
			c.spoolFatalErrors.Add(1)
			c.report(err)
			return
		}
		if len(batch.Observations) == 0 {
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-c.ctx.Done():
				stopTimer(timer)
				return
			case <-c.wake:
				stopTimer(timer)
			case <-timer.C:
			}
			continue
		}
		wireBatch, err := BuildWireBatch(
			batch.Observations[0].Day, batch.Observations[0].Hour, batch.Observations)
		if err != nil {
			c.spoolFatalErrors.Add(1)
			c.report(err)
			return
		}
		payload, err := EncodeWire(wireBatch)
		if err != nil {
			c.spoolFatalErrors.Add(1)
			c.report(err)
			return
		}
		receipt := buildDeliveryReceipt(c.crawlerID, c.hmacSecret, batch, payload)
		var outcome deliveryOutcome
		for {
			outcome, err = c.send(payload, receipt)
			if err == nil {
				break
			}
			c.exportRetries.Add(1)
			var status *httpStatusError
			if errors.As(err, &status) && status.permanent() {
				c.exportPermanentFailures.Add(1)
			}
			c.report(err)
			if !c.waitRetry() {
				return
			}
		}
		if outcome == deliveryClosedDay {
			for {
				if err := c.completion.markDirtyDurable(batch.Observations[0].Day); err == nil {
					break
				} else {
					c.report(err)
				}
				if !c.waitRetry() {
					return
				}
			}
		}
		for {
			if err := c.spool.commit(batch); err == nil {
				break
			} else {
				c.spoolRetries.Add(1)
				c.report(err)
			}
			if !c.waitRetry() {
				return
			}
		}
		if outcome == deliveryClosedDay {
			c.closedDayRejectedBatches.Add(1)
			c.closedDayRejectedRecords.Add(uint64(len(batch.Observations)))
			c.report(fmt.Errorf("heat: durable negative receipt day_closed rejected day=%s epoch=%d sequences=%d..%d records=%d; coverage is partial",
				receipt.Day, receipt.Epoch, receipt.StartSequence, receipt.EndSequence, len(batch.Observations)))
		} else {
			c.exportBatches.Add(1)
			c.exported.Add(uint64(len(batch.Observations)))
		}
	}
}

type completionRequest struct {
	Crawler       string
	Day           string
	DayNumber     uint32
	Epoch         uint64
	StartSequence uint64
	NextSequence  uint64
	Clean         bool
	Signature     string
}

type completionOutcome uint8

const (
	completionAccepted completionOutcome = iota
	completionClosedDay
	completionRejected
)

func (c *Collector) exportReadyCompletions() bool {
	_, _, cursor, err := c.spool.sequenceState()
	if err != nil {
		c.report(err)
		return false
	}
	for _, request := range c.completion.ready(cursor) {
		request.Signature = signCompletion(request, c.hmacSecret)
		var outcome completionOutcome
		for {
			outcome, err = c.sendCompletion(request)
			if err == nil {
				break
			}
			c.exportRetries.Add(1)
			c.report(err)
			if !c.waitRetry() {
				return false
			}
		}
		if outcome != completionAccepted {
			if err := c.completion.markDirtyDurable(request.DayNumber); err != nil {
				c.report(err)
				return false
			}
			code := "completion_conflict"
			if outcome == completionClosedDay {
				code = "day_closed"
			}
			c.report(fmt.Errorf("heat: completion rejected %s day=%s; coverage is partial", code, request.Day))
			continue
		}
		if err := c.completion.acknowledge(request.DayNumber); err != nil {
			c.report(err)
			return false
		}
	}
	return true
}

func signCompletion(request completionRequest, secret string) string {
	prefix := fmt.Sprintf("CHHT-COMPLETE/1\n%s\n%s\n%d\n%d\n%d\n1\n",
		request.Crawler, request.Day, request.Epoch, request.StartSequence, request.NextSequence)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(prefix))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Collector) sendCompletion(request completionRequest) (completionOutcome, error) {
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.completionEndpoint, http.NoBody)
	if err != nil {
		return completionAccepted, err
	}
	req.Header.Set("Content-Type", "application/vnd.cherry.heat-completion-v1")
	req.Header.Set("X-CHHT-Crawler", request.Crawler)
	req.Header.Set("X-CHHT-Day", request.Day)
	req.Header.Set("X-CHHT-Epoch", strconv.FormatUint(request.Epoch, 10))
	req.Header.Set("X-CHHT-Start-Sequence", strconv.FormatUint(request.StartSequence, 10))
	req.Header.Set("X-CHHT-Next-Sequence", strconv.FormatUint(request.NextSequence, 10))
	req.Header.Set("X-CHHT-Clean", "1")
	req.Header.Set("X-CHHT-Signature", request.Signature)
	response, err := c.client.Do(req)
	if err != nil {
		return completionAccepted, fmt.Errorf("heat: completion request: %w", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	switch response.StatusCode {
	case http.StatusOK:
		if err := validateCompletionReceipt(body, request, ""); err != nil {
			return completionAccepted, err
		}
		return completionAccepted, nil
	case http.StatusGone:
		if err := validateCompletionReceipt(body, request, "day_closed"); err != nil {
			return completionAccepted, &httpStatusError{code: response.StatusCode, body: err.Error()}
		}
		return completionClosedDay, nil
	case http.StatusConflict:
		if err := validateCompletionReceipt(body, request, "completion_conflict"); err != nil {
			return completionAccepted, &httpStatusError{code: response.StatusCode, body: err.Error()}
		}
		return completionRejected, nil
	default:
		return completionAccepted, &httpStatusError{
			code: response.StatusCode, body: strings.TrimSpace(string(body)),
		}
	}
}

type completionReceipt struct {
	Crawler       *string         `json:"crawler"`
	Day           *string         `json:"day"`
	Epoch         *uint64         `json:"epoch"`
	StartSequence *uint64         `json:"startSequence"`
	NextSequence  *uint64         `json:"nextSequence"`
	Clean         *bool           `json:"clean"`
	Replay        *bool           `json:"replay"`
	Code          json.RawMessage `json:"code"`
	Error         json.RawMessage `json:"error"`
}

func validateCompletionReceipt(body []byte, want completionRequest, rejectionCode string) error {
	var got completionReceipt
	if err := decodeStrictJSON(body, &got); err != nil {
		return fmt.Errorf("heat: invalid completion receipt: %w", err)
	}
	if got.Crawler == nil || *got.Crawler != want.Crawler || got.Day == nil || *got.Day != want.Day ||
		got.Epoch == nil || *got.Epoch != want.Epoch || got.StartSequence == nil ||
		*got.StartSequence != want.StartSequence || got.NextSequence == nil ||
		*got.NextSequence != want.NextSequence || got.Clean == nil || !*got.Clean {
		return errors.New("heat: completion receipt identity mismatch")
	}
	if rejectionCode != "" {
		var code, message string
		if err := json.Unmarshal(got.Code, &code); err != nil || code != rejectionCode ||
			json.Unmarshal(got.Error, &message) != nil || message == "" || got.Replay != nil {
			return errors.New("heat: invalid day_closed completion result")
		}
		return nil
	}
	if got.Replay == nil || !isJSONNull(got.Code) || !isJSONNull(got.Error) {
		return errors.New("heat: invalid accepted completion result")
	}
	return nil
}

type deliveryOutcome uint8

const (
	deliveryAccepted deliveryOutcome = iota
	deliveryClosedDay
)

func (c *Collector) send(payload []byte, receipt deliveryReceipt) (deliveryOutcome, error) {
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return deliveryAccepted, err
	}
	req.Header.Set("Content-Type", "application/vnd.cherry.heat-v2")
	req.Header.Set("X-CHHT-Crawler", receipt.Crawler)
	req.Header.Set("X-CHHT-Epoch", strconv.FormatUint(receipt.Epoch, 10))
	req.Header.Set("X-CHHT-Sequence", strconv.FormatUint(receipt.StartSequence, 10))
	req.Header.Set("X-CHHT-End-Sequence", strconv.FormatUint(receipt.EndSequence, 10))
	req.Header.Set("X-CHHT-Payload-SHA256", receipt.PayloadSHA256)
	req.Header.Set("X-CHHT-Signature", receipt.Signature)
	resp, err := c.client.Do(req)
	if err != nil {
		return deliveryAccepted, fmt.Errorf("heat: export request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch resp.StatusCode {
	case http.StatusOK:
		if err := validateAcceptedReceipt(body, receipt); err != nil {
			return deliveryAccepted, err
		}
		return deliveryAccepted, nil
	case http.StatusGone:
		if err := validateClosedDayReceipt(body, receipt); err != nil {
			return deliveryAccepted, &httpStatusError{code: resp.StatusCode, body: err.Error()}
		}
		return deliveryClosedDay, nil
	default:
		return deliveryAccepted, &httpStatusError{code: resp.StatusCode, body: strings.TrimSpace(string(body))}
	}
}

type deliveryReceipt struct {
	Crawler       string
	Day           string
	Epoch         uint64
	StartSequence uint64
	EndSequence   uint64
	PayloadSHA256 string
	Signature     string
}

func buildDeliveryReceipt(crawlerID, hmacSecret string, batch spoolBatch, payload []byte) deliveryReceipt {
	digest := sha256.Sum256(payload)
	digestHex := hex.EncodeToString(digest[:])
	prefix := fmt.Sprintf("CHHT/2\n%s\n%d\n%d\n%d\n%s\n",
		crawlerID, batch.Epoch, batch.StartSequence, batch.EndSequence, digestHex)
	mac := hmac.New(sha256.New, []byte(hmacSecret))
	_, _ = mac.Write([]byte(prefix))
	_, _ = mac.Write(payload)
	return deliveryReceipt{
		Crawler: crawlerID, Day: wireDayString(payload), Epoch: batch.Epoch, StartSequence: batch.StartSequence,
		EndSequence: batch.EndSequence, PayloadSHA256: digestHex,
		Signature: hex.EncodeToString(mac.Sum(nil)),
	}
}

type acceptedReceipt struct {
	Crawler       *string         `json:"crawler"`
	Day           *string         `json:"day"`
	Epoch         *uint64         `json:"epoch"`
	StartSequence *uint64         `json:"startSequence"`
	EndSequence   *uint64         `json:"endSequence"`
	PayloadSHA256 *string         `json:"payloadSha256"`
	Received      *int64          `json:"received"`
	Inserted      *int64          `json:"inserted"`
	NextSequence  *uint64         `json:"nextSequence"`
	Replay        *bool           `json:"replay"`
	Code          json.RawMessage `json:"code"`
	Error         json.RawMessage `json:"error"`
}

type closedDayReceipt struct {
	Code          *string `json:"code"`
	Crawler       *string `json:"crawler"`
	Day           *string `json:"day"`
	Epoch         *uint64 `json:"epoch"`
	StartSequence *uint64 `json:"startSequence"`
	EndSequence   *uint64 `json:"endSequence"`
	NextSequence  *uint64 `json:"nextSequence"`
	PayloadSHA256 *string `json:"payloadSha256"`
	Received      *int64  `json:"received"`
	Inserted      *int64  `json:"inserted"`
	Replay        *bool   `json:"replay"`
	Error         *string `json:"error"`
}

func validateAcceptedReceipt(body []byte, want deliveryReceipt) error {
	var got acceptedReceipt
	if err := decodeStrictJSON(body, &got); err != nil {
		return fmt.Errorf("heat: invalid HTTP 200 receipt: %w", err)
	}
	if !receiptIdentityMatches(got.Crawler, got.Day, got.Epoch, got.StartSequence,
		got.EndSequence, got.NextSequence, got.PayloadSHA256, want) {
		return errors.New("heat: HTTP 200 receipt identity mismatch")
	}
	if got.Received == nil || got.Inserted == nil || got.Replay == nil || *got.Received < 0 ||
		*got.Inserted < 0 || *got.Inserted > *got.Received || !isJSONNull(got.Code) || !isJSONNull(got.Error) {
		return errors.New("heat: HTTP 200 receipt result fields are missing or invalid")
	}
	return nil
}

func validateClosedDayReceipt(body []byte, want deliveryReceipt) error {
	var got closedDayReceipt
	if err := decodeStrictJSON(body, &got); err != nil {
		return fmt.Errorf("invalid day_closed receipt: %w", err)
	}
	if got.Code == nil || *got.Code != "day_closed" ||
		!receiptIdentityMatches(got.Crawler, got.Day, got.Epoch, got.StartSequence,
			got.EndSequence, got.NextSequence, got.PayloadSHA256, want) {
		return errors.New("day_closed receipt identity mismatch")
	}
	return nil
}

func receiptIdentityMatches(crawler, day *string, epoch, start, end, next *uint64,
	digest *string, want deliveryReceipt) bool {
	return crawler != nil && *crawler == want.Crawler && day != nil && *day == want.Day &&
		epoch != nil && *epoch == want.Epoch && start != nil && *start == want.StartSequence &&
		end != nil && *end == want.EndSequence && next != nil && want.EndSequence != ^uint64(0) &&
		*next == want.EndSequence+1 && digest != nil && *digest == want.PayloadSHA256
}

func decodeStrictJSON(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func isJSONNull(value json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func wireDayString(payload []byte) string {
	if len(payload) < wireHeaderSize || !bytes.Equal(payload[:4], wireMagic[:]) || payload[4] != wireVersion {
		return ""
	}
	day := binary.BigEndian.Uint32(payload[5:9])
	return time.Unix(int64(day)*86_400, 0).UTC().Format(time.DateOnly)
}

func formatObservationDay(day uint32) string {
	return time.Unix(int64(day)*86_400, 0).UTC().Format(time.DateOnly)
}

func (c *Collector) waitRetry() bool {
	timer := time.NewTimer(c.retryDelay)
	select {
	case <-timer.C:
		return true
	case <-c.ctx.Done():
		stopTimer(timer)
		return false
	}
}

func (c *Collector) report(err error) {
	select {
	case c.errors <- err:
	default:
	}
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

type httpStatusError struct {
	code int
	body string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("heat: endpoint HTTP %d: %s", e.code, e.body)
}

func (e *httpStatusError) permanent() bool {
	return e.code >= 400 && e.code < 500 && e.code != http.StatusRequestTimeout &&
		e.code != http.StatusConflict && e.code != http.StatusTooEarly && e.code != http.StatusTooManyRequests
}

func resolveSecret(direct []byte, path, label string) ([]byte, error) {
	if len(direct) > 0 && strings.TrimSpace(path) != "" {
		return nil, fmt.Errorf("heat: configure %s directly or by file, not both", label)
	}
	if len(direct) > 0 {
		return append([]byte(nil), direct...), nil
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("heat: %s file is required", label)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("heat: stat %s file: %w", label, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("heat: %s path is not a regular file", label)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("heat: %s file permissions %o expose the secret; require 0600", label, info.Mode().Perm())
	}
	if info.Size() > 4096 {
		return nil, fmt.Errorf("heat: %s file exceeds 4 KiB", label)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("heat: read %s file: %w", label, err)
	}
	// Secret files commonly end in one editor-added line ending. Preserve every
	// other byte: the backend decodes the same raw secret from Base64 and must
	// not accidentally sign with a whitespace-normalized key.
	data = bytes.TrimSuffix(data, []byte("\n"))
	data = bytes.TrimSuffix(data, []byte("\r"))
	if len(data) == 0 {
		return nil, fmt.Errorf("heat: %s file is empty", label)
	}
	return data, nil
}

func validateEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("heat: endpoint must be an absolute URL without credentials, query or fragment")
	}
	if parsed.Scheme != "https" {
		host := parsed.Hostname()
		addr, _ := netip.ParseAddr(host)
		if parsed.Scheme != "http" || (host != "localhost" && (!addr.IsValid() || !addr.IsLoopback())) {
			return "", errors.New("heat: endpoint must use HTTPS (HTTP is allowed only on loopback)")
		}
	}
	return raw, nil
}

func deriveCompletionEndpoint(batchEndpoint string) (string, error) {
	parsed, err := url.Parse(batchEndpoint)
	if err != nil {
		return "", errors.New("heat: invalid batch endpoint")
	}
	if strings.HasSuffix(parsed.Path, "/batches") {
		parsed.Path = strings.TrimSuffix(parsed.Path, "/batches") + "/completions"
	} else {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/completions"
	}
	return parsed.String(), nil
}

func validateCrawlerID(value string) error {
	if len(value) == 0 || len(value) > 64 {
		return errors.New("heat: stable crawler ID must contain 1..64 characters")
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return errors.New("heat: crawler ID may contain only ASCII letters, digits, dot, dash and underscore")
	}
	return nil
}
