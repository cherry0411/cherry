package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"cherry-picker/internal/spool"
)

// SpoolExporter drains already-durable metadata with exactly one HTTP batch in
// flight. It never owns the producer durability boundary; DurableIngestor does.
type SpoolExporter struct {
	logger    *log.Logger
	spool     *spool.Spool
	client    *http.Client
	url       string
	apiKey    string
	batchSize int
	retry     time.Duration
}

type SpoolExporterOptions struct {
	Logger       *log.Logger
	Spool        *spool.Spool
	URL          string
	APIKey       string
	BatchSize    int
	RetryBackoff time.Duration
	HTTPTimeout  time.Duration
}

var (
	// ErrConflict means the durable receipt authority rejected sequence or
	// checksum identity. Retrying cannot repair it.
	ErrConflict = errors.New("export: durable batch conflict")
	// ErrProtocol means a 2xx ACK or a non-retryable 4xx violated the durable
	// contract. Local data remains untouched for operator recovery.
	ErrProtocol = errors.New("export: durable protocol violation")
)

func NewSpoolExporter(opts SpoolExporterOptions) (*SpoolExporter, error) {
	if opts.Spool == nil {
		return nil, errors.New("export: durable exporter requires a spool")
	}
	if strings.TrimSpace(opts.URL) == "" {
		return nil, errors.New("export: durable exporter requires an endpoint")
	}
	// Durable ingest is intended for an Internet-facing, cross-region endpoint.
	// Refuse to silently fall back to unauthenticated writes.
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, errors.New("export: durable exporter requires an API key")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 512
	}
	if opts.BatchSize > durableProtocolMaxEvents {
		opts.BatchSize = durableProtocolMaxEvents
	}
	if opts.RetryBackoff <= 0 {
		opts.RetryBackoff = time.Second
	}
	if opts.HTTPTimeout <= 0 {
		opts.HTTPTimeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	return &SpoolExporter{
		logger:    opts.Logger,
		spool:     opts.Spool,
		client:    &http.Client{Timeout: opts.HTTPTimeout},
		url:       strings.TrimSpace(opts.URL),
		apiKey:    strings.TrimSpace(opts.APIKey),
		batchSize: opts.BatchSize,
		retry:     opts.RetryBackoff,
	}, nil
}

// Deliver retries the same in-flight batch until a matching ACK arrives. It
// advances the durable cursor once and only once; protocol/storage errors halt
// the loop with all unacknowledged records retained.
func (e *SpoolExporter) Deliver(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		batch, err := e.spool.NextBatch(e.batchSize)
		if err != nil {
			return fmt.Errorf("export: load durable batch: %w", err)
		}
		if batch.Empty() {
			if !sleepCtx(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}

		for {
			err = e.sendBatch(ctx, batch)
			switch {
			case err == nil:
				if err := e.spool.CommitBatch(batch); err != nil {
					return fmt.Errorf("export: commit durable ACK cursor: %w", err)
				}
				goto nextBatch
			case errors.Is(err, ErrConflict), errors.Is(err, ErrProtocol):
				return err
			case ctx.Err() != nil:
				return nil
			default:
				e.logger.Printf("durable export retry: %v", err)
				if !sleepCtx(ctx, e.retry) {
					return nil
				}
			}
		}
	nextBatch:
	}
}

func (e *SpoolExporter) sendBatch(ctx context.Context, batch spool.Batch) error {
	events := make([]DurableEvent, len(batch.Records))
	for i := range batch.Records {
		events[i] = durableEventFromRecord(batch.Records[i])
	}
	eventsJSON, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("%w: encode events: %v", ErrProtocol, err)
	}
	sum := sha256.Sum256(eventsJSON)
	checksum := hex.EncodeToString(sum[:])

	// RawMessage guarantees the bytes covered by checksum are the bytes placed
	// in the request's events value. The backend hashes that raw JSON slice.
	wire := struct {
		SchemaVersion int             `json:"schema_version"`
		CrawlerID     string          `json:"crawler_id"`
		Epoch         uint64          `json:"epoch"`
		StartSequence uint64          `json:"start_sequence"`
		EndSequence   uint64          `json:"end_sequence"`
		PayloadSHA256 string          `json:"payload_sha256"`
		Events        json.RawMessage `json:"events"`
	}{
		SchemaVersion: durableProtocolSchemaVersion,
		CrawlerID:     batch.CrawlerID,
		Epoch:         batch.Epoch,
		StartSequence: batch.StartSeq,
		EndSequence:   batch.EndSeq,
		PayloadSHA256: checksum,
		Events:        eventsJSON,
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("%w: encode envelope: %v", ErrProtocol, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: create request: %v", ErrProtocol, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", e.apiKey)

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return readErr
	}

	switch {
	case resp.StatusCode == http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrConflict, boundedBody(respBody))
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("retryable HTTP status %d", resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return fmt.Errorf("%w: HTTP status %d: %s", ErrProtocol, resp.StatusCode, boundedBody(respBody))
	}

	var ack DurableBatchResponse
	dec := json.NewDecoder(bytes.NewReader(respBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ack); err != nil {
		return fmt.Errorf("%w: decode ACK: %v", ErrProtocol, err)
	}
	if ack.CrawlerID != wire.CrawlerID || ack.Epoch != wire.Epoch ||
		ack.StartSequence != wire.StartSequence || ack.EndSequence != wire.EndSequence ||
		ack.PayloadSHA256 != wire.PayloadSHA256 || !ack.Committed {
		return fmt.Errorf("%w: ACK identity mismatch", ErrProtocol)
	}
	if ack.Accepted < 0 || ack.Duplicates < 0 || ack.Errors != 0 ||
		ack.Accepted+ack.Duplicates != len(events) {
		return fmt.Errorf("%w: ACK counts accepted=%d duplicates=%d errors=%d events=%d",
			ErrProtocol, ack.Accepted, ack.Duplicates, ack.Errors, len(events))
	}
	return nil
}

func boundedBody(body []byte) string {
	const max = 512
	if len(body) > max {
		body = body[:max]
	}
	return strings.TrimSpace(string(body))
}

func sleepCtx(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
