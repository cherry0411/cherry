package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"cherry-picker/internal/config"
	"cherry-picker/internal/pipeline"
)

type Sink interface {
	WriteBatch(context.Context, []pipeline.Event) error
	Close(context.Context) error
}

type BatchExporter struct {
	logger        *log.Logger
	sink          Sink
	batchSize     int
	flushInterval time.Duration
	flushTimeout  time.Duration
	events        <-chan pipeline.Event
}

func NewSink(cfg config.ExporterConfig) (Sink, error) {
	switch cfg.Kind {
	case "stdout":
		return &stdoutSink{}, nil
	case "file":
		return newFileSink(cfg.FilePath)
	case "http":
		if cfg.HTTPEndpoint == "" {
			return nil, errors.New("http exporter requires CHERRY_PICKER_EXPORTER_URL")
		}
		return &httpSink{
			client:       &http.Client{Timeout: cfg.HTTPTimeout},
			url:          cfg.HTTPEndpoint,
			retries:      cfg.HTTPRetries,
			retryBackoff: cfg.RetryBackoff,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported exporter kind %q", cfg.Kind)
	}
}

func NewBatchExporter(logger *log.Logger, sink Sink, batchSize int, flushInterval time.Duration, events <-chan pipeline.Event) *BatchExporter {
	if batchSize <= 0 {
		batchSize = 1
	}
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	return &BatchExporter{
		logger:        logger,
		sink:          sink,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		flushTimeout:  10 * time.Second,
		events:        events,
	}
}

func (be *BatchExporter) Run(ctx context.Context) error {
	defer be.sink.Close(context.Background())

	ticker := time.NewTicker(be.flushInterval)
	defer ticker.Stop()

	batch := make([]pipeline.Event, 0, be.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), be.flushTimeout)
		defer cancel()
		if err := be.sink.WriteBatch(flushCtx, batch); err != nil {
			be.logger.Printf("export batch failed: %v", err)
			} else {
				be.logger.Printf("export: %d events sent", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case event := <-be.events:
			batch = append(batch, event)
			if len(batch) >= be.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

type stdoutSink struct{}

func (s *stdoutSink) WriteBatch(_ context.Context, batch []pipeline.Event) error {
	encoder := json.NewEncoder(os.Stdout)
	for _, event := range batch {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *stdoutSink) Close(context.Context) error { return nil }

type fileSink struct {
	mu   sync.Mutex
	file *os.File
}

func newFileSink(path string) (Sink, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileSink{file: file}, nil
}

func (s *fileSink) WriteBatch(_ context.Context, batch []pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	encoder := json.NewEncoder(s.file)
	for _, event := range batch {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileSink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

type httpSink struct {
	client       *http.Client
	url          string
	retries      int
	retryBackoff time.Duration
	logger       *log.Logger
}

func (s *httpSink) WriteBatch(ctx context.Context, batch []pipeline.Event) error {
	payload, err := json.Marshal(map[string]any{"events": batch})
	if err != nil {
		return err
	}

	attempts := s.retries + 1
	if attempts <= 0 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")

		response, err := s.client.Do(request)
		if err == nil {
			if response.StatusCode < 300 {
				var result struct {
					Accepted   int `json:"accepted"`
					Duplicates int `json:"duplicates"`
					Errors     int `json:"errors"`
				}
				json.NewDecoder(response.Body).Decode(&result)
				response.Body.Close()
				log.Printf("api response: %d accepted, %d duplicates, %d errors", result.Accepted, result.Duplicates, result.Errors)
				return nil
			}
			response.Body.Close()
			lastErr = fmt.Errorf("http exporter returned status %s", response.Status)
		} else {
			lastErr = err
		}

		if attempt == attempts {
			break
		}

		backoff := time.Duration(attempt) * s.retryBackoff
		if backoff <= 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return lastErr
}

func (s *httpSink) Close(context.Context) error { return nil }
