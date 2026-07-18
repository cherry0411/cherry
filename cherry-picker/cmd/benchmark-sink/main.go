// benchmark-sink is a deliberately small, persistent global-uniqueness oracle.
// It implements the crawler's HTTP endpoints but stores only 20-byte hashes,
// keeping benchmark overhead bounded on a 2C/4G crawler host.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	recordMetadata byte = 'M'
	recordRejected byte = 'R'
	recordSize          = 21
	maxRequestBody      = 128 << 20
)

type hashKey [20]byte

type store struct {
	mu       sync.RWMutex
	metadata map[hashKey]struct{}
	rejected map[hashKey]struct{}
	file     *os.File
	started  time.Time

	batchRequests  atomic.Uint64
	checkRequests  atomic.Uint64
	checkHashes    atomic.Uint64
	checkFound     atomic.Uint64
	rejectRequests atomic.Uint64
	duplicates     atomic.Uint64
	invalid        atomic.Uint64
}

type crawlerEvent struct {
	Type       string `json:"type"`
	InstanceID string `json:"instance_id"`
	InfoHash   string `json:"info_hash"`
}

type batchRequest struct {
	Events []crawlerEvent `json:"events"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:5070", "HTTP listen address")
	data := flag.String("data", "benchmark-hashes.bin", "append-only 21-byte record file")
	flag.Parse()

	logger := log.New(os.Stdout, "benchmark-sink ", log.LstdFlags|log.Lmicroseconds)
	s, err := openStore(*data)
	if err != nil {
		logger.Fatal(err)
	}
	defer s.close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/torrents/batch", s.handleBatch)
	mux.HandleFunc("POST /api/v1/torrents/check", s.handleCheck)
	mux.HandleFunc("POST /api/v1/torrents/reject", s.handleReject)
	mux.HandleFunc("POST /api/v1/torrents/peers", emptyOK)
	mux.HandleFunc("GET /api/v1/torrents/pending", emptyList)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go s.syncLoop(ctx, logger)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Printf("started: listen=%s data=%s metadata=%d rejected=%d", *listen, *data, len(s.metadata), len(s.rejected))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal(err)
	}
}

func openStore(path string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s := &store{
		metadata: make(map[hashKey]struct{}),
		rejected: make(map[hashKey]struct{}),
		file:     f,
		started:  time.Now().UTC(),
	}
	if err := s.load(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *store) load() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(s.file, 1<<20)
	record := make([]byte, recordSize)
	for {
		_, err := io.ReadFull(reader, record)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("corrupt store: trailing partial record")
		}
		if err != nil {
			return err
		}
		var key hashKey
		copy(key[:], record[1:])
		switch record[0] {
		case recordMetadata:
			s.metadata[key] = struct{}{}
		case recordRejected:
			s.rejected[key] = struct{}{}
		default:
			return fmt.Errorf("corrupt store: record type %q", record[0])
		}
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *store) syncLoop(ctx context.Context, logger *log.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			err := s.file.Sync()
			s.mu.Unlock()
			if err != nil {
				logger.Printf("sync data: %v", err)
			}
		}
	}
}

func parseHash(value string) (hashKey, bool) {
	var key hashKey
	value = strings.TrimSpace(value)
	if len(value) != 40 {
		return key, false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(key) {
		return key, false
	}
	copy(key[:], decoded)
	return key, true
}

func (s *store) handleBatch(w http.ResponseWriter, r *http.Request) {
	s.batchRequests.Add(1)
	var request batchRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	candidates := make([]hashKey, 0, len(request.Events))
	for _, event := range request.Events {
		if event.Type != "metadata_fetched" {
			continue
		}
		key, ok := parseHash(event.InfoHash)
		if !ok {
			s.invalid.Add(1)
			continue
		}
		candidates = append(candidates, key)
	}
	accepted, duplicates, err := s.add(recordMetadata, candidates)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	s.duplicates.Add(uint64(duplicates))
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": accepted, "duplicates": duplicates, "errors": 0, "backpressure": false,
	})
}

func (s *store) handleCheck(w http.ResponseWriter, r *http.Request) {
	s.checkRequests.Add(1)
	var hashes []string
	if err := decodeBody(w, r, &hashes); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	found := make([]string, 0, len(hashes))
	valid := 0
	s.mu.RLock()
	for _, value := range hashes {
		key, ok := parseHash(value)
		if !ok {
			continue
		}
		valid++
		if _, ok := s.metadata[key]; ok {
			found = append(found, strings.ToLower(value))
			continue
		}
		if _, ok := s.rejected[key]; ok {
			found = append(found, strings.ToLower(value))
		}
	}
	s.mu.RUnlock()
	s.checkHashes.Add(uint64(valid))
	s.checkFound.Add(uint64(len(found)))
	writeJSON(w, http.StatusOK, found)
}

func (s *store) handleReject(w http.ResponseWriter, r *http.Request) {
	s.rejectRequests.Add(1)
	var hashes []string
	if err := decodeBody(w, r, &hashes); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	candidates := make([]hashKey, 0, len(hashes))
	for _, value := range hashes {
		if key, ok := parseHash(value); ok {
			candidates = append(candidates, key)
		}
	}
	accepted, _, err := s.add(recordRejected, candidates)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"accepted": accepted})
}

func (s *store) add(kind byte, candidates []hashKey) (int, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.metadata
	if kind == recordRejected {
		target = s.rejected
	}
	pending := make(map[hashKey]struct{}, len(candidates))
	duplicates := 0
	for _, key := range candidates {
		if _, exists := s.metadata[key]; exists {
			duplicates++
			continue
		}
		if _, exists := s.rejected[key]; exists {
			duplicates++
			continue
		}
		if _, exists := pending[key]; exists {
			duplicates++
			continue
		}
		pending[key] = struct{}{}
	}
	if len(pending) == 0 {
		return 0, duplicates, nil
	}
	buf := make([]byte, 0, len(pending)*recordSize)
	for key := range pending {
		buf = append(buf, kind)
		buf = append(buf, key[:]...)
	}
	if _, err := s.file.Write(buf); err != nil {
		return 0, duplicates, err
	}
	for key := range pending {
		target[key] = struct{}{}
	}
	return len(pending), duplicates, nil
}

func (s *store) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	metadata := len(s.metadata)
	rejected := len(s.rejected)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"started_at":          s.started,
		"metadata_unique":     metadata,
		"rejected_unique":     rejected,
		"batch_requests":      s.batchRequests.Load(),
		"check_requests":      s.checkRequests.Load(),
		"check_hashes":        s.checkHashes.Load(),
		"check_found":         s.checkFound.Load(),
		"reject_requests":     s.rejectRequests.Load(),
		"metadata_duplicates": s.duplicates.Load(),
		"invalid_hashes":      s.invalid.Load(),
	})
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	return decoder.Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func emptyOK(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func emptyList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []string{})
}
