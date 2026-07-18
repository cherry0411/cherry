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
	// M/R are the legacy on-disk encodings and remain readable forever. New
	// typed observations use F/S/H/R. M is semantically equivalent to F.
	recordMetadata byte = 'M'
	recordFull     byte = 'F'
	recordSummary  byte = 'S'
	recordHashOnly byte = 'H'
	recordRejected byte = 'R'
	recordSize          = 21
	maxRequestBody      = 128 << 20
)

type hashKey [20]byte

type store struct {
	mu               sync.RWMutex
	metadata         map[hashKey]struct{} // full, including legacy M
	summary          map[hashKey]struct{}
	hashOnly         map[hashKey]struct{}
	rejected         map[hashKey]struct{}
	baselineMetadata int
	baselineFull     int
	baselineSummary  int
	baselineHashOnly int
	baselineRejected int
	file             *os.File
	started          time.Time

	batchRequests       atomic.Uint64
	checkRequests       atomic.Uint64
	checkHashes         atomic.Uint64
	checkFound          atomic.Uint64
	rejectRequests      atomic.Uint64
	duplicates          atomic.Uint64
	invalid             atomic.Uint64
	observationRequests atomic.Uint64
	observationFull     atomic.Uint64
	observationSummary  atomic.Uint64
	observationHashOnly atomic.Uint64
	observationReject   atomic.Uint64
}

type crawlerEvent struct {
	Type       string `json:"type"`
	InstanceID string `json:"instance_id"`
	InfoHash   string `json:"info_hash"`
}

type batchRequest struct {
	Events []crawlerEvent `json:"events"`
}

type observation struct {
	InfoHash string `json:"info_hash"`
	Action   string `json:"action"`
}

type observationRequest struct {
	Observations []observation `json:"observations"`
}

type stringListFlag []string

func (values *stringListFlag) String() string { return strings.Join(*values, ",") }
func (values *stringListFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type mergeStats struct {
	MetadataAdded int
	RejectedAdded int
	FullAdded     int
	SummaryAdded  int
	HashOnlyAdded int
}

func main() {
	listen := flag.String("listen", "127.0.0.1:5070", "HTTP listen address")
	data := flag.String("data", "benchmark-hashes.bin", "append-only 21-byte record file")
	baseline := flag.String("baseline", "", "optional read-only oracle baseline; -data becomes this run's overlay")
	finalizeProduction := flag.String("finalize-production", "", "atomically merge overlays into this production oracle and exit")
	var mergeOverlays stringListFlag
	flag.Var(&mergeOverlays, "merge-overlay", "overlay to merge during -finalize-production; repeatable")
	flag.Parse()

	logger := log.New(os.Stdout, "benchmark-sink ", log.LstdFlags|log.Lmicroseconds)
	if *finalizeProduction != "" {
		if len(mergeOverlays) == 0 {
			logger.Fatal("-finalize-production requires at least one -merge-overlay")
		}
		stats, err := mergeStoresAtomically(*finalizeProduction, mergeOverlays)
		if err != nil {
			logger.Fatal(err)
		}
		logger.Printf("finalized: production=%s overlays=%d searchable_added=%d full_added=%d summary_added=%d hash_only_added=%d rejected_added=%d",
			*finalizeProduction, len(mergeOverlays), stats.MetadataAdded,
			stats.FullAdded, stats.SummaryAdded, stats.HashOnlyAdded, stats.RejectedAdded)
		return
	}
	if len(mergeOverlays) != 0 {
		logger.Fatal("-merge-overlay is valid only with -finalize-production")
	}
	s, err := openStoreWithBaseline(*data, *baseline)
	if err != nil {
		logger.Fatal(err)
	}
	defer s.close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/torrents/batch", s.handleBatch)
	mux.HandleFunc("POST /api/v1/oracle/observations", s.handleObservations)
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

	logger.Printf("started: listen=%s data=%s baseline=%s full=%d summary=%d hash_only=%d rejected=%d baseline_searchable=%d baseline_rejected=%d",
		*listen, *data, *baseline, len(s.metadata), len(s.summary), len(s.hashOnly), len(s.rejected),
		s.baselineMetadata, s.baselineRejected)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal(err)
	}
}

// mergeStoresAtomically produces a fully validated replacement next to the
// production file, fsyncs it, and only then replaces production. Metadata is
// merged before rejections across every overlay, so a successful fetch wins
// over a block-local rejection for a hash that production has not classified.
// The source overlays are immutable inputs and are never removed here.
func mergeStoresAtomically(productionPath string, overlayPaths []string) (mergeStats, error) {
	var stats mergeStats
	if len(overlayPaths) == 0 {
		return stats, errors.New("no oracle overlays supplied")
	}
	productionAbs, err := canonicalOraclePath(productionPath)
	if err != nil {
		return stats, fmt.Errorf("resolve production oracle: %w", err)
	}
	seenPaths := map[string]struct{}{productionAbs: {}}
	for _, path := range overlayPaths {
		overlayAbs, err := canonicalOraclePath(path)
		if err != nil {
			return stats, fmt.Errorf("resolve overlay %q: %w", path, err)
		}
		if _, exists := seenPaths[overlayAbs]; exists {
			return stats, fmt.Errorf("oracle paths must be distinct: %s", overlayAbs)
		}
		seenPaths[overlayAbs] = struct{}{}
	}

	dir := filepath.Dir(productionAbs)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return stats, fmt.Errorf("create oracle directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".oracle-finalize-*.bin")
	if err != nil {
		return stats, fmt.Errorf("create finalize file: %w", err)
	}
	tempPath := temp.Name()
	keepTemp := false
	defer func() {
		_ = temp.Close()
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if source, err := os.Open(productionAbs); err == nil {
		if _, err := io.Copy(temp, source); err != nil {
			source.Close()
			return stats, fmt.Errorf("copy production oracle: %w", err)
		}
		if err := source.Close(); err != nil {
			return stats, fmt.Errorf("close production oracle: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return stats, fmt.Errorf("open production oracle: %w", err)
	}
	if err := temp.Close(); err != nil {
		return stats, fmt.Errorf("close finalize seed: %w", err)
	}

	merged, err := openStore(tempPath)
	if err != nil {
		return stats, fmt.Errorf("validate production oracle: %w", err)
	}
	closeMerged := true
	defer func() {
		if closeMerged {
			_ = merged.close()
		}
	}()
	searchableBefore := len(merged.metadata) + len(merged.summary)
	rejectedBefore := len(merged.rejected)
	// Higher-information classifications win regardless of overlay order.
	// Legacy M is equal to full F. Sources remain immutable.
	for _, kind := range []byte{recordFull, recordMetadata, recordSummary, recordHashOnly, recordRejected} {
		for _, overlay := range overlayPaths {
			added, err := mergeRecordsOfKind(merged, overlay, kind)
			if err != nil {
				return mergeStats{}, fmt.Errorf("merge overlay %s: %w", overlay, err)
			}
			switch kind {
			case recordFull, recordMetadata:
				stats.FullAdded += added
			case recordSummary:
				stats.SummaryAdded += added
			case recordHashOnly:
				stats.HashOnlyAdded += added
			case recordRejected:
				stats.RejectedAdded += added
			}
		}
	}
	stats.MetadataAdded = max(0, len(merged.metadata)+len(merged.summary)-searchableBefore)
	stats.RejectedAdded = max(0, len(merged.rejected)-rejectedBefore)
	if err := merged.close(); err != nil {
		return mergeStats{}, fmt.Errorf("sync finalized oracle: %w", err)
	}
	closeMerged = false
	if err := replaceFile(tempPath, productionAbs); err != nil {
		return mergeStats{}, fmt.Errorf("replace production oracle: %w", err)
	}
	keepTemp = true // replaceFile moved the temporary path.
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return stats, nil
}

func canonicalOraclePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return resolved, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return abs, nil
	}
	return "", err
}

func mergeRecordsOfKind(target *store, path string, wanted byte) (int, error) {
	source, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	reader := bufio.NewReaderSize(source, 1<<20)
	record := make([]byte, recordSize)
	batch := make([]hashKey, 0, 4096)
	added := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		accepted, _, err := target.add(wanted, batch)
		added += accepted
		batch = batch[:0]
		return err
	}
	for {
		_, err := io.ReadFull(reader, record)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, errors.New("corrupt store: trailing partial record")
		}
		if err != nil {
			return 0, err
		}
		if !validRecordKind(record[0]) {
			return 0, fmt.Errorf("corrupt store: record type %q", record[0])
		}
		if record[0] != wanted {
			continue
		}
		var key hashKey
		copy(key[:], record[1:])
		batch = append(batch, key)
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return added, nil
}

func replaceFile(source, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}
	// Windows cannot replace an existing destination with Rename. This backup
	// path keeps the old oracle recoverable; Linux takes the atomic branch above.
	backup := target + ".finalize-backup"
	_ = os.Remove(backup)
	if err := os.Rename(target, backup); err != nil {
		return err
	}
	if err := os.Rename(source, target); err != nil {
		_ = os.Rename(backup, target)
		return err
	}
	return os.Remove(backup)
}

func openStore(path string) (*store, error) {
	return openStoreWithBaseline(path, "")
}

// openStoreWithBaseline loads an optional immutable experiment baseline before
// the writable data file. The in-memory maps provide normal global deduplication
// across both layers, while every newly accepted record is appended only to the
// overlay. Independent experiment blocks can therefore start from the exact
// same known set without copying or mutating the production oracle.
func openStoreWithBaseline(path, baselinePath string) (*store, error) {
	if baselinePath != "" {
		dataAbs, dataErr := filepath.Abs(path)
		baselineAbs, baselineErr := filepath.Abs(baselinePath)
		if dataErr != nil || baselineErr != nil {
			return nil, fmt.Errorf("resolve oracle paths: data=%v baseline=%v", dataErr, baselineErr)
		}
		if dataAbs == baselineAbs {
			return nil, errors.New("writable overlay and read-only baseline must be different files")
		}
		if dataInfo, dataErr := os.Stat(path); dataErr == nil {
			if baselineInfo, baselineErr := os.Stat(baselinePath); baselineErr == nil && os.SameFile(dataInfo, baselineInfo) {
				return nil, errors.New("writable overlay and read-only baseline resolve to the same file")
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s := &store{
		metadata: make(map[hashKey]struct{}),
		summary:  make(map[hashKey]struct{}),
		hashOnly: make(map[hashKey]struct{}),
		rejected: make(map[hashKey]struct{}),
		file:     f,
		started:  time.Now().UTC(),
	}
	if baselinePath != "" {
		baseline, err := os.Open(baselinePath)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("open baseline: %w", err)
		}
		if err := s.loadRecords(baseline); err != nil {
			baseline.Close()
			f.Close()
			return nil, fmt.Errorf("load baseline: %w", err)
		}
		if err := baseline.Close(); err != nil {
			f.Close()
			return nil, fmt.Errorf("close baseline: %w", err)
		}
		s.baselineFull = len(s.metadata)
		s.baselineSummary = len(s.summary)
		s.baselineHashOnly = len(s.hashOnly)
		s.baselineMetadata = s.baselineFull + s.baselineSummary
		s.baselineRejected = len(s.rejected)
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
	if err := s.loadRecords(s.file); err != nil {
		return err
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *store) loadRecords(source io.Reader) error {
	reader := bufio.NewReaderSize(source, 1<<20)
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
		if !validRecordKind(record[0]) {
			return fmt.Errorf("corrupt store: record type %q", record[0])
		}
		s.applyLoadedRecord(record[0], key)
	}
	return nil
}

func validRecordKind(kind byte) bool {
	switch kind {
	case recordMetadata, recordFull, recordSummary, recordHashOnly, recordRejected:
		return true
	default:
		return false
	}
}

func canonicalRecordKind(kind byte) byte {
	if kind == recordMetadata {
		return recordFull
	}
	return kind
}

func recordPriority(kind byte) int {
	switch canonicalRecordKind(kind) {
	case recordFull:
		return 4
	case recordSummary:
		return 3
	case recordHashOnly:
		return 2
	case recordRejected:
		return 1
	default:
		return 0
	}
}

func (s *store) currentKindLocked(key hashKey) byte {
	if _, ok := s.metadata[key]; ok {
		return recordFull
	}
	if _, ok := s.summary[key]; ok {
		return recordSummary
	}
	if _, ok := s.hashOnly[key]; ok {
		return recordHashOnly
	}
	if _, ok := s.rejected[key]; ok {
		return recordRejected
	}
	return 0
}

func (s *store) applyLoadedRecord(kind byte, key hashKey) {
	if recordPriority(kind) <= recordPriority(s.currentKindLocked(key)) {
		return
	}
	s.applyKindLocked(kind, key)
}

func (s *store) applyKindLocked(kind byte, key hashKey) {
	delete(s.metadata, key)
	delete(s.summary, key)
	delete(s.hashOnly, key)
	delete(s.rejected, key)
	switch canonicalRecordKind(kind) {
	case recordFull:
		s.metadata[key] = struct{}{}
	case recordSummary:
		s.summary[key] = struct{}{}
	case recordHashOnly:
		s.hashOnly[key] = struct{}{}
	case recordRejected:
		s.rejected[key] = struct{}{}
	}
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

func observationKind(action string) (byte, bool) {
	switch action {
	case "full":
		return recordFull, true
	case "summary":
		return recordSummary, true
	case "hash_only":
		return recordHashOnly, true
	case "reject":
		return recordRejected, true
	default:
		return 0, false
	}
}

// handleObservations accepts a closed hash+typed-action union. Invalid hashes,
// actions, unknown JSON fields, and trailing JSON reject the whole request;
// partial evidence is never made to look like a valid experiment window.
func (s *store) handleObservations(w http.ResponseWriter, r *http.Request) {
	s.observationRequests.Add(1)
	var request observationRequest
	if err := decodeClosedBody(w, r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	best := make(map[hashKey]byte, len(request.Observations))
	counts := make(map[byte]uint64, 4)
	duplicates := 0
	for i, item := range request.Observations {
		key, ok := parseHash(item.InfoHash)
		if !ok {
			s.invalid.Add(1)
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("observation %d has invalid info_hash", i)})
			return
		}
		kind, ok := observationKind(item.Action)
		if !ok {
			s.invalid.Add(1)
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("observation %d has invalid action", i)})
			return
		}
		counts[kind]++
		if previous, exists := best[key]; exists {
			duplicates++
			if recordPriority(kind) <= recordPriority(previous) {
				continue
			}
		}
		best[key] = kind
	}
	s.observationFull.Add(counts[recordFull])
	s.observationSummary.Add(counts[recordSummary])
	s.observationHashOnly.Add(counts[recordHashOnly])
	s.observationReject.Add(counts[recordRejected])

	accepted := 0
	for _, kind := range []byte{recordFull, recordSummary, recordHashOnly, recordRejected} {
		candidates := make([]hashKey, 0)
		for key, candidateKind := range best {
			if candidateKind == kind {
				candidates = append(candidates, key)
			}
		}
		added, repeated, err := s.add(kind, candidates)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		accepted += added
		duplicates += repeated
	}
	s.duplicates.Add(uint64(duplicates))
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": accepted, "duplicates": duplicates, "errors": 0,
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
		if s.currentKindLocked(key) != 0 {
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
	if !validRecordKind(kind) {
		return 0, 0, fmt.Errorf("invalid record kind %q", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := make(map[hashKey]struct{}, len(candidates))
	duplicates := 0
	for _, key := range candidates {
		if recordPriority(s.currentKindLocked(key)) >= recordPriority(kind) {
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
	if written, err := s.file.Write(buf); err != nil {
		return 0, duplicates, err
	} else if written != len(buf) {
		return 0, duplicates, io.ErrShortWrite
	}
	for key := range pending {
		s.applyKindLocked(kind, key)
	}
	return len(pending), duplicates, nil
}

func (s *store) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	full := len(s.metadata)
	summary := len(s.summary)
	hashOnly := len(s.hashOnly)
	metadata := full + summary
	rejected := len(s.rejected)
	baselineMetadata := s.baselineMetadata
	baselineRejected := s.baselineRejected
	baselineFull := s.baselineFull
	baselineSummary := s.baselineSummary
	baselineHashOnly := s.baselineHashOnly
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"started_at":                s.started,
		"metadata_unique":           metadata,
		"searchable_unique":         metadata,
		"full_unique":               full,
		"summary_unique":            summary,
		"hash_only_unique":          hashOnly,
		"rejected_unique":           rejected,
		"baseline_metadata_unique":  baselineMetadata,
		"baseline_rejected_unique":  baselineRejected,
		"baseline_full_unique":      baselineFull,
		"baseline_summary_unique":   baselineSummary,
		"baseline_hash_only_unique": baselineHashOnly,
		"overlay_metadata_unique":   metadata - baselineMetadata,
		"overlay_full_unique":       full - baselineFull,
		"overlay_summary_unique":    summary - baselineSummary,
		"overlay_hash_only_unique":  hashOnly - baselineHashOnly,
		"overlay_rejected_unique":   rejected - baselineRejected,
		"batch_requests":            s.batchRequests.Load(),
		"check_requests":            s.checkRequests.Load(),
		"check_hashes":              s.checkHashes.Load(),
		"check_found":               s.checkFound.Load(),
		"reject_requests":           s.rejectRequests.Load(),
		"metadata_duplicates":       s.duplicates.Load(),
		"invalid_hashes":            s.invalid.Load(),
		"observation_requests":      s.observationRequests.Load(),
		"observation_full":          s.observationFull.Load(),
		"observation_summary":       s.observationSummary.Load(),
		"observation_hash_only":     s.observationHashOnly.Load(),
		"observation_reject":        s.observationReject.Load(),
	})
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	return decoder.Decode(target)
}

func decodeClosedBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
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
