package spool

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options configures a Spool. Zero values use bounded 2C4G-safe defaults.
type Options struct {
	Dir           string
	CrawlerID     string
	SegmentBytes  int64
	SyncEveryN    int
	SyncInterval  time.Duration
	MaxBytes      int64
	HighWatermark float64
	LowWatermark  float64
	MaxBatchBytes int64
}

func (o *Options) withDefaults() error {
	if o.Dir == "" {
		return errors.New("spool: Dir is required")
	}
	if !validBoundedText(o.CrawlerID, maxCrawlerIDBytes, false) {
		return errors.New("spool: valid CrawlerID is required")
	}
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	if o.SegmentBytes < headerSize+1 {
		return errors.New("spool: SegmentBytes is too small")
	}
	if o.SyncEveryN <= 0 {
		o.SyncEveryN = 128
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 50 * time.Millisecond
	}
	if o.MaxBytes < 0 {
		return errors.New("spool: MaxBytes cannot be negative")
	}
	if o.HighWatermark == 0 {
		o.HighWatermark = 0.9
	}
	if o.LowWatermark == 0 {
		o.LowWatermark = 0.7
	}
	if o.HighWatermark <= 0 || o.HighWatermark > 1 || o.LowWatermark <= 0 || o.LowWatermark >= o.HighWatermark {
		return errors.New("spool: invalid capacity watermarks")
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = 16 << 20
	}
	if o.MaxBatchBytes < maxRecordLength+headerSize {
		return fmt.Errorf("spool: MaxBatchBytes must be at least %d", maxRecordLength+headerSize)
	}
	if o.MaxBatchBytes > 64<<20 {
		return errors.New("spool: MaxBatchBytes cannot exceed 64 MiB")
	}
	if o.MaxBytes > 0 {
		if o.SegmentBytes > math.MaxInt64-o.MaxBatchBytes {
			return errors.New("spool: SegmentBytes plus MaxBatchBytes overflows int64")
		}
		high := int64(float64(o.MaxBytes) * o.HighWatermark)
		// This is a fail-fast liveness bound: a retained active segment plus
		// one maximum production batch must fit below the rejection threshold,
		// otherwise a rejected append cannot rotate and make progress.
		if high <= o.SegmentBytes+o.MaxBatchBytes {
			return fmt.Errorf("spool: capacity high-watermark (%d) must exceed SegmentBytes + MaxBatchBytes (%d)",
				high, o.SegmentBytes+o.MaxBatchBytes)
		}
	}
	return nil
}

const (
	cursorSchemaVersion   = 1
	cursorEnvelopeVersion = 1
)

// cursorState is both the durable delivery cursor and the spool-directory
// identity manifest. Epoch is created once for a new directory and never
// changes on Open.
type cursorState struct {
	Version       int       `json:"version"`
	CrawlerID     string    `json:"crawler_id"`
	Epoch         uint64    `json:"epoch"`
	NextSequence  uint64    `json:"next_sequence"`
	AckedSequence uint64    `json:"acked_sequence"`
	AckedSegment  segmentID `json:"acked_segment"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type cursorEnvelope struct {
	Version int         `json:"version"`
	State   cursorState `json:"state"`
	SHA256  string      `json:"sha256"`
}

type segmentFile interface {
	io.Writer
	io.Seeker
	Sync() error
	Truncate(int64) error
	Close() error
}

var (
	ErrAtCapacity    = errors.New("spool: at capacity high-watermark")
	ErrClosed        = errors.New("spool: closed")
	ErrPoisoned      = errors.New("spool: poisoned; restart and recover before use")
	ErrBadSequence   = errors.New("spool: sequence is outside the appended range")
	ErrEmptyBatch    = errors.New("spool: durable append batch is empty")
	ErrBatchTooLarge = errors.New("spool: durable append batch exceeds MaxBatchBytes")
)

// DurableAppend is returned only after every record in the range and required
// spool metadata have been synchronized successfully.
type DurableAppend struct {
	CrawlerID   string
	Epoch       uint64
	StartSeq    uint64
	EndSeq      uint64
	RecordCount int
}

type inflightBatch struct {
	token       uint64
	crawlerID   string
	epoch       uint64
	start       uint64
	end         uint64
	recordCount int
	lastSegment segmentID
}

// Spool is a locked, single-writer durable log. Append writes but does not make
// a record durable; SyncThrough is the group-writer durability boundary.
type Spool struct {
	opts     Options
	lockFile *os.File

	mu              sync.Mutex
	activeID        segmentID
	activePath      string
	activeFile      segmentFile
	activeSize      int64
	unsynced        int
	needsSync       bool
	lastSync        time.Time
	durableSequence uint64
	cursor          cursorState
	atCapacity      bool
	closed          bool
	poisonErr       error
	batchLoading    bool
	inflight        *inflightBatch
	nextBatchToken  uint64

	syncStop chan struct{}
	syncDone chan struct{}
	syncErr  chan error

	// createSegment is injectable only for deterministic storage fault tests.
	// Production always uses createSegmentFile.
	createSegment func(string) (segmentFile, error)
}

func createSegmentFile(path string) (segmentFile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
}

func Open(opts Options) (*Spool, error) {
	if err := opts.withDefaults(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("spool: create dir: %w", err)
	}
	lock, err := acquireLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	s := &Spool{
		opts:          opts,
		lockFile:      lock,
		lastSync:      time.Now(),
		syncStop:      make(chan struct{}),
		syncDone:      make(chan struct{}),
		syncErr:       make(chan error, 1),
		createSegment: createSegmentFile,
	}
	cursorExists, err := s.loadCursor()
	if err == nil {
		err = s.recover(cursorExists)
	}
	if err == nil {
		err = s.updateCapacityLocked()
	}
	if err != nil {
		if s.activeFile != nil {
			_ = s.activeFile.Close()
		}
		_ = releaseLock(lock, opts.Dir)
		return nil, err
	}
	go s.syncLoop()
	return s, nil
}

func (s *Spool) cursorPath() string { return filepath.Join(s.opts.Dir, cursorName) }

func (s *Spool) loadCursor() (bool, error) {
	data, err := os.ReadFile(s.cursorPath())
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("spool: read cursor: %w", err)
	}
	var envelope cursorEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return false, fmt.Errorf("%w: decode cursor: %v", ErrCorruption, err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return false, fmt.Errorf("%w: %v", ErrCorruption, err)
	}
	if envelope.Version != cursorEnvelopeVersion {
		return false, fmt.Errorf("%w: unsupported cursor envelope version %d", ErrCorruption, envelope.Version)
	}
	payload, err := json.Marshal(envelope.State)
	if err != nil {
		return false, fmt.Errorf("%w: canonicalize cursor: %v", ErrCorruption, err)
	}
	want := sha256.Sum256(payload)
	got, err := hex.DecodeString(envelope.SHA256)
	if err != nil || len(got) != sha256.Size || !bytes.Equal(got, want[:]) {
		return false, fmt.Errorf("%w: cursor checksum mismatch", ErrCorruption)
	}
	c := envelope.State
	if c.Version != cursorSchemaVersion || c.CrawlerID != s.opts.CrawlerID || c.Epoch == 0 || c.Epoch > math.MaxInt64 ||
		c.AckedSequence > math.MaxInt64 || c.NextSequence == 0 || c.NextSequence > uint64(math.MaxInt64)+1 ||
		c.NextSequence <= c.AckedSequence || c.AckedSegment == 0 {
		return false, fmt.Errorf("%w: invalid cursor identity or bounds", ErrCorruption)
	}
	s.cursor = c
	return true, nil
}

func (s *Spool) saveCursorLocked() error {
	// NextSequence on disk is a durability boundary, not merely the next value
	// reserved in process memory. Otherwise committing an older durable batch
	// while newer Append calls are still unsynced could persist a cursor ahead
	// of the bytes that survive a crash.
	persisted := s.cursor
	persisted.NextSequence = s.durableSequence + 1
	if persisted.NextSequence == 0 || persisted.NextSequence <= persisted.AckedSequence {
		return fmt.Errorf("%w: cursor durability boundary is behind ack", ErrCorruption)
	}
	persisted.UpdatedAt = time.Now().UTC()
	payload, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(cursorEnvelope{
		Version: cursorEnvelopeVersion,
		State:   persisted,
		SHA256:  hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return err
	}
	tmp := s.cursorPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if os.IsExist(err) {
		if removeErr := os.Remove(tmp); removeErr != nil {
			return fmt.Errorf("spool: remove stale cursor temp: %w", removeErr)
		}
		f, err = os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	}
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if n, err := f.Write(data); err != nil || n != len(data) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.cursorPath()); err != nil {
		return err
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return err
	}
	s.cursor.UpdatedAt = persisted.UpdatedAt
	ok = true
	return nil
}

func (s *Spool) recover(cursorExists bool) error {
	if err := s.recoverBatchIntent(cursorExists); err != nil {
		return err
	}
	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		return err
	}
	if !cursorExists {
		if len(ids) != 0 {
			return fmt.Errorf("%w: segments exist without cursor", ErrCorruption)
		}
		epoch, err := generateEpoch()
		if err != nil {
			return err
		}
		s.cursor = cursorState{
			Version:      cursorSchemaVersion,
			CrawlerID:    s.opts.CrawlerID,
			Epoch:        epoch,
			NextSequence: 1,
			AckedSegment: 1,
		}
		// Cursor first: if segment creation is interrupted, the next Open can
		// finish initialization without minting a new epoch.
		if err := s.saveCursorLocked(); err != nil {
			return fmt.Errorf("spool: initialize cursor: %w", err)
		}
		return s.openNewSegmentLocked(1)
	}

	if len(ids) == 0 {
		// The only valid zero-segment state is an interrupted first creation.
		if s.cursor.NextSequence != 1 || s.cursor.AckedSequence != 0 || s.cursor.AckedSegment != 1 {
			return fmt.Errorf("%w: established spool has no segments", ErrCorruption)
		}
		return s.openNewSegmentLocked(1)
	}
	// A durable ACK is persisted before deletion of fully acknowledged sealed
	// segments. A crash between those operations legitimately leaves an old
	// prefix behind. The cursor segment itself must still be present.
	if ids[0] > s.cursor.AckedSegment || ids[len(ids)-1] < s.cursor.AckedSegment {
		return fmt.Errorf("%w: cursor segment %d is outside retained range %d..%d", ErrCorruption,
			s.cursor.AckedSegment, ids[0], ids[len(ids)-1])
	}

	active := ids[len(ids)-1]
	var first, high uint64
	for _, id := range ids {
		path := segmentPath(s.opts.Dir, id)
		res, scanErr := scanSegment(path, id, func(_ segmentID, _, _ int64, payload []byte) error {
			rec, err := decodeRecord(payload)
			if err != nil {
				return fmt.Errorf("%w: invalid typed record in %s: %v", ErrCorruption, segmentName(id), err)
			}
			if rec.CrawlerID != s.cursor.CrawlerID || rec.Epoch != s.cursor.Epoch {
				return fmt.Errorf("%w: record identity mismatch in %s", ErrCorruption, segmentName(id))
			}
			if high != 0 && rec.Sequence != high+1 {
				return fmt.Errorf("%w: sequence gap %d -> %d", ErrCorruption, high, rec.Sequence)
			}
			if first == 0 {
				first = rec.Sequence
			}
			high = rec.Sequence
			return nil
		})
		if scanErr != nil {
			return scanErr
		}
		if res.TornTail {
			if id != active {
				return fmt.Errorf("%w: torn tail in sealed %s", ErrCorruption, segmentName(id))
			}
			if err := truncateDurable(path, res.ValidOffset, s.opts.Dir); err != nil {
				return fmt.Errorf("spool: truncate torn active tail: %w", err)
			}
		}
	}
	if first == 0 {
		// A fully drained commit rotates to a new empty active segment, then
		// durably points AckedSegment at it before deleting old data segments.
		// After cleanup there are intentionally no retained frames; the ACK is
		// the recovered high/durable baseline. No other empty layout is valid.
		if len(ids) != 1 || ids[0] != s.cursor.AckedSegment {
			return fmt.Errorf("%w: empty retained log does not match cursor segment %d", ErrCorruption, s.cursor.AckedSegment)
		}
		high = s.cursor.AckedSequence
	}
	if first != 0 && first > s.cursor.AckedSequence+1 {
		return fmt.Errorf("%w: first retained sequence %d is after cursor %d", ErrCorruption, first, s.cursor.AckedSequence)
	}
	if high < s.cursor.AckedSequence {
		return fmt.Errorf("%w: cursor ack %d exceeds high sequence %d", ErrCorruption, s.cursor.AckedSequence, high)
	}
	if s.cursor.NextSequence > high+1 {
		return fmt.Errorf("%w: cursor next %d exceeds recovered next %d", ErrCorruption, s.cursor.NextSequence, high+1)
	}

	path := segmentPath(s.opts.Dir, active)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("spool: reopen active segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: stat active segment: %w", err)
	}
	// Make every complete frame that survived recovery durable before exposing
	// it to NextBatch.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: sync recovered active segment: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: seek recovered active segment: %w", err)
	}
	s.activeFile = f
	s.activePath = path
	s.activeID = active
	s.activeSize = info.Size()
	s.durableSequence = high
	s.cursor.NextSequence = high + 1
	if s.cursor.NextSequence == 0 {
		_ = f.Close()
		return fmt.Errorf("%w: sequence overflow", ErrCorruption)
	}
	if err := s.saveCursorLocked(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: persist recovered cursor: %w", err)
	}
	if err := s.deleteFullyAckedLocked(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: recover acknowledged segment cleanup: %w", err)
	}
	return nil
}

func generateEpoch() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("spool: generate epoch: %w", err)
		}
		if epoch := binary.BigEndian.Uint64(b[:]) & uint64(^uint64(0)>>1); epoch != 0 {
			return epoch, nil
		}
	}
}

func (s *Spool) openNewSegmentLocked(id segmentID) error {
	path := segmentPath(s.opts.Dir, id)
	f, err := s.createSegment(path)
	if err != nil {
		return fmt.Errorf("spool: create segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: sync new segment: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: sync dir after segment create: %w", err)
	}
	s.activeFile = f
	s.activePath = path
	s.activeID = id
	s.activeSize = 0
	return nil
}

func (s *Spool) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor.Epoch
}

// Append assigns identity and writes a complete frame. A nil error means the
// bytes were accepted into the active file, not that they are durable. Call
// SyncThrough(sequence) before handing the record to an external consumer.
func (s *Spool) Append(rec Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return 0, err
	}
	if s.atCapacity {
		return 0, ErrAtCapacity
	}
	if s.cursor.NextSequence > math.MaxInt64 {
		return 0, s.poisonLocked(fmt.Errorf("spool: sequence exhausted PostgreSQL bigint range"))
	}

	rec.CrawlerID = s.opts.CrawlerID
	rec.Epoch = s.cursor.Epoch
	rec.Sequence = s.cursor.NextSequence
	payload, err := rec.encode()
	if err != nil {
		return 0, err
	}
	if len(payload) > maxRecordLength {
		return 0, ErrRecordTooLarge
	}
	if err := s.writeFrameLocked(encodeFrame(payload)); err != nil {
		return 0, err
	}
	s.cursor.NextSequence++
	s.unsynced++
	s.needsSync = true
	seq := rec.Sequence
	if s.unsynced >= s.opts.SyncEveryN {
		if err := s.syncLocked(); err != nil {
			return seq, err
		}
	}
	if err := s.updateCapacityLocked(); err != nil {
		return seq, s.poisonLocked(err)
	}
	return seq, nil
}

// AppendBatchDurable is the production producer API. It validates the entire
// typed batch before writing, appends it in one sequence range, synchronizes all
// touched segments, and durably persists the resulting cursor metadata. Any
// storage failure poisons the spool; no further append/read/commit is allowed.
func (s *Spool) AppendBatchDurable(records []Record) (DurableAppend, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return DurableAppend{}, err
	}
	if len(records) == 0 {
		return DurableAppend{}, ErrEmptyBatch
	}
	// Every record requires at least a frame header and one payload byte. Reject
	// an impossible slice before allocating a pointer array proportional to an
	// untrusted caller-provided record count.
	if int64(len(records)) > s.opts.MaxBatchBytes/(headerSize+1) {
		return DurableAppend{}, ErrBatchTooLarge
	}
	if s.atCapacity {
		return DurableAppend{}, ErrAtCapacity
	}
	if s.cursor.NextSequence > math.MaxInt64 || uint64(len(records)-1) > uint64(math.MaxInt64)-s.cursor.NextSequence {
		return DurableAppend{}, s.poisonLocked(fmt.Errorf("spool: sequence overflow"))
	}

	start := s.cursor.NextSequence
	frames := make([][]byte, len(records))
	var frameBytes int64
	for i := range records {
		rec := records[i]
		rec.CrawlerID = s.opts.CrawlerID
		rec.Epoch = s.cursor.Epoch
		rec.Sequence = start + uint64(i)
		payload, err := rec.encode()
		if err != nil {
			return DurableAppend{}, fmt.Errorf("spool: validate durable batch record %d: %w", i, err)
		}
		if len(payload) > maxRecordLength {
			return DurableAppend{}, fmt.Errorf("spool: durable batch record %d: %w", i, ErrRecordTooLarge)
		}
		frames[i] = encodeFrame(payload)
		if int64(len(frames[i])) > s.opts.MaxBatchBytes-frameBytes {
			return DurableAppend{}, ErrBatchTooLarge
		}
		frameBytes += int64(len(frames[i]))
	}
	if s.opts.MaxBytes > 0 {
		used, err := s.pendingBytesLocked()
		if err != nil {
			return DurableAppend{}, s.poisonLocked(err)
		}
		high := int64(float64(s.opts.MaxBytes) * s.opts.HighWatermark)
		if frameBytes > math.MaxInt64-used || used+frameBytes >= high {
			return DurableAppend{}, ErrAtCapacity
		}
	}
	// The durable intent records a byte offset that recovery must be able to
	// truncate back to after sudden power loss. Make every pre-intent Append
	// frame durable before capturing that offset; otherwise the marker could
	// outlive its own rollback starting point.
	if s.needsSync {
		if err := s.syncLocked(); err != nil {
			return DurableAppend{}, err
		}
	}
	startCursor := s.cursor
	startActiveID := s.activeID
	startActiveSize := s.activeSize
	startAtCapacity := s.atCapacity
	intent := batchIntent{
		CrawlerID:    s.cursor.CrawlerID,
		Epoch:        s.cursor.Epoch,
		StartSeq:     start,
		StartSegment: startActiveID,
		StartOffset:  startActiveSize,
	}
	if err := s.writeBatchIntentLocked(intent); err != nil {
		return DurableAppend{}, s.poisonLocked(fmt.Errorf("spool: persist durable batch intent: %w", err))
	}

	for i := range frames {
		if err := s.writeFrameLocked(frames[i]); err != nil {
			cause := fmt.Errorf("spool: durable batch write record %d: %w", i, err)
			return DurableAppend{}, s.abortDurableBatchLocked(startCursor, startActiveID, startActiveSize, startAtCapacity, cause)
		}
		s.cursor.NextSequence++
		s.unsynced++
		s.needsSync = true
	}
	if err := s.syncLocked(); err != nil {
		return DurableAppend{}, s.abortDurableBatchLocked(startCursor, startActiveID, startActiveSize, startAtCapacity, err)
	}
	if err := s.saveCursorLocked(); err != nil {
		cause := fmt.Errorf("spool: persist durable append cursor: %w", err)
		return DurableAppend{}, s.abortDurableBatchLocked(startCursor, startActiveID, startActiveSize, startAtCapacity, cause)
	}
	if err := s.updateCapacityLocked(); err != nil {
		return DurableAppend{}, s.abortDurableBatchLocked(startCursor, startActiveID, startActiveSize, startAtCapacity, err)
	}
	if err := s.clearBatchIntentLocked(); err != nil {
		return DurableAppend{}, s.abortDurableBatchLocked(startCursor, startActiveID, startActiveSize, startAtCapacity, err)
	}
	return DurableAppend{
		CrawlerID:   s.cursor.CrawlerID,
		Epoch:       s.cursor.Epoch,
		StartSeq:    start,
		EndSeq:      s.cursor.NextSequence - 1,
		RecordCount: len(records),
	}, nil
}

// abortDurableBatchLocked makes AppendBatchDurable definitive: after any
// storage failure, the batch either has a receipt or none of its frames can be
// recovered. The spool remains poisoned because the underlying storage fault
// still requires restart and inspection even when rollback succeeds.
func (s *Spool) abortDurableBatchLocked(start cursorState, startID segmentID, startOffset int64, startAtCapacity bool, cause error) error {
	rollbackErr := s.rollbackDurableBatchLocked(start, startID, startOffset, startAtCapacity)
	if rollbackErr == nil {
		rollbackErr = s.clearBatchIntentLocked()
	}
	if s.poisonErr == nil {
		return s.poisonLocked(errors.Join(cause, rollbackErr))
	}
	if rollbackErr != nil {
		s.poisonErr = errors.Join(s.poisonErr, fmt.Errorf("spool: durable batch rollback: %w", rollbackErr))
	}
	return s.poisonErr
}

func (s *Spool) rollbackDurableBatchLocked(start cursorState, startID segmentID, startOffset int64, startAtCapacity bool) error {
	var errs []error
	if s.activeFile != nil {
		if err := s.activeFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close failed batch segment: %w", err))
		}
		s.activeFile = nil
	}

	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		errs = append(errs, err)
	} else {
		for i := len(ids) - 1; i >= 0; i-- {
			if ids[i] <= startID {
				break
			}
			if err := os.Remove(segmentPath(s.opts.Dir, ids[i])); err != nil {
				errs = append(errs, fmt.Errorf("remove failed batch %s: %w", segmentName(ids[i]), err))
			}
		}
	}

	startPath := segmentPath(s.opts.Dir, startID)
	f, openErr := os.OpenFile(startPath, os.O_RDWR, 0o600)
	if openErr != nil {
		errs = append(errs, fmt.Errorf("reopen batch start %s: %w", segmentName(startID), openErr))
	} else {
		info, err := f.Stat()
		if err != nil {
			errs = append(errs, fmt.Errorf("stat batch start: %w", err))
		} else if startOffset > info.Size() {
			errs = append(errs, fmt.Errorf("%w: batch start offset %d exceeds segment size %d", ErrCorruption, startOffset, info.Size()))
		} else if err := f.Truncate(startOffset); err != nil {
			errs = append(errs, fmt.Errorf("truncate batch start to %d: %w", startOffset, err))
		}
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			errs = append(errs, fmt.Errorf("seek batch start to %d: %w", startOffset, err))
		}
		if err := f.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync batch rollback: %w", err))
		}
		s.activeFile = f
		s.activePath = startPath
		s.activeID = startID
		s.activeSize = startOffset
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		errs = append(errs, fmt.Errorf("sync directory after batch rollback: %w", err))
	}

	s.cursor = start
	s.durableSequence = start.NextSequence - 1
	s.unsynced = 0
	s.needsSync = false
	s.lastSync = time.Now()
	s.atCapacity = startAtCapacity
	if s.activeFile != nil {
		if err := s.saveCursorLocked(); err != nil {
			errs = append(errs, fmt.Errorf("persist rolled-back cursor: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (s *Spool) writeFrameLocked(frame []byte) error {
	frameSize := int64(len(frame))
	if frameSize <= 0 || s.activeSize < 0 || s.activeSize > math.MaxInt64-frameSize {
		return s.poisonLocked(fmt.Errorf("%w: active segment size overflow", ErrCorruption))
	}
	if s.activeSize > 0 && s.activeSize+frameSize > s.opts.SegmentBytes {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	start := s.activeSize
	n, writeErr := s.activeFile.Write(frame)
	if writeErr != nil || n != len(frame) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		rollbackErr := s.rollbackAppendLocked(start)
		if rollbackErr != nil {
			return s.poisonLocked(errors.Join(fmt.Errorf("spool: append write: %w", writeErr), rollbackErr))
		}
		return fmt.Errorf("spool: append rolled back after write failure: %w", writeErr)
	}
	s.activeSize += int64(n)
	return nil
}

func (s *Spool) rollbackAppendLocked(start int64) error {
	if err := s.activeFile.Truncate(start); err != nil {
		return fmt.Errorf("spool: rollback truncate: %w", err)
	}
	if _, err := s.activeFile.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("spool: rollback seek: %w", err)
	}
	s.activeSize = start
	if err := s.activeFile.Sync(); err != nil {
		return fmt.Errorf("spool: rollback sync: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return fmt.Errorf("spool: rollback directory sync: %w", err)
	}
	return nil
}

func (s *Spool) rotateLocked() error {
	if err := s.syncLocked(); err != nil {
		return err
	}
	if err := s.activeFile.Close(); err != nil {
		return s.poisonLocked(fmt.Errorf("spool: close sealed segment: %w", err))
	}
	if s.activeID == segmentID(math.MaxUint64) {
		return s.poisonLocked(fmt.Errorf("spool: segment id exhausted"))
	}
	if err := s.openNewSegmentLocked(s.activeID + 1); err != nil {
		return s.poisonLocked(err)
	}
	return nil
}

// Sync forces all accepted records durable.
func (s *Spool) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	if !s.needsSync {
		return nil
	}
	return s.syncLocked()
}

// SyncThrough is the group-writer durability API. It returns nil only when the
// requested sequence and every sequence before it have been successfully
// synchronized.
func (s *Spool) SyncThrough(sequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	if sequence == 0 || sequence >= s.cursor.NextSequence {
		return fmt.Errorf("%w: %d", ErrBadSequence, sequence)
	}
	if s.durableSequence >= sequence {
		return nil
	}
	if !s.needsSync {
		return s.poisonLocked(fmt.Errorf("spool: durability invariant violated for sequence %d", sequence))
	}
	if err := s.syncLocked(); err != nil {
		return err
	}
	if s.durableSequence < sequence {
		return s.poisonLocked(fmt.Errorf("spool: sync did not cover sequence %d", sequence))
	}
	return nil
}

func (s *Spool) syncLocked() error {
	if err := s.activeFile.Sync(); err != nil {
		return s.poisonLocked(fmt.Errorf("spool: sync active segment: %w", err))
	}
	s.durableSequence = s.cursor.NextSequence - 1
	s.unsynced = 0
	s.needsSync = false
	s.lastSync = time.Now()
	return nil
}

func (s *Spool) DurableSequence() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return 0, err
	}
	return s.durableSequence, nil
}

func (s *Spool) syncLoop() {
	defer close(s.syncDone)
	ticker := time.NewTicker(s.opts.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.syncStop:
			return
		case <-ticker.C:
			s.mu.Lock()
			if s.closed || s.poisonErr != nil || !s.needsSync || time.Since(s.lastSync) < s.opts.SyncInterval {
				s.mu.Unlock()
				continue
			}
			_ = s.syncLocked()
			s.mu.Unlock()
		}
	}
}

func (s *Spool) SyncError() <-chan error { return s.syncErr }

func (s *Spool) healthLocked() error {
	if s.closed {
		return ErrClosed
	}
	if s.poisonErr != nil {
		return s.poisonErr
	}
	return nil
}

func (s *Spool) poisonLocked(cause error) error {
	if s.poisonErr == nil {
		s.poisonErr = fmt.Errorf("%w: %v", ErrPoisoned, cause)
		select {
		case s.syncErr <- s.poisonErr:
		default:
		}
	}
	return s.poisonErr
}

func (s *Spool) updateCapacityLocked() error {
	if s.opts.MaxBytes <= 0 {
		return nil
	}
	used, err := s.pendingBytesLocked()
	if err != nil {
		return err
	}
	high := int64(float64(s.opts.MaxBytes) * s.opts.HighWatermark)
	low := int64(float64(s.opts.MaxBytes) * s.opts.LowWatermark)
	if !s.atCapacity && used >= high {
		s.atCapacity = true
	} else if s.atCapacity && used <= low {
		s.atCapacity = false
	}
	return nil
}

func (s *Spool) pendingBytesLocked() (int64, error) {
	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, id := range ids {
		if id < s.cursor.AckedSegment {
			continue
		}
		info, err := os.Stat(segmentPath(s.opts.Dir, id))
		if err != nil {
			return 0, fmt.Errorf("spool: stat %s for capacity: %w", segmentName(id), err)
		}
		if info.Size() < 0 || info.Size() > math.MaxInt64-total {
			return 0, fmt.Errorf("%w: retained segment byte count overflow", ErrCorruption)
		}
		total += info.Size()
	}
	return total, nil
}

func (s *Spool) PendingBytes() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return 0, err
	}
	return s.pendingBytesLocked()
}

func (s *Spool) AtCapacity() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return false, err
	}
	return s.atCapacity, nil
}

func (s *Spool) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.syncStop)
	s.mu.Unlock()
	<-s.syncDone

	s.mu.Lock()
	var errs []error
	if s.needsSync && s.activeFile != nil {
		if err := s.activeFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("spool: close sync: %w", err))
		} else {
			s.durableSequence = s.cursor.NextSequence - 1
			s.needsSync = false
		}
	}
	if s.activeFile != nil {
		if err := s.activeFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("spool: close active segment: %w", err))
		}
	}
	if s.poisonErr != nil {
		errs = append(errs, s.poisonErr)
	}
	s.mu.Unlock()
	if err := releaseLock(s.lockFile, s.opts.Dir); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func truncateDurable(path string, size int64, dir string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fsyncDir(dir)
}
