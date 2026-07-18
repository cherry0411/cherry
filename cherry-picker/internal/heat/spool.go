package heat

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	spoolVersion        = byte(1)
	spoolHeaderSize     = int64(32)
	spoolPayloadSize    = 32
	spoolFrameSize      = int64(4 + spoolPayloadSize + 4)
	cursorSize          = 4 + 1 + 3 + 8 + 8 + 8 + 4
	defaultSpoolBytes   = int64(512 << 20)
	defaultSegmentBytes = int64(8 << 20)
)

var (
	spoolMagic      = [4]byte{'C', 'H', 'H', 'S'}
	cursorMagic     = [4]byte{'C', 'H', 'H', 'C'}
	ErrAtCapacity   = errors.New("heat: spool at capacity")
	ErrCorruptSpool = errors.New("heat: corrupt spool")
)

type spoolOptions struct {
	dir          string
	maxBytes     int64
	segmentBytes int64
}

type heatSegment struct {
	id           uint64
	path         string
	file         *os.File
	size         int64
	baseSequence uint64
	records      uint64
}

type heatSpool struct {
	mu            sync.Mutex
	dir           string
	cursorPath    string
	maxBytes      int64
	segmentBytes  int64
	lock          *os.File
	epoch         uint64
	segments      []*heatSegment
	cursorSegment uint64
	cursorOffset  int64
	totalBytes    int64
	records       uint64
	closed        bool
}

type spoolBatch struct {
	SegmentID     uint64
	Start         int64
	End           int64
	Epoch         uint64
	StartSequence uint64
	EndSequence   uint64
	Observations  []Observation
}

func openHeatSpool(opts spoolOptions) (*heatSpool, error) {
	if strings.TrimSpace(opts.dir) == "" {
		return nil, errors.New("heat: spool directory is required")
	}
	if opts.maxBytes <= 0 {
		opts.maxBytes = defaultSpoolBytes
	}
	if opts.maxBytes < 4*(spoolHeaderSize+spoolFrameSize) {
		return nil, errors.New("heat: spool max bytes is too small")
	}
	if opts.segmentBytes <= 0 {
		opts.segmentBytes = min(defaultSegmentBytes, opts.maxBytes/4)
	}
	if opts.segmentBytes < spoolHeaderSize+spoolFrameSize {
		opts.segmentBytes = spoolHeaderSize + spoolFrameSize
	}
	if err := os.MkdirAll(opts.dir, 0o700); err != nil {
		return nil, fmt.Errorf("heat: create spool directory: %w", err)
	}
	lock, err := openAndLock(filepath.Join(opts.dir, "heat.lock"))
	if err != nil {
		return nil, err
	}
	s := &heatSpool{
		dir: opts.dir, cursorPath: filepath.Join(opts.dir, "heat.cursor"),
		maxBytes: opts.maxBytes, segmentBytes: opts.segmentBytes, lock: lock,
	}
	if err := s.openAndRecover(); err != nil {
		s.closeSegments()
		_ = unlockFile(lock)
		_ = lock.Close()
		return nil, err
	}
	return s, nil
}

func openAndLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) && canReclaimLockFile() {
		file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("heat: acquire spool lock: %w", err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("heat: acquire spool lock: %w", err)
	}
	return file, nil
}

func (s *heatSpool) openAndRecover() error {
	ids, err := listSegmentIDs(s.dir)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		epoch, err := randomNonzeroUint64()
		if err != nil {
			return fmt.Errorf("heat: generate spool epoch: %w", err)
		}
		s.epoch = epoch
		segment, err := s.createSegment(1, 1)
		if err != nil {
			return err
		}
		s.segments = append(s.segments, segment)
		s.totalBytes = segment.size
	}
	for idx, id := range ids {
		segment, epoch, err := openSegment(s.dir, id, idx == len(ids)-1)
		if err != nil {
			return err
		}
		if idx == 0 {
			s.epoch = epoch
		} else {
			previous := s.segments[len(s.segments)-1]
			if epoch != s.epoch || id != previous.id+1 ||
				segment.baseSequence != previous.baseSequence+previous.records {
				_ = segment.file.Close()
				return fmt.Errorf("%w: non-contiguous segment identity", ErrCorruptSpool)
			}
		}
		s.segments = append(s.segments, segment)
		s.totalBytes += segment.size
	}
	if s.totalBytes > s.maxBytes {
		return fmt.Errorf("heat: recovered spool uses %d bytes above configured max %d", s.totalBytes, s.maxBytes)
	}
	cursor, exists, err := readCursor(s.cursorPath)
	if err != nil {
		return err
	}
	if !exists {
		cursor = spoolCursor{epoch: s.epoch, segmentID: s.segments[0].id, offset: spoolHeaderSize}
		if err := s.persistCursor(cursor); err != nil {
			return err
		}
	}
	if cursor.epoch != s.epoch {
		return fmt.Errorf("%w: cursor epoch mismatch", ErrCorruptSpool)
	}
	cursorIdx := s.segmentIndex(cursor.segmentID)
	if cursorIdx < 0 {
		return fmt.Errorf("%w: cursor segment is missing", ErrCorruptSpool)
	}
	cursorSegment := s.segments[cursorIdx]
	if cursor.offset < spoolHeaderSize || cursor.offset > cursorSegment.size ||
		(cursor.offset-spoolHeaderSize)%spoolFrameSize != 0 {
		return fmt.Errorf("%w: cursor is not a valid record boundary", ErrCorruptSpool)
	}
	// A crash after cursor persistence but before acknowledged segment removal
	// leaves safe garbage strictly before the cursor segment.
	for cursorIdx > 0 {
		old := s.segments[0]
		if err := old.file.Close(); err != nil {
			return err
		}
		if err := os.Remove(old.path); err != nil {
			return fmt.Errorf("heat: recover acknowledged segment cleanup: %w", err)
		}
		s.totalBytes -= old.size
		s.segments = s.segments[1:]
		cursorIdx--
	}
	if err := fsyncDir(s.dir); err != nil {
		return fmt.Errorf("heat: sync recovered segment cleanup: %w", err)
	}
	s.cursorSegment, s.cursorOffset = cursor.segmentID, cursor.offset
	s.recountUnacked()
	active := s.segments[len(s.segments)-1]
	_, err = active.file.Seek(active.size, io.SeekStart)
	return err
}

func listSegmentIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("heat: list spool segments: %w", err)
	}
	var ids []uint64
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "heat-") || !strings.HasSuffix(name, ".spool") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, "heat-"), ".spool")
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%w: invalid segment name %q", ErrCorruptSpool, name)
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}

func segmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("heat-%020d.spool", id))
}

func openSegment(dir string, id uint64, allowTornTail bool) (*heatSegment, uint64, error) {
	path := segmentPath(dir, id)
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, err
	}
	fail := func(err error) (*heatSegment, uint64, error) {
		_ = file.Close()
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if info.Size() < spoolHeaderSize {
		return fail(fmt.Errorf("%w: truncated segment header", ErrCorruptSpool))
	}
	header := make([]byte, spoolHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return fail(err)
	}
	epoch, headerID, base, err := decodeSegmentHeader(header)
	if err != nil || headerID != id {
		return fail(fmt.Errorf("%w: invalid segment header", ErrCorruptSpool))
	}
	end, records, torn, err := scanSegment(file, info.Size())
	if err != nil {
		return fail(err)
	}
	if torn && !allowTornTail {
		return fail(fmt.Errorf("%w: torn tail in sealed segment", ErrCorruptSpool))
	}
	if torn {
		if err := file.Truncate(end); err != nil {
			return fail(fmt.Errorf("heat: truncate torn active tail: %w", err))
		}
		if err := file.Sync(); err != nil {
			return fail(fmt.Errorf("heat: sync recovered active tail: %w", err))
		}
	}
	return &heatSegment{id: id, path: path, file: file, size: end, baseSequence: base, records: records}, epoch, nil
}

func scanSegment(file *os.File, size int64) (int64, uint64, bool, error) {
	offset := spoolHeaderSize
	var records uint64
	frame := make([]byte, spoolFrameSize)
	for offset < size {
		if size-offset < spoolFrameSize {
			return offset, records, true, nil
		}
		if _, err := file.ReadAt(frame, offset); err != nil {
			return 0, 0, false, fmt.Errorf("heat: scan segment: %w", err)
		}
		if binary.BigEndian.Uint32(frame[:4]) != spoolPayloadSize {
			return 0, 0, false, fmt.Errorf("%w: invalid record length at %d", ErrCorruptSpool, offset)
		}
		want := binary.BigEndian.Uint32(frame[4+spoolPayloadSize:])
		if crc32.ChecksumIEEE(frame[4:4+spoolPayloadSize]) != want {
			if offset+spoolFrameSize == size {
				return offset, records, true, nil
			}
			return 0, 0, false, fmt.Errorf("%w: checksum mismatch at %d", ErrCorruptSpool, offset)
		}
		offset += spoolFrameSize
		records++
	}
	return offset, records, false, nil
}

func (s *heatSpool) createSegment(id, baseSequence uint64) (*heatSegment, error) {
	if id == 0 || baseSequence == 0 {
		return nil, fmt.Errorf("%w: zero segment identity", ErrCorruptSpool)
	}
	path := segmentPath(s.dir, id)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("heat: create segment: %w", err)
	}
	header := encodeSegmentHeader(s.epoch, id, baseSequence)
	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := fsyncDir(s.dir); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &heatSegment{id: id, path: path, file: file, size: spoolHeaderSize, baseSequence: baseSequence}, nil
}

func (s *heatSpool) rotateLocked() error {
	if s.totalBytes > s.maxBytes-spoolHeaderSize {
		return ErrAtCapacity
	}
	active := s.segments[len(s.segments)-1]
	if active.id == ^uint64(0) || active.records > ^uint64(0)-active.baseSequence {
		return fmt.Errorf("%w: segment/sequence exhausted", ErrCorruptSpool)
	}
	next, err := s.createSegment(active.id+1, active.baseSequence+active.records)
	if err != nil {
		return err
	}
	s.segments = append(s.segments, next)
	s.totalBytes += next.size
	return nil
}

func (s *heatSpool) appendDurable(observations []Observation) error {
	if len(observations) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("heat: spool closed")
	}
	bytesNeeded := int64(len(observations)) * spoolFrameSize
	if bytesNeeded/spoolFrameSize != int64(len(observations)) {
		return ErrAtCapacity
	}
	active := s.segments[len(s.segments)-1]
	rotate := active.records > 0 && active.size+bytesNeeded > s.segmentBytes
	extra := bytesNeeded
	if rotate {
		extra += spoolHeaderSize
	}
	// Keep one header in reserve so committing the last active segment can
	// durably create its successor before deleting acknowledged data.
	if s.totalBytes+extra > s.maxBytes-spoolHeaderSize {
		return ErrAtCapacity
	}
	if rotate {
		if err := s.rotateLocked(); err != nil {
			return err
		}
		active = s.segments[len(s.segments)-1]
	}
	if uint64(len(observations)) > ^uint64(0)-active.baseSequence-active.records {
		return fmt.Errorf("%w: sequence exhausted", ErrCorruptSpool)
	}
	buf := make([]byte, 0, bytesNeeded)
	for _, obs := range observations {
		var payload [spoolPayloadSize]byte
		binary.BigEndian.PutUint32(payload[:4], obs.Day)
		copy(payload[4:24], obs.InfoHash[:])
		binary.BigEndian.PutUint64(payload[24:32], obs.Actor)
		buf = binary.BigEndian.AppendUint32(buf, spoolPayloadSize)
		buf = append(buf, payload[:]...)
		buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(payload[:]))
	}
	start := active.size
	if _, err := active.file.WriteAt(buf, start); err != nil {
		return rollbackSegmentAppend(active, start, fmt.Errorf("write: %w", err))
	}
	if err := active.file.Sync(); err != nil {
		return rollbackSegmentAppend(active, start, fmt.Errorf("fsync: %w", err))
	}
	active.size += bytesNeeded
	active.records += uint64(len(observations))
	s.totalBytes += bytesNeeded
	s.records += uint64(len(observations))
	_, err := active.file.Seek(active.size, io.SeekStart)
	return err
}

func rollbackSegmentAppend(segment *heatSegment, start int64, cause error) error {
	if err := segment.file.Truncate(start); err != nil {
		return fmt.Errorf("%w: append %v; rollback truncate: %v", ErrCorruptSpool, cause, err)
	}
	if err := segment.file.Sync(); err != nil {
		return fmt.Errorf("%w: append %v; rollback fsync: %v", ErrCorruptSpool, cause, err)
	}
	_, _ = segment.file.Seek(start, io.SeekStart)
	return fmt.Errorf("heat: durable append rolled back: %w", cause)
}

func (s *heatSpool) readBatch(maxRecords int) (spoolBatch, error) {
	if maxRecords <= 0 {
		return spoolBatch{}, errors.New("heat: max records must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return spoolBatch{}, errors.New("heat: spool closed")
	}
	idx := s.segmentIndex(s.cursorSegment)
	if idx < 0 {
		return spoolBatch{}, fmt.Errorf("%w: cursor segment missing", ErrCorruptSpool)
	}
	segment := s.segments[idx]
	if s.cursorOffset == segment.size {
		return spoolBatch{}, nil
	}
	startIndex := uint64((s.cursorOffset - spoolHeaderSize) / spoolFrameSize)
	result := spoolBatch{
		SegmentID: segment.id, Start: s.cursorOffset, Epoch: s.epoch,
		StartSequence: segment.baseSequence + startIndex,
		Observations:  make([]Observation, 0, maxRecords),
	}
	offset := s.cursorOffset
	frame := make([]byte, spoolFrameSize)
	var firstDay uint32
	for len(result.Observations) < maxRecords && offset+spoolFrameSize <= segment.size {
		if _, err := segment.file.ReadAt(frame, offset); err != nil {
			return spoolBatch{}, err
		}
		payload := frame[4 : 4+spoolPayloadSize]
		var obs Observation
		obs.Day = binary.BigEndian.Uint32(payload[:4])
		copy(obs.InfoHash[:], payload[4:24])
		obs.Actor = binary.BigEndian.Uint64(payload[24:32])
		if len(result.Observations) == 0 {
			firstDay = obs.Day
		} else if obs.Day != firstDay {
			break
		}
		result.Observations = append(result.Observations, obs)
		offset += spoolFrameSize
	}
	result.End = offset
	result.EndSequence = result.StartSequence + uint64(len(result.Observations)) - 1
	return result, nil
}

func (s *heatSpool) commit(batch spoolBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.segmentIndex(s.cursorSegment)
	if idx < 0 || batch.SegmentID != s.cursorSegment || batch.Start != s.cursorOffset ||
		batch.End <= batch.Start || (batch.End-spoolHeaderSize)%spoolFrameSize != 0 {
		return errors.New("heat: stale or invalid spool commit")
	}
	segment := s.segments[idx]
	acked := uint64((batch.End - batch.Start) / spoolFrameSize)
	wantStart := segment.baseSequence + uint64((batch.Start-spoolHeaderSize)/spoolFrameSize)
	if batch.Epoch != s.epoch || batch.StartSequence != wantStart || batch.EndSequence != wantStart+acked-1 ||
		batch.End > segment.size || acked > s.records {
		return errors.New("heat: spool receipt identity mismatch")
	}
	if batch.End < segment.size {
		next := spoolCursor{epoch: s.epoch, segmentID: segment.id, offset: batch.End}
		if err := s.persistCursor(next); err != nil {
			return err
		}
		s.cursorOffset = batch.End
		s.records -= acked
		return nil
	}
	if idx == len(s.segments)-1 {
		// Seal an empty successor even when producer and consumer momentarily
		// meet. This avoids unsafe in-place compaction and preserves monotonic
		// sequences across crashes.
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	nextSegment := s.segments[idx+1]
	next := spoolCursor{epoch: s.epoch, segmentID: nextSegment.id, offset: spoolHeaderSize}
	if err := s.persistCursor(next); err != nil {
		return err
	}
	s.cursorSegment, s.cursorOffset = next.segmentID, next.offset
	s.records -= acked
	if err := segment.file.Close(); err != nil {
		return fmt.Errorf("heat: close acknowledged segment: %w", err)
	}
	if err := os.Remove(segment.path); err != nil {
		return fmt.Errorf("heat: remove acknowledged segment: %w", err)
	}
	s.totalBytes -= segment.size
	s.segments = append(s.segments[:idx], s.segments[idx+1:]...)
	if err := fsyncDir(s.dir); err != nil {
		return fmt.Errorf("heat: sync acknowledged segment removal: %w", err)
	}
	return nil
}

type spoolCursor struct {
	epoch     uint64
	segmentID uint64
	offset    int64
}

func (s *heatSpool) persistCursor(cursor spoolCursor) error {
	var body [cursorSize]byte
	copy(body[:4], cursorMagic[:])
	body[4] = spoolVersion
	binary.BigEndian.PutUint64(body[8:16], cursor.epoch)
	binary.BigEndian.PutUint64(body[16:24], cursor.segmentID)
	binary.BigEndian.PutUint64(body[24:32], uint64(cursor.offset))
	binary.BigEndian.PutUint32(body[32:36], crc32.ChecksumIEEE(body[:32]))
	tmp := s.cursorPath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("heat: create cursor temp: %w", err)
	}
	if _, err = file.Write(body[:]); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("heat: persist cursor temp: %w", err)
	}
	if err := replaceFile(tmp, s.cursorPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("heat: replace cursor: %w", err)
	}
	if err := fsyncDir(s.dir); err != nil {
		return fmt.Errorf("heat: sync cursor directory: %w", err)
	}
	return nil
}

func replaceFile(source, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}
	// Windows does not atomically replace an existing destination through
	// os.Rename. Production is Linux; this fallback keeps tests functional but
	// does not claim directory-entry power-loss atomicity on Windows.
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(source, target)
}

func readCursor(path string) (spoolCursor, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return spoolCursor{}, false, nil
	}
	if err != nil {
		return spoolCursor{}, false, fmt.Errorf("heat: read cursor: %w", err)
	}
	if len(data) != cursorSize || !bytes.Equal(data[:4], cursorMagic[:]) || data[4] != spoolVersion ||
		!bytes.Equal(data[5:8], []byte{0, 0, 0}) ||
		binary.BigEndian.Uint32(data[32:36]) != crc32.ChecksumIEEE(data[:32]) {
		return spoolCursor{}, false, fmt.Errorf("%w: invalid cursor", ErrCorruptSpool)
	}
	value := binary.BigEndian.Uint64(data[24:32])
	if value > uint64(^uint64(0)>>1) {
		return spoolCursor{}, false, fmt.Errorf("%w: cursor overflow", ErrCorruptSpool)
	}
	return spoolCursor{
		epoch:     binary.BigEndian.Uint64(data[8:16]),
		segmentID: binary.BigEndian.Uint64(data[16:24]), offset: int64(value),
	}, true, nil
}

func encodeSegmentHeader(epoch, id, baseSequence uint64) []byte {
	header := make([]byte, spoolHeaderSize)
	copy(header[:4], spoolMagic[:])
	header[4] = spoolVersion
	binary.BigEndian.PutUint64(header[8:16], epoch)
	binary.BigEndian.PutUint64(header[16:24], id)
	binary.BigEndian.PutUint64(header[24:32], baseSequence)
	return header
}

func decodeSegmentHeader(header []byte) (epoch, id, base uint64, err error) {
	if len(header) != int(spoolHeaderSize) || !bytes.Equal(header[:4], spoolMagic[:]) ||
		header[4] != spoolVersion || !bytes.Equal(header[5:8], []byte{0, 0, 0}) {
		return 0, 0, 0, errors.New("invalid segment header")
	}
	epoch = binary.BigEndian.Uint64(header[8:16])
	id = binary.BigEndian.Uint64(header[16:24])
	base = binary.BigEndian.Uint64(header[24:32])
	if epoch == 0 || id == 0 || base == 0 {
		return 0, 0, 0, errors.New("zero segment identity")
	}
	return epoch, id, base, nil
}

func randomNonzeroUint64() (uint64, error) {
	var raw [8]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		if value := binary.BigEndian.Uint64(raw[:]); value != 0 {
			return value, nil
		}
	}
}

func (s *heatSpool) segmentIndex(id uint64) int {
	return slices.IndexFunc(s.segments, func(segment *heatSegment) bool { return segment.id == id })
}

func (s *heatSpool) recountUnacked() {
	idx := s.segmentIndex(s.cursorSegment)
	if idx < 0 {
		return
	}
	records := uint64((s.segments[idx].size - s.cursorOffset) / spoolFrameSize)
	for _, segment := range s.segments[idx+1:] {
		records += segment.records
	}
	s.records = records
}

func (s *heatSpool) snapshot() (bytes int64, max int64, records uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalBytes, s.maxBytes, s.records
}

// sequenceState returns the stable spool epoch, the sequence that will be
// assigned to the next durable append, and the first not-yet-committed
// sequence. UTC completion anchors use these values without introducing a
// second receipt counter.
func (s *heatSpool) sequenceState() (epoch, next, cursor uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.segments) == 0 {
		return 0, 0, 0, errors.New("heat: spool closed")
	}
	active := s.segments[len(s.segments)-1]
	if active.records > ^uint64(0)-active.baseSequence {
		return 0, 0, 0, fmt.Errorf("%w: sequence exhausted", ErrCorruptSpool)
	}
	cursorIndex := s.segmentIndex(s.cursorSegment)
	if cursorIndex < 0 {
		return 0, 0, 0, fmt.Errorf("%w: cursor segment missing", ErrCorruptSpool)
	}
	cursorSegment := s.segments[cursorIndex]
	cursorRecords := uint64((s.cursorOffset - spoolHeaderSize) / spoolFrameSize)
	return s.epoch, active.baseSequence + active.records, cursorSegment.baseSequence + cursorRecords, nil
}

func (s *heatSpool) closeSegments() {
	for _, segment := range s.segments {
		if segment.file != nil {
			_ = segment.file.Close()
		}
	}
}

func (s *heatSpool) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	segments, lock := s.segments, s.lock
	s.mu.Unlock()
	var result error
	for _, segment := range segments {
		if err := segment.file.Sync(); err != nil {
			result = errors.Join(result, err)
		}
		if err := segment.file.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	if err := unlockFile(lock); err != nil {
		result = errors.Join(result, err)
	}
	if err := lock.Close(); err != nil {
		result = errors.Join(result, err)
	}
	if !canReclaimLockFile() {
		if err := os.Remove(filepath.Join(s.dir, "heat.lock")); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}
