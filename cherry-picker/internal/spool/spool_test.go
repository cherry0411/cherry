package spool

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func testRecord(n int) Record {
	return Record{
		InfoHash: fmt.Sprintf("%040x", n+1),
		Encoding: EncodingNormalized,
		Normalized: &NormalizedMetadata{
			Name:        fmt.Sprintf("torrent-%d", n),
			TotalLength: 1,
			Files:       []File{{Path: "file.bin", Length: 1}},
		},
	}
}

func openTest(t *testing.T, dir string, mutate func(*Options)) *Spool {
	t.Helper()
	opts := Options{
		Dir:           dir,
		CrawlerID:     "crawler-A",
		SegmentBytes:  4096,
		SyncEveryN:    128,
		SyncInterval:  time.Hour,
		MaxBatchBytes: maxRecordLength + headerSize,
	}
	if mutate != nil {
		mutate(&opts)
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func appendDurable(t *testing.T, s *Spool, records ...Record) DurableAppend {
	t.Helper()
	r, err := s.AppendBatchDurable(records)
	if err != nil {
		t.Fatalf("AppendBatchDurable: %v", err)
	}
	return r
}

func closeOK(t *testing.T, s *Spool) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDurableBatchRoundTripAndStrictCommit(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer closeOK(t, s)
	records := make([]Record, 10)
	for i := range records {
		records[i] = testRecord(i)
	}
	receipt := appendDurable(t, s, records...)
	if receipt.StartSeq != 1 || receipt.EndSeq != 10 || receipt.RecordCount != 10 || receipt.Epoch != s.Epoch() {
		t.Fatalf("bad durable receipt: %+v", receipt)
	}

	b, err := s.NextBatch(4)
	if err != nil {
		t.Fatal(err)
	}
	if b.CrawlerID != "crawler-A" || b.StartSeq != 1 || b.EndSeq != 4 || len(b.Records) != 4 {
		t.Fatalf("bad batch: %+v", b)
	}
	if _, err := s.NextBatch(4); !errors.Is(err, ErrBatchInFlight) {
		t.Fatalf("second NextBatch error=%v, want ErrBatchInFlight", err)
	}
	mutated := b
	mutated.Epoch++
	if err := s.CommitBatch(mutated); !errors.Is(err, ErrCommitMismatch) {
		t.Fatalf("mutated commit error=%v", err)
	}
	if err := s.CommitBatch(b); err != nil {
		t.Fatal(err)
	}
	b2, err := s.NextBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	if b2.StartSeq != 5 || b2.EndSeq != 10 || len(b2.Records) != 6 {
		t.Fatalf("second batch=[%d,%d] n=%d", b2.StartSeq, b2.EndSeq, len(b2.Records))
	}
}

func TestEpochPersistsAcrossOpenAndSequenceContinues(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	epoch := s.Epoch()
	if epoch == 0 || epoch > math.MaxInt64 {
		t.Fatalf("epoch %d is outside PostgreSQL bigint positive range", epoch)
	}
	appendDurable(t, s, testRecord(0))
	closeOK(t, s)

	s2 := openTest(t, dir, nil)
	defer closeOK(t, s2)
	if s2.Epoch() != epoch {
		t.Fatalf("epoch changed across Open: %d -> %d", epoch, s2.Epoch())
	}
	receipt := appendDurable(t, s2, testRecord(1))
	if receipt.StartSeq != 2 || receipt.Epoch != epoch {
		t.Fatalf("identity regressed after reopen: %+v", receipt)
	}
	b, err := s2.NextBatch(10)
	if err != nil {
		t.Fatal(err)
	}
	if b.StartSeq != 1 || b.EndSeq != 2 || b.Records[0].Epoch != epoch || b.Records[1].Epoch != epoch {
		t.Fatalf("old unacked identity changed: %+v", b)
	}
}

func TestDurableBatchValidationFailureDoesNotPoison(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer closeOK(t, s)

	invalid := testRecord(0)
	invalid.InfoHash = "not-an-info-hash"
	if _, err := s.AppendBatchDurable([]Record{invalid}); err == nil || errors.Is(err, ErrPoisoned) {
		t.Fatalf("invalid durable batch error=%v, want non-poisoning validation error", err)
	}
	receipt := appendDurable(t, s, testRecord(1))
	if receipt.StartSeq != 1 || receipt.EndSeq != 1 {
		t.Fatalf("validation failure consumed identity: %+v", receipt)
	}
}

func TestDurableBatchCapacityPrecheckDoesNotLatch(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) {
		o.SegmentBytes = 4 << 20
		o.MaxBatchBytes = maxRecordLength + headerSize
		o.MaxBytes = 10 << 20
		o.HighWatermark = 0.9
		o.LowWatermark = 0.5
	})
	defer closeOK(t, s)
	appendDurable(t, s, largeSummaryRecord(0), largeSummaryRecord(1), largeSummaryRecord(2), largeSummaryRecord(3))
	appendDurable(t, s, largeSummaryRecord(4), largeSummaryRecord(5), largeSummaryRecord(6), largeSummaryRecord(7))

	if _, err := s.AppendBatchDurable([]Record{largeSummaryRecord(8), largeSummaryRecord(9)}); !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("oversized proposed batch error=%v", err)
	}
	atCapacity, err := s.AtCapacity()
	if err != nil || atCapacity {
		t.Fatalf("precheck latched capacity=%v err=%v", atCapacity, err)
	}
	if receipt := appendDurable(t, s, largeSummaryRecord(10)); receipt.StartSeq != 9 {
		t.Fatalf("precheck consumed sequence or blocked smaller batch: %+v", receipt)
	}
}

func TestCursorPersistsOnlyDurableNextSequence(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	defer closeOK(t, s)
	appendDurable(t, s, testRecord(0))
	b, err := s.NextBatch(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(testRecord(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch(b); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, cursorName))
	if err != nil {
		t.Fatal(err)
	}
	var envelope cursorEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	persisted := envelope.State
	if persisted.NextSequence != 2 || s.cursor.NextSequence != 3 {
		t.Fatalf("persisted next=%d in-memory next=%d, want 2 and 3", persisted.NextSequence, s.cursor.NextSequence)
	}
}

func TestAppendIsNotReadableUntilSyncThrough(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer closeOK(t, s)
	seq, err := s.Append(testRecord(0))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.NextBatch(1)
	if err != nil || !b.Empty() {
		t.Fatalf("unsynced record became readable: batch=%+v err=%v", b, err)
	}
	if err := s.SyncThrough(seq); err != nil {
		t.Fatal(err)
	}
	b, err = s.NextBatch(1)
	if err != nil || b.StartSeq != seq {
		t.Fatalf("synced record not readable: batch=%+v err=%v", b, err)
	}
}

func TestNextBatchStopsBeforeConcurrentPartialTail(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer closeOK(t, s)
	appendDurable(t, s, testRecord(0))

	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], recordMagic)
	binary.BigEndian.PutUint16(hdr[4:6], frameVersion)
	binary.BigEndian.PutUint32(hdr[8:12], 100)
	binary.BigEndian.PutUint32(hdr[12:16], crc32.Checksum([]byte("partial"), castagnoli))
	appendTail(t, s.activePath, append(hdr[:], []byte("partial")...))

	b, err := s.NextBatch(10)
	if err != nil || len(b.Records) != 1 || b.EndSeq != 1 {
		t.Fatalf("durable batch before partial writer tail=%+v err=%v", b, err)
	}
}

func TestNextBatchPoisonsWhenCompleteDurableTailDisappears(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	appendDurable(t, s, testRecord(0), testRecord(1), testRecord(2))

	var secondEnd int64
	seen := 0
	_, err := scanSegment(s.activePath, s.activeID, func(_ segmentID, _, recordEnd int64, _ []byte) error {
		seen++
		if seen == 2 {
			secondEnd = recordEnd
			return errStopScan
		}
		return nil
	})
	if err != nil || secondEnd == 0 {
		t.Fatalf("find second frame end=%d err=%v", secondEnd, err)
	}
	if err := os.Truncate(s.activePath, secondEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NextBatch(100); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("missing complete durable tail error=%v", err)
	}
	_ = s.Close()
}

func TestReplayAfterAckLoss(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	appendDurable(t, s, testRecord(0), testRecord(1), testRecord(2))
	b, err := s.NextBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	closeOK(t, s) // delivery happened, cursor commit did not

	s2 := openTest(t, dir, nil)
	defer closeOK(t, s2)
	replayed, err := s2.NextBatch(2)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.StartSeq != b.StartSeq || replayed.EndSeq != b.EndSeq {
		t.Fatalf("replay=[%d,%d], original=[%d,%d]", replayed.StartSeq, replayed.EndSeq, b.StartSeq, b.EndSeq)
	}
}

func TestTypedZeroRawSchema(t *testing.T) {
	r := testRecord(0)
	r.CrawlerID, r.Epoch, r.Sequence, r.Schema = "c", 1, 1, recordSchemaVersion
	encoded, err := r.encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeRecord(encoded); err != nil {
		t.Fatal(err)
	}

	bad := testRecord(0)
	bad.CrawlerID, bad.Epoch, bad.Sequence, bad.Schema = "c", 1, 1, recordSchemaVersion
	bad.Summary = &SummaryMetadata{Name: "also-set", TotalLength: 1, FileCount: 1}
	if _, err := bad.encode(); !errors.Is(err, errBadEncoding) {
		t.Fatalf("tagged-union mismatch error=%v", err)
	}

	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	object["pieces"] = "forbidden"
	withPieces, _ := json.Marshal(object)
	if _, err := decodeRecord(withPieces); err == nil {
		t.Fatal("unknown pieces field was accepted")
	}
	object = map[string]any{}
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	normalized := object["normalized"].(map[string]any)
	normalized["raw"] = "forbidden"
	withNestedRaw, _ := json.Marshal(object)
	if _, err := decodeRecord(withNestedRaw); err == nil {
		t.Fatal("nested raw field was accepted")
	}

	reject := Record{
		Schema:    recordSchemaVersion,
		CrawlerID: "c",
		Epoch:     1,
		Sequence:  1,
		InfoHash:  fmt.Sprintf("%040x", 99),
		Encoding:  EncodingReject,
		Reject:    &RejectMetadata{Reason: "policy-noise"},
	}
	rejectJSON, err := reject.encode()
	if err != nil {
		t.Fatal(err)
	}
	decodedReject, err := decodeRecord(rejectJSON)
	if err != nil || decodedReject.Encoding != EncodingReject || decodedReject.Reject == nil {
		t.Fatalf("reject action lost: record=%+v err=%v", decodedReject, err)
	}
}

type faultFile struct {
	segmentFile
	shortWrite  int
	failWriteAt int
	writes      int
	writeErr    error
	syncErr     error
	truncateErr error
}

func (f *faultFile) Write(p []byte) (int, error) {
	f.writes++
	if f.shortWrite > 0 && f.shortWrite < len(p) && (f.failWriteAt == 0 || f.writes == f.failWriteAt) {
		n, _ := f.segmentFile.Write(p[:f.shortWrite])
		return n, f.writeErr
	}
	return f.segmentFile.Write(p)
}

func (f *faultFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.segmentFile.Sync()
}

func (f *faultFile) Truncate(n int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	return f.segmentFile.Truncate(n)
}

func TestShortWriteRollsBackDurablyAndReusesSequence(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer closeOK(t, s)
	real := s.activeFile
	s.activeFile = &faultFile{segmentFile: real, shortWrite: 7}
	if _, err := s.Append(testRecord(0)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error=%v", err)
	}
	s.activeFile = real
	receipt := appendDurable(t, s, testRecord(1))
	if receipt.StartSeq != 1 {
		t.Fatalf("rolled-back append consumed sequence: %+v", receipt)
	}
	b, err := s.NextBatch(10)
	if err != nil || len(b.Records) != 1 || b.Records[0].InfoHash != testRecord(1).InfoHash {
		t.Fatalf("batch after rollback=%+v err=%v", b, err)
	}
}

func TestRollbackFailurePoisonsAllOperations(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	real := s.activeFile
	s.activeFile = &faultFile{segmentFile: real, shortWrite: 3, truncateErr: errors.New("truncate fault")}
	if _, err := s.Append(testRecord(0)); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append error=%v, want poison", err)
	}
	if _, err := s.Append(testRecord(1)); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("subsequent append error=%v", err)
	}
	if _, err := s.NextBatch(1); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("NextBatch after poison error=%v", err)
	}
	if err := s.CommitBatch(Batch{}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Commit after poison error=%v", err)
	}
	_ = s.Close()
}

func TestDurableBatchSyncFailurePoisons(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	real := s.activeFile
	s.activeFile = &faultFile{segmentFile: real, syncErr: errors.New("sync fault")}
	if _, err := s.AppendBatchDurable([]Record{testRecord(0)}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("durable append error=%v", err)
	}
	if _, err := s.Append(testRecord(1)); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append after sync poison error=%v", err)
	}
	_ = s.Close()
}

func TestDurableBatchFailureAcrossRotationRollsBackWholeBatch(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	s.createSegment = func(path string) (segmentFile, error) {
		f, err := createSegmentFile(path)
		if err != nil {
			return nil, err
		}
		return &faultFile{segmentFile: f, shortWrite: 7, failWriteAt: 1}, nil
	}
	if _, err := s.AppendBatchDurable([]Record{testRecord(0), testRecord(1), testRecord(2)}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("cross-segment durable append error=%v", err)
	}
	_ = s.Close()

	s2 := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	defer closeOK(t, s2)
	b, err := s2.NextBatch(100)
	if err != nil || !b.Empty() {
		t.Fatalf("failed durable batch leaked a prefix after restart: batch=%+v err=%v", b, err)
	}
	receipt := appendDurable(t, s2, testRecord(3))
	if receipt.StartSeq != 1 || receipt.EndSeq != 1 {
		t.Fatalf("failed batch consumed sequence range: %+v", receipt)
	}
}

func TestDurableBatchIntentStartsAfterDurableLegacyAppend(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	seq, err := s.Append(testRecord(0))
	if err != nil {
		t.Fatal(err)
	}
	real := s.activeFile
	s.activeFile = &faultFile{segmentFile: real, shortWrite: 7, failWriteAt: 1}
	if _, err := s.AppendBatchDurable([]Record{testRecord(1)}); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("durable batch fault error=%v", err)
	}
	_ = s.Close()

	s2 := openTest(t, dir, nil)
	defer closeOK(t, s2)
	b, err := s2.NextBatch(10)
	if err != nil || len(b.Records) != 1 || b.StartSeq != seq || b.EndSeq != seq {
		t.Fatalf("pre-intent legacy append was not preserved durably: batch=%+v err=%v", b, err)
	}
}

func TestRecoveryRollsBackPublishedCursorWhenBatchIntentRemains(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })

	s.mu.Lock()
	intent := batchIntent{
		CrawlerID:    s.cursor.CrawlerID,
		Epoch:        s.cursor.Epoch,
		StartSeq:     s.cursor.NextSequence,
		StartSegment: s.activeID,
		StartOffset:  s.activeSize,
	}
	if err := s.writeBatchIntentLocked(intent); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		rec := testRecord(i)
		rec.CrawlerID = s.cursor.CrawlerID
		rec.Epoch = s.cursor.Epoch
		rec.Sequence = s.cursor.NextSequence
		payload, err := rec.encode()
		if err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		if err := s.writeFrameLocked(encodeFrame(payload)); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
		s.cursor.NextSequence++
		s.unsynced++
		s.needsSync = true
	}
	if err := s.syncLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	// This is the latest crash point before publish: all frames and the advanced
	// cursor are durable, but the intent has not been removed/fsynced.
	if err := s.saveCursorLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.mu.Unlock()
	closeOK(t, s)

	s2 := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	defer closeOK(t, s2)
	if _, err := os.Stat(filepath.Join(dir, batchIntentName)); !os.IsNotExist(err) {
		t.Fatalf("recovered batch intent still exists: %v", err)
	}
	b, err := s2.NextBatch(100)
	if err != nil || !b.Empty() {
		t.Fatalf("unpublished crash batch leaked after recovery: batch=%+v err=%v", b, err)
	}
	receipt := appendDurable(t, s2, testRecord(9))
	if receipt.StartSeq != 1 || receipt.EndSeq != 1 {
		t.Fatalf("intent recovery did not restore sequence: %+v", receipt)
	}
}

func TestSparseTrafficBackgroundSyncBecomesObservable(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) {
		o.SyncEveryN = 1000
		o.SyncInterval = 10 * time.Millisecond
	})
	defer closeOK(t, s)
	seq, err := s.Append(testRecord(0))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		durable, err := s.DurableSequence()
		if err != nil {
			t.Fatal(err)
		}
		if durable >= seq {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background group sync did not advance durable sequence")
}

func appendTail(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write(data); err != nil || n != len(data) {
		t.Fatalf("append tail n=%d err=%v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func durableSegmentWithOneRecord(t *testing.T) (string, string, int64) {
	t.Helper()
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	appendDurable(t, s, testRecord(0))
	closeOK(t, s)
	ids, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := segmentPath(dir, ids[len(ids)-1])
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return dir, path, info.Size()
}

func TestOnlyShortHeaderOrRecordPastEOFAreTornTail(t *testing.T) {
	t.Run("short-header", func(t *testing.T) {
		dir, path, goodSize := durableSegmentWithOneRecord(t)
		appendTail(t, path, []byte("short"))
		s := openTest(t, dir, nil)
		defer closeOK(t, s)
		info, _ := os.Stat(path)
		if info.Size() != goodSize {
			t.Fatalf("tail not truncated: size=%d want=%d", info.Size(), goodSize)
		}
	})
	t.Run("record-end-after-eof", func(t *testing.T) {
		dir, path, goodSize := durableSegmentWithOneRecord(t)
		var hdr [headerSize]byte
		binary.BigEndian.PutUint32(hdr[0:4], recordMagic)
		binary.BigEndian.PutUint16(hdr[4:6], frameVersion)
		binary.BigEndian.PutUint32(hdr[8:12], 100)
		binary.BigEndian.PutUint32(hdr[12:16], crc32.Checksum([]byte("partial"), castagnoli))
		appendTail(t, path, append(hdr[:], []byte("partial")...))
		s := openTest(t, dir, nil)
		defer closeOK(t, s)
		info, _ := os.Stat(path)
		if info.Size() != goodSize {
			t.Fatalf("tail not truncated: size=%d want=%d", info.Size(), goodSize)
		}
	})
}

func TestCompleteFrameCorruptionIsAlwaysFatal(t *testing.T) {
	tests := map[string]func([]byte){
		"magic":   func(b []byte) { b[0] ^= 0xff },
		"version": func(b []byte) { b[5] ^= 0xff },
		"crc":     func(b []byte) { b[headerSize] ^= 0xff },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			dir, path, _ := durableSegmentWithOneRecord(t)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mutate(data)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrCorruption) {
				t.Fatalf("Open error=%v, want fatal corruption", err)
			}
		})
	}
}

func TestTornTailInSealedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	appendDurable(t, s, testRecord(0), testRecord(1), testRecord(2))
	closeOK(t, s)
	ids, _ := listSegments(dir)
	if len(ids) < 2 {
		t.Fatalf("expected sealed segments, got %v", ids)
	}
	appendTail(t, segmentPath(dir, ids[0]), []byte("torn"))
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("Open error=%v", err)
	}
}

func largeSummaryRecord(n int) Record {
	files := make([]File, maxSummaryFiles)
	for i := range files {
		files[i] = File{Path: strings.Repeat("x", 15_000) + fmt.Sprintf("-%02d", i), Length: 1}
	}
	return Record{
		InfoHash: fmt.Sprintf("%040x", n+1),
		Encoding: EncodingSummary,
		Summary: &SummaryMetadata{
			Name:                "large-summary",
			TotalLength:         uint64(len(files)),
			FileCount:           uint32(len(files)),
			RepresentativeFiles: files,
		},
	}
}

func TestNextBatchMemoryIsBoundedBySerializedBytes(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) {
		o.SegmentBytes = 64 << 20
		o.MaxBatchBytes = maxRecordLength + headerSize
	})
	defer closeOK(t, s)
	appendDurable(t, s, largeSummaryRecord(0), largeSummaryRecord(1), largeSummaryRecord(2))
	appendDurable(t, s, largeSummaryRecord(3), largeSummaryRecord(4), largeSummaryRecord(5))
	b, err := s.NextBatch(1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Records) == 0 || len(b.Records) >= 6 {
		t.Fatalf("batch byte bound ineffective, records=%d", len(b.Records))
	}
	var bytesInBatch int
	for i := range b.Records {
		encoded, err := b.Records[i].encode()
		if err != nil {
			t.Fatal(err)
		}
		bytesInBatch += len(encoded)
	}
	if int64(bytesInBatch) > s.opts.MaxBatchBytes {
		t.Fatalf("batch payload bytes=%d cap=%d", bytesInBatch, s.opts.MaxBatchBytes)
	}
}

func TestCommitDeletesOnlyDurablyAckedSealedSegments(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	defer closeOK(t, s)
	records := make([]Record, 8)
	for i := range records {
		records[i] = testRecord(i)
	}
	appendDurable(t, s, records...)
	before, _ := listSegments(dir)
	if len(before) < 3 {
		t.Fatalf("expected several segments, got %v", before)
	}
	b, err := s.NextBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch(b); err != nil {
		t.Fatal(err)
	}
	after, _ := listSegments(dir)
	if len(after) != 1 {
		t.Fatalf("segments after commit=%v, want active cursor segment only", after)
	}
}

func TestRecoveryFinishesCleanupAfterDurableCommitCrashWindow(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	records := make([]Record, 8)
	for i := range records {
		records[i] = testRecord(i)
	}
	appendDurable(t, s, records...)
	b, err := s.NextBatch(100)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := listSegments(dir)
	if err != nil || len(ids) < 3 || b.lastSegment <= ids[0] {
		t.Fatalf("test needs an acknowledged segment prefix: ids=%v batchSegment=%d err=%v", ids, b.lastSegment, err)
	}

	// Reproduce the crash window after the ACK cursor rename/fsync and before
	// deleteFullyAckedLocked has removed the acknowledged segment prefix.
	s.mu.Lock()
	s.cursor.AckedSequence = b.EndSeq
	s.cursor.AckedSegment = b.lastSegment
	if err := s.saveCursorLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.inflight = nil
	s.mu.Unlock()
	closeOK(t, s)

	s2 := openTest(t, dir, func(o *Options) { o.SegmentBytes = 400 })
	defer closeOK(t, s2)
	b2, err := s2.NextBatch(100)
	if err != nil || !b2.Empty() {
		t.Fatalf("acknowledged records were replayed after cleanup recovery: batch=%+v err=%v", b2, err)
	}
	after, err := listSegments(dir)
	if err != nil || len(after) != 1 || after[0] != b.lastSegment {
		t.Fatalf("recovery did not finish acknowledged prefix cleanup: segments=%v err=%v", after, err)
	}
}

func TestSingleWriterLockAndCleanReopen(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("second Open error=%v", err)
	}
	closeOK(t, s)
	s2 := openTest(t, dir, nil)
	closeOK(t, s2)
}

func TestNonUnixExistingLockFailsClosed(t *testing.T) {
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		t.Skip("non-Unix fail-closed behavior")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, lockName), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("existing lock error=%v", err)
	}
}

func TestCapacityBackpressureReportsErrors(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) {
		o.SegmentBytes = 4 << 20
		o.MaxBatchBytes = maxRecordLength + headerSize
		o.MaxBytes = 10 << 20
		o.HighWatermark = 0.9
		o.LowWatermark = 0.5
	})
	defer closeOK(t, s)
	for i := 0; i < 100; i++ {
		_, err := s.Append(largeSummaryRecord(i))
		if errors.Is(err, ErrAtCapacity) {
			atCapacity, stateErr := s.AtCapacity()
			if stateErr != nil || !atCapacity {
				t.Fatalf("AtCapacity=%v err=%v", atCapacity, stateErr)
			}
			if _, err := s.PendingBytes(); err != nil {
				t.Fatal(err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("capacity never tripped")
}

func TestCapacityConfigRejectsDeadlockBoundary(t *testing.T) {
	_, err := Open(Options{
		Dir:           t.TempDir(),
		CrawlerID:     "crawler-A",
		SegmentBytes:  4 << 20,
		MaxBatchBytes: maxRecordLength + headerSize,
		MaxBytes:      8 << 20,
		HighWatermark: 0.9,
		LowWatermark:  0.5,
	})
	if err == nil || !strings.Contains(err.Error(), "must exceed SegmentBytes + MaxBatchBytes") {
		t.Fatalf("deadlocking capacity config error=%v", err)
	}
}

func TestFullAckRotatesActiveAndReleasesCapacity(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) {
		o.SegmentBytes = 4 << 20
		o.MaxBatchBytes = maxRecordLength + headerSize
		o.MaxBytes = 10 << 20
		o.HighWatermark = 0.9
		o.LowWatermark = 0.5
	})
	defer closeOK(t, s)
	appendDurable(t, s, largeSummaryRecord(0), largeSummaryRecord(1), largeSummaryRecord(2), largeSummaryRecord(3))
	appendDurable(t, s, largeSummaryRecord(4), largeSummaryRecord(5), largeSummaryRecord(6), largeSummaryRecord(7))

	for {
		b, err := s.NextBatch(100)
		if err != nil {
			t.Fatal(err)
		}
		if b.Empty() {
			break
		}
		if err := s.CommitBatch(b); err != nil {
			t.Fatal(err)
		}
	}
	used, err := s.PendingBytes()
	if err != nil || used != 0 {
		t.Fatalf("fully acknowledged physical bytes=%d err=%v", used, err)
	}
	atCapacity, err := s.AtCapacity()
	if err != nil || atCapacity {
		t.Fatalf("capacity remained latched=%v err=%v", atCapacity, err)
	}
	if receipt := appendDurable(t, s, largeSummaryRecord(9)); receipt.StartSeq != 9 {
		t.Fatalf("append did not resume after full ACK: %+v", receipt)
	}
}

func TestFullAckEmptyCheckpointReopensAndContinuesSequence(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	appendDurable(t, s, testRecord(0))
	b, err := s.NextBatch(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitBatch(b); err != nil {
		t.Fatal(err)
	}
	closeOK(t, s)

	s2 := openTest(t, dir, nil)
	defer closeOK(t, s2)
	empty, err := s2.NextBatch(1)
	if err != nil || !empty.Empty() {
		t.Fatalf("reopened full-ACK checkpoint batch=%+v err=%v", empty, err)
	}
	receipt := appendDurable(t, s2, testRecord(1))
	if receipt.StartSeq != 2 || receipt.EndSeq != 2 {
		t.Fatalf("sequence did not continue after empty checkpoint: %+v", receipt)
	}
}

func TestCorruptCursorAndWrongCrawlerFailClosed(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	appendDurable(t, s, testRecord(0))
	closeOK(t, s)

	original, err := os.ReadFile(filepath.Join(dir, cursorName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, cursorName), []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("corrupt cursor Open error=%v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, cursorName), original, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-B"}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("wrong crawler Open error=%v", err)
	}
}

func TestCursorValidNumericTamperFailsChecksum(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	appendDurable(t, s, testRecord(0))
	closeOK(t, s)

	path := filepath.Join(dir, cursorName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope cursorEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	// This remains a structurally valid, in-range cursor. Only the checksum
	// distinguishes the silent numeric change from an authentic manifest.
	envelope.State.NextSequence++
	data, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("numeric cursor tamper Open error=%v", err)
	}
}

func TestMalformedSegmentFilenameFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	closeOK(t, s)
	if err := os.WriteFile(filepath.Join(dir, "seg_not-a-number.spool"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(Options{Dir: dir, CrawlerID: "crawler-A"})
	if opened != nil {
		_ = opened.Close()
	}
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("malformed segment filename Open error=%v", err)
	}
}
