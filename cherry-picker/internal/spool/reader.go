package spool

import (
	"errors"
	"fmt"
	"os"
)

// Batch is one strictly ordered delivery unit. Unexported identity fields make
// CommitBatch reject fabricated or stale cursors even when exported fields are
// copied or modified.
type Batch struct {
	CrawlerID string
	Epoch     uint64
	StartSeq  uint64
	EndSeq    uint64
	Records   []Record

	token       uint64
	recordCount int
	lastSegment segmentID
}

func (b Batch) Empty() bool { return len(b.Records) == 0 }

var (
	ErrCommitMismatch   = errors.New("spool: commit does not match outstanding batch")
	ErrBatchInFlight    = errors.New("spool: a batch is already loading or in flight")
	ErrInvalidBatchSize = errors.New("spool: maxRecords must be positive")
	// ErrCommittedCleanup means the delivery cursor was durably committed, but
	// post-commit segment cleanup/capacity accounting failed. The caller must
	// not resend the batch; the spool is poisoned and requires recovery.
	ErrCommittedCleanup = errors.New("spool: batch committed but cleanup failed")
)

// NextBatch returns only records at or below DurableSequence. Batch payload
// memory is bounded by Options.MaxBatchBytes even if maxRecords is enormous.
// Exactly one batch may be loading or awaiting CommitBatch at a time.
func (s *Spool) NextBatch(maxRecords int) (Batch, error) {
	if maxRecords <= 0 {
		return Batch{}, ErrInvalidBatchSize
	}

	s.mu.Lock()
	if err := s.healthLocked(); err != nil {
		s.mu.Unlock()
		return Batch{}, err
	}
	if s.batchLoading || s.inflight != nil {
		s.mu.Unlock()
		return Batch{}, ErrBatchInFlight
	}
	// Do not touch the active tail when there is no new durable record. An
	// Append may be partway through a frame after we release the mutex below;
	// that is normal writer state, not a torn durable record.
	if s.durableSequence <= s.cursor.AckedSequence {
		batch := Batch{CrawlerID: s.cursor.CrawlerID, Epoch: s.cursor.Epoch}
		s.mu.Unlock()
		return batch, nil
	}
	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		s.mu.Unlock()
		return Batch{}, s.poisonLocked(err)
	}
	s.batchLoading = true
	acked := s.cursor.AckedSequence
	ackedSegment := s.cursor.AckedSegment
	epoch := s.cursor.Epoch
	durable := s.durableSequence
	crawlerID := s.cursor.CrawlerID
	s.mu.Unlock()

	batch := Batch{CrawlerID: crawlerID, Epoch: epoch}
	want := acked + 1
	var payloadBytes int64
	var scanErr error
	stopped := false
	limited := false
	reachedDurable := false
	for _, id := range ids {
		if id < ackedSegment {
			continue
		}
		var result scanResult
		result, scanErr = scanSegment(segmentPath(s.opts.Dir, id), id, func(seg segmentID, _, _ int64, payload []byte) error {
			rec, err := decodeRecord(payload)
			if err != nil {
				return fmt.Errorf("%w: invalid typed record in %s: %v", ErrCorruption, segmentName(seg), err)
			}
			if rec.CrawlerID != crawlerID || rec.Epoch != epoch {
				return fmt.Errorf("%w: identity mismatch in %s", ErrCorruption, segmentName(seg))
			}
			if rec.Sequence < want {
				return nil
			}
			if rec.Sequence > durable {
				return fmt.Errorf("%w: durable sequence gap have=%d want=%d boundary=%d", ErrCorruption, rec.Sequence, want, durable)
			}
			if rec.Sequence != want {
				return fmt.Errorf("%w: sequence gap have=%d want=%d", ErrCorruption, rec.Sequence, want)
			}
			if len(batch.Records) >= maxRecords ||
				(len(batch.Records) > 0 && payloadBytes+int64(len(payload)) > s.opts.MaxBatchBytes) {
				limited = true
				return errStopScan
			}
			if batch.Empty() {
				batch.StartSeq = rec.Sequence
			}
			batch.Records = append(batch.Records, rec)
			batch.EndSeq = rec.Sequence
			batch.lastSegment = seg
			payloadBytes += int64(len(payload))
			want++
			// Stop the scanner at the captured fsync boundary. Continuing into
			// the next frame could observe a legitimate concurrent partial write.
			if rec.Sequence >= durable {
				reachedDurable = true
				return errStopScan
			}
			if len(batch.Records) >= maxRecords || payloadBytes >= s.opts.MaxBatchBytes {
				limited = true
				return errStopScan
			}
			return nil
		})
		stopped = result.Stopped
		if result.TornTail && !result.Stopped {
			scanErr = fmt.Errorf("%w: torn tail encountered while spool is open in %s", ErrCorruption, segmentName(id))
		}
		if scanErr != nil || len(batch.Records) >= maxRecords || payloadBytes >= s.opts.MaxBatchBytes {
			break
		}
		if stopped || batch.EndSeq >= durable {
			break
		}
	}
	if scanErr == nil && !limited && !reachedDurable && want <= durable {
		scanErr = fmt.Errorf("%w: retained log ended at %d before durable boundary %d", ErrCorruption, want-1, durable)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchLoading = false
	if scanErr != nil {
		return Batch{}, s.poisonLocked(scanErr)
	}
	if err := s.healthLocked(); err != nil {
		return Batch{}, err
	}
	if s.cursor.AckedSequence != acked || s.cursor.Epoch != epoch || s.inflight != nil {
		return Batch{}, s.poisonLocked(fmt.Errorf("spool: cursor changed during batch scan"))
	}
	if batch.Empty() {
		return Batch{}, s.poisonLocked(fmt.Errorf("%w: durable sequence %d is missing after ack %d", ErrCorruption, durable, acked))
	}

	s.nextBatchToken++
	if s.nextBatchToken == 0 {
		return Batch{}, s.poisonLocked(fmt.Errorf("spool: batch token overflow"))
	}
	batch.token = s.nextBatchToken
	batch.recordCount = len(batch.Records)
	s.inflight = &inflightBatch{
		token:       batch.token,
		crawlerID:   batch.CrawlerID,
		epoch:       batch.Epoch,
		start:       batch.StartSeq,
		end:         batch.EndSeq,
		recordCount: batch.recordCount,
		lastSegment: batch.lastSegment,
	}
	return batch, nil
}

func (s *Spool) CommitBatch(b Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	if s.batchLoading || s.inflight == nil || !s.batchMatchesLocked(b) {
		return ErrCommitMismatch
	}
	if b.StartSeq != s.cursor.AckedSequence+1 || b.EndSeq > s.durableSequence {
		return ErrCommitMismatch
	}
	for i := range b.Records {
		want := b.StartSeq + uint64(i)
		if b.Records[i].CrawlerID != b.CrawlerID || b.Records[i].Epoch != b.Epoch || b.Records[i].Sequence != want {
			return ErrCommitMismatch
		}
		if err := b.Records[i].validate(); err != nil {
			return ErrCommitMismatch
		}
	}

	previous := s.cursor
	// If this ACK drains every appended record from the active segment, rotate
	// before persisting the ACK. The new empty active segment can become the
	// cursor segment, allowing cleanup to delete the old active file and release
	// its physical bytes. Without this, capacity can remain latched forever on
	// a fully acknowledged but undeletable active segment.
	fullyDrainedActive := b.EndSeq == s.cursor.NextSequence-1 &&
		b.EndSeq == s.durableSequence && !s.needsSync && b.lastSegment == s.activeID && s.activeSize > 0
	if fullyDrainedActive {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	s.cursor.AckedSequence = b.EndSeq
	if fullyDrainedActive {
		s.cursor.AckedSegment = s.activeID
	} else {
		s.cursor.AckedSegment = b.lastSegment
	}
	if err := s.saveCursorLocked(); err != nil {
		s.cursor = previous
		return s.poisonLocked(fmt.Errorf("spool: persist commit cursor: %w", err))
	}
	// From this point the ACK is durable. Clear the in-flight token before
	// cleanup so an error can never invite the caller to commit it again.
	s.inflight = nil
	if err := s.deleteFullyAckedLocked(); err != nil {
		poison := s.poisonLocked(err)
		return errors.Join(ErrCommittedCleanup, poison)
	}
	if err := s.updateCapacityLocked(); err != nil {
		poison := s.poisonLocked(err)
		return errors.Join(ErrCommittedCleanup, poison)
	}
	return nil
}

func (s *Spool) batchMatchesLocked(b Batch) bool {
	i := s.inflight
	return i != nil && b.token != 0 && b.token == i.token &&
		b.CrawlerID == i.crawlerID && b.Epoch == i.epoch &&
		b.StartSeq == i.start && b.EndSeq == i.end &&
		b.recordCount == i.recordCount && len(b.Records) == i.recordCount &&
		b.lastSegment == i.lastSegment &&
		b.EndSeq >= b.StartSeq && uint64(i.recordCount) == b.EndSeq-b.StartSeq+1
}

// AbandonBatch releases a validated in-flight batch without advancing the
// cursor. The next NextBatch call re-reads the same sequence range.
func (s *Spool) AbandonBatch(b Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	if !s.batchMatchesLocked(b) {
		return ErrCommitMismatch
	}
	s.inflight = nil
	return nil
}

func (s *Spool) deleteFullyAckedLocked() error {
	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		return err
	}
	deleted := false
	for _, id := range ids {
		if id >= s.cursor.AckedSegment {
			break
		}
		if id == s.activeID {
			return fmt.Errorf("%w: active segment %d precedes ack cursor", ErrCorruption, id)
		}
		if err := os.Remove(segmentPath(s.opts.Dir, id)); err != nil {
			return fmt.Errorf("spool: remove acked %s: %w", segmentName(id), err)
		}
		deleted = true
	}
	if deleted {
		if err := fsyncDir(s.opts.Dir); err != nil {
			return fmt.Errorf("spool: sync directory after segment deletion: %w", err)
		}
	}
	return nil
}

// CursorPosition reports only validated in-memory state and returns poison or
// closed errors instead of silently returning zero gauges.
func (s *Spool) CursorPosition() (epoch, ackedSequence, nextSequence, durableSequence uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if healthErr := s.healthLocked(); healthErr != nil {
		return 0, 0, 0, 0, healthErr
	}
	return s.cursor.Epoch, s.cursor.AckedSequence, s.cursor.NextSequence, s.durableSequence, nil
}
