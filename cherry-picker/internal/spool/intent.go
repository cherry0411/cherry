package spool

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	batchIntentName            = "batch.intent"
	batchIntentTempName        = "batch.intent.tmp"
	batchIntentEnvelopeVersion = 1
)

type batchIntent struct {
	CrawlerID    string    `json:"crawler_id"`
	Epoch        uint64    `json:"epoch"`
	StartSeq     uint64    `json:"start_sequence"`
	StartSegment segmentID `json:"start_segment"`
	StartOffset  int64     `json:"start_offset"`
}

type batchIntentEnvelope struct {
	Version int         `json:"version"`
	Intent  batchIntent `json:"intent"`
	SHA256  string      `json:"sha256"`
}

func (s *Spool) batchIntentPath() string { return filepath.Join(s.opts.Dir, batchIntentName) }

func (s *Spool) writeBatchIntentLocked(intent batchIntent) error {
	if _, err := os.Stat(s.batchIntentPath()); err == nil {
		return fmt.Errorf("%w: batch intent already exists", ErrCorruption)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("spool: stat batch intent: %w", err)
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(batchIntentEnvelope{
		Version: batchIntentEnvelopeVersion,
		Intent:  intent,
		SHA256:  hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return err
	}

	tmp := filepath.Join(s.opts.Dir, batchIntentTempName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("spool: create batch intent temp: %w", err)
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
		return fmt.Errorf("spool: write batch intent: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("spool: sync batch intent: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("spool: close batch intent: %w", err)
	}
	if err := os.Rename(tmp, s.batchIntentPath()); err != nil {
		return fmt.Errorf("spool: publish batch intent: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return fmt.Errorf("spool: sync batch intent directory: %w", err)
	}
	ok = true
	return nil
}

func loadBatchIntent(path string) (batchIntent, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return batchIntent{}, false, nil
		}
		return batchIntent{}, false, fmt.Errorf("spool: read batch intent: %w", err)
	}
	var envelope batchIntentEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return batchIntent{}, false, fmt.Errorf("%w: decode batch intent: %v", ErrCorruption, err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return batchIntent{}, false, fmt.Errorf("%w: batch intent: %v", ErrCorruption, err)
	}
	if envelope.Version != batchIntentEnvelopeVersion {
		return batchIntent{}, false, fmt.Errorf("%w: unsupported batch intent version %d", ErrCorruption, envelope.Version)
	}
	payload, err := json.Marshal(envelope.Intent)
	if err != nil {
		return batchIntent{}, false, fmt.Errorf("%w: canonicalize batch intent: %v", ErrCorruption, err)
	}
	want := sha256.Sum256(payload)
	got, err := hex.DecodeString(envelope.SHA256)
	if err != nil || len(got) != sha256.Size || !bytes.Equal(got, want[:]) {
		return batchIntent{}, false, fmt.Errorf("%w: batch intent checksum mismatch", ErrCorruption)
	}
	return envelope.Intent, true, nil
}

// recoverBatchIntent runs before segment recovery. A present intent means the
// batch never crossed its durable publish point, so complete-looking frames are
// rollback material and must never be promoted by the normal scanner.
func (s *Spool) recoverBatchIntent(cursorExists bool) error {
	intent, exists, err := loadBatchIntent(s.batchIntentPath())
	if err != nil {
		return err
	}
	if !exists {
		// A crash before the atomic rename cannot have written any batch frame.
		// Removing the unpublished temp is therefore safe.
		tmp := filepath.Join(s.opts.Dir, batchIntentTempName)
		if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("spool: remove stale batch intent temp: %w", err)
		} else if err == nil {
			if err := fsyncDir(s.opts.Dir); err != nil {
				return fmt.Errorf("spool: sync stale batch intent removal: %w", err)
			}
		}
		return nil
	}
	if !cursorExists {
		return fmt.Errorf("%w: batch intent exists without cursor", ErrCorruption)
	}
	if intent.CrawlerID != s.cursor.CrawlerID || intent.Epoch != s.cursor.Epoch ||
		intent.StartSeq == 0 || intent.StartSeq > math.MaxInt64 || intent.StartSeq <= s.cursor.AckedSequence ||
		intent.StartSegment == 0 || intent.StartSegment < s.cursor.AckedSegment || intent.StartOffset < 0 {
		return fmt.Errorf("%w: invalid batch intent identity or bounds", ErrCorruption)
	}

	ids, err := listSegments(s.opts.Dir)
	if err != nil {
		return err
	}
	if len(ids) == 0 || ids[0] > intent.StartSegment || ids[len(ids)-1] < intent.StartSegment {
		return fmt.Errorf("%w: batch intent start segment %d is not retained", ErrCorruption, intent.StartSegment)
	}
	for i := len(ids) - 1; i >= 0; i-- {
		if ids[i] <= intent.StartSegment {
			break
		}
		if err := os.Remove(segmentPath(s.opts.Dir, ids[i])); err != nil {
			return fmt.Errorf("spool: remove uncommitted %s: %w", segmentName(ids[i]), err)
		}
	}

	path := segmentPath(s.opts.Dir, intent.StartSegment)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("spool: reopen intent start %s: %w", segmentName(intent.StartSegment), err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: stat intent start: %w", err)
	}
	if intent.StartOffset > info.Size() {
		_ = f.Close()
		return fmt.Errorf("%w: batch intent offset %d exceeds segment size %d", ErrCorruption, intent.StartOffset, info.Size())
	}
	if err := f.Truncate(intent.StartOffset); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: truncate uncommitted batch: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("spool: sync uncommitted batch rollback: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("spool: close uncommitted batch rollback: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return fmt.Errorf("spool: sync uncommitted batch rollback directory: %w", err)
	}

	// The rollback sync also makes any pre-intent Append frames durable.
	s.cursor.NextSequence = intent.StartSeq
	s.durableSequence = intent.StartSeq - 1
	if err := s.saveCursorLocked(); err != nil {
		return fmt.Errorf("spool: persist cursor after intent rollback: %w", err)
	}
	if err := os.Remove(s.batchIntentPath()); err != nil {
		return fmt.Errorf("spool: remove recovered batch intent: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return fmt.Errorf("spool: sync recovered batch intent removal: %w", err)
	}
	return nil
}

func (s *Spool) clearBatchIntentLocked() error {
	if err := os.Remove(s.batchIntentPath()); err != nil {
		return fmt.Errorf("spool: remove committed batch intent: %w", err)
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return fmt.Errorf("spool: sync committed batch intent removal: %w", err)
	}
	return nil
}
