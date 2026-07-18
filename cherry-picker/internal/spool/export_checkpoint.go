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
	"strings"
)

const (
	exportCheckpointName            = "export-inflight.json"
	exportCheckpointEnvelopeVersion = 1
	exportCheckpointSchemaVersion   = 1
	exportReplayMaxRecords          = 5_000
	// A replay may ignore a newly lowered MaxBatchBytes setting, but remains
	// bounded by the largest batch this spool implementation can ever create.
	exportReplayMaxPayloadBytes int64 = 64 << 20
)

// exportCheckpointState is the durable identity of the one HTTP metadata batch
// that may have reached the receipt authority. The spool records only the
// boundary and digest; records remain authoritative in the segment log.
type exportCheckpointState struct {
	Version       int    `json:"version"`
	CrawlerID     string `json:"crawler_id"`
	Epoch         uint64 `json:"epoch"`
	StartSequence uint64 `json:"start_sequence"`
	EndSequence   uint64 `json:"end_sequence"`
	RecordCount   int    `json:"record_count"`
	WireSchema    int    `json:"wire_schema"`
	PayloadSHA256 string `json:"payload_sha256"`
}

type exportCheckpointEnvelope struct {
	Version int                   `json:"version"`
	State   exportCheckpointState `json:"state"`
	SHA256  string                `json:"sha256"`
}

func (s *Spool) exportCheckpointPath() string {
	return filepath.Join(s.opts.Dir, exportCheckpointName)
}

func (s *Spool) exportCheckpointTempPath() string {
	return s.exportCheckpointPath() + ".tmp"
}

func validateExportCheckpoint(c exportCheckpointState) error {
	if c.Version != exportCheckpointSchemaVersion ||
		!validBoundedText(c.CrawlerID, maxCrawlerIDBytes, false) ||
		c.Epoch == 0 || c.Epoch > math.MaxInt64 || c.StartSequence == 0 ||
		c.StartSequence > math.MaxInt64 || c.EndSequence < c.StartSequence || c.EndSequence > math.MaxInt64 ||
		c.RecordCount <= 0 || c.RecordCount > exportReplayMaxRecords ||
		uint64(c.RecordCount) != c.EndSequence-c.StartSequence+1 ||
		c.WireSchema <= 0 || len(c.PayloadSHA256) != sha256.Size*2 ||
		strings.ToLower(c.PayloadSHA256) != c.PayloadSHA256 {
		return fmt.Errorf("%w: invalid export checkpoint", ErrCorruption)
	}
	digest, err := hex.DecodeString(c.PayloadSHA256)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("%w: invalid export checkpoint payload digest", ErrCorruption)
	}
	return nil
}

func (s *Spool) loadExportCheckpointLocked() error {
	data, err := os.ReadFile(s.exportCheckpointPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("spool: read export checkpoint: %w", err)
	}
	var envelope exportCheckpointEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return fmt.Errorf("%w: decode export checkpoint: %v", ErrCorruption, err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return fmt.Errorf("%w: export checkpoint: %v", ErrCorruption, err)
	}
	if envelope.Version != exportCheckpointEnvelopeVersion {
		return fmt.Errorf("%w: unsupported export checkpoint envelope version %d", ErrCorruption, envelope.Version)
	}
	payload, err := json.Marshal(envelope.State)
	if err != nil {
		return fmt.Errorf("spool: canonicalize export checkpoint: %w", err)
	}
	want := sha256.Sum256(payload)
	got, err := hex.DecodeString(envelope.SHA256)
	if err != nil || len(got) != sha256.Size || !bytes.Equal(got, want[:]) {
		return fmt.Errorf("%w: export checkpoint checksum mismatch", ErrCorruption)
	}
	c := envelope.State
	if err := validateExportCheckpoint(c); err != nil {
		return err
	}
	if c.CrawlerID != s.cursor.CrawlerID || c.Epoch != s.cursor.Epoch {
		return fmt.Errorf("%w: export checkpoint identity mismatch", ErrCorruption)
	}
	if s.cursor.AckedSequence >= c.EndSequence {
		// CommitBatch persists the ACK cursor before checkpoint cleanup. A crash
		// in that window leaves a harmless stale checkpoint which recovery must
		// remove before exposing the spool again.
		if err := s.removeExportCheckpointLocked(); err != nil {
			return fmt.Errorf("spool: clean committed export checkpoint: %w", err)
		}
		return nil
	}
	if c.StartSequence != s.cursor.AckedSequence+1 || c.EndSequence > s.durableSequence {
		return fmt.Errorf("%w: export checkpoint range %d..%d is inconsistent with ack=%d durable=%d",
			ErrCorruption, c.StartSequence, c.EndSequence, s.cursor.AckedSequence, s.durableSequence)
	}
	s.exportCheckpoint = &c
	return nil
}

// EnsureExportCheckpoint durably publishes the exact HTTP batch identity
// before the exporter is allowed to perform network I/O. On replay it verifies
// that regenerated wire bytes have exactly the persisted schema and digest.
func (s *Spool) EnsureExportCheckpoint(b Batch, wireSchema int, payloadSHA256 string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	if !s.batchMatchesLocked(b) {
		return ErrCommitMismatch
	}
	c := exportCheckpointState{
		Version:       exportCheckpointSchemaVersion,
		CrawlerID:     b.CrawlerID,
		Epoch:         b.Epoch,
		StartSequence: b.StartSeq,
		EndSequence:   b.EndSeq,
		RecordCount:   len(b.Records),
		WireSchema:    wireSchema,
		PayloadSHA256: payloadSHA256,
	}
	if err := validateExportCheckpoint(c); err != nil {
		return err
	}
	if s.exportCheckpoint != nil {
		if *s.exportCheckpoint != c {
			return s.poisonLocked(fmt.Errorf("%w: regenerated export batch does not match durable checkpoint", ErrCorruption))
		}
		return nil
	}
	if err := s.writeExportCheckpointLocked(c); err != nil {
		return s.poisonLocked(fmt.Errorf("spool: persist export checkpoint: %w", err))
	}
	s.exportCheckpoint = &c
	return nil
}

// ClearExportCheckpoint is called only after CommitBatch has durably advanced
// the local ACK cursor. Removal is directory-synced; a crash before it becomes
// durable is recovered as a stale committed checkpoint on the next Open.
func (s *Spool) ClearExportCheckpoint(b Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthLocked(); err != nil {
		return err
	}
	c := s.exportCheckpoint
	if c == nil || c.CrawlerID != b.CrawlerID || c.Epoch != b.Epoch ||
		c.StartSequence != b.StartSeq || c.EndSequence != b.EndSeq ||
		s.cursor.AckedSequence < b.EndSeq || s.inflight != nil {
		return ErrCommitMismatch
	}
	if err := s.removeExportCheckpointLocked(); err != nil {
		return s.poisonLocked(fmt.Errorf("spool: clear export checkpoint: %w", err))
	}
	s.exportCheckpoint = nil
	return nil
}

func (s *Spool) writeExportCheckpointLocked(c exportCheckpointState) error {
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(exportCheckpointEnvelope{
		Version: exportCheckpointEnvelopeVersion,
		State:   c,
		SHA256:  hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return err
	}
	tmp := s.exportCheckpointTempPath()
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale export checkpoint temp: %w", err)
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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
	if err := os.Rename(tmp, s.exportCheckpointPath()); err != nil {
		return err
	}
	if err := fsyncDir(s.opts.Dir); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Spool) removeExportCheckpointLocked() error {
	err := os.Remove(s.exportCheckpointPath())
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		return fsyncDir(s.opts.Dir)
	}
	return nil
}

// exportReplayBoundaryLocked returns the immutable end/count for a batch that
// may already have reached the server. The caller must hold s.mu.
func (s *Spool) exportReplayBoundaryLocked() (end uint64, count int, ok bool) {
	if s.exportCheckpoint == nil {
		return 0, 0, false
	}
	return s.exportCheckpoint.EndSequence, s.exportCheckpoint.RecordCount, true
}
