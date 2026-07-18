package spool

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// Encoding is a closed set of normalized spool schemas. There is deliberately
// no representation for raw bencode, an info dictionary, or piece hashes.
type Encoding string

const (
	EncodingNormalized Encoding = "normalized"
	EncodingSummary    Encoding = "summary"
	EncodingHashOnly   Encoding = "hash_only"
	EncodingReject     Encoding = "reject"
)

const (
	recordSchemaVersion = 1
	maxCrawlerIDBytes   = 256
	maxPolicyIDBytes    = 256
	maxRegionBytes      = 64
	maxNameBytes        = 16 << 10
	maxPathBytes        = 16 << 10
	maxReasonBytes      = 1024
	maxExtensionBytes   = 32
	maxNormalizedFiles  = 10_000
	maxSummaryFiles     = 64
	maxSummaryExts      = 128
)

var (
	errEmptyCrawlerID = errors.New("spool: record missing crawler_id")
	errBadEncoding    = errors.New("spool: record has invalid encoding/body combination")
	errBadInfoHash    = errors.New("spool: record has invalid infohash")
	errInvalidRecord  = errors.New("spool: invalid typed record")
)

// File is the lossless normalized file representation. Path is normalized
// text, never raw bencode bytes.
type File struct {
	Path   string `json:"path"`
	Length uint64 `json:"length"`
}

// ExtensionSummary is a bounded aggregate used by summary records.
type ExtensionSummary struct {
	Extension string `json:"extension"`
	Files     uint32 `json:"files"`
	Bytes     uint64 `json:"bytes"`
}

// NormalizedMetadata is the full, typed zero-raw representation.
type NormalizedMetadata struct {
	Name        string `json:"name"`
	TotalLength uint64 `json:"total_length"`
	PieceLength uint32 `json:"piece_length,omitempty"`
	Files       []File `json:"files"`
}

// SummaryMetadata is the bounded representation for torrents whose complete
// path list is too expensive. RepresentativeFiles contains basenames or short
// normalized paths only and is capped by validation.
type SummaryMetadata struct {
	Name                string             `json:"name"`
	TotalLength         uint64             `json:"total_length"`
	FileCount           uint32             `json:"file_count"`
	RepresentativeFiles []File             `json:"representative_files,omitempty"`
	Extensions          []ExtensionSummary `json:"extensions,omitempty"`
}

// HashOnlyMetadata preserves exact identity and a bounded policy reason. It is
// intentionally not searchable metadata.
type HashOnlyMetadata struct {
	Reason string `json:"reason"`
}

// RejectMetadata records an explicit policy rejection. Keeping reject distinct
// from hash_only lets central ingest mark exact processed state without
// pretending the record is eligible for refetch or search.
type RejectMetadata struct {
	Reason string `json:"reason"`
}

// Record is a tagged union. Exactly one body matching Encoding must be set.
// Delivery identity is assigned by Spool.Append and persisted inline.
type Record struct {
	Schema     int                 `json:"v"`
	CrawlerID  string              `json:"cid"`
	Epoch      uint64              `json:"ep"`
	Sequence   uint64              `json:"seq"`
	InfoHash   string              `json:"info_hash"`
	Encoding   Encoding            `json:"enc"`
	PolicyID   string              `json:"policy_id,omitempty"`
	FirstSeen  time.Time           `json:"first_seen,omitempty"`
	Region     string              `json:"region,omitempty"`
	Normalized *NormalizedMetadata `json:"normalized,omitempty"`
	Summary    *SummaryMetadata    `json:"summary,omitempty"`
	HashOnly   *HashOnlyMetadata   `json:"hash_only,omitempty"`
	Reject     *RejectMetadata     `json:"reject,omitempty"`
}

func (r *Record) validate() error {
	if r.Schema != recordSchemaVersion {
		return fmt.Errorf("%w: schema version %d", errInvalidRecord, r.Schema)
	}
	if !validBoundedText(r.CrawlerID, maxCrawlerIDBytes, false) {
		return errEmptyCrawlerID
	}
	if r.Epoch == 0 || r.Epoch > math.MaxInt64 || r.Sequence == 0 || r.Sequence > math.MaxInt64 {
		return fmt.Errorf("%w: epoch or sequence outside positive int64", errInvalidRecord)
	}
	if !validInfoHash(r.InfoHash) {
		return errBadInfoHash
	}
	if !validBoundedText(r.PolicyID, maxPolicyIDBytes, true) ||
		!validBoundedText(r.Region, maxRegionBytes, true) {
		return fmt.Errorf("%w: oversized or invalid envelope text", errInvalidRecord)
	}
	if !r.FirstSeen.IsZero() && r.FirstSeen.Location() != time.UTC {
		return fmt.Errorf("%w: first_seen must be UTC", errInvalidRecord)
	}

	bodies := 0
	if r.Normalized != nil {
		bodies++
	}
	if r.Summary != nil {
		bodies++
	}
	if r.HashOnly != nil {
		bodies++
	}
	if r.Reject != nil {
		bodies++
	}
	if bodies != 1 {
		return errBadEncoding
	}

	switch r.Encoding {
	case EncodingNormalized:
		if r.Normalized == nil {
			return errBadEncoding
		}
		return validateNormalized(r.Normalized)
	case EncodingSummary:
		if r.Summary == nil {
			return errBadEncoding
		}
		return validateSummary(r.Summary)
	case EncodingHashOnly:
		if r.HashOnly == nil {
			return errBadEncoding
		}
		return validateReason(r.HashOnly.Reason, "hash-only")
	case EncodingReject:
		if r.Reject == nil {
			return errBadEncoding
		}
		return validateReason(r.Reject.Reason, "reject")
	default:
		return errBadEncoding
	}
}

func validateNormalized(m *NormalizedMetadata) error {
	if !validBoundedText(m.Name, maxNameBytes, true) {
		return fmt.Errorf("%w: invalid name", errInvalidRecord)
	}
	if len(m.Files) == 0 || len(m.Files) > maxNormalizedFiles {
		return fmt.Errorf("%w: normalized file count %d", errInvalidRecord, len(m.Files))
	}
	var total uint64
	for i := range m.Files {
		if !validBoundedText(m.Files[i].Path, maxPathBytes, false) {
			return fmt.Errorf("%w: invalid file path at %d", errInvalidRecord, i)
		}
		if math.MaxUint64-total < m.Files[i].Length {
			return fmt.Errorf("%w: file length overflow", errInvalidRecord)
		}
		total += m.Files[i].Length
	}
	if total != m.TotalLength {
		return fmt.Errorf("%w: total_length=%d file sum=%d", errInvalidRecord, m.TotalLength, total)
	}
	return nil
}

func validateSummary(m *SummaryMetadata) error {
	if !validBoundedText(m.Name, maxNameBytes, true) {
		return fmt.Errorf("%w: invalid name", errInvalidRecord)
	}
	if m.FileCount == 0 || len(m.RepresentativeFiles) > maxSummaryFiles ||
		uint32(len(m.RepresentativeFiles)) > m.FileCount {
		return fmt.Errorf("%w: invalid summary file bounds", errInvalidRecord)
	}
	for i := range m.RepresentativeFiles {
		if !validBoundedText(m.RepresentativeFiles[i].Path, maxPathBytes, false) {
			return fmt.Errorf("%w: invalid representative path at %d", errInvalidRecord, i)
		}
	}
	if len(m.Extensions) > maxSummaryExts {
		return fmt.Errorf("%w: too many extension summaries", errInvalidRecord)
	}
	var files uint64
	var bytesTotal uint64
	seen := make(map[string]struct{}, len(m.Extensions))
	for i := range m.Extensions {
		e := m.Extensions[i]
		if !validBoundedText(e.Extension, maxExtensionBytes, false) || e.Files == 0 {
			return fmt.Errorf("%w: invalid extension summary at %d", errInvalidRecord, i)
		}
		key := strings.ToLower(e.Extension)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate extension %q", errInvalidRecord, e.Extension)
		}
		seen[key] = struct{}{}
		files += uint64(e.Files)
		if math.MaxUint64-bytesTotal < e.Bytes {
			return fmt.Errorf("%w: extension bytes overflow", errInvalidRecord)
		}
		bytesTotal += e.Bytes
	}
	if files > uint64(m.FileCount) || bytesTotal > m.TotalLength {
		return fmt.Errorf("%w: extension aggregate exceeds torrent totals", errInvalidRecord)
	}
	return nil
}

func validateReason(reason, kind string) error {
	if !validBoundedText(reason, maxReasonBytes, false) {
		return fmt.Errorf("%w: invalid %s reason", errInvalidRecord, kind)
	}
	return nil
}

func validInfoHash(s string) bool {
	if len(s) != 40 || strings.ToLower(s) != s {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 20
}

func validBoundedText(s string, maxBytes int, allowEmpty bool) bool {
	return (allowEmpty || s != "") && len(s) <= maxBytes && utf8.ValidString(s) && !strings.ContainsRune(s, '\x00')
}

func (r *Record) encode() ([]byte, error) {
	if r.Schema == 0 {
		r.Schema = recordSchemaVersion
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	if r.minimumTextBytes() > maxRecordLength {
		return nil, ErrRecordTooLarge
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxRecordLength {
		return nil, ErrRecordTooLarge
	}
	return encoded, nil
}

// minimumTextBytes is a cheap lower bound for JSON size. It prevents a record
// containing thousands of individually valid maximum-size paths from forcing
// an enormous temporary json.Marshal allocation merely to discover that the
// 4 MiB frame limit is exceeded.
func (r *Record) minimumTextBytes() int {
	total := len(r.CrawlerID) + len(r.InfoHash) + len(r.PolicyID) + len(r.Region)
	add := func(s string) {
		if total <= maxRecordLength {
			total += len(s)
		}
	}
	switch r.Encoding {
	case EncodingNormalized:
		add(r.Normalized.Name)
		for i := range r.Normalized.Files {
			add(r.Normalized.Files[i].Path)
		}
	case EncodingSummary:
		add(r.Summary.Name)
		for i := range r.Summary.RepresentativeFiles {
			add(r.Summary.RepresentativeFiles[i].Path)
		}
		for i := range r.Summary.Extensions {
			add(r.Summary.Extensions[i].Extension)
		}
	case EncodingHashOnly:
		add(r.HashOnly.Reason)
	case EncodingReject:
		add(r.Reject.Reason)
	}
	return total
}

func decodeRecord(b []byte) (Record, error) {
	var r Record
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return Record{}, fmt.Errorf("spool: decode record: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return Record{}, err
	}
	if err := r.validate(); err != nil {
		return Record{}, err
	}
	return r, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("spool: trailing JSON value")
		}
		return fmt.Errorf("spool: trailing JSON: %w", err)
	}
	return nil
}
