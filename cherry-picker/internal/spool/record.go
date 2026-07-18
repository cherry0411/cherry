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

// DecisionCode is the compact, closed authority shared with durable ingest.
// Zero is reserved for searchable normalized/summary records.
type DecisionCode uint8

const (
	DecisionHashOnly        DecisionCode = 1
	DecisionReject          DecisionCode = 2
	DecisionHashOnlyFileCap DecisionCode = 3
	DecisionRejectFileCap   DecisionCode = 4
	DecisionInvalidMetadata DecisionCode = 5
)

const (
	recordSchemaVersion = 2
	maxCrawlerIDBytes   = 256
	maxNameBytes        = 16 << 10
	maxPathBytes        = 16 << 10
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
	// ErrIncompatibleSchema requires an explicit operator archive/reset. The
	// spool never drops or rewrites an unacknowledged record during Open.
	ErrIncompatibleSchema = errors.New("spool: incompatible record schema")
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
	Files       []File `json:"files"`
}

// SummaryMetadata is the bounded representation for torrents whose complete
// path list is too expensive. RepresentativeFiles is retained only so rolling
// upgrades can decode and replay older durable summaries; the current storage
// policy always leaves it empty. Validation remains bounded for legacy input.
type SummaryMetadata struct {
	Name                string             `json:"name"`
	TotalLength         uint64             `json:"total_length"`
	FileCount           uint32             `json:"file_count"`
	RepresentativeFiles []File             `json:"representative_files,omitempty"`
	Extensions          []ExtensionSummary `json:"extensions,omitempty"`
}

// Record is a tagged union. Searchable encodings carry exactly one matching
// body. Hash-only/reject encodings are bodyless and carry one closed decision
// code. Delivery identity is assigned by Spool.Append and persisted inline.
type Record struct {
	Schema       int                 `json:"v"`
	CrawlerID    string              `json:"cid"`
	Epoch        uint64              `json:"ep"`
	Sequence     uint64              `json:"seq"`
	InfoHash     string              `json:"info_hash"`
	Encoding     Encoding            `json:"enc"`
	DecisionCode DecisionCode        `json:"decision_code,omitempty"`
	FirstSeen    time.Time           `json:"first_seen,omitempty"`
	Normalized   *NormalizedMetadata `json:"normalized,omitempty"`
	Summary      *SummaryMetadata    `json:"summary,omitempty"`
}

func (r *Record) validate() error {
	if r.Schema != recordSchemaVersion {
		return fmt.Errorf("%w: found record v%d, require v%d; archive the old spool directory and start with a new empty directory",
			ErrIncompatibleSchema, r.Schema, recordSchemaVersion)
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
	switch r.Encoding {
	case EncodingNormalized:
		if r.Normalized == nil || bodies != 1 || r.DecisionCode != 0 {
			return errBadEncoding
		}
		return validateNormalized(r.Normalized)
	case EncodingSummary:
		if r.Summary == nil || bodies != 1 || r.DecisionCode != 0 {
			return errBadEncoding
		}
		return validateSummary(r.Summary)
	case EncodingHashOnly:
		if bodies != 0 || !isHashOnlyDecision(r.DecisionCode) {
			return errBadEncoding
		}
		return nil
	case EncodingReject:
		if bodies != 0 || !isRejectDecision(r.DecisionCode) {
			return errBadEncoding
		}
		return nil
	default:
		return errBadEncoding
	}
}

func isHashOnlyDecision(code DecisionCode) bool {
	return code == DecisionHashOnly || code == DecisionHashOnlyFileCap || code == DecisionInvalidMetadata
}

func isRejectDecision(code DecisionCode) bool {
	return code == DecisionReject || code == DecisionRejectFileCap
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
	total := len(r.CrawlerID) + len(r.InfoHash)
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
	}
	return total
}

func decodeRecord(b []byte) (Record, error) {
	var version struct {
		Schema int `json:"v"`
	}
	if err := json.Unmarshal(b, &version); err != nil {
		return Record{}, fmt.Errorf("spool: decode record: %w", err)
	}
	if version.Schema != recordSchemaVersion {
		return Record{}, fmt.Errorf("%w: found record v%d, require v%d; archive the old spool directory and start with a new empty directory",
			ErrIncompatibleSchema, version.Schema, recordSchemaVersion)
	}
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
