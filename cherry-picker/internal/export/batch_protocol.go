package export

import (
	"time"

	"cherry-picker/internal/spool"
)

// durableProtocolMaxEvents must stay aligned with the backend validator. The
// exporter clamps operator-supplied batch sizes so a valid local configuration
// cannot create a permanent non-retryable 400 at the head of the spool.
const durableProtocolMaxEvents = 5_000

// DurableBatchRequest documents the durable ingest wire contract. The sender
// writes Events as pre-marshaled JSON so PayloadSHA256 covers the exact bytes
// present in the HTTP body, not a backend re-serialization.
type DurableBatchRequest struct {
	CrawlerID     string         `json:"crawler_id"`
	Epoch         uint64         `json:"epoch"`
	StartSequence uint64         `json:"start_sequence"`
	EndSequence   uint64         `json:"end_sequence"`
	PayloadSHA256 string         `json:"payload_sha256"`
	Events        []DurableEvent `json:"events"`
}

// DurableEvent is a closed, typed, zero-raw union. There is intentionally no
// RawMessage escape hatch and no field for bencode, pieces, or piece hashes.
type DurableEvent struct {
	InfoHash   string                    `json:"info_hash"`
	Encoding   spool.Encoding            `json:"encoding"`
	PolicyID   string                    `json:"policy_id,omitempty"`
	FirstSeen  time.Time                 `json:"first_seen,omitempty"`
	Region     string                    `json:"region,omitempty"`
	Normalized *spool.NormalizedMetadata `json:"normalized,omitempty"`
	Summary    *spool.SummaryMetadata    `json:"summary,omitempty"`
	HashOnly   *spool.HashOnlyMetadata   `json:"hash_only,omitempty"`
	Reject     *spool.RejectMetadata     `json:"reject,omitempty"`
}

func durableEventFromRecord(record spool.Record) DurableEvent {
	return DurableEvent{
		InfoHash:   record.InfoHash,
		Encoding:   record.Encoding,
		PolicyID:   record.PolicyID,
		FirstSeen:  record.FirstSeen,
		Region:     record.Region,
		Normalized: record.Normalized,
		Summary:    record.Summary,
		HashOnly:   record.HashOnly,
		Reject:     record.Reject,
	}
}

// DurableBatchResponse is the only ACK that permits local cursor advancement.
// The crawler validates every identity field, committed=true, checksum, and
// result counts before deleting any local data.
type DurableBatchResponse struct {
	CrawlerID     string `json:"crawler_id"`
	Epoch         uint64 `json:"epoch"`
	StartSequence uint64 `json:"start_sequence"`
	EndSequence   uint64 `json:"end_sequence"`
	PayloadSHA256 string `json:"payload_sha256"`
	Accepted      int    `json:"accepted"`
	Duplicates    int    `json:"duplicates"`
	Errors        int    `json:"errors"`
	Committed     bool   `json:"committed"`
	ExpectedStart uint64 `json:"expected_start,omitempty"`
	Error         string `json:"error,omitempty"`
}
