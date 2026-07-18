package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	"cherry-picker/internal/spool"
)

// durableEvent must remain byte-for-byte JSON compatible with the metadata
// export protocol. The recovery command verifies the server receipt digest
// before it advances a local cursor, so it cannot acknowledge a different
// prefix merely because the server reports a later expected sequence.
type durableEvent struct {
	InfoHash     string                    `json:"info_hash"`
	Encoding     spool.Encoding            `json:"encoding"`
	DecisionCode spool.DecisionCode        `json:"decision_code,omitempty"`
	FirstSeen    time.Time                 `json:"first_seen,omitempty"`
	Normalized   *spool.NormalizedMetadata `json:"normalized,omitempty"`
	Summary      *spool.SummaryMetadata    `json:"summary,omitempty"`
}

func main() {
	var (
		dir           = flag.String("dir", "", "durable metadata spool directory")
		crawler       = flag.String("crawler", "", "expected crawler ID")
		expectedStart = flag.Uint64("server-expected-start", 0, "server receipt head next sequence")
		receiptSHA    = flag.String("server-receipt-sha256", "", "SHA-256 from the server's last receipt")
		commit        = flag.Bool("commit", false, "advance the local cursor after every identity check passes")
	)
	flag.Parse()
	if strings.TrimSpace(*dir) == "" || strings.TrimSpace(*crawler) == "" || *expectedStart < 2 {
		fatalf("dir, crawler, and server-expected-start>=2 are required")
	}
	currentUser, err := user.Current()
	if err != nil {
		fatalf("resolve current user before opening spool: %v", err)
	}
	if err := validateOperator(currentUser.Uid); err != nil {
		fatalf("%v", err)
	}
	wantSHA, err := hex.DecodeString(strings.TrimSpace(*receiptSHA))
	if err != nil || len(wantSHA) != sha256.Size {
		fatalf("server-receipt-sha256 must be exactly 64 hex characters")
	}

	s, err := spool.Open(spool.Options{Dir: *dir, CrawlerID: *crawler})
	if err != nil {
		fatalf("open spool: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			fatalf("close spool: %v", err)
		}
	}()

	epoch, acked, next, durable, err := s.CursorPosition()
	if err != nil {
		fatalf("read cursor: %v", err)
	}
	localStart := acked + 1
	if localStart >= *expectedStart {
		fatalf("server expected start %d does not advance local start %d", *expectedStart, localStart)
	}
	count := *expectedStart - localStart
	if count > 5_000 {
		fatalf("receipt prefix %d exceeds protocol batch maximum", count)
	}
	batch, err := s.NextBatch(int(count))
	if err != nil {
		fatalf("load receipt prefix: %v", err)
	}
	if batch.CrawlerID != *crawler || batch.Epoch != epoch || batch.StartSeq != localStart ||
		batch.EndSeq != *expectedStart-1 || uint64(len(batch.Records)) != count {
		fatalf("prefix identity mismatch crawler=%q epoch=%d range=%d..%d records=%d",
			batch.CrawlerID, batch.Epoch, batch.StartSeq, batch.EndSeq, len(batch.Records))
	}
	events := make([]durableEvent, len(batch.Records))
	for i, record := range batch.Records {
		events[i] = durableEvent{
			InfoHash: record.InfoHash, Encoding: record.Encoding,
			DecisionCode: record.DecisionCode, FirstSeen: record.FirstSeen,
			Normalized: record.Normalized, Summary: record.Summary,
		}
	}
	payload, err := json.Marshal(events)
	if err != nil {
		fatalf("encode receipt prefix: %v", err)
	}
	gotSHA := sha256.Sum256(payload)
	if !strings.EqualFold(hex.EncodeToString(gotSHA[:]), strings.TrimSpace(*receiptSHA)) {
		fatalf("receipt digest mismatch local=%x server=%s", gotSHA, strings.ToLower(strings.TrimSpace(*receiptSHA)))
	}

	result := map[string]any{
		"crawler_id": batch.CrawlerID, "epoch": epoch,
		"local_acked_before": acked, "local_next": next, "local_durable": durable,
		"verified_start": batch.StartSeq, "verified_end": batch.EndSeq,
		"verified_records": len(batch.Records), "payload_sha256": fmt.Sprintf("%x", gotSHA),
		"committed": false,
	}
	if *commit {
		if err := s.CommitBatch(batch); err != nil {
			fatalf("commit verified receipt prefix: %v", err)
		}
		_, ackedAfter, nextAfter, durableAfter, err := s.CursorPosition()
		if err != nil {
			fatalf("read committed cursor: %v", err)
		}
		if ackedAfter != *expectedStart-1 {
			fatalf("committed cursor=%d want=%d", ackedAfter, *expectedStart-1)
		}
		result["local_acked_after"] = ackedAfter
		result["local_next_after"] = nextAfter
		result["local_durable_after"] = durableAfter
		result["committed"] = true
	}
	encoded, _ := json.Marshal(result)
	fmt.Println(string(encoded))
}

func validateOperator(uid string) error {
	if strings.TrimSpace(uid) == "" {
		return fmt.Errorf("current user has no stable UID; refusing to open spool")
	}
	if uid == "0" {
		return fmt.Errorf("refusing to run as root; run as the spool service user")
	}
	return nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "spool-reconcile: "+format+"\n", args...)
	os.Exit(1)
}
