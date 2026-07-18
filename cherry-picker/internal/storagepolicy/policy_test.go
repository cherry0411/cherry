package storagepolicy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"cherry-picker/internal/pipeline"
	"cherry-picker/internal/spool"
)

const validHash = "aabbccddeeff00112233445566778899aabbccdd"

func TestDefaultPolicyFileThreshold(t *testing.T) {
	policy := MustDefault()
	full := policy.Decide(validHash, time.Time{}, metadataWithFiles(2000), "")
	if full.Action != ActionFull || full.Record.Encoding != spool.EncodingNormalized {
		t.Fatalf("at threshold: %+v", full)
	}
	summary := policy.Decide(validHash, time.Time{}, metadataWithFiles(2001), "")
	if summary.Action != ActionSummary || summary.Record.Encoding != spool.EncodingSummary {
		t.Fatalf("above threshold: %+v", summary)
	}
	if summary.Record.Summary.FileCount != 2001 || len(summary.Record.Summary.RepresentativeFiles) > 32 || len(summary.Record.Summary.Extensions) > 32 {
		t.Fatalf("unbounded summary: %+v", summary.Record.Summary)
	}
}

func TestDefaultPolicyPathBudget(t *testing.T) {
	metadata := metadataWithFiles(200)
	for i := range metadata.Files {
		metadata.Files[i].PathText = strings.Repeat("x", 3000) + fmt.Sprint(i)
	}
	decision := MustDefault().Decide(validHash, time.Time{}, metadata, "")
	if decision.Action != ActionSummary || decision.Reason != ReasonPathBytes {
		t.Fatalf("decision=%+v", decision)
	}
	aliasBytes := 0
	for _, file := range decision.Record.Summary.RepresentativeFiles {
		aliasBytes += len(file.Path)
	}
	if aliasBytes > DefaultConfig().SummaryAliasBytes {
		t.Fatalf("alias bytes=%d exceed budget", aliasBytes)
	}
}

func TestLegacyFilterOnlyDowngradesToSummary(t *testing.T) {
	decision := MustDefault().Decide(validHash, time.Now(), metadataWithFiles(2), "numeric_file_names")
	if decision.Action != ActionSummary || !strings.Contains(decision.Reason, "numeric_file_names") {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestOptionalHashOnlyAndRejectCaps(t *testing.T) {
	config := DefaultConfig()
	config.HashOnlyAboveFiles = 3000
	config.RejectAboveFiles = 4000
	policy, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	hashOnly := policy.Decide(validHash, time.Time{}, metadataWithFiles(3001), "")
	if hashOnly.Action != ActionHashOnly || hashOnly.Record.DecisionCode != spool.DecisionHashOnlyFileCap {
		t.Fatalf("hash-only=%+v", hashOnly)
	}
	reject := policy.Decide(validHash, time.Time{}, metadataWithFiles(4001), "")
	if reject.Action != ActionReject || reject.Record.Encoding != spool.EncodingReject ||
		reject.Record.DecisionCode != spool.DecisionRejectFileCap ||
		reject.Record.Normalized != nil || reject.Record.Summary != nil {
		t.Fatalf("reject=%+v", reject)
	}
}

func TestCompactDecisionCodeClosedMapping(t *testing.T) {
	tests := []struct {
		action Action
		reason string
		want   spool.DecisionCode
	}{
		{ActionHashOnly, "other", spool.DecisionHashOnly},
		{ActionReject, "other", spool.DecisionReject},
		{ActionHashOnly, ReasonHashOnlyCap, spool.DecisionHashOnlyFileCap},
		{ActionReject, ReasonRejectCap, spool.DecisionRejectFileCap},
		{ActionHashOnly, ReasonInvalid, spool.DecisionInvalidMetadata},
	}
	for _, test := range tests {
		if got := compactDecisionCode(test.action, test.reason); got != test.want {
			t.Errorf("action=%s reason=%s code=%d want=%d", test.action, test.reason, got, test.want)
		}
	}
}

func TestPolicyRejectsBudgetsOutsideClosedWireSchema(t *testing.T) {
	tests := []func(*Config){
		func(config *Config) { config.SummaryAboveFiles = maxWireNormalizedFiles + 1 },
		func(config *Config) { config.MaxNameBytes = maxWireNameBytes + 1 },
		func(config *Config) { config.MaxFullPathBytes = maxWirePathBytes + 1 },
		func(config *Config) { config.SummaryMaxAliases = maxWireSummaryAliases + 1 },
		func(config *Config) { config.SummaryMaxExtensions = maxWireSummaryExts + 1 },
	}
	for index, mutate := range tests {
		config := DefaultConfig()
		mutate(&config)
		if _, err := New(config); err == nil {
			t.Fatalf("case %d accepted an unwritable wire budget: %+v", index, config)
		}
	}
}

func TestInvalidMetadataBecomesExactHashOnly(t *testing.T) {
	metadata := metadataWithFiles(1)
	metadata.Files[0].Length = -1
	decision := MustDefault().Decide(validHash, time.Time{}, metadata, "")
	if decision.Action != ActionHashOnly || decision.Record.InfoHash != validHash ||
		decision.Reason != ReasonInvalid || decision.Record.DecisionCode != spool.DecisionInvalidMetadata {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestRuntimePolicyConfigChangesDecisionWithoutPersistedIdentity(t *testing.T) {
	metadata := metadataWithFiles(DefaultConfig().SummaryAboveFiles)
	if got := MustDefault().Decide(validHash, time.Time{}, metadata, ""); got.Action != ActionFull {
		t.Fatalf("default decision=%+v", got)
	}
	config := DefaultConfig()
	config.SummaryAboveFiles--
	policy, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	decision := policy.Decide(validHash, time.Time{}, metadata, "")
	if decision.Action != ActionSummary {
		t.Fatalf("configured decision=%+v", decision)
	}
	encoded, err := json.Marshal(decision.Record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "policy_id") {
		t.Fatalf("policy identity leaked into record: %s", encoded)
	}
}

func TestDecisionJSONHasNoRawOrPiecesFields(t *testing.T) {
	decision := MustDefault().Decide(validHash, time.Now(), metadataWithFiles(3), "")
	encoded, err := json.Marshal(decision.Record)
	if err != nil {
		t.Fatal(err)
	}
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				switch strings.ToLower(key) {
				case "raw", "raw_bytes", "bencode", "pieces", "piece_hashes", "piece_length",
					"policy_id", "region", "source", "reason", "hash_only", "reject":
					t.Fatalf("forbidden key %q in %s", key, encoded)
				}
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	walk(decoded)
}

func TestDecisionCanEnterTypedSpool(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), CrawlerID: "jp-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	decision := MustDefault().Decide(validHash, time.Now(), metadataWithFiles(2001), "")
	if _, err := sp.AppendBatchDurable([]spool.Record{decision.Record}); err != nil {
		t.Fatalf("append policy record: %v", err)
	}
}

func metadataWithFiles(count int) *pipeline.Metadata {
	files := make([]pipeline.MetadataFile, count)
	for i := range files {
		name := fmt.Sprintf("folder/file-%06d.mp4", i)
		files[i] = pipeline.MetadataFile{Path: strings.Split(name, "/"), PathText: name, Length: int64(i + 1)}
	}
	return &pipeline.Metadata{Name: "测试 package", FileCount: count, Files: files}
}

func BenchmarkDecide(b *testing.B) {
	policy := MustDefault()
	for _, fileCount := range []int{100, 2_000, 10_000} {
		metadata := metadataWithFiles(fileCount)
		b.Run(fmt.Sprintf("files_%d", fileCount), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = policy.Decide(validHash, time.Time{}, metadata, "")
			}
		})
	}
	metadata := metadataWithFiles(10_000)
	for index := range metadata.Files {
		metadata.Files[index].PathText = fmt.Sprintf("file-%05d.ext%05d", index, index)
	}
	b.Run("files_10000_unique_extensions", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = policy.Decide(validHash, time.Time{}, metadata, "")
		}
	})
}
