// Package storagepolicy converts parsed metadata into a bounded, versioned,
// zero-raw storage decision. It never accepts or emits bencode or piece hashes.
package storagepolicy

import (
	"errors"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"cherry-picker/internal/pipeline"
	"cherry-picker/internal/spool"
)

type Action string

const (
	ActionFull     Action = "full"
	ActionSummary  Action = "summary"
	ActionHashOnly Action = "hash_only"
	ActionReject   Action = "reject"
)

const (
	ReasonFull          = "within_budget"
	ReasonFileCount     = "file_count_budget"
	ReasonPathBytes     = "path_bytes_budget"
	ReasonRuleDowngrade = "filter_rule_downgrade"
	ReasonHashOnlyCap   = "hash_only_file_cap"
	ReasonRejectCap     = "reject_file_cap"
	ReasonInvalid       = "invalid_normalized_metadata"

	// These are the closed wire schema's hard safety limits. Policy budgets may
	// be stricter, but accepting looser values would turn an otherwise valid
	// crawler configuration into a fatal spool/backend validation error.
	maxWireNormalizedFiles = 10_000
	maxWireNameBytes       = 16 << 10
	maxWirePathBytes       = 16 << 10
	maxWireSummaryAliases  = 64
	maxWireSummaryExts     = 128
)

// Config remains runtime-tunable, but its identity is deliberately not stored
// in durable metadata. Callers should start with DefaultConfig and override
// explicit fields.
type Config struct {
	Version               int `json:"version"`
	SummaryAboveFiles     int `json:"summary_above_files"`
	SummaryAbovePathBytes int `json:"summary_above_path_bytes"`
	MaxFullPathBytes      int `json:"max_full_path_bytes"`
	MaxNameBytes          int `json:"max_name_bytes"`
	SummaryMaxAliases     int `json:"summary_max_aliases"`
	SummaryAliasBytes     int `json:"summary_alias_bytes"`
	SummaryMaxExtensions  int `json:"summary_max_extensions"`
	HashOnlyAboveFiles    int `json:"hash_only_above_files"`
	RejectAboveFiles      int `json:"reject_above_files"`
}

func DefaultConfig() Config {
	return Config{
		Version:               1,
		SummaryAboveFiles:     2000,
		SummaryAbovePathBytes: 512 << 10,
		MaxFullPathBytes:      4096,
		MaxNameBytes:          1024,
		SummaryMaxAliases:     32,
		SummaryAliasBytes:     4096,
		SummaryMaxExtensions:  32,
		// Irreversible/low-retention actions remain disabled until the corpus
		// benchmark demonstrates a worthwhile storage/quality tradeoff.
		HashOnlyAboveFiles: 0,
		RejectAboveFiles:   0,
	}
}

type Policy struct {
	config Config
}

func New(config Config) (*Policy, error) {
	if config.Version <= 0 || config.SummaryAboveFiles <= 0 ||
		config.SummaryAbovePathBytes <= 0 || config.MaxFullPathBytes <= 0 ||
		config.MaxNameBytes <= 0 || config.SummaryMaxAliases <= 0 ||
		config.SummaryAliasBytes <= 0 || config.SummaryMaxExtensions <= 0 ||
		config.HashOnlyAboveFiles < 0 || config.RejectAboveFiles < 0 {
		return nil, errors.New("storagepolicy: invalid non-positive budget")
	}
	if config.SummaryAboveFiles > maxWireNormalizedFiles ||
		config.MaxNameBytes > maxWireNameBytes ||
		config.MaxFullPathBytes > maxWirePathBytes ||
		config.SummaryMaxAliases > maxWireSummaryAliases ||
		config.SummaryMaxExtensions > maxWireSummaryExts {
		return nil, errors.New("storagepolicy: budget exceeds durable wire schema")
	}
	if config.HashOnlyAboveFiles > 0 && config.HashOnlyAboveFiles <= config.SummaryAboveFiles {
		return nil, errors.New("storagepolicy: hash-only cap must exceed summary cap")
	}
	if config.RejectAboveFiles > 0 {
		floor := config.SummaryAboveFiles
		if config.HashOnlyAboveFiles > floor {
			floor = config.HashOnlyAboveFiles
		}
		if config.RejectAboveFiles <= floor {
			return nil, errors.New("storagepolicy: reject cap must exceed lower-retention caps")
		}
	}
	return &Policy{config: config}, nil
}

func MustDefault() *Policy {
	p, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	return p
}

type Decision struct {
	Action Action
	Reason string
	Record spool.Record
}

// Decide creates a bounded record. downgradeReason is an optional legacy
// content-filter signal; in the compact policy it lowers full to summary rather
// than discarding an already-downloaded title.
func (p *Policy) Decide(infoHash string, firstSeen time.Time, metadata *pipeline.Metadata, downgradeReason string) Decision {
	record := spool.Record{
		InfoHash:  strings.ToLower(strings.TrimSpace(infoHash)),
		FirstSeen: utcOrZero(firstSeen),
	}
	if metadata == nil || len(metadata.Files) == 0 {
		return p.hashDecision(record, ActionHashOnly, ReasonInvalid)
	}

	fileCount := len(metadata.Files)
	if uint64(fileCount) > uint64(^uint32(0)) {
		return p.hashDecision(record, ActionHashOnly, ReasonInvalid)
	}
	if p.config.RejectAboveFiles > 0 && fileCount > p.config.RejectAboveFiles {
		return p.hashDecision(record, ActionReject, ReasonRejectCap)
	}
	if p.config.HashOnlyAboveFiles > 0 && fileCount > p.config.HashOnlyAboveFiles {
		return p.hashDecision(record, ActionHashOnly, ReasonHashOnlyCap)
	}

	files, total, pathBytes, oversizedPath, valid := projectFiles(metadata.Files, p.config.MaxFullPathBytes)
	if !valid {
		return p.hashDecision(record, ActionHashOnly, ReasonInvalid)
	}
	reason := ReasonFull
	summarize := false
	if downgradeReason != "" {
		summarize = true
		reason = ReasonRuleDowngrade + ":" + boundedText(downgradeReason, 128)
	} else if fileCount > p.config.SummaryAboveFiles {
		summarize = true
		reason = ReasonFileCount
	} else if pathBytes > p.config.SummaryAbovePathBytes || oversizedPath {
		summarize = true
		reason = ReasonPathBytes
	}

	name := boundedText(metadata.Name, p.config.MaxNameBytes)
	if summarize {
		record.Encoding = spool.EncodingSummary
		record.Summary = p.makeSummary(name, total, files)
		return Decision{Action: ActionSummary, Reason: reason, Record: record}
	}

	normalizedFiles := make([]spool.File, len(files))
	for i := range files {
		normalizedFiles[i] = spool.File{Path: files[i].path, Length: files[i].length}
	}
	record.Encoding = spool.EncodingNormalized
	record.Normalized = &spool.NormalizedMetadata{
		Name: name, TotalLength: total, Files: normalizedFiles,
	}
	return Decision{Action: ActionFull, Reason: ReasonFull, Record: record}
}

func (p *Policy) hashDecision(record spool.Record, action Action, reason string) Decision {
	record.Encoding = spool.EncodingHashOnly
	if action == ActionReject {
		record.Encoding = spool.EncodingReject
	}
	record.DecisionCode = compactDecisionCode(action, reason)
	return Decision{Action: action, Reason: reason, Record: record}
}

func compactDecisionCode(action Action, reason string) spool.DecisionCode {
	switch {
	case reason == ReasonInvalid:
		return spool.DecisionInvalidMetadata
	case action == ActionHashOnly && reason == ReasonHashOnlyCap:
		return spool.DecisionHashOnlyFileCap
	case action == ActionReject && reason == ReasonRejectCap:
		return spool.DecisionRejectFileCap
	case action == ActionReject:
		return spool.DecisionReject
	default:
		return spool.DecisionHashOnly
	}
}

func (p *Policy) makeSummary(name string, total uint64, files []projectedFile) *spool.SummaryMetadata {
	return &spool.SummaryMetadata{
		Name:                name,
		TotalLength:         total,
		FileCount:           uint32(len(files)),
		RepresentativeFiles: representativeFiles(files, p.config.SummaryMaxAliases, p.config.SummaryAliasBytes),
		Extensions:          extensionSummaries(files, p.config.SummaryMaxExtensions),
	}
}

type projectedFile struct {
	path   string
	length uint64
}

// projectFiles normalizes each path exactly once. Large metadata used to walk
// and normalize the complete list three times (budgeting, body construction,
// extension aggregation), wasting CPU on the crawler's hottest path.
func projectFiles(source []pipeline.MetadataFile, maxFullPathBytes int) (
	files []projectedFile,
	total uint64,
	pathBytes int,
	oversizedPath bool,
	valid bool,
) {
	files = make([]projectedFile, len(source))
	for i := range source {
		file := source[i]
		if file.Length < 0 {
			return nil, 0, 0, false, false
		}
		length := uint64(file.Length)
		if ^uint64(0)-total < length {
			return nil, 0, 0, false, false
		}
		total += length
		normalized := normalizedPath(file)
		files[i] = projectedFile{path: normalized, length: length}
		pathLength := len(normalized)
		if pathLength > int(^uint(0)>>1)-pathBytes {
			return nil, 0, 0, false, false
		}
		pathBytes += pathLength
		oversizedPath = oversizedPath || pathLength > maxFullPathBytes
	}
	return files, total, pathBytes, oversizedPath, true
}

func normalizedPath(file pipeline.MetadataFile) string {
	value := file.PathText
	if value == "" {
		value = strings.Join(file.Path, "/")
	}
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.Trim(strings.TrimSpace(strings.ToValidUTF8(value, "�")), "/")
	value = strings.ReplaceAll(value, "\x00", "")
	if value == "" {
		return "_"
	}
	return value
}

type aliasCandidate struct {
	path   string
	length uint64
	score  int
}

func representativeFiles(files []projectedFile, maxCount, maxBytes int) []spool.File {
	// Keep only a small oversampled top set. Building a map and sorting every
	// unique path made a 10k-file summary allocate megabytes even though at most
	// a few dozen aliases can ever be stored.
	poolLimit := maxCount * 4
	pool := make(aliasHeap, 0, poolLimit)
	seenLengths := make(map[string]uint64, min(len(files), 1024))
	for i := range files {
		name := path.Base(files[i].path)
		name = boundedText(name, 256)
		if name == "" || name == "." {
			continue
		}
		candidate := aliasCandidate{path: name, length: files[i].length, score: informationScore(name)}
		identity := strings.ToLower(name)
		if prior, exists := seenLengths[identity]; exists && prior >= candidate.length {
			continue
		}
		seenLengths[identity] = candidate.length
		if len(pool) < poolLimit {
			aliasHeapPush(&pool, candidate)
			continue
		}
		if aliasBetter(candidate, pool[0]) {
			pool[0] = candidate
			aliasHeapFix(pool, 0)
		}
	}
	sort.Slice(pool, func(i, j int) bool {
		return aliasBetter(pool[i], pool[j])
	})
	result := make([]spool.File, 0, min(maxCount, len(pool)))
	used := 0
	for _, candidate := range pool {
		if len(result) >= maxCount || used+len(candidate.path) > maxBytes {
			continue
		}
		duplicate := false
		for _, selected := range result {
			if strings.EqualFold(selected.Path, candidate.path) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		result = append(result, spool.File{Path: candidate.path, Length: candidate.length})
		used += len(candidate.path)
	}
	return result
}

type aliasHeap []aliasCandidate

func aliasHeapPush(target *aliasHeap, candidate aliasCandidate) {
	*target = append(*target, candidate)
	aliasHeapUp(*target, len(*target)-1)
}

func aliasHeapFix(target aliasHeap, index int) {
	if !aliasHeapDown(target, index) {
		aliasHeapUp(target, index)
	}
}

func aliasHeapUp(target aliasHeap, index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !aliasBetter(target[parent], target[index]) {
			return
		}
		target[parent], target[index] = target[index], target[parent]
		index = parent
	}
}

func aliasHeapDown(target aliasHeap, index int) bool {
	original := index
	for {
		left := index*2 + 1
		if left >= len(target) {
			break
		}
		worst := left
		right := left + 1
		if right < len(target) && aliasBetter(target[left], target[right]) {
			worst = right
		}
		if !aliasBetter(target[index], target[worst]) {
			break
		}
		target[index], target[worst] = target[worst], target[index]
		index = worst
	}
	return index != original
}

func aliasBetter(left, right aliasCandidate) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	if left.length != right.length {
		return left.length > right.length
	}
	return left.path < right.path
}

func informationScore(value string) int {
	score := 0
	// A tiny stack-resident bloom bitmap is sufficient for ranking aliases. It
	// avoids allocating a map for every file name (tens of thousands of maps for
	// a large torrent) while preserving the useful "character variety" signal.
	var seen [2]uint64
	for _, r := range value {
		if unicode.IsLetter(r) {
			score += 4
		} else if unicode.IsDigit(r) {
			score++
		}
		bucket := uint32(r) * 2654435761 >> 25
		word, bit := bucket>>6, uint64(1)<<(bucket&63)
		if seen[word]&bit == 0 {
			seen[word] |= bit
			score++
		}
	}
	return score
}

func extensionSummaries(files []projectedFile, maxCount int) []spool.ExtensionSummary {
	type aggregate struct {
		files uint32
		bytes uint64
	}
	// Bound adversarial extension cardinality. The stored summary is explicitly
	// lossy and only exposes the top maxCount entries, so constructing an
	// unbounded 10k-key map would spend memory on data that can never cross the
	// wire. Oversampling preserves normal mixed-media distributions.
	aggregateLimit := maxCount * 8
	aggregates := make(map[string]aggregate, min(aggregateLimit, 32))
	for i := range files {
		ext := strings.TrimPrefix(strings.ToLower(path.Ext(files[i].path)), ".")
		ext = boundedText(ext, 32)
		if ext == "" {
			ext = "(none)"
		}
		a, exists := aggregates[ext]
		if !exists && len(aggregates) >= aggregateLimit {
			continue
		}
		a.files++
		a.bytes += files[i].length
		aggregates[ext] = a
	}
	result := make([]spool.ExtensionSummary, 0, len(aggregates))
	for ext, a := range aggregates {
		result = append(result, spool.ExtensionSummary{Extension: ext, Files: a.files, Bytes: a.bytes})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Bytes != result[j].Bytes {
			return result[i].Bytes > result[j].Bytes
		}
		if result[i].Files != result[j].Files {
			return result[i].Files > result[j].Files
		}
		return result[i].Extension < result[j].Extension
	})
	if len(result) > maxCount {
		result = result[:maxCount]
	}
	return result
}

func boundedText(value string, maxBytes int) string {
	value = strings.ReplaceAll(strings.ToValidUTF8(value, "�"), "\x00", "")
	value = strings.TrimSpace(value)
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func utcOrZero(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC()
}
