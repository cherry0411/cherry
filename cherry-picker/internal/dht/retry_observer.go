package dht

import (
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// RetryCohortObserverOptions configures the optional retry-cohort-observer-v1.
// The observer is diagnostic only: capacity pressure drops observations and
// never changes peer-wire admission, scheduling, or completion semantics.
type RetryCohortObserverOptions struct {
	// SampleDenominator is applied independently to keyed pair, endpoint, and
	// infohash fingerprints. Each selected identity sees all of its candidates.
	SampleDenominator int
	Window            time.Duration
	PairCapacity      int
	// RequestQueueCapacity must exactly match Wire.RequestCapacity. It budgets
	// the full replacement channel allocation during observer installation.
	RequestQueueCapacity int
	// seed and elapsed are deterministic package-test hooks. Production cannot
	// configure them and receives a random seed plus monotonic time.
	seed    *maphash.Seed
	elapsed func() time.Duration
}

const (
	defaultRetrySampleDenominator = 64
	defaultRetryWindow            = 31 * time.Minute
	defaultRetryPairCapacity      = 131_072
	retryObserverShardCount       = 64
	retryObserverProbeLimit       = 16
	retryObserverMemoryLimit      = 16 << 20
	maxRetryPairCapacity          = 262_144
)

type retryCohortClass uint8

const (
	retryClassFirst retryCohortClass = iota
	retryClassUnder2m
	retryClass2to8m
	retryClass8to16m
	retryClass16to31m
	retryClassCount
)

var retryCohortClassNames = [...]string{
	"first", "retry_lt2m", "retry_2_8m", "retry_8_16m", "retry_16_31m",
}

// RetryCohortClassName returns the stable metric suffix for a cohort bucket.
func RetryCohortClassName(index int) string {
	if index < 0 || index >= len(retryCohortClassNames) {
		return "unknown"
	}
	return retryCohortClassNames[index]
}

type retryOutcome uint8

const (
	retryOutcomeSuccess retryOutcome = iota
	retryOutcomeQueueFull
	retryOutcomeBlacklisted
	retryOutcomeInflightDuplicate
	retryOutcomeDialTimeout
	retryOutcomeDialError
	retryOutcomeHandshakeWrite
	retryOutcomeHandshakeReadTimeout
	retryOutcomeHandshakeReadError
	retryOutcomeHandshakeProtocol
	retryOutcomeExtensionHandshakeWrite
	retryOutcomeDownloadDeadline
	retryOutcomeMessageReadTimeout
	retryOutcomeMessageReadError
	retryOutcomeExtensionProtocol
	retryOutcomeMetadataSize
	retryOutcomePieceProtocol
	retryOutcomeMetadataHashMismatch
	retryOutcomePanic
	retryOutcomeCount
)

var retryOutcomeNames = [...]string{
	"ok",
	"queue_full",
	"blacklisted",
	"inflight_duplicate",
	"dial_timeout",
	"dial_error",
	"handshake_write",
	"handshake_read_timeout",
	"handshake_read_error",
	"handshake_protocol",
	"extension_handshake_write",
	"download_deadline",
	"message_read_timeout",
	"message_read_error",
	"extension_protocol",
	"metadata_size",
	"piece_protocol",
	"metadata_hash_mismatch",
	"panic",
}

// RetryOutcomeName returns the stable metric suffix for a peer-wire outcome.
func RetryOutcomeName(index int) string {
	if index < 0 || index >= len(retryOutcomeNames) {
		return "unknown"
	}
	return retryOutcomeNames[index]
}

// RetryCohortClassSnapshot contains cumulative counters. LatencyBuckets uses
// <=1ms, <=10ms, <=100ms, <=500ms, <=1s, <=2s, <=5s, <=10s, and >10s.
type RetryCohortClassSnapshot struct {
	Attempts           uint64
	Successes          uint64
	Outcomes           [retryOutcomeCount]uint64
	LatencyMicros      uint64
	LatencyBuckets     [9]uint64
	EchoHashMismatches uint64
}

// RetryCohortSnapshot is safe to read from the 30-second runtime loop. Its
// independently loaded atomics are intentionally approximate at a concurrent
// snapshot boundary; interval deltas may momentarily differ by one event.
type RetryCohortSnapshot struct {
	Enabled                   bool
	PairSampleDenominator     uint64
	EndpointSampleDenominator uint64
	InfoHashSampleDenominator uint64
	WindowSeconds             uint64
	PairCapacity              uint64
	IdentityCapacity          uint64
	EstimatedBytes            uint64
	Candidates                uint64
	PairSampledAttempts       uint64
	PairCapacityDropped       uint64
	EndpointSampledAttempts   uint64
	EndpointOtherActiveEvents uint64
	EndpointCapacityDropped   uint64
	InfoHashSampledAttempts   uint64
	InfoHashOtherActiveEvents uint64
	InfoHashCapacityDropped   uint64
	PairSlotsUsed             uint64
	EndpointSlotsUsed         uint64
	InfoHashSlotsUsed         uint64
	Classes                   [retryClassCount]RetryCohortClassSnapshot
}

type retryCohortEntry struct {
	key      uint64
	firstSec uint32
}

type retryIdentityEntry struct {
	key            uint64
	current        uint64
	currentLastSec uint32
	// Retained while current repeats, so A->B->B reports both B observations as
	// having another active counterpart. Zero is none; otherwise timestamp+1.
	otherLastSecPlus uint32
}

type retryCohortShard struct {
	mu      sync.Mutex
	entries []retryCohortEntry
}

type retryIdentityShard struct {
	mu      sync.Mutex
	entries []retryIdentityEntry
}

type retryClassCounters struct {
	attempts           atomic.Uint64
	outcomes           [retryOutcomeCount]atomic.Uint64
	latencyMicros      atomic.Uint64
	latencyBuckets     [9]atomic.Uint64
	echoHashMismatches atomic.Uint64
}

// RetryCohortObserver is a bounded, sharded in-memory cohort table. It stores
// only 64-bit fingerprints and coarse timestamps; no IP, hash, or metadata is
// retained and the hot path never grows a map or launches a goroutine.
type RetryCohortObserver struct {
	sampleDenominator       uint64
	windowSec               uint32
	seed                    maphash.Seed
	elapsed                 func() time.Duration
	pairs                   [retryObserverShardCount]retryCohortShard
	endpoints               [retryObserverShardCount]retryIdentityShard
	infoHashes              [retryObserverShardCount]retryIdentityShard
	pairCapacity            uint64
	identityCapacity        uint64
	requestQueueCapacity    uint64
	estimatedBytes          uint64
	candidates              atomic.Uint64
	pairSampledAttempts     atomic.Uint64
	pairCapacityDropped     atomic.Uint64
	endpointSampledAttempts atomic.Uint64
	endpointOtherActive     atomic.Uint64
	endpointCapacityDropped atomic.Uint64
	infoHashSampledAttempts atomic.Uint64
	infoHashOtherActive     atomic.Uint64
	infoHashCapacityDropped atomic.Uint64
	pairSlotsUsed           atomic.Uint64
	endpointSlotsUsed       atomic.Uint64
	infoHashSlotsUsed       atomic.Uint64
	classes                 [retryClassCount]retryClassCounters
}

// retryObservation packs the cohort class and monotonic start nanoseconds into
// one uint64 carried beside Request in the internal queue. Zero means
// unobserved. This adds 8 bytes to each fixed request slot and no allocation.
type retryObservation uint64

func NewRetryCohortObserver(opts RetryCohortObserverOptions) (*RetryCohortObserver, error) {
	if opts.SampleDenominator <= 0 {
		opts.SampleDenominator = defaultRetrySampleDenominator
	}
	if opts.Window <= 0 {
		opts.Window = defaultRetryWindow
	}
	if opts.PairCapacity <= 0 {
		opts.PairCapacity = defaultRetryPairCapacity
	}
	if opts.RequestQueueCapacity < 0 {
		return nil, errors.New("retry observer request queue capacity cannot be negative")
	}
	if opts.SampleDenominator > 1<<20 {
		return nil, errors.New("retry observer sample denominator exceeds 1048576")
	}
	if opts.Window != defaultRetryWindow {
		return nil, errors.New("retry observer window must be exactly 31m for comparable cohort metrics")
	}
	if opts.PairCapacity < retryObserverShardCount*retryObserverProbeLimit {
		return nil, errors.New("retry observer pair capacity is too small for sharding")
	}
	if opts.PairCapacity > maxRetryPairCapacity {
		return nil, errors.New("retry observer pair capacity exceeds safe maximum 262144")
	}

	pairPerShard := ceilDivBounded(opts.PairCapacity, retryObserverShardCount)
	identityRequested := max(opts.PairCapacity/2, retryObserverShardCount*retryObserverProbeLimit)
	identityPerShard := ceilDivBounded(identityRequested, retryObserverShardCount)
	pairCapacity := pairPerShard * retryObserverShardCount
	identityCapacity := identityPerShard * retryObserverShardCount
	pairBytes, ok := checkedMulUint64(uint64(pairCapacity), uint64(unsafe.Sizeof(retryCohortEntry{})))
	if !ok {
		return nil, errors.New("retry observer pair backing size overflows")
	}
	identityBytes, ok := checkedMulUint64(uint64(identityCapacity), uint64(unsafe.Sizeof(retryIdentityEntry{})))
	if !ok {
		return nil, errors.New("retry observer identity backing size overflows")
	}
	identityBytes, ok = checkedMulUint64(identityBytes, 2)
	if !ok {
		return nil, errors.New("retry observer identity table total overflows")
	}
	// SetRetryCohortObserver replaces the baseline chan Request at startup. Until
	// the old channel is collected, enabling can transiently add the complete
	// observedRequest channel rather than just its 8-byte slot delta. Budget the
	// full replacement allocation so the 16 MiB limit remains a hard peak bound.
	observedQueueBytes, ok := checkedMulUint64(uint64(opts.RequestQueueCapacity), uint64(unsafe.Sizeof(observedRequest{})))
	if !ok {
		return nil, errors.New("retry observer request channel size overflows")
	}
	rawBacking, ok := checkedAddUint64(pairBytes, identityBytes)
	if !ok {
		return nil, errors.New("retry observer backing size overflows")
	}
	rawBacking, ok = checkedAddUint64(rawBacking, observedQueueBytes)
	if !ok {
		return nil, errors.New("retry observer request channel total overflows")
	}
	rawBacking, ok = checkedAddUint64(rawBacking, uint64(unsafe.Sizeof(RetryCohortObserver{})))
	if !ok {
		return nil, errors.New("retry observer object size overflows")
	}
	// Keep a conservative 12.5% allocator/fragmentation reserve so the
	// configured hard limit remains meaningful rather than merely summing
	// payload bytes.
	estimated, ok := checkedAddUint64(rawBacking, rawBacking/8)
	if !ok {
		return nil, errors.New("retry observer reserved size overflows")
	}
	estimated, ok = checkedAddUint64(estimated, 4096)
	if !ok {
		return nil, errors.New("retry observer estimated size overflows")
	}
	if estimated > retryObserverMemoryLimit {
		return nil, errors.New("retry observer tables exceed the 16 MiB hard limit")
	}

	seed := maphash.MakeSeed()
	if opts.seed != nil {
		seed = *opts.seed
	}
	started := time.Now()
	elapsed := func() time.Duration { return time.Since(started) }
	if opts.elapsed != nil {
		elapsed = opts.elapsed
	}
	o := &RetryCohortObserver{
		sampleDenominator:    uint64(opts.SampleDenominator),
		windowSec:            uint32(opts.Window / time.Second),
		seed:                 seed,
		elapsed:              elapsed,
		pairCapacity:         uint64(pairCapacity),
		identityCapacity:     uint64(identityCapacity),
		requestQueueCapacity: uint64(opts.RequestQueueCapacity),
		estimatedBytes:       estimated,
	}
	for i := 0; i < retryObserverShardCount; i++ {
		o.pairs[i].entries = make([]retryCohortEntry, pairPerShard)
		o.endpoints[i].entries = make([]retryIdentityEntry, identityPerShard)
		o.infoHashes[i].entries = make([]retryIdentityEntry, identityPerShard)
	}
	return o, nil
}

func ceilDivBounded(value, divisor int) int {
	quotient := value / divisor
	if value%divisor != 0 {
		quotient++
	}
	return quotient
}

func checkedMulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, false
	}
	return a * b, true
}

func checkedAddUint64(a, b uint64) (uint64, bool) {
	if b > ^uint64(0)-a {
		return 0, false
	}
	return a + b, true
}

func (o *RetryCohortObserver) begin(r Request) retryObservation {
	o.candidates.Add(1)
	infoKey := retryHashInfoHash(o.seed, r.InfoHash)
	endpointKey := retryHashEndpoint(o.seed, r.IP, r.Port)
	pairKey := retryMix(infoKey, endpointKey)
	pairSampled := retryFingerprintSampled(pairKey, o.sampleDenominator)
	endpointSampled := retryFingerprintSampled(endpointKey, o.sampleDenominator)
	infoHashSampled := retryFingerprintSampled(infoKey, o.sampleDenominator)
	if !pairSampled && !endpointSampled && !infoHashSampled {
		return 0
	}
	nowNanos := o.elapsedNanos()
	// Reserve MaxUint32 as room for the identity timestamp+1 sentinel. A process
	// would need to run for more than 136 years to reach this clamp.
	nowSec := uint32(min(nowNanos/uint64(time.Second), uint64(^uint32(0)-1)))
	if endpointSampled {
		o.endpointSampledAttempts.Add(1)
		if o.observeIdentity(o.endpoints[:], endpointKey, infoKey, nowSec, &o.endpointSlotsUsed, &o.endpointCapacityDropped) {
			o.endpointOtherActive.Add(1)
		}
	}
	if infoHashSampled {
		o.infoHashSampledAttempts.Add(1)
		if o.observeIdentity(o.infoHashes[:], infoKey, endpointKey, nowSec, &o.infoHashSlotsUsed, &o.infoHashCapacityDropped) {
			o.infoHashOtherActive.Add(1)
		}
	}
	if !pairSampled {
		return 0
	}
	o.pairSampledAttempts.Add(1)
	class, ok := o.observePair(pairKey, nowSec)
	if !ok {
		o.pairCapacityDropped.Add(1)
		return 0
	}
	o.classes[class].attempts.Add(1)
	return encodeRetryObservation(class, nowNanos)
}

func retryFingerprintSampled(key, denominator uint64) bool {
	// Sampling uses the upper half while sharding uses the low six bits. Keeping
	// them independent prevents every 1/N sample from collapsing into shard 0.
	return (key>>32)%denominator == 0
}

func (o *RetryCohortObserver) elapsedNanos() uint64 {
	elapsed := o.elapsed()
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed)
}

func encodeRetryObservation(class retryCohortClass, startedNanos uint64) retryObservation {
	const maxStart = (^uint64(0) >> 3) - 1
	if startedNanos > maxStart {
		startedNanos = maxStart
	}
	return retryObservation(((startedNanos + 1) << 3) | uint64(class))
}

func (o *RetryCohortObserver) observePair(key uint64, nowSec uint32) (retryCohortClass, bool) {
	key = nonzeroFingerprint(key)
	shard := &o.pairs[key&(retryObserverShardCount-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	start := int((key >> 6) % uint64(len(shard.entries)))
	reclaim := -1
	for probe := 0; probe < retryObserverProbeLimit; probe++ {
		index := (start + probe) % len(shard.entries)
		entry := &shard.entries[index]
		if entry.key == key {
			age := elapsedSeconds(nowSec, entry.firstSec)
			if age > o.windowSec {
				entry.firstSec = nowSec
				return retryClassFirst, true
			}
			return retryClassForAge(age), true
		}
		if reclaim < 0 && (entry.key == 0 || elapsedSeconds(nowSec, entry.firstSec) > o.windowSec) {
			reclaim = index
		}
	}
	if reclaim < 0 {
		return 0, false
	}
	entry := &shard.entries[reclaim]
	if entry.key == 0 {
		o.pairSlotsUsed.Add(1)
	}
	*entry = retryCohortEntry{key: key, firstSec: nowSec}
	return retryClassFirst, true
}

func (o *RetryCohortObserver) observeIdentity(shards []retryIdentityShard, key, counterpart uint64, nowSec uint32, used, dropped *atomic.Uint64) bool {
	key = nonzeroFingerprint(key)
	shard := &shards[key&(retryObserverShardCount-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	start := int((key >> 6) % uint64(len(shard.entries)))
	reclaim := -1
	for probe := 0; probe < retryObserverProbeLimit; probe++ {
		index := (start + probe) % len(shard.entries)
		entry := &shard.entries[index]
		if entry.key == key {
			otherActive := entry.otherLastSecPlus != 0 &&
				elapsedSeconds(nowSec, entry.otherLastSecPlus-1) <= o.windowSec
			if entry.current == counterpart {
				entry.currentLastSec = nowSec
				return otherActive
			}
			currentActive := elapsedSeconds(nowSec, entry.currentLastSec) <= o.windowSec
			if currentActive {
				entry.otherLastSecPlus = entry.currentLastSec + 1
			} else {
				// currentLastSec is always the newest observation for this identity,
				// so an expired current also proves any saved other is expired.
				entry.otherLastSecPlus = 0
			}
			entry.current = counterpart
			entry.currentLastSec = nowSec
			return currentActive || otherActive
		}
		if reclaim < 0 && (entry.key == 0 || elapsedSeconds(nowSec, entry.currentLastSec) > o.windowSec) {
			reclaim = index
		}
	}
	if reclaim < 0 {
		// Identity-table saturation loses only reuse evidence. The pair outcome
		// remains valid; expose the missing evidence without changing admission.
		dropped.Add(1)
		return false
	}
	entry := &shard.entries[reclaim]
	if entry.key == 0 {
		used.Add(1)
	}
	*entry = retryIdentityEntry{key: key, current: counterpart, currentLastSec: nowSec}
	return false
}

func retryClassForAge(age uint32) retryCohortClass {
	switch {
	case age < 2*60:
		return retryClassUnder2m
	case age < 8*60:
		return retryClass2to8m
	case age < 16*60:
		return retryClass8to16m
	default:
		return retryClass16to31m
	}
}

func elapsedSeconds(now, then uint32) uint32 {
	if now < then {
		return 0
	}
	return now - then
}

func nonzeroFingerprint(key uint64) uint64 {
	if key == 0 {
		return 1
	}
	return key
}

func retryHashInfoHash(seed maphash.Seed, infoHash []byte) uint64 {
	var hash maphash.Hash
	hash.SetSeed(seed)
	_ = hash.WriteByte(0x49)
	_, _ = hash.Write(infoHash)
	return hash.Sum64()
}

func retryHashEndpoint(seed maphash.Seed, ip string, port int) uint64 {
	var hash maphash.Hash
	hash.SetSeed(seed)
	_ = hash.WriteByte(0x45)
	_, _ = hash.WriteString(ip)
	_ = hash.WriteByte(0)
	_ = hash.WriteByte(byte(port))
	_ = hash.WriteByte(byte(port >> 8))
	_ = hash.WriteByte(byte(port >> 16))
	_ = hash.WriteByte(byte(port >> 24))
	return hash.Sum64()
}

func retryMix(a, b uint64) uint64 {
	return retryAvalanche(a ^ (b + 0x9e3779b97f4a7c15 + (a << 6) + (a >> 2)))
}

func retryAvalanche(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (o *RetryCohortObserver) finish(observation *retryObservation, outcome retryOutcome) {
	if o == nil || observation == nil || *observation == 0 {
		return
	}
	token := uint64(*observation)
	*observation = 0
	if outcome >= retryOutcomeCount {
		outcome = retryOutcomePanic
	}
	classIndex := retryCohortClass(token & 7)
	if classIndex >= retryClassCount {
		return
	}
	class := &o.classes[classIndex]
	class.outcomes[outcome].Add(1)
	startedNanos := (token >> 3) - 1
	nowNanos := o.elapsedNanos()
	elapsedNanos := uint64(0)
	if nowNanos >= startedNanos {
		elapsedNanos = nowNanos - startedNanos
	}
	micros := elapsedNanos / uint64(time.Microsecond)
	class.latencyMicros.Add(micros)
	class.latencyBuckets[retryLatencyBucket(micros)].Add(1)
}

func (o *RetryCohortObserver) echoMismatch(observation retryObservation) {
	if o == nil || observation == 0 {
		return
	}
	class := retryCohortClass(uint64(observation) & 7)
	if class < retryClassCount {
		o.classes[class].echoHashMismatches.Add(1)
	}
}

func retryLatencyBucket(micros uint64) int {
	limits := [...]uint64{1_000, 10_000, 100_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000}
	for i, limit := range limits {
		if micros <= limit {
			return i
		}
	}
	return len(limits)
}

func (o *RetryCohortObserver) Snapshot() RetryCohortSnapshot {
	snapshot := RetryCohortSnapshot{
		Enabled:                   true,
		PairSampleDenominator:     o.sampleDenominator,
		EndpointSampleDenominator: o.sampleDenominator,
		InfoHashSampleDenominator: o.sampleDenominator,
		WindowSeconds:             uint64(o.windowSec),
		PairCapacity:              o.pairCapacity,
		IdentityCapacity:          o.identityCapacity,
		EstimatedBytes:            o.estimatedBytes,
		Candidates:                o.candidates.Load(),
		PairSampledAttempts:       o.pairSampledAttempts.Load(),
		PairCapacityDropped:       o.pairCapacityDropped.Load(),
		EndpointSampledAttempts:   o.endpointSampledAttempts.Load(),
		EndpointOtherActiveEvents: o.endpointOtherActive.Load(),
		EndpointCapacityDropped:   o.endpointCapacityDropped.Load(),
		InfoHashSampledAttempts:   o.infoHashSampledAttempts.Load(),
		InfoHashOtherActiveEvents: o.infoHashOtherActive.Load(),
		InfoHashCapacityDropped:   o.infoHashCapacityDropped.Load(),
		PairSlotsUsed:             o.pairSlotsUsed.Load(),
		EndpointSlotsUsed:         o.endpointSlotsUsed.Load(),
		InfoHashSlotsUsed:         o.infoHashSlotsUsed.Load(),
	}
	for i := range snapshot.Classes {
		counters := &o.classes[i]
		class := &snapshot.Classes[i]
		class.Attempts = counters.attempts.Load()
		class.Successes = counters.outcomes[retryOutcomeSuccess].Load()
		class.LatencyMicros = counters.latencyMicros.Load()
		class.EchoHashMismatches = counters.echoHashMismatches.Load()
		for j := range class.Outcomes {
			class.Outcomes[j] = counters.outcomes[j].Load()
		}
		for j := range class.LatencyBuckets {
			class.LatencyBuckets[j] = counters.latencyBuckets[j].Load()
		}
	}
	return snapshot
}
