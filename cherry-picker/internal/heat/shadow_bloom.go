package heat

import (
	"encoding/binary"
	"errors"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"
)

const (
	defaultShadowBloomCapacity      = 1_000_000
	defaultShadowBloomFalsePositive = 1_000 // parts per million
	defaultShadowSampleCapacity     = 4_096
	shadowSampleMask                = 1_024 - 1
	shadowBloomShards               = 32
	shadowBloomBlockBits            = 512
	shadowBloomBlockWords           = shadowBloomBlockBits / 64
	shadowBloomSizeHeadroom         = 1.25
)

type shadowBloomOptions struct {
	Enabled        bool
	Capacity       int
	FalsePositive  int
	SampleCapacity int
}

type shadowBloomSnapshot struct {
	Enabled              bool
	Checks               uint64
	New                  uint64
	ProbableDuplicates   uint64
	Bypassed             uint64
	Rotations            uint64
	CurrentHour          uint64
	CurrentBitsSet       uint64
	CurrentBitFillPPM    uint64
	Capacity             uint64
	Bytes                uint64
	SampleCapacity       uint64
	SampleEntries        uint64
	SampledTruePositive  uint64
	SampledFalsePositive uint64
	SampledBypassed      uint64
}

type shadowBloomResult struct {
	new               bool
	probableDuplicate bool
	bypassed          bool
	sampled           bool
	sampledTrue       bool
	sampledFalse      bool
	sampledBypassed   bool
}

type shadowBloomKey [36]byte

type shadowBloomShard struct {
	mu    sync.Mutex
	words []uint64
}

type shadowBloomHourSlot struct {
	hour       uint64
	valid      bool
	shards     []shadowBloomShard
	blocks     uint64
	capacity   uint64
	sampleCap  int
	newCount   atomic.Uint64
	bitsSet    atomic.Uint64
	sampleMu   sync.Mutex
	sampleKeys map[shadowBloomKey]struct{}
}

type hourBloomShadow struct {
	// gate is held for the complete check/add operation. Hourly rotation takes
	// the write lock, waits for in-flight readers, and safely reuses the older
	// of two preallocated slots without a third-slot allocation spike.
	gate           sync.RWMutex
	slots          [2]*shadowBloomHourSlot
	current        int
	seed           maphash.Seed
	k              uint8
	bytes          uint64
	capacity       uint64
	sampleCapacity uint64

	checks               atomic.Uint64
	new                  atomic.Uint64
	probableDuplicates   atomic.Uint64
	bypassed             atomic.Uint64
	rotations            atomic.Uint64
	sampledTruePositive  atomic.Uint64
	sampledFalsePositive atomic.Uint64
	sampledBypassed      atomic.Uint64
}

func newHourBloomShadow(opts shadowBloomOptions) (*hourBloomShadow, error) {
	if !opts.Enabled {
		return nil, nil
	}
	if opts.Capacity < 0 {
		return nil, errors.New("heat: shadow bloom capacity cannot be negative")
	}
	if opts.Capacity == 0 {
		opts.Capacity = defaultShadowBloomCapacity
	}
	if opts.FalsePositive < 0 || opts.FalsePositive >= 1_000_000 {
		return nil, errors.New("heat: shadow bloom false-positive PPM must be between 1 and 999999")
	}
	if opts.FalsePositive == 0 {
		opts.FalsePositive = defaultShadowBloomFalsePositive
	}
	if opts.SampleCapacity < 0 {
		return nil, errors.New("heat: shadow bloom sample capacity cannot be negative")
	}
	if opts.SampleCapacity == 0 {
		opts.SampleCapacity = defaultShadowSampleCapacity
	}

	p := float64(opts.FalsePositive) / 1_000_000
	rawBits := -float64(opts.Capacity) * math.Log(p) / (math.Ln2 * math.Ln2)
	bits := rawBits * shadowBloomSizeHeadroom
	blockCount := math.Ceil(bits / shadowBloomBlockBits)
	if math.IsInf(blockCount, 0) || blockCount > float64(^uint64(0)-shadowBloomShards) {
		return nil, errors.New("heat: shadow bloom configuration is too large")
	}
	blocks := uint64(blockCount)
	if blocks < shadowBloomShards {
		blocks = shadowBloomShards
	}
	blocks = (blocks + shadowBloomShards - 1) / shadowBloomShards * shadowBloomShards
	wordsPerShard := blocks / shadowBloomShards * shadowBloomBlockWords
	if wordsPerShard > uint64(maxInt()) {
		return nil, errors.New("heat: shadow bloom configuration is too large")
	}
	words := blocks * shadowBloomBlockWords
	if words > ^uint64(0)/16 {
		return nil, errors.New("heat: shadow bloom byte size overflows")
	}

	// Size headroom compensates for per-block load variance, but retaining the
	// classic (un-padded) k avoids spending extra hot-path CPU on that padding.
	optimalK := int(math.Round(rawBits / float64(opts.Capacity) * math.Ln2))
	if optimalK < 1 {
		optimalK = 1
	}
	if optimalK > 16 {
		optimalK = 16
	}

	s := &hourBloomShadow{
		current:        -1,
		seed:           maphash.MakeSeed(),
		k:              uint8(optimalK),
		bytes:          words * 8 * 2,
		capacity:       uint64(opts.Capacity),
		sampleCapacity: uint64(opts.SampleCapacity),
	}
	for i := range s.slots {
		slot := &shadowBloomHourSlot{
			shards:     make([]shadowBloomShard, shadowBloomShards),
			blocks:     blocks,
			capacity:   uint64(opts.Capacity),
			sampleCap:  opts.SampleCapacity,
			sampleKeys: make(map[shadowBloomKey]struct{}, opts.SampleCapacity),
		}
		for j := range slot.shards {
			slot.shards[j].words = make([]uint64, int(wordsPerShard))
		}
		s.slots[i] = slot
	}
	return s, nil
}

func (s *hourBloomShadow) observe(obs Observation) shadowBloomResult {
	s.checks.Add(1)
	hour := uint64(obs.Day)*24 + uint64(obs.Hour)
	key := makeShadowBloomKey(hour, obs)

	for {
		s.gate.RLock()
		slot := s.slotForHourLocked(hour)
		if slot != nil {
			result := s.observeSlot(slot, key)
			s.gate.RUnlock()
			s.record(result)
			return result
		}
		s.gate.RUnlock()

		s.gate.Lock()
		slot, tooOld := s.prepareHourLocked(hour)
		if tooOld {
			s.gate.Unlock()
			result := shadowBloomResult{bypassed: true}
			s.record(result)
			return result
		}
		// Downgrading a Go RWMutex is impossible. Perform this uncommon first
		// observation of an hour while holding the exclusive rotation lock.
		result := s.observeSlot(slot, key)
		s.gate.Unlock()
		s.record(result)
		return result
	}
}

func (s *hourBloomShadow) slotForHourLocked(hour uint64) *shadowBloomHourSlot {
	for _, slot := range s.slots {
		if slot.valid && slot.hour == hour {
			return slot
		}
	}
	return nil
}

func (s *hourBloomShadow) prepareHourLocked(hour uint64) (*shadowBloomHourSlot, bool) {
	if slot := s.slotForHourLocked(hour); slot != nil {
		return slot, false
	}
	if s.current < 0 {
		s.current = 0
		s.resetSlotLocked(s.slots[0], hour)
		return s.slots[0], false
	}

	current := s.slots[s.current]
	if hour > current.hour {
		next := 1 - s.current
		if hour == current.hour+1 {
			s.resetSlotLocked(s.slots[next], hour)
		} else {
			// A jump of more than one hour invalidates both retained hours.
			current.valid = false
			s.resetSlotLocked(s.slots[next], hour)
		}
		s.current = next
		s.rotations.Add(1)
		return s.slots[next], false
	}

	if hour+1 == current.hour {
		previous := s.slots[1-s.current]
		if !previous.valid {
			s.resetSlotLocked(previous, hour)
			return previous, false
		}
	}
	return nil, true
}

func (s *hourBloomShadow) resetSlotLocked(slot *shadowBloomHourSlot, hour uint64) {
	for i := range slot.shards {
		clear(slot.shards[i].words)
	}
	slot.sampleMu.Lock()
	clear(slot.sampleKeys)
	slot.sampleMu.Unlock()
	slot.newCount.Store(0)
	slot.bitsSet.Store(0)
	slot.hour = hour
	slot.valid = true
}

func (s *hourBloomShadow) observeSlot(slot *shadowBloomHourSlot, key shadowBloomKey) shadowBloomResult {
	sampled := deterministicShadowHash(key)&shadowSampleMask == 0
	if !sampled {
		return slot.testAndAdd(key, s.seed, s.k)
	}

	slot.sampleMu.Lock()
	defer slot.sampleMu.Unlock()
	_, exactDuplicate := slot.sampleKeys[key]
	result := slot.testAndAdd(key, s.seed, s.k)
	result.sampled = true
	if result.bypassed {
		result.sampledBypassed = true
		return result
	}
	if !exactDuplicate {
		if len(slot.sampleKeys) >= slot.sampleCap {
			result.sampledBypassed = true
			return result
		}
		slot.sampleKeys[key] = struct{}{}
	}
	if result.probableDuplicate {
		if exactDuplicate {
			result.sampledTrue = true
		} else {
			result.sampledFalse = true
		}
	}
	return result
}

func (slot *shadowBloomHourSlot) testAndAdd(key shadowBloomKey, seed maphash.Seed, k uint8) shadowBloomResult {
	if slot.newCount.Load() >= slot.capacity {
		return shadowBloomResult{bypassed: true}
	}
	h1 := maphash.Bytes(seed, key[:])
	h2 := mixShadowHash(h1^deterministicShadowHash(key)) | 1
	block := (h1 >> 9) % slot.blocks
	shardIndex := block % uint64(len(slot.shards))
	localBlock := block / uint64(len(slot.shards))
	shard := &slot.shards[shardIndex]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	base := localBlock * shadowBloomBlockWords
	missing := false
	for i := uint64(0); i < uint64(k); i++ {
		bit := (h1 + i*h2) & (shadowBloomBlockBits - 1)
		word := base + bit/64
		mask := uint64(1) << (bit & 63)
		if shard.words[word]&mask == 0 {
			missing = true
		}
	}
	if !missing {
		return shadowBloomResult{probableDuplicate: true}
	}

	// Reserve one of the bounded per-hour "new" admissions before mutating
	// the filter. If another shard fills the capacity first, fail open.
	for {
		count := slot.newCount.Load()
		if count >= slot.capacity {
			return shadowBloomResult{bypassed: true}
		}
		if slot.newCount.CompareAndSwap(count, count+1) {
			break
		}
	}
	var added uint64
	for i := uint64(0); i < uint64(k); i++ {
		bit := (h1 + i*h2) & (shadowBloomBlockBits - 1)
		word := base + bit/64
		mask := uint64(1) << (bit & 63)
		if shard.words[word]&mask == 0 {
			shard.words[word] |= mask
			added++
		}
	}
	slot.bitsSet.Add(added)
	return shadowBloomResult{new: true}
}

func (s *hourBloomShadow) record(result shadowBloomResult) {
	switch {
	case result.bypassed:
		s.bypassed.Add(1)
	case result.probableDuplicate:
		s.probableDuplicates.Add(1)
	case result.new:
		s.new.Add(1)
	}
	if result.sampledTrue {
		s.sampledTruePositive.Add(1)
	}
	if result.sampledFalse {
		s.sampledFalsePositive.Add(1)
	}
	if result.sampledBypassed {
		s.sampledBypassed.Add(1)
	}
}

func (s *hourBloomShadow) snapshot() shadowBloomSnapshot {
	snapshot := shadowBloomSnapshot{
		Enabled:              true,
		Checks:               s.checks.Load(),
		New:                  s.new.Load(),
		ProbableDuplicates:   s.probableDuplicates.Load(),
		Bypassed:             s.bypassed.Load(),
		Rotations:            s.rotations.Load(),
		Bytes:                s.bytes,
		Capacity:             s.capacity,
		SampleCapacity:       s.sampleCapacity,
		SampledTruePositive:  s.sampledTruePositive.Load(),
		SampledFalsePositive: s.sampledFalsePositive.Load(),
		SampledBypassed:      s.sampledBypassed.Load(),
	}
	s.gate.RLock()
	if s.current >= 0 {
		slot := s.slots[s.current]
		snapshot.CurrentHour = slot.hour
		snapshot.CurrentBitsSet = slot.bitsSet.Load()
		totalBits := slot.blocks * shadowBloomBlockBits
		if totalBits > 0 {
			snapshot.CurrentBitFillPPM = snapshot.CurrentBitsSet * 1_000_000 / totalBits
		}
		slot.sampleMu.Lock()
		snapshot.SampleEntries = uint64(len(slot.sampleKeys))
		slot.sampleMu.Unlock()
	}
	s.gate.RUnlock()
	return snapshot
}

func makeShadowBloomKey(hour uint64, obs Observation) shadowBloomKey {
	var key shadowBloomKey
	binary.BigEndian.PutUint64(key[0:8], hour)
	copy(key[8:28], obs.InfoHash[:])
	binary.BigEndian.PutUint64(key[28:36], obs.Actor)
	return key
}

// deterministicShadowHash is used only for stable sampling. The Bloom itself
// uses a per-process randomized maphash seed so externally chosen keys cannot
// deliberately concentrate bits in a block.
func deterministicShadowHash(key shadowBloomKey) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	h := offset
	for _, value := range key {
		h ^= uint64(value)
		h *= prime
	}
	return mixShadowHash(h)
}

func mixShadowHash(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func maxInt() int { return int(^uint(0) >> 1) }
