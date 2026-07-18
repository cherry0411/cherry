package heat

import (
	"hash/maphash"
	"sync"
	"testing"
)

func TestHourBloomShadowDisabledAllocatesNothing(t *testing.T) {
	shadow, err := newHourBloomShadow(shadowBloomOptions{})
	if err != nil || shadow != nil {
		t.Fatalf("shadow=%v err=%v", shadow, err)
	}
}

func TestShadowBloomHardDropDecisionFailsOpen(t *testing.T) {
	for _, test := range []struct {
		name    string
		enabled bool
		result  shadowBloomResult
		want    bool
	}{
		{name: "disabled", result: shadowBloomResult{probableDuplicate: true}},
		{name: "new", enabled: true, result: shadowBloomResult{new: true}},
		{name: "exceptional zero result", enabled: true, result: shadowBloomResult{}},
		{name: "capacity or stale-hour bypass", enabled: true, result: shadowBloomResult{probableDuplicate: true, bypassed: true}},
		{name: "probable duplicate", enabled: true, result: shadowBloomResult{probableDuplicate: true}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldDropShadowBloomResult(test.enabled, test.result); got != test.want {
				t.Fatalf("shouldDropShadowBloomResult()=%t want=%t", got, test.want)
			}
		})
	}
}

func TestHourBloomShadowEnabledUsesBoundedDefaults(t *testing.T) {
	shadow, err := newHourBloomShadow(shadowBloomOptions{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := shadow.snapshot()
	if snapshot.Capacity != defaultShadowBloomCapacity ||
		snapshot.SampleCapacity != defaultShadowSampleCapacity || snapshot.Bytes == 0 ||
		snapshot.Bytes > 5<<20 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestHourBloomShadowWarmPathDoesNotAllocate(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 100, 32)
	obs := testShadowObservation(20_000, 2, 99)
	shadow.observe(obs)
	if allocs := testing.AllocsPerRun(1_000, func() { shadow.observe(obs) }); allocs != 0 {
		t.Fatalf("warm-path allocations=%f", allocs)
	}
}

func TestHourBloomShadowSamePairAndCrossHour(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 100, 32)
	first := testShadowObservation(20_000, 23, 7)
	if result := shadow.observe(first); !result.new || result.bypassed {
		t.Fatalf("first result=%+v", result)
	}
	if result := shadow.observe(first); !result.probableDuplicate || result.new {
		t.Fatalf("same-hour duplicate result=%+v", result)
	}

	nextHour := first
	nextHour.Day++
	nextHour.Hour = 0
	if result := shadow.observe(nextHour); !result.new || result.probableDuplicate {
		t.Fatalf("cross-hour pair must be new: %+v", result)
	}
	snapshot := shadow.snapshot()
	if snapshot.Checks != 3 || snapshot.New != 2 || snapshot.ProbableDuplicates != 1 || snapshot.Rotations != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if snapshot.CurrentHour != uint64(nextHour.Day)*24 || snapshot.CurrentBitFillPPM == 0 || snapshot.Bytes == 0 {
		t.Fatalf("current gauges=%+v", snapshot)
	}
}

func TestHourBloomShadowRestartIsCold(t *testing.T) {
	obs := testShadowObservation(20_000, 4, 19)
	first := newTestHourBloomShadow(t, 100, 32)
	if result := first.observe(obs); !result.new {
		t.Fatalf("first process result=%+v", result)
	}
	if result := first.observe(obs); !result.probableDuplicate {
		t.Fatalf("warmed process result=%+v", result)
	}

	restarted := newTestHourBloomShadow(t, 100, 32)
	if result := restarted.observe(obs); !result.new || result.probableDuplicate {
		t.Fatalf("restart must begin cold: %+v", result)
	}
	if snapshot := restarted.snapshot(); snapshot.Rotations != 0 || snapshot.New != 1 {
		t.Fatalf("restart snapshot=%+v", snapshot)
	}
}

func TestHourBloomShadowCapacityFailsOpen(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 2, 32)
	for actor := uint64(1); actor <= 2; actor++ {
		if result := shadow.observe(testShadowObservation(20_000, 5, actor)); !result.new {
			t.Fatalf("actor %d result=%+v", actor, result)
		}
	}
	if result := shadow.observe(testShadowObservation(20_000, 5, 3)); !result.bypassed || result.new {
		t.Fatalf("capacity result=%+v", result)
	}
	snapshot := shadow.snapshot()
	if snapshot.New != 2 || snapshot.Bypassed != 1 || snapshot.Capacity != 2 || snapshot.Checks != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestHourBloomHardAdmissionFailureDoesNotPoisonRetry(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 100, 32)
	obs := testShadowObservation(20_000, 6, 77)

	admitted, first := shadow.observeAndAdmit(obs, func() bool { return false })
	if admitted || !first.new || first.probableDuplicate || first.bypassed {
		t.Fatalf("failed admission result=%+v admitted=%t", first, admitted)
	}
	admitted, retry := shadow.observeAndAdmit(obs, func() bool { return true })
	if !admitted || !retry.new || retry.probableDuplicate || retry.bypassed {
		t.Fatalf("retry result=%+v admitted=%t", retry, admitted)
	}
	admitted, duplicate := shadow.observeAndAdmit(obs, func() bool {
		t.Fatal("probable duplicate unexpectedly reached admission callback")
		return true
	})
	if admitted || !duplicate.probableDuplicate {
		t.Fatalf("duplicate result=%+v admitted=%t", duplicate, admitted)
	}

	snapshot := shadow.snapshot()
	if snapshot.New != 2 || snapshot.ProbableDuplicates != 1 || snapshot.Bypassed != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestHourBloomShadowPreviousHourAndTooOld(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 100, 32)
	current := testShadowObservation(20_000, 8, 1)
	if result := shadow.observe(current); !result.new {
		t.Fatal(result)
	}
	previous := current
	previous.Hour = 7
	if result := shadow.observe(previous); !result.new {
		t.Fatalf("late previous-hour result=%+v", result)
	}
	if result := shadow.observe(previous); !result.probableDuplicate {
		t.Fatalf("previous-hour duplicate result=%+v", result)
	}
	tooOld := current
	tooOld.Hour = 6
	if result := shadow.observe(tooOld); !result.bypassed {
		t.Fatalf("too-old hour must fail open: %+v", result)
	}
}

func TestHourBloomShadowConcurrentCheckAndRotation(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 10_000, 1_024)
	base := testShadowObservation(20_000, 10, 42)
	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(offset int) {
			defer wg.Done()
			obs := base
			if offset%2 == 1 {
				obs.Hour++
			}
			for j := 0; j < 100; j++ {
				shadow.observe(obs)
			}
		}(i)
	}
	wg.Wait()
	snapshot := shadow.snapshot()
	if snapshot.Checks != workers*100 {
		t.Fatalf("checks=%d", snapshot.Checks)
	}
	if snapshot.Bypassed != 0 || snapshot.New != 2 || snapshot.ProbableDuplicates != workers*100-2 {
		t.Fatalf("concurrent snapshot=%+v", snapshot)
	}
}

func TestHourBloomShadowSampleOracleTrueAndFalsePositive(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 100, 8)
	obs := findSampledObservation(t, 20_000, 12, 1)
	if result := shadow.observe(obs); !result.new || !result.sampled {
		t.Fatalf("sample first result=%+v", result)
	}
	if result := shadow.observe(obs); !result.sampledTrue {
		t.Fatalf("sample duplicate result=%+v", result)
	}

	falsePositive := findSampledObservation(t, 20_000, 12, obs.Actor+1)
	hour := uint64(falsePositive.Day)*24 + uint64(falsePositive.Hour)
	key := makeShadowBloomKey(hour, falsePositive)
	shadow.gate.RLock()
	slot := shadow.slotForHourLocked(hour)
	h1 := maphashBytesForTest(shadow, key)
	h2 := mixShadowHash(h1^deterministicShadowHash(key)) | 1
	block := (h1 >> 9) % slot.blocks
	shard := &slot.shards[block%uint64(len(slot.shards))]
	base := block / uint64(len(slot.shards)) * shadowBloomBlockWords
	shard.mu.Lock()
	for i := uint64(0); i < uint64(shadow.k); i++ {
		bit := (h1 + i*h2) & (shadowBloomBlockBits - 1)
		shard.words[base+bit/64] |= uint64(1) << (bit & 63)
	}
	shard.mu.Unlock()
	shadow.gate.RUnlock()

	if result := shadow.observe(falsePositive); !result.sampledFalse || !result.probableDuplicate {
		t.Fatalf("forced false-positive result=%+v", result)
	}
	snapshot := shadow.snapshot()
	if snapshot.SampledTruePositive != 1 || snapshot.SampledFalsePositive != 1 {
		t.Fatalf("sample snapshot=%+v", snapshot)
	}
}

func TestHourBloomShadowSampleSetIsBounded(t *testing.T) {
	shadow := newTestHourBloomShadow(t, 10_000, 1)
	first := findSampledObservation(t, 20_000, 13, 1)
	second := findSampledObservation(t, 20_000, 13, first.Actor+1)
	if result := shadow.observe(first); result.sampledBypassed {
		t.Fatalf("first sample result=%+v", result)
	}
	if result := shadow.observe(second); !result.sampledBypassed {
		t.Fatalf("second sample must bypass exact oracle: %+v", result)
	}
	snapshot := shadow.snapshot()
	if snapshot.SampleEntries != 1 || snapshot.SampleCapacity != 1 || snapshot.SampledBypassed != 1 {
		t.Fatalf("sample bound snapshot=%+v", snapshot)
	}
}

func newTestHourBloomShadow(t *testing.T, capacity, sampleCapacity int) *hourBloomShadow {
	t.Helper()
	shadow, err := newHourBloomShadow(shadowBloomOptions{
		Enabled: true, Capacity: capacity, FalsePositive: 1, SampleCapacity: sampleCapacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return shadow
}

func testShadowObservation(day uint32, hour uint8, actor uint64) Observation {
	var obs Observation
	obs.Day = day
	obs.Hour = hour
	obs.Actor = actor
	for i := range obs.InfoHash {
		obs.InfoHash[i] = byte(actor + uint64(i))
	}
	return obs
}

func findSampledObservation(t *testing.T, day uint32, hour uint8, start uint64) Observation {
	t.Helper()
	for actor := start; actor < start+10_000_000; actor++ {
		obs := testShadowObservation(day, hour, actor)
		key := makeShadowBloomKey(uint64(day)*24+uint64(hour), obs)
		if deterministicShadowHash(key)&shadowSampleMask == 0 {
			return obs
		}
	}
	t.Fatal("could not find deterministic sampled observation")
	return Observation{}
}

func maphashBytesForTest(shadow *hourBloomShadow, key shadowBloomKey) uint64 {
	return maphash.Bytes(shadow.seed, key[:])
}
