package dht

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestRetryObserverDeterministicCohortAgesAndReuse(t *testing.T) {
	elapsed := time.Duration(0)
	seed := maphash.MakeSeed()
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1,
		Window:            31 * time.Minute,
		PairCapacity:      4096,
		seed:              &seed,
		elapsed:           func() time.Duration { return elapsed },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{InfoHash: bytes20(1), IP: "203.0.113.10", Port: 6881}

	first := observer.begin(request)
	if got := observationClass(first); got != retryClassFirst {
		t.Fatalf("first class = %d", got)
	}
	elapsed += 250 * time.Millisecond
	observer.echoMismatch(first)
	observer.finish(&first, retryOutcomeSuccess)

	for _, test := range []struct {
		advance time.Duration
		want    retryCohortClass
		outcome retryOutcome
	}{
		{59*time.Second + 750*time.Millisecond, retryClassUnder2m, retryOutcomeDialTimeout},
		{2 * time.Minute, retryClass2to8m, retryOutcomeHandshakeProtocol},
		{6 * time.Minute, retryClass8to16m, retryOutcomeMessageReadTimeout},
		{8 * time.Minute, retryClass16to31m, retryOutcomeMetadataHashMismatch},
	} {
		elapsed += test.advance
		observation := observer.begin(request)
		if got := observationClass(observation); got != test.want {
			t.Fatalf("class at %s = %d, want %d", elapsed, got, test.want)
		}
		elapsed += 10 * time.Millisecond
		observer.finish(&observation, test.outcome)
	}

	// A new endpoint for the same hash proves infohash repetition independently
	// of pair retries. A new hash on the old endpoint proves endpoint reuse.
	newEndpoint := request
	newEndpoint.Port++
	newEndpointObservation := observer.begin(newEndpoint)
	observer.finish(&newEndpointObservation, retryOutcomeBlacklisted)
	newHash := request
	newHash.InfoHash = bytes20(2)
	newHashObservation := observer.begin(newHash)
	observer.finish(&newHashObservation, retryOutcomeInflightDuplicate)

	// The pair cohort resets to first after its fixed (not sliding) 31m window.
	elapsed = 32 * time.Minute
	reset := observer.begin(request)
	if got := observationClass(reset); got != retryClassFirst {
		t.Fatalf("expired cohort class = %d, want first", got)
	}
	observer.finish(&reset, retryOutcomeSuccess)

	snapshot := observer.Snapshot()
	if snapshot.Classes[retryClassFirst].Attempts != 4 {
		t.Fatalf("first attempts = %d, want 4", snapshot.Classes[retryClassFirst].Attempts)
	}
	if snapshot.Classes[retryClassUnder2m].Outcomes[retryOutcomeDialTimeout] != 1 ||
		snapshot.Classes[retryClass2to8m].Outcomes[retryOutcomeHandshakeProtocol] != 1 ||
		snapshot.Classes[retryClass8to16m].Outcomes[retryOutcomeMessageReadTimeout] != 1 ||
		snapshot.Classes[retryClass16to31m].Outcomes[retryOutcomeMetadataHashMismatch] != 1 {
		t.Fatalf("unexpected class outcomes: %+v", snapshot.Classes)
	}
	if snapshot.Classes[retryClassFirst].EchoHashMismatches != 1 {
		t.Fatalf("echo mismatches = %d, want 1", snapshot.Classes[retryClassFirst].EchoHashMismatches)
	}
	if snapshot.InfoHashOtherActiveEvents != 2 || snapshot.EndpointOtherActiveEvents != 2 {
		t.Fatalf("independent reuse evidence missing: %+v", snapshot)
	}
	if snapshot.EndpointSampledAttempts != 8 || snapshot.InfoHashSampledAttempts != 8 {
		t.Fatalf("identity sampled attempts endpoint=%d hash=%d, want 8 each",
			snapshot.EndpointSampledAttempts, snapshot.InfoHashSampledAttempts)
	}
}

func observationClass(observation retryObservation) retryCohortClass {
	return retryCohortClass(uint64(observation) & 7)
}

func TestRetryObserverStableOneIn64Sampling(t *testing.T) {
	seed := maphash.MakeSeed()
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	const candidates = 64_000
	var sampled int
	for i := 0; i < candidates; i++ {
		request := Request{InfoHash: bytes20(byte(i)), IP: net.IPv4(198, 51, byte(i>>8), byte(i)).String(), Port: 1024 + i}
		// Add the full integer so requests remain distinct after byte wraparound.
		request.InfoHash[1] = byte(i >> 8)
		request.InfoHash[2] = byte(i >> 16)
		first := observer.begin(request)
		second := observer.begin(request)
		if (first == 0) != (second == 0) {
			t.Fatal("sampling decision changed for the same pair")
		}
		if first != 0 {
			sampled++
			observer.finish(&first, retryOutcomeSuccess)
			observer.finish(&second, retryOutcomeSuccess)
		}
	}
	// A deterministic hash should land close to 1/64 without demanding an
	// artificially exact count (which could hide a biased hash function).
	if sampled < 800 || sampled > 1200 {
		t.Fatalf("sampled %d/%d, want approximately 1/64", sampled, candidates)
	}
}

func TestRetryObserverInjectedKeyControlsSampling(t *testing.T) {
	seed := maphash.MakeSeed()
	otherSeed := maphash.MakeSeed()
	one, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &otherSeed,
	})
	if err != nil {
		t.Fatal(err)
	}
	var different int
	for i := 0; i < 20_000; i++ {
		request := Request{InfoHash: bytes20(byte(i)), IP: "198.51.100.4", Port: i + 1}
		request.InfoHash[1] = byte(i >> 8)
		a := one.begin(request)
		b := two.begin(request)
		c := other.begin(request)
		if (a == 0) != (b == 0) {
			t.Fatal("same injected keyed seed produced a different decision")
		}
		if (a == 0) != (c == 0) {
			different++
		}
	}
	if different < 300 {
		t.Fatalf("independent random keyed seeds changed only %d decisions", different)
	}
}

func TestRetryObserverIdentitySamplingIsIndependentOfPairSampling(t *testing.T) {
	seed := maphash.MakeSeed()
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	endpointRequest, ok := findIdentitySampleWithoutPair(seed, true)
	if !ok {
		t.Fatal("could not find endpoint-sampled/pair-unsampled request")
	}
	if observation := observer.begin(endpointRequest); observation != 0 {
		t.Fatal("pair-unsampled request unexpectedly returned a pair token")
	}
	snapshot := observer.Snapshot()
	if snapshot.PairSampledAttempts != 0 || snapshot.EndpointSampledAttempts != 1 {
		t.Fatalf("endpoint sample was gated by pair sample: %+v", snapshot)
	}
	for i := 1; i < 64; i++ {
		request := endpointRequest
		request.InfoHash = bytes20(byte(i + 40))
		observation := observer.begin(request)
		observer.finish(&observation, retryOutcomeDialTimeout)
	}
	if got := observer.Snapshot().EndpointSampledAttempts; got != 64 {
		t.Fatalf("stable sampled endpoint attempts=%d, want 64", got)
	}

	observer, err = NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 4096, seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	hashRequest, ok := findIdentitySampleWithoutPair(seed, false)
	if !ok {
		t.Fatal("could not find infohash-sampled/pair-unsampled request")
	}
	if observation := observer.begin(hashRequest); observation != 0 {
		t.Fatal("pair-unsampled request unexpectedly returned a pair token")
	}
	snapshot = observer.Snapshot()
	if snapshot.PairSampledAttempts != 0 || snapshot.InfoHashSampledAttempts != 1 {
		t.Fatalf("infohash sample was gated by pair sample: %+v", snapshot)
	}
	for i := 1; i < 64; i++ {
		request := hashRequest
		request.Port += i
		observation := observer.begin(request)
		observer.finish(&observation, retryOutcomeDialTimeout)
	}
	if got := observer.Snapshot().InfoHashSampledAttempts; got != 64 {
		t.Fatalf("stable sampled infohash attempts=%d, want 64", got)
	}
}

func findIdentitySampleWithoutPair(seed maphash.Seed, endpoint bool) (Request, bool) {
	for i := 1; i < 200_000; i++ {
		request := Request{
			InfoHash: bytes20(byte(i)),
			IP:       net.IPv4(198, 51, byte(i>>8), byte(i)).String(),
			Port:     1024 + i%50_000,
		}
		request.InfoHash[1] = byte(i >> 8)
		infoKey := retryHashInfoHash(seed, request.InfoHash)
		endpointKey := retryHashEndpoint(seed, request.IP, request.Port)
		identityKey := infoKey
		if endpoint {
			identityKey = endpointKey
		}
		if retryFingerprintSampled(identityKey, 64) && !retryFingerprintSampled(retryMix(infoKey, endpointKey), 64) {
			return request, true
		}
	}
	return Request{}, false
}

func TestRetryObserverOtherActivePersistsAcrossRepeatAndRotation(t *testing.T) {
	elapsed := time.Duration(0)
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, elapsed: func() time.Duration { return elapsed },
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := Request{IP: "192.0.2.10", Port: 6881}
	for _, hashSeed := range []byte{1, 2, 2} { // A -> B -> B
		endpoint.InfoHash = bytes20(hashSeed)
		observation := observer.begin(endpoint)
		observer.finish(&observation, retryOutcomeDialTimeout)
		elapsed += time.Second
	}
	if got := observer.Snapshot().EndpointOtherActiveEvents; got != 2 {
		t.Fatalf("endpoint A-B-B other-active events=%d, want 2", got)
	}
	for _, hashSeed := range []byte{3, 3} { // ... -> C -> C
		endpoint.InfoHash = bytes20(hashSeed)
		observation := observer.begin(endpoint)
		observer.finish(&observation, retryOutcomeDialTimeout)
		elapsed += time.Second
	}
	if got := observer.Snapshot().EndpointOtherActiveEvents; got != 4 {
		t.Fatalf("endpoint A-B-C rotation other-active events=%d, want 4", got)
	}

	observer, err = NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, elapsed: func() time.Duration { return elapsed },
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := bytes20(9)
	for _, port := range []int{7001, 7002, 7002, 7003, 7003} { // A-B-B-C-C
		request := Request{InfoHash: hash, IP: "192.0.2.20", Port: port}
		observation := observer.begin(request)
		observer.finish(&observation, retryOutcomeDialTimeout)
		elapsed += time.Second
	}
	if got := observer.Snapshot().InfoHashOtherActiveEvents; got != 4 {
		t.Fatalf("infohash A-B-B-C-C other-active events=%d, want 4", got)
	}
}

func TestRetryObserverOtherActiveExpiresAfter31Minutes(t *testing.T) {
	for _, endpointIdentity := range []bool{true, false} {
		identityName := map[bool]string{true: "endpoint", false: "infohash"}[endpointIdentity]
		for _, boundary := range []struct {
			at   time.Duration
			want uint64
		}{{31 * time.Minute, 1}, {31*time.Minute + time.Second, 0}} {
			t.Run(identityName+"/"+boundary.at.String(), func(t *testing.T) {
				elapsed := time.Duration(0)
				observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
					SampleDenominator: 1, PairCapacity: 4096, elapsed: func() time.Duration { return elapsed },
				})
				if err != nil {
					t.Fatal(err)
				}
				first := Request{InfoHash: bytes20(1), IP: "192.0.2.30", Port: 7101}
				second := Request{InfoHash: bytes20(2), IP: first.IP, Port: first.Port}
				if !endpointIdentity {
					second = Request{InfoHash: first.InfoHash, IP: first.IP, Port: 7102}
				}
				observation := observer.begin(first)
				observer.finish(&observation, retryOutcomeDialTimeout)
				elapsed = boundary.at
				observation = observer.begin(second)
				observer.finish(&observation, retryOutcomeDialTimeout)
				snapshot := observer.Snapshot()
				got := snapshot.InfoHashOtherActiveEvents
				if endpointIdentity {
					got = snapshot.EndpointOtherActiveEvents
				}
				if got != boundary.want {
					t.Fatalf("%s other-active at %s=%d, want %d", identityName, boundary.at, got, boundary.want)
				}
			})
		}
	}
}

func TestRetryObserverPairAgeBoundaries(t *testing.T) {
	elapsed := time.Duration(0)
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, elapsed: func() time.Duration { return elapsed },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{InfoHash: bytes20(5), IP: "192.0.2.40", Port: 6881}
	for _, test := range []struct {
		at   time.Duration
		want retryCohortClass
	}{
		{0, retryClassFirst},
		{2*time.Minute - time.Second, retryClassUnder2m},
		{2 * time.Minute, retryClass2to8m},
		{8*time.Minute - time.Second, retryClass2to8m},
		{8 * time.Minute, retryClass8to16m},
		{16*time.Minute - time.Second, retryClass8to16m},
		{16 * time.Minute, retryClass16to31m},
		{31 * time.Minute, retryClass16to31m},
		{31*time.Minute + time.Second, retryClassFirst},
	} {
		elapsed = test.at
		observation := observer.begin(request)
		if got := observationClass(observation); got != test.want {
			t.Fatalf("class at %s=%d, want %d", test.at, got, test.want)
		}
		observer.finish(&observation, retryOutcomeDialTimeout)
	}
}

func TestRetryObserverCapacityIsBoundedAndFailOpen(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: retryObserverShardCount * retryObserverProbeLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := uint32(time.Now().Unix())
	entriesPerShard := len(observer.pairs[1].entries)
	// Force all fingerprints into shard 1 and the same probe run. The 17th
	// insert cannot be represented, but the caller merely loses observation.
	for i := 0; i < retryObserverProbeLimit; i++ {
		key := (uint64(i*entriesPerShard) << 6) | 1
		if _, ok := observer.observePair(key, now); !ok {
			t.Fatalf("insert %d unexpectedly dropped", i)
		}
	}
	overflow := (uint64(retryObserverProbeLimit*entriesPerShard) << 6) | 1
	if _, ok := observer.observePair(overflow, now); ok {
		t.Fatal("probe-run overflow was admitted")
	}
	if observer.pairSlotsUsed.Load() > observer.pairCapacity {
		t.Fatalf("slots used %d exceed capacity %d", observer.pairSlotsUsed.Load(), observer.pairCapacity)
	}
	identityPerShard := len(observer.endpoints[1].entries)
	for i := 0; i < retryObserverProbeLimit; i++ {
		key := (uint64(i*identityPerShard) << 6) | 1
		observer.observeIdentity(observer.endpoints[:], key, uint64(i+100), now,
			&observer.endpointSlotsUsed, &observer.endpointCapacityDropped)
	}
	identityOverflow := (uint64(retryObserverProbeLimit*identityPerShard) << 6) | 1
	observer.observeIdentity(observer.endpoints[:], identityOverflow, 999, now,
		&observer.endpointSlotsUsed, &observer.endpointCapacityDropped)
	if got := observer.Snapshot().EndpointCapacityDropped; got != 1 {
		t.Fatalf("endpoint capacity drops = %d, want 1", got)
	}
}

func TestRetryObserverConcurrentBeginFinish(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 16_384,
	})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 64
	const perGoroutine = 500
	var group sync.WaitGroup
	group.Add(goroutines)
	for worker := 0; worker < goroutines; worker++ {
		go func(worker int) {
			defer group.Done()
			for i := 0; i < perGoroutine; i++ {
				request := Request{InfoHash: bytes20(byte(i % 64)), IP: "192.0.2.1", Port: 1024 + worker%8}
				observation := observer.begin(request)
				observer.finish(&observation, retryOutcomeSuccess)
			}
		}(worker)
	}
	group.Wait()
	snapshot := observer.Snapshot()
	var attempts, successes uint64
	for _, class := range snapshot.Classes {
		attempts += class.Attempts
		successes += class.Outcomes[retryOutcomeSuccess]
	}
	if attempts != goroutines*perGoroutine || successes != attempts {
		t.Fatalf("attempts=%d successes=%d want=%d", attempts, successes, goroutines*perGoroutine)
	}
}

func TestRetryObserverMemoryEstimateIncludesBackingTables(t *testing.T) {
	const requestQueueCapacity = 32_768
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		RequestQueueCapacity: requestQueueCapacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	actualBacking := uint64(unsafe.Sizeof(*observer))
	for i := 0; i < retryObserverShardCount; i++ {
		actualBacking += uint64(cap(observer.pairs[i].entries)) * uint64(unsafe.Sizeof(retryCohortEntry{}))
		actualBacking += uint64(cap(observer.endpoints[i].entries)) * uint64(unsafe.Sizeof(retryIdentityEntry{}))
		actualBacking += uint64(cap(observer.infoHashes[i].entries)) * uint64(unsafe.Sizeof(retryIdentityEntry{}))
	}
	actualBacking += requestQueueCapacity * uint64(unsafe.Sizeof(observedRequest{}))
	if observer.estimatedBytes < actualBacking {
		t.Fatalf("estimate=%d smaller than backing=%d", observer.estimatedBytes, actualBacking)
	}
	if observer.estimatedBytes > retryObserverMemoryLimit {
		t.Fatalf("estimate=%d exceeds hard limit=%d", observer.estimatedBytes, retryObserverMemoryLimit)
	}
	if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{PairCapacity: 1_000_000}); err == nil {
		t.Fatal("oversized observer was accepted")
	}
}

func TestRetryObserverCapacityValidationHasSafeBoundaries(t *testing.T) {
	for _, capacity := range []int{math.MinInt, -1, 1, retryObserverShardCount*retryObserverProbeLimit - 1, maxRetryPairCapacity + 1, math.MaxInt} {
		if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{PairCapacity: capacity}); err == nil && capacity > 0 {
			t.Fatalf("unsafe pair capacity %d was accepted", capacity)
		}
	}
	maximum, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		PairCapacity: maxRetryPairCapacity, RequestQueueCapacity: 32_768,
	})
	if err != nil {
		t.Fatalf("safe maximum pair capacity was rejected: %v", err)
	}
	if maximum.estimatedBytes > retryObserverMemoryLimit {
		t.Fatalf("safe maximum estimate=%d exceeds limit=%d", maximum.estimatedBytes, retryObserverMemoryLimit)
	}
	if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{RequestQueueCapacity: 2_000_000}); err == nil {
		t.Fatal("request queue that exceeds the hard memory budget was accepted")
	}
	if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{RequestQueueCapacity: math.MaxInt}); err == nil {
		t.Fatal("overflowing request queue token allocation was accepted")
	}
}

func FuzzRetryObserverRejectsUnsafeCapacityWithoutPanic(f *testing.F) {
	for _, capacity := range []int{math.MinInt, -1, 1, 1023, maxRetryPairCapacity + 1, math.MaxInt} {
		f.Add(capacity)
	}
	f.Fuzz(func(t *testing.T, capacity int) {
		if capacity <= 0 {
			// Non-positive values intentionally select the default. Exercise the
			// actual unsafe-rejection path instead of allocating on every fuzz run.
			capacity = retryObserverShardCount*retryObserverProbeLimit - 1
		} else if capacity >= retryObserverShardCount*retryObserverProbeLimit && capacity <= maxRetryPairCapacity {
			capacity = maxRetryPairCapacity + 1
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("capacity %d panicked: %v", capacity, recovered)
			}
		}()
		if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{PairCapacity: capacity}); err == nil {
			t.Fatalf("unsafe capacity %d was accepted", capacity)
		}
	})
}

func TestRetryObserverRequiresFixed31MinuteWindow(t *testing.T) {
	for _, window := range []time.Duration{30 * time.Minute, 31*time.Minute - time.Second, 31*time.Minute + time.Second, 32 * time.Minute} {
		if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{Window: window}); err == nil {
			t.Fatalf("non-comparable window %s was accepted", window)
		}
	}
	if _, err := NewRetryCohortObserver(RetryCohortObserverOptions{Window: 31 * time.Minute}); err != nil {
		t.Fatalf("fixed 31m window was rejected: %v", err)
	}
}

func TestRetryObserverUsesMonotonicElapsedTimeAcrossWallClockRollback(t *testing.T) {
	mono := time.Duration(0)
	wall := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1,
		PairCapacity:      4096,
		elapsed:           func() time.Duration { return mono },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{InfoHash: bytes20(3), IP: "192.0.2.8", Port: 6881}
	first := observer.begin(request)
	observer.finish(&first, retryOutcomeDialTimeout)

	wall = wall.Add(-6 * time.Hour) // Simulate an NTP/manual wall-clock rollback.
	mono = 17 * time.Minute
	retry := observer.begin(request)
	if got := observationClass(retry); got != retryClass16to31m {
		t.Fatalf("wall clock %s changed monotonic cohort: got %d", wall, got)
	}
	observer.finish(&retry, retryOutcomeDialTimeout)

	mono = 32 * time.Minute
	reset := observer.begin(request)
	if got := observationClass(reset); got != retryClassFirst {
		t.Fatalf("monotonic expiry class = %d, want first", got)
	}
	observer.finish(&reset, retryOutcomeDialTimeout)
}

func TestWireObserverRecordsQueueFullFromAdmission(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, RequestQueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := NewWire(64, 1, 1)
	wire.SetRetryCohortObserver(observer)
	if !wire.RequestFromSource(bytes20(1), "192.0.2.1", 6881, PeerSourceGetPeers) {
		t.Fatal("first request was not admitted")
	}
	if wire.RequestFromSource(bytes20(2), "192.0.2.2", 6882, PeerSourceAnnounce) {
		t.Fatal("second request was admitted into a full queue")
	}
	snapshot := observer.Snapshot()
	if snapshot.Classes[retryClassFirst].Attempts != 2 || snapshot.Classes[retryClassFirst].Outcomes[retryOutcomeQueueFull] != 1 {
		t.Fatalf("queue-full admission outcome missing: %+v", snapshot.Classes[retryClassFirst])
	}
	queued := <-wire.observedRequests
	observer.finish(&queued.retryObservation, retryOutcomeSuccess)
}

func TestWireObserverRequestTokenChannelIsLazy(t *testing.T) {
	wire := NewWire(64, 7, 1)
	if wire.observedRequests != nil || wire.requests == nil {
		t.Fatal("disabled wire allocated the observed request channel")
	}
	if delta := unsafe.Sizeof(observedRequest{}) - unsafe.Sizeof(Request{}); delta != unsafe.Sizeof(retryObservation(0)) {
		t.Fatalf("observed request slot delta=%d, want %d", delta, unsafe.Sizeof(retryObservation(0)))
	}
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, RequestQueueCapacity: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire.SetRetryCohortObserver(observer)
	if wire.requests != nil || wire.observedRequests == nil || cap(wire.observedRequests) != 7 {
		t.Fatal("observer installation did not replace the baseline request channel")
	}
}

func TestWireObserverRejectsMismatchedRequestQueueCapacity(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, RequestQueueCapacity: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := NewWire(64, 7, 1)
	defer func() {
		if recover() == nil {
			t.Fatal("observer with mismatched request queue capacity was installed")
		}
	}()
	wire.SetRetryCohortObserver(observer)
}

func TestRetryObserverFinishIsExactOnce(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{SampleDenominator: 1, PairCapacity: 4096})
	if err != nil {
		t.Fatal(err)
	}
	observation := observer.begin(Request{InfoHash: bytes20(1), IP: "192.0.2.1", Port: 6881})
	observer.finish(&observation, retryOutcomeSuccess)
	observer.finish(&observation, retryOutcomeDialTimeout)
	snapshot := observer.Snapshot().Classes[retryClassFirst]
	if snapshot.Outcomes[retryOutcomeSuccess] != 1 || snapshot.Outcomes[retryOutcomeDialTimeout] != 0 {
		t.Fatalf("observation finished more than once: %+v", snapshot.Outcomes)
	}
}

func TestWireObserverSuccessFinishesBeforeResponseBackpressure(t *testing.T) {
	metadata := []byte("d6:lengthi1e4:name4:teste")
	digest := sha1.Sum(metadata)
	infoHash := append([]byte(nil), digest[:]...)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverResult := make(chan error, 1)
	go func() { serverResult <- serveMetadataOnce(listener, metadata) }()

	var elapsedNanos atomic.Int64
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator:    1,
		PairCapacity:         4096,
		RequestQueueCapacity: 1,
		elapsed:              func() time.Duration { return time.Duration(elapsedNanos.Load()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := NewWire(64, 1, 1)
	wire.SetRetryCohortObserver(observer)
	for i := 0; i < cap(wire.responses); i++ {
		wire.responses <- Response{}
	}
	addr := listener.Addr().(*net.TCPAddr)
	if !wire.RequestFromSource(infoHash, addr.IP.String(), addr.Port, PeerSourceGetPeers) {
		t.Fatal("request was not admitted")
	}
	request := <-wire.observedRequests
	elapsedNanos.Store(int64(100 * time.Millisecond))
	handleDone := make(chan struct{})
	go func() {
		defer close(handleDone)
		wire.handleObservedRequest(request.Request, request.retryObservation)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for observer.Snapshot().Classes[retryClassFirst].Outcomes[retryOutcomeSuccess] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	before := observer.Snapshot().Classes[retryClassFirst]
	if before.Outcomes[retryOutcomeSuccess] != 1 || before.LatencyMicros != 100_000 {
		t.Fatalf("success was not finished at SHA1 verification: %+v", before)
	}
	select {
	case <-handleDone:
		t.Fatal("handler unexpectedly bypassed full response queue")
	default:
	}

	// Advancing the observer clock while the response send is blocked must not
	// inflate protocol latency or create a second completion in the deferred path.
	elapsedNanos.Store(int64(10 * time.Second))
	<-wire.responses
	select {
	case <-handleDone:
	case <-time.After(3 * time.Second):
		t.Fatal("handler remained blocked after response capacity was released")
	}
	if serverErr := <-serverResult; serverErr != nil {
		t.Fatal(serverErr)
	}
	after := observer.Snapshot().Classes[retryClassFirst]
	var outcomeTotal uint64
	for _, count := range after.Outcomes {
		outcomeTotal += count
	}
	if after.LatencyMicros != 100_000 || outcomeTotal != 1 {
		t.Fatalf("response backpressure changed completed observation: %+v", after)
	}
	var completed Response
	for i := 0; i < cap(wire.responses); i++ {
		response := <-wire.responses
		if len(response.MetadataInfo) != 0 {
			completed = response
		}
	}
	if len(completed.MetadataInfo) == 0 {
		t.Fatal("completed response was not queued")
	}
}

func serveMetadataOnce(listener net.Listener, metadata []byte) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("accepted connection is %T, want *net.TCPConn", conn)
	}
	handshake := make([]byte, 68)
	if _, err := io.ReadFull(tcp, handshake); err != nil {
		return fmt.Errorf("read handshake: %w", err)
	}
	if _, err := tcp.Write(handshake); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}
	if _, err := readTestFrame(tcp); err != nil {
		return fmt.Errorf("read extension handshake: %w", err)
	}
	extensionHandshake := append([]byte{EXTENDED, HANDSHAKE}, []byte(Encode(map[string]interface{}{
		"m":             map[string]interface{}{"ut_metadata": 1},
		"metadata_size": len(metadata),
	}))...)
	if err := sendMessage(tcp, extensionHandshake); err != nil {
		return fmt.Errorf("write extension handshake: %w", err)
	}
	if _, err := readTestFrame(tcp); err != nil {
		return fmt.Errorf("read metadata request: %w", err)
	}
	dataHeader := []byte(Encode(map[string]interface{}{
		"msg_type":   DATA,
		"piece":      0,
		"total_size": len(metadata),
	}))
	payload := append([]byte{EXTENDED, 1}, dataHeader...)
	payload = append(payload, metadata...)
	if err := sendMessage(tcp, payload); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}
	return nil
}

func readTestFrame(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func TestWireObserverDetectsEchoedInfohashMismatchWithoutNewRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		handshake := make([]byte, 68)
		if _, readErr := io.ReadFull(conn, handshake); readErr != nil {
			return
		}
		handshake[28] ^= 0xff
		_, _ = conn.Write(handshake)
		// Read the extension handshake so the client follows its pre-observer
		// behavior before the server closes the stream.
		_, _ = io.CopyN(io.Discard, conn, 4)
	}()

	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, RequestQueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := NewWire(64, 1, 1)
	wire.SetRetryCohortObserver(observer)
	addr := listener.Addr().(*net.TCPAddr)
	if !wire.RequestFromSource(bytes20(7), addr.IP.String(), addr.Port, PeerSourceUnknown) {
		t.Fatal("request was not admitted")
	}
	request := <-wire.observedRequests
	wire.handleObservedRequest(request.Request, request.retryObservation)
	<-serverDone
	snapshot := observer.Snapshot()
	if snapshot.Classes[retryClassFirst].EchoHashMismatches != 1 {
		t.Fatalf("echo mismatch count = %d, want 1", snapshot.Classes[retryClassFirst].EchoHashMismatches)
	}
	if wire.Stats.HandshakeOK.Load() != 1 {
		t.Fatalf("mismatched echo changed handshake admission: HandshakeOK=%d", wire.Stats.HandshakeOK.Load())
	}
}

func TestWireObserverCapacityDropDoesNotChangeDial(t *testing.T) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 1, PairCapacity: 4096, RequestQueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := uint32(time.Now().Unix())
	// Saturate every pair slot with a non-expired fingerprint. The next pair is
	// unobservable regardless of its shard/probe start.
	for shardIndex := range observer.pairs {
		for entryIndex := range observer.pairs[shardIndex].entries {
			key := (uint64(entryIndex+1) << 16) | uint64(shardIndex)
			observer.pairs[shardIndex].entries[entryIndex] = retryCohortEntry{
				key: key, firstSec: now,
			}
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	wire := NewWire(64, 1, 1)
	wire.SetRetryCohortObserver(observer)
	if !wire.RequestFromSource(bytes20(9), addr.IP.String(), addr.Port, PeerSourceUnknown) {
		t.Fatal("request was not admitted")
	}
	request := <-wire.observedRequests
	wire.handleObservedRequest(request.Request, request.retryObservation)
	if got := observer.Snapshot().PairCapacityDropped; got != 1 {
		t.Fatalf("capacity drops = %d, want 1", got)
	}
	if got := wire.Stats.DialAttempts.Load(); got != 1 {
		t.Fatalf("observer capacity changed dial semantics: attempts=%d", got)
	}
}

func BenchmarkRetryCohortObserverBeginFinish(b *testing.B) {
	for _, benchmark := range []struct {
		name        string
		denominator int
	}{
		{"sample_1_of_64", 64},
		{"sample_all", 1},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
				SampleDenominator: benchmark.denominator, PairCapacity: 131_072, RequestQueueCapacity: 32_768,
			})
			if err != nil {
				b.Fatal(err)
			}
			requests := make([]Request, 4096)
			for i := range requests {
				requests[i] = Request{InfoHash: bytes20(byte(i)), IP: "198.51.100.10", Port: 1024 + i}
				requests[i].InfoHash[1] = byte(i >> 8)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				observation := observer.begin(requests[i&(len(requests)-1)])
				observer.finish(&observation, retryOutcomeDialTimeout)
			}
			b.ReportMetric(float64(observer.estimatedBytes), "observer_bytes")
		})
	}
}

func BenchmarkRetryCohortObserverParallel(b *testing.B) {
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator: 64, PairCapacity: 131_072, RequestQueueCapacity: 32_768,
	})
	if err != nil {
		b.Fatal(err)
	}
	requests := make([]Request, 4096)
	for i := range requests {
		requests[i] = Request{InfoHash: bytes20(byte(i)), IP: "198.51.100.20", Port: 1024 + i}
		requests[i].InfoHash[1] = byte(i >> 8)
	}
	var starts atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := int(starts.Add(37))
		for pb.Next() {
			observation := observer.begin(requests[index&(len(requests)-1)])
			observer.finish(&observation, retryOutcomeDialTimeout)
			index += 37
		}
	})
	b.ReportMetric(float64(observer.estimatedBytes), "observer_bytes")
}

func BenchmarkWireRetryObserverDisabledAdmission(b *testing.B) {
	infoHash := bytes20(1)
	for _, benchmark := range []struct {
		name string
		run  func(*Wire) bool
	}{
		{
			name: "branchless_baseline",
			run: func(wire *Wire) bool {
				request := Request{InfoHash: infoHash, IP: "192.0.2.1", Port: 6881, Source: PeerSourceGetPeers}
				select {
				case wire.requests <- request:
					return true
				default:
					return false
				}
			},
		},
		{
			name: "observer_disabled",
			run: func(wire *Wire) bool {
				return wire.RequestFromSource(infoHash, "192.0.2.1", 6881, PeerSourceGetPeers)
			},
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			wire := NewWire(64, 1, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !benchmark.run(wire) {
					b.Fatal("request was not admitted")
				}
				<-wire.requests
			}
		})
	}
}

func bytes20(seed byte) []byte {
	value := make([]byte, 20)
	for i := range value {
		value[i] = seed + byte(i*17)
	}
	return value
}
