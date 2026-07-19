package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLRUSetContainsDelete(t *testing.T) {
	cache := NewLRU(8)

	if !cache.Set("alpha") {
		t.Fatal("first Set(alpha) = false, want true")
	}
	if cache.Set("alpha") {
		t.Fatal("second Set(alpha) = true, want false")
	}
	if !cache.Contains("alpha") {
		t.Fatal("Contains(alpha) = false, want true")
	}
	if !cache.Delete("alpha") {
		t.Fatal("Delete(alpha) = false, want true")
	}
	if cache.Contains("alpha") {
		t.Fatal("Contains(alpha) = true after Delete")
	}
	if cache.Delete("alpha") {
		t.Fatal("Delete(alpha) = true on missing key")
	}
	if !cache.Set("alpha") {
		t.Fatal("Set(alpha) after Delete = false, want true")
	}
}

func TestLRUEvictsWithinCapacity(t *testing.T) {
	cache := NewLRU(2)
	if !cache.Set("alpha") || !cache.Set("beta") {
		t.Fatal("expected inserts for alpha and beta")
	}
	if !cache.Set("gamma") {
		t.Fatal("Set(gamma) = false, want true")
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	if cache.Contains("alpha") && cache.Contains("beta") && cache.Contains("gamma") {
		t.Fatal("expected one key to be evicted when capacity is exceeded")
	}
}

func TestLRUContainsAndTouchRefreshesEvictionOrder(t *testing.T) {
	cache := NewLRU(128)
	keys := make([]string, 0, 3)
	targetShard := cache.shards[0]
	for i := 0; len(keys) < 3; i++ {
		key := fmt.Sprintf("key-%d", i)
		if cache.shardFor(key) == targetShard {
			keys = append(keys, key)
		}
	}

	cache.Set(keys[0])
	cache.Set(keys[1])
	if !cache.ContainsAndTouch(keys[0]) {
		t.Fatal("ContainsAndTouch(existing) = false")
	}
	cache.Set(keys[2])

	if !cache.Contains(keys[0]) {
		t.Fatal("touched key was evicted")
	}
	if cache.Contains(keys[1]) {
		t.Fatal("least-recently-used key was retained")
	}
	if cache.ContainsAndTouch("missing") {
		t.Fatal("ContainsAndTouch(missing) = true")
	}
}

func TestLRUSnapshotCountersAndOldestAge(t *testing.T) {
	cache := NewLRU(1)
	cache.observedUnix.Store(uint32(time.Now().Add(-90 * time.Second).Unix()))

	cache.Set("alpha")        // miss, insert
	cache.Set("alpha")        // hit
	cache.Contains("alpha")   // hit
	cache.Contains("missing") // miss
	cache.Delete("missing")   // delete miss
	cache.Set("beta")         // miss, insert, eviction

	stats := cache.Snapshot()
	if stats.Len != 1 || stats.Capacity != 1 {
		t.Fatalf("gauges = len %d cap %d, want 1/1", stats.Len, stats.Capacity)
	}
	if stats.Hits != 2 || stats.Misses != 3 || stats.Inserts != 2 ||
		stats.Evicts != 1 || stats.DeleteMisses != 1 {
		t.Fatalf("unexpected snapshot counters: %+v", stats)
	}
	if stats.OldestAgeSeconds < 89 {
		t.Fatalf("OldestAgeSeconds = %d, want about 90", stats.OldestAgeSeconds)
	}

	// Counters are cumulative and a snapshot itself does not mutate them.
	again := cache.Snapshot()
	if again.Hits != stats.Hits || again.Misses != stats.Misses ||
		again.Inserts != stats.Inserts || again.Evicts != stats.Evicts ||
		again.DeleteMisses != stats.DeleteMisses {
		t.Fatalf("snapshot changed counters: before=%+v after=%+v", stats, again)
	}
}

func TestLRUHotHitHasNoAllocations(t *testing.T) {
	cache := NewLRU(128)
	cache.Set("alpha")
	if allocs := testing.AllocsPerRun(1000, func() {
		cache.Set("alpha")
	}); allocs != 0 {
		t.Fatalf("Set(existing) allocations = %f, want 0", allocs)
	}
}

func TestLRUWithTTLExpiresFromFirstInsertionWithoutSliding(t *testing.T) {
	cache := NewLRUWithTTL(8, 2*time.Second)
	cache.observedUnix.Store(100)
	if !cache.Set("dead-peer") {
		t.Fatal("first attempt was not admitted")
	}

	cache.observedUnix.Store(101)
	if cache.Set("dead-peer") {
		t.Fatal("attempt became retryable before cooldown")
	}

	// The duplicate at second 101 must not slide the original deadline.
	cache.observedUnix.Store(102)
	if !cache.Set("dead-peer") {
		t.Fatal("attempt did not become retryable at fixed cooldown")
	}
	stats := cache.Snapshot()
	if stats.Expirations != 1 {
		t.Fatalf("Expirations = %d, want 1", stats.Expirations)
	}
}

func TestLRUWithTTLConcurrentExpiryAdmitsExactlyOneRetry(t *testing.T) {
	cache := NewLRUWithTTL(8, time.Second)
	cache.observedUnix.Store(100)
	cache.Set("dead-peer")
	cache.observedUnix.Store(101)

	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cache.Set("dead-peer") {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := admitted.Load(); got != 1 {
		t.Fatalf("admitted retries = %d, want exactly 1", got)
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

func TestLRUWithNonPositiveTTLPreservesLegacyBehavior(t *testing.T) {
	cache := NewLRUWithTTL(8, 0)
	cache.observedUnix.Store(100)
	cache.Set("peer")
	cache.observedUnix.Store(^uint32(0))
	if cache.Set("peer") {
		t.Fatal("zero TTL unexpectedly expired a legacy entry")
	}
}

func BenchmarkLRUSetExistingTelemetry(b *testing.B) {
	cache := NewLRU(1 << 16)
	cache.Set("alpha")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set("alpha")
	}
}

func BenchmarkLRUSnapshot64Shards(b *testing.B) {
	cache := NewLRU(1 << 16)
	for i := 0; i < 64; i++ {
		cache.Set(fmt.Sprintf("key-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Snapshot()
	}
}
