package cache

import "testing"

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
