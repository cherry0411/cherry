package dht

import "testing"

func TestKeyedDequePushReplacesWithoutLeakingReverseIndex(t *testing.T) {
	deque := newKeyedDeque()
	for i := 0; i < 10_000; i++ {
		deque.Push("node", i)
	}

	if got := deque.Len(); got != 1 {
		t.Fatalf("deque length = %d, want 1", got)
	}
	if got := len(deque.index); got != 1 {
		t.Fatalf("primary index length = %d, want 1", got)
	}
	if got := len(deque.invertedIndex); got != 1 {
		t.Fatalf("reverse index length = %d, want 1", got)
	}
	e, ok := deque.Get("node")
	if !ok || e.Value.(int) != 9_999 {
		t.Fatalf("replacement value = %v, found=%v", e, ok)
	}
}

func TestKeyedDequeCandidateCapRemainsEffective(t *testing.T) {
	const capacity = 8
	deque := newKeyedDeque()
	for i := 0; i < 10_000; i++ {
		deque.Push(i, i)
		if deque.Len() > capacity {
			if removed := deque.Remove(deque.Front()); removed == nil {
				t.Fatal("indexed candidate could not be removed")
			}
		}
	}

	if got := deque.Len(); got != capacity {
		t.Fatalf("deque length = %d, want %d", got, capacity)
	}
	if len(deque.index) != capacity || len(deque.invertedIndex) != capacity {
		t.Fatalf("index sizes = %d/%d, want %d/%d",
			len(deque.index), len(deque.invertedIndex), capacity, capacity)
	}
}
