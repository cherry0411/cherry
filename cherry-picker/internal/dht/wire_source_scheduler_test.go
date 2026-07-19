package dht

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scheduledValue struct {
	source PeerSource
	id     int
}

func TestWireSourceSchedulerIsolatesAdmissionAndSnapshotsDepth(t *testing.T) {
	wire := NewWire(64, 8, 1)
	if err := wire.ConfigureSourceScheduler(SourceSchedulerOptions{AnnounceSharePercent: 75}); err != nil {
		t.Fatal(err)
	}

	initial := wire.SourceQueueSnapshot()
	if !initial.Enabled || initial.AnnounceCapacity != 6 || initial.LookupCapacity != 2 {
		t.Fatalf("initial snapshot = %+v, want isolated 6/2 split", initial)
	}
	for i := 0; i < initial.LookupCapacity; i++ {
		if !wire.RequestFromSource(make([]byte, 20), "192.0.2.1", 1000+i, PeerSourceGetPeers) {
			t.Fatalf("lookup admission %d unexpectedly failed", i)
		}
	}
	if wire.RequestFromSource(make([]byte, 20), "192.0.2.1", 2000, PeerSourceGetPeers) {
		t.Fatal("lookup admission exceeded its isolated capacity")
	}
	// A lookup burst cannot consume the passive announce reservation.
	if !wire.RequestFromSource(make([]byte, 20), "192.0.2.2", 3000, PeerSourceAnnounce) {
		t.Fatal("announce reservation was displaced by lookup backlog")
	}

	got := wire.SourceQueueSnapshot()
	if got.LookupDepth != 2 || got.AnnounceDepth != 1 || got.LookupAdmitted != 2 || got.LookupDropped != 1 ||
		got.AnnounceAdmitted != 1 || got.AnnounceDropped != 0 || wire.RequestDepth() != 3 || wire.RequestCapacity() != 8 {
		t.Fatalf("populated snapshot = %+v depth=%d capacity=%d", got, wire.RequestDepth(), wire.RequestCapacity())
	}
	if dropped := wire.Stats.QueueDropped.Load(); dropped != 1 {
		t.Fatalf("QueueDropped = %d, want 1", dropped)
	}
}

func TestSourceRequestQueueWeightedFairnessAndIdleFallback(t *testing.T) {
	queue, err := newSourceRequestQueue[scheduledValue](100, 75)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 75; i++ {
		if !queue.tryEnqueue(scheduledValue{source: PeerSourceAnnounce, id: i}, PeerSourceAnnounce) {
			t.Fatalf("announce enqueue %d failed", i)
		}
	}
	for i := 0; i < 25; i++ {
		if !queue.tryEnqueue(scheduledValue{source: PeerSourceGetPeers, id: i}, PeerSourceGetPeers) {
			t.Fatalf("lookup enqueue %d failed", i)
		}
	}

	announce, lookup := 0, 0
	for i := 0; i < 100; i++ {
		value, ok := queue.dequeue()
		if !ok {
			t.Fatalf("dequeue stopped at %d", i)
		}
		switch value.source {
		case PeerSourceAnnounce:
			announce++
		case PeerSourceGetPeers:
			lookup++
		default:
			t.Fatalf("unexpected source %d", value.source)
		}
	}
	if announce != 75 || lookup != 25 {
		t.Fatalf("weighted dequeue announce/lookup = %d/%d, want 75/25", announce, lookup)
	}

	// The next ticket prefers announce, but an empty announce queue must not
	// idle a worker while lookup supply is available.
	if !queue.tryEnqueue(scheduledValue{source: PeerSourceGetPeers}, PeerSourceGetPeers) {
		t.Fatal("lookup fallback enqueue failed")
	}
	value, ok := queue.dequeue()
	if !ok || value.source != PeerSourceGetPeers {
		t.Fatalf("idle fallback = (%+v,%t), want lookup", value, ok)
	}
}

func TestSourceRequestQueueConcurrentDrain(t *testing.T) {
	const total = 4000
	queue, err := newSourceRequestQueue[scheduledValue](total, 50)
	if err != nil {
		t.Fatal(err)
	}
	var producers sync.WaitGroup
	var admissionFailed atomic.Bool
	const producersPerSource = 4
	const recordsPerProducer = total / 2 / producersPerSource
	for _, source := range []PeerSource{PeerSourceAnnounce, PeerSourceGetPeers} {
		for producer := 0; producer < producersPerSource; producer++ {
			producers.Add(1)
			go func(source PeerSource, producer int) {
				defer producers.Done()
				for i := 0; i < recordsPerProducer; i++ {
					id := producer*recordsPerProducer + i
					if !queue.tryEnqueue(scheduledValue{source: source, id: id}, source) {
						admissionFailed.Store(true)
					}
				}
			}(source, producer)
		}
	}
	producers.Wait()
	if admissionFailed.Load() {
		t.Fatal("concurrent admission failed within reserved capacities")
	}
	queue.close()

	var consumed atomic.Int64
	seen := make([]atomic.Int32, total)
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				value, ok := queue.dequeue()
				if !ok {
					return
				}
				offset := 0
				if value.source == PeerSourceGetPeers {
					offset = total / 2
				}
				seen[offset+value.id].Add(1)
				consumed.Add(1)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent drain did not observe closed queues")
	}
	if got := consumed.Load(); got != total {
		t.Fatalf("consumed = %d, want %d", got, total)
	}
	for i := range seen {
		if got := seen[i].Load(); got != 1 {
			t.Fatalf("item %d consumed %d times", i, got)
		}
	}
	// Close is idempotent and a fully drained queue reports shutdown.
	queue.close()
	if _, ok := queue.dequeue(); ok {
		t.Fatal("dequeue succeeded after close and drain")
	}
}

func TestWireSourceSchedulerComposesWithRetryObserver(t *testing.T) {
	wire := NewWire(64, 8, 1)
	if err := wire.ConfigureSourceScheduler(SourceSchedulerOptions{AnnounceSharePercent: 75}); err != nil {
		t.Fatal(err)
	}
	observer, err := NewRetryCohortObserver(RetryCohortObserverOptions{
		SampleDenominator:    1,
		Window:               31 * time.Minute,
		PairCapacity:         1024,
		RequestQueueCapacity: wire.RequestCapacity(),
	})
	if err != nil {
		t.Fatal(err)
	}
	wire.SetRetryCohortObserver(observer)
	if !wire.RequestFromSource(make([]byte, 20), "192.0.2.3", 6881, PeerSourceAnnounce) {
		t.Fatal("observed announce admission failed")
	}
	if got := wire.SourceQueueSnapshot(); !got.Enabled || got.AnnounceDepth != 1 || got.AnnounceCapacity != 6 || got.LookupCapacity != 2 {
		t.Fatalf("observed scheduler snapshot = %+v", got)
	}
}

func TestWireSourceSchedulerRejectsUnsafeStartup(t *testing.T) {
	if err := NewWire(64, 1, 1).ConfigureSourceScheduler(SourceSchedulerOptions{}); err == nil {
		t.Fatal("scheduler accepted a queue too small to reserve both sources")
	}
	wire := NewWire(64, 4, 1)
	if !wire.RequestFromSource(make([]byte, 20), "192.0.2.4", 6881, PeerSourceGetPeers) {
		t.Fatal("legacy admission failed")
	}
	if err := wire.ConfigureSourceScheduler(SourceSchedulerOptions{}); err == nil {
		t.Fatal("scheduler accepted configuration after request admission")
	}
}

func BenchmarkSourceRequestQueue(b *testing.B) {
	queue, err := newSourceRequestQueue[scheduledValue](1024, 75)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := PeerSourceAnnounce
		if i%4 == 3 {
			source = PeerSourceGetPeers
		}
		if !queue.tryEnqueue(scheduledValue{source: source}, source) {
			b.Fatal("enqueue failed")
		}
		if _, ok := queue.dequeue(); !ok {
			b.Fatal("dequeue failed")
		}
	}
}

func BenchmarkLegacyRequestChannel(b *testing.B) {
	queue := make(chan scheduledValue, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queue <- scheduledValue{}
		<-queue
	}
}
