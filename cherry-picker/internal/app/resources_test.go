package app

import (
	"context"
	runtimemetrics "runtime/metrics"
	"slices"
	"testing"

	"cherry-picker/internal/config"
	"cherry-picker/internal/pipeline"
)

func TestSubmitMetadataEventSendsWhenSpace(t *testing.T) {
	app := New(defaultTestConfig(), testLogger())
	events := make(chan pipeline.Event, 1)
	stats := &runtimeStats{}

	app.submitMetadataEvent(context.Background(), events, pipeline.Event{
		Type: pipeline.EventMetadataFetched, Metadata: &pipeline.Metadata{Name: "x"},
	}, stats)

	if got := len(events); got != 1 {
		t.Fatalf("len(events) = %d, want 1", got)
	}
	if got := stats.metadataEventsSent.Load(); got != 1 {
		t.Fatalf("sent = %d, want 1", got)
	}
	if got := stats.metadataEventsDropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
}

func TestSubmitMetadataEventBlocksThenDropsOnCancel(t *testing.T) {
	app := New(defaultTestConfig(), testLogger())
	events := make(chan pipeline.Event, 1)
	events <- pipeline.Event{Type: pipeline.EventMetadataFetched} // fill it
	stats := &runtimeStats{}

	// 通道已满 + ctx 已取消 → 不应无限阻塞，应记为 drop（而非静默丢弃或死锁）
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app.submitMetadataEvent(ctx, events, pipeline.Event{
		Type: pipeline.EventMetadataFetched, Metadata: &pipeline.Metadata{Name: "y"},
	}, stats)

	if got := stats.metadataEventsSent.Load(); got != 0 {
		t.Fatalf("sent = %d, want 0", got)
	}
	if got := stats.metadataEventsDropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1 (should count drop on ctx cancel, not deadlock)", got)
	}
}

func TestResolveMemLimitExplicitConfig(t *testing.T) {
	cfg := config.Config{MemLimitMB: 4096}
	if got := resolveMemLimit(cfg); got != 4096*1024*1024 {
		t.Fatalf("resolveMemLimit = %d, want 4GB", got)
	}
}

func TestNewLRUCapsScalesWithMemory(t *testing.T) {
	small := newLRUCaps(config.Config{}, 2<<30)  // 2GB
	large := newLRUCaps(config.Config{}, 16<<30) // 16GB

	if large.remoteKnown <= small.remoteKnown {
		t.Fatalf("remoteKnown cap should scale with memory: small=%d large=%d",
			small.remoteKnown, large.remoteKnown)
	}
	// 2GB 上限 → 预算 ~307MB → ~160 万条目 → remoteKnown ~48 万
	if small.remoteKnown < 200_000 {
		t.Fatalf("remoteKnown cap too small for 2GB machine: %d", small.remoteKnown)
	}
}

func TestNewLRUCapsZeroMemoryFallback(t *testing.T) {
	caps := newLRUCaps(config.Config{}, 0)
	if caps.remoteKnown <= 0 || caps.metadataRequestSeen <= 0 {
		t.Fatalf("caps should be positive with unknown memory: %+v", caps)
	}
}

func TestTuneGCByHeapWatermark(t *testing.T) {
	a := &Application{memLimit: 1000, logger: testLogger()}

	if got := a.tuneGC(gogcDefault, 400); got != gogcMax {
		t.Fatalf("low watermark: gogc = %d, want %d", got, gogcMax)
	}
	if got := a.tuneGC(gogcMax, 600); got != gogcDefault {
		t.Fatalf("mid watermark: gogc = %d, want %d", got, gogcDefault)
	}
	if got := a.tuneGC(gogcDefault, 800); got != gogcMin {
		t.Fatalf("high watermark: gogc = %d, want %d", got, gogcMin)
	}
}

func TestReadRuntimeResources(t *testing.T) {
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	heapAlloc, heapInUse, _, totalCPU, ok := readRuntimeResources(samples)
	if !ok {
		t.Fatal("stable runtime resource metrics should be available")
	}
	if heapAlloc == 0 || heapInUse < heapAlloc {
		t.Fatalf("invalid heap metrics: alloc=%d inuse=%d", heapAlloc, heapInUse)
	}
	if totalCPU < 0 {
		t.Fatalf("total CPU must be non-negative: %f", totalCPU)
	}
}

func TestReadRuntimeResourcesRejectsUnavailableMetric(t *testing.T) {
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/not-a-real-metric:bytes"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	if _, _, _, _, ok := readRuntimeResources(samples); ok {
		t.Fatal("unavailable metric should be rejected instead of panicking")
	}
}

func TestListenAddrsExpandsInstances(t *testing.T) {
	a := &Application{cfg: config.Config{
		ListenAddr: ":20003",
		Discovery:  config.DiscoveryConfig{Instances: 3},
	}}
	got, err := a.listenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{":20003", ":20004", ":20005"}
	if !slices.Equal(got, want) {
		t.Fatalf("listenAddrs = %v, want %v", got, want)
	}
}

func TestListenAddrsRejectsPortOverflow(t *testing.T) {
	a := &Application{cfg: config.Config{
		ListenAddr: ":65535",
		Discovery:  config.DiscoveryConfig{Instances: 2},
	}}
	if _, err := a.listenAddrs(); err == nil {
		t.Fatal("listenAddrs accepted a port range above 65535")
	}
}
