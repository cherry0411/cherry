package app

import (
	"testing"

	"cherry-picker/internal/config"
)

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
