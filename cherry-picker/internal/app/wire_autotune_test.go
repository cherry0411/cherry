package app

import (
	"context"
	"testing"

	"cherry-picker/internal/config"
	"cherry-picker/internal/dht"
)

func TestWireWorkerControllerAutoTuneFalseDoesNotStartTuner(t *testing.T) {
	cfg := config.Config{
		AutoTune: false,
		Metadata: config.MetadataConfig{
			WorkerInitial: 96,
			WorkerMin:     64,
			WorkerMax:     128,
		},
	}
	controller := newWireWorkerController(cfg)
	downloader := dht.NewWire(128, 256, controller.maxWorkers)
	controller.applyTarget(downloader, controller.initialWorkers)

	app := &Application{cfg: cfg}
	if started := app.startWireWorkerTuner(context.Background(), downloader, controller); started {
		t.Fatal("auto_tune=false started the wire worker tuning goroutine")
	}
	if !controller.pinned {
		t.Fatal("auto_tune=false must report a pinned worker policy")
	}
	if got := controller.TargetWorkers(); got != 96 {
		t.Fatalf("target workers = %d, want 96", got)
	}
	if got := downloader.ActiveWorkers(); got != 96 {
		t.Fatalf("active ceiling = %d, want 96", got)
	}
}

func TestWireWorkerControllerExplicitFixedBoundsPinEvenWithAutoTune(t *testing.T) {
	cfg := config.Config{
		AutoTune: true,
		Metadata: config.MetadataConfig{
			WorkerInitial: 192,
			WorkerMin:     192,
			WorkerMax:     192,
		},
	}
	controller := newWireWorkerController(cfg)
	if !controller.pinned {
		t.Fatal("initial=min=max must pin the worker policy")
	}
	if started := (&Application{cfg: cfg}).startWireWorkerTuner(context.Background(), dht.NewWire(128, 256, 192), controller); started {
		t.Fatal("fixed worker bounds started the wire worker tuning goroutine")
	}
}

func TestWireWorkerControllerAppliesObservableTargetAndActiveCeiling(t *testing.T) {
	cfg := config.Config{
		AutoTune: true,
		Metadata: config.MetadataConfig{
			WorkerInitial: 64,
			WorkerMin:     32,
			WorkerMax:     128,
		},
	}
	controller := newWireWorkerController(cfg)
	downloader := dht.NewWire(128, 256, controller.maxWorkers)
	controller.applyTarget(downloader, 80)
	if got := controller.TargetWorkers(); got != 80 {
		t.Fatalf("target workers = %d, want 80", got)
	}
	if got := downloader.ActiveWorkers(); got != 80 {
		t.Fatalf("active ceiling = %d, want 80", got)
	}
}

func TestNextWireWorkerTargetKeepsUsefulWorkersAtHighCPU(t *testing.T) {
	sample := wireTuneSample{
		active: 1024, minWorkers: 128, maxWorkers: 1024,
		attempts: 27_636, dialOK: 6_814, handshakeOK: 5_861, downloadOK: 2_469,
		cpuUtil: 0.99,
	}
	if got := nextWireWorkerTarget(sample); got != 1024 {
		t.Fatalf("target=%d, want 1024: healthy saturated work must not contract", got)
	}
}

func TestNextWireWorkerTargetHighCPUBlocksExpansion(t *testing.T) {
	sample := wireTuneSample{
		active: 256, minWorkers: 128, maxWorkers: 1024, requestDepth: 32_768,
		attempts: 10_000, dialOK: 2_500, handshakeOK: 2_000, downloadOK: 1_000,
		cpuUtil: 0.91,
	}
	if got := nextWireWorkerTarget(sample); got != 256 {
		t.Fatalf("target=%d, want 256: high CPU must gate expansion", got)
	}
}

func TestNextWireWorkerTargetStillContractsBadFunnel(t *testing.T) {
	sample := wireTuneSample{
		active: 512, minWorkers: 128, maxWorkers: 1024,
		attempts: 10_000, dialOK: 10, handshakeOK: 0, cpuUtil: 0.99,
	}
	if got := nextWireWorkerTarget(sample); got != 384 {
		t.Fatalf("target=%d, want 384: unproductive funnel must contract", got)
	}
}

func TestNextWireWorkerTargetContractsResponseBacklog(t *testing.T) {
	sample := wireTuneSample{
		active: 512, minWorkers: 128, maxWorkers: 1024,
		responseDepth: 4_097,
	}
	if got := nextWireWorkerTarget(sample); got != 384 {
		t.Fatalf("target=%d, want 384: response backlog must contract", got)
	}
}

func TestNextWireWorkerTargetPausedUsesMinimum(t *testing.T) {
	sample := wireTuneSample{
		active: 1024, minWorkers: 128, maxWorkers: 1024, paused: true,
	}
	if got := nextWireWorkerTarget(sample); got != 128 {
		t.Fatalf("target=%d, want 128 while metadata is paused", got)
	}
}

func TestNextWireWorkerTargetExpandsHealthyBacklogWithCPUHeadroom(t *testing.T) {
	sample := wireTuneSample{
		active: 256, minWorkers: 128, maxWorkers: 1024, requestDepth: 32_768,
		attempts: 10_000, dialOK: 2_500, handshakeOK: 2_000, downloadOK: 1_000,
		cpuUtil: 0.70,
	}
	if got := nextWireWorkerTarget(sample); got != 320 {
		t.Fatalf("target=%d, want 320 for a healthy queued funnel", got)
	}
}
