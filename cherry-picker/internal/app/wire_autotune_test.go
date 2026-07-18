package app

import "testing"

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
