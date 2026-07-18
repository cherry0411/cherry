package app

import (
	"strings"
	"testing"

	"cherry-picker/internal/cache"
)

func TestFormatRuntimeGaugesUsesGaugesAndCounterDeltas(t *testing.T) {
	previous := statsSnapshot{
		dhtBlacklistRejected: 3,
		heatShadow: heatShadowStatsSnapshot{
			dropped: 5, checks: 100, probableDuplicates: 20, sampledTruePositive: 3,
		},
		infohashLRU: cache.LRUStats{
			Hits: 10, Misses: 20, Inserts: 20, Evicts: 2, DeleteMisses: 1,
		},
	}
	current := statsSnapshot{
		wireTargetWorkers:    96,
		wireActiveWorkers:    96,
		wireMaxWorkers:       128,
		wireBusyWorkers:      80,
		wireWorkersPinned:    true,
		wireRequestDepth:     123,
		wireRequestCap:       1000,
		wireResponseDepth:    5,
		wireResponseCap:      100,
		dhtBlacklistSize:     900,
		dhtBlacklistMax:      1000,
		dhtBlacklistRejected: 7,
		heatShadow: heatShadowStatsSnapshot{
			enabled: true, dropProbableDuplicates: true, dropped: 11,
			checks: 130, new: 21, probableDuplicates: 29,
			currentBitFillPPM: 12345, capacity: 1_000_000, bytes: 4_500_000,
			sampledTruePositive: 7, sampledFalsePositive: 2,
		},
		infohashLRU: cache.LRUStats{
			Len: 90, Capacity: 100, OldestAgeSeconds: 60,
			Hits: 25, Misses: 30, Inserts: 30, Evicts: 7, DeleteMisses: 3,
		},
	}

	line := formatRuntimeGauges(current, previous)
	for _, want := range []string{
		"wire_target=96", "wire_active=96", "wire_busy=80", "wire_pinned=true", "wire_req_depth=123",
		"dht_bl_size=900", "dht_bl_reject=4",
		"lru_ih_len=90", "lru_ih_oldest_s=60", "lru_ih_hit=15",
		"lru_ih_miss=10", "lru_ih_insert=10", "lru_ih_evict=5",
		"lru_ih_del_miss=2",
		"heat_shadow=true", "heat_shadow_drop_enabled=true", "heat_shadow_drop=6",
		"heat_shadow_check=30", "heat_shadow_prob_dup=9",
		"heat_shadow_fill_ppm=12345", "heat_shadow_cap=1000000", "heat_shadow_bytes=4500000",
		"heat_shadow_sample_tp=4", "heat_shadow_sample_fp=2",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("runtime fields missing %q: %s", want, line)
		}
	}
}

func TestAddLRUWorkerStatsKeepsCumulativeCounters(t *testing.T) {
	out := make(map[string]uint64)
	addLRUWorkerStats(out, "remote_known", cache.LRUStats{
		Len: 7, Capacity: 10, OldestAgeSeconds: 45,
		Hits: 11, Misses: 12, Inserts: 13, Evicts: 14, DeleteMisses: 15,
	})

	want := map[string]uint64{
		"lru_remote_known_len":                7,
		"lru_remote_known_capacity":           10,
		"lru_remote_known_oldest_age_seconds": 45,
		"lru_remote_known_hits":               11,
		"lru_remote_known_misses":             12,
		"lru_remote_known_inserts":            13,
		"lru_remote_known_evicts":             14,
		"lru_remote_known_delete_misses":      15,
	}
	for key, value := range want {
		if out[key] != value {
			t.Errorf("worker stat %s = %d, want %d", key, out[key], value)
		}
	}
}

func TestAddWireWorkerStatsExposesTargetActiveCeilingAndPinned(t *testing.T) {
	out := make(map[string]uint64)
	addWireWorkerStats(out, statsSnapshot{
		wireTargetWorkers: 96,
		wireActiveWorkers: 96,
		wireMaxWorkers:    128,
		wireBusyWorkers:   80,
		wireWorkersPinned: true,
	})

	want := map[string]uint64{
		"wire_target_workers": 96,
		"wire_active_workers": 96,
		"wire_active_ceiling": 96,
		"wire_max_workers":    128,
		"wire_busy_workers":   80,
		"wire_workers_pinned": 1,
	}
	for key, value := range want {
		if out[key] != value {
			t.Errorf("worker stat %s = %d, want %d", key, out[key], value)
		}
	}
}
