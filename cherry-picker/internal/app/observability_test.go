package app

import (
	"strings"
	"testing"

	"cherry-picker/internal/cache"
	"cherry-picker/internal/dht"
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

func TestRetryCohortRuntimeAndWorkerMetricsUseWindowDeltas(t *testing.T) {
	previous := dht.RetryCohortSnapshot{Enabled: true, Candidates: 100, PairSampledAttempts: 10}
	previous.Classes[0].Attempts = 5
	previous.Classes[0].Successes = 4
	previous.Classes[0].Outcomes[0] = 4
	previous.Classes[0].LatencyMicros = 4000
	previous.Classes[0].LatencyBuckets[0] = 4
	current := previous
	current.PairSampleDenominator = 64
	current.EndpointSampleDenominator = 64
	current.InfoHashSampleDenominator = 64
	current.WindowSeconds = 1860
	current.PairCapacity = 1024
	current.IdentityCapacity = 512
	current.EstimatedBytes = 50_000
	current.Candidates = 164
	current.PairSampledAttempts = 11
	current.EndpointSampledAttempts = 2
	current.EndpointOtherActiveEvents = 1
	current.InfoHashSampledAttempts = 3
	current.InfoHashOtherActiveEvents = 1
	current.PairSlotsUsed = 7
	current.EndpointSlotsUsed = 6
	current.InfoHashSlotsUsed = 5
	current.Classes[0].Attempts = 6
	current.Classes[0].Successes = 5
	current.Classes[0].Outcomes[0] = 5
	current.Classes[0].LatencyMicros = 14_000
	current.Classes[0].LatencyBuckets[1] = 1
	current.Classes[0].EchoHashMismatches = 1
	current.Classes[1].Attempts = 1
	current.Classes[1].Outcomes[4] = 1 // dial_timeout
	current.Classes[1].LatencyMicros = 500_000
	current.Classes[1].LatencyBuckets[3] = 1

	var builder strings.Builder
	appendRetryCohortRuntimeFields(&builder, &current, &previous)
	line := builder.String()
	for _, want := range []string{
		"retry_obs_pair_den=64", "retry_obs_endpoint_den=64", "retry_obs_hash_den=64",
		"retry_obs_candidate=64", "retry_obs_pair_sample=1", "retry_obs_pair_slots=7/1024",
		"retry_obs_endpoint_sample=2", "retry_obs_endpoint_other_active=1",
		"retry_obs_hash_sample=3", "retry_obs_hash_other_active=1",
		"retry_obs_first_attempt=1", "retry_obs_first_ok=1", "retry_obs_first_avg_us=10000",
		"retry_obs_first_ok_ppm=1000000", "retry_obs_first_p95_le_ms=10", "retry_obs_first_echo_bad=1",
		"retry_obs_retry_lt2m_dial_timeout=1",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("runtime fields missing %q: %s", want, line)
		}
	}

	out := make(map[string]uint64)
	addRetryCohortWorkerStats(out, &current)
	if out["retry_observer_enabled"] != 1 || out["retry_observer_first_attempts"] != 6 ||
		out["retry_observer_pair_sample_denominator"] != 64 ||
		out["retry_observer_endpoint_sample_denominator"] != 64 ||
		out["retry_observer_infohash_sample_denominator"] != 64 ||
		out["retry_observer_retry_lt2m_outcome_dial_timeout"] != 1 ||
		out["retry_observer_endpoint_sampled_attempts"] != 2 ||
		out["retry_observer_endpoint_other_active_events"] != 1 ||
		out["retry_observer_infohash_sampled_attempts"] != 3 ||
		out["retry_observer_infohash_other_active_events"] != 1 ||
		out["retry_observer_first_echo_hash_mismatch"] != 1 {
		t.Fatalf("unexpected worker stats: %+v", out)
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
