import importlib.util
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).with_name("redis_heat_s005.py")
SPEC = importlib.util.spec_from_file_location("redis_heat_s005", MODULE_PATH)
assert SPEC and SPEC.loader
s005 = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = s005
SPEC.loader.exec_module(s005)


def test_splitmix_and_oracle_sampling_are_deterministic():
    first = [s005.splitmix64(index) for index in range(20)]
    second = [s005.splitmix64(index) for index in range(20)]
    assert first == second
    sampled = sum(s005.is_oracle_torrent(index) for index in range(1, 100_001))
    assert 900 <= sampled <= 1100


def test_workload_is_deterministic_and_contains_replays_and_long_tail():
    one = s005.generate_workload(10_000, 1_000, 5_000, 123)
    two = s005.generate_workload(10_000, 1_000, 5_000, 123)
    assert one.events == two.events
    assert one.truth == two.truth
    assert one.unique_pairs < len(one.events)
    unique_counts = sorted((len(actors) for actors in one.truth.values()), reverse=True)
    assert unique_counts[0] > unique_counts[len(unique_counts) // 2] * 10
    assert s005.workload_summary(one)["event_stream_sha256"] == s005.workload_summary(two)[
        "event_stream_sha256"
    ]


@pytest.mark.parametrize(
    "counts",
    [
        {},
        {1: 1},
        {1: 127, 2: 128, 10_000: 1_000_000},
        {index * index + 1: index * 17 for index in range(1, 500)},
    ],
)
def test_delta_varint_round_trip(counts):
    encoded = s005.encode_daily_counts(counts)
    assert s005.decode_daily_counts(encoded) == counts


def test_compare_counts_reports_under_and_over_separately():
    truth = {1: {1, 2, 3}, 2: {4, 5}, 3: {6}}
    report = s005.compare_counts(truth, {1: 2, 2: 4, 3: 1})
    assert report["logical_unique"] == 6
    assert report["estimated_unique"] == 7
    assert report["undercount"] == 1
    assert report["overcount"] == 2
    assert report["hashes_exact"] == 1


def test_pinned_image_and_strict_resource_shape():
    assert s005.REDIS_IMAGE.endswith(s005.REDIS_AMD64_DIGEST)
    assert s005.RESOURCE_CPUS == "2"
    assert s005.RESOURCE_MEMORY == "2g"


def test_function_is_single_atomic_bloom_and_counter_operation():
    assert "redis.call('BF.ADD'" in s005.FUNCTION_LIBRARY
    assert "redis.call('HINCRBY'" in s005.FUNCTION_LIBRARY
    assert "redis.register_function('bloom_count'" in s005.FUNCTION_LIBRARY


def test_module_list_parser_supports_resp2_flat_pairs():
    parsed = s005.parse_module_list([[b"name", b"bf", b"ver", 80800]])
    assert parsed == [{"name": "bf", "ver": 80800}]
