#!/usr/bin/env python3
"""Redis daily heat-screen benchmark (S-005).

This is a synthetic, bounded comparison of three *current-day* actor/hash
deduplication shapes.  It is intentionally not a capacity forecast:

* exact: 64 sharded Redis Sets plus one compact counter Hash;
* hll: one HyperLogLog per active torrent;
* bloom: one global 0.1% Bloom filter plus one compact counter Hash.

Every arm also maintains the same deterministic ~1% exact shadow oracle.  A
fresh, pinned Redis container is used for every arm.  AOF is enabled and the
container is constrained to two CPUs and 2 GiB, which is stricter than the
target 2C4G host.  Containers and volumes are removed even after failures.

The workload is deterministic and long-tailed, but synthetic.  Absolute
throughput and byte counts must not be extrapolated to production traffic.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import heapq
import math
import os
import random
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import uuid
import zlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

try:
    import redis
except ImportError as exc:  # pragma: no cover - exercised by operator setup
    raise SystemExit("redis-py is required (tested with redis-py 4.6+)") from exc


REDIS_IMAGE = (
    "redis:8.8.0-alpine@"
    "sha256:cd5f3ac681c77791c6a8eaa62de876ad2be043ee5a428afb7c0095aa08246277"
)
REDIS_INDEX_DIGEST = "sha256:9d317178eceac8454a2284a9e6df2466b93c745529947f0cd42a0fa9609d7005"
REDIS_AMD64_DIGEST = "sha256:cd5f3ac681c77791c6a8eaa62de876ad2be043ee5a428afb7c0095aa08246277"
RESOURCE_CPUS = "2"
RESOURCE_MEMORY = "2g"
DEFAULT_BATCH_SIZE = 500
DEFAULT_WAIT_EVERY = 10_000
EXACT_SHARDS = 64
ORACLE_SHARDS = 4
ORACLE_PERCENT = 1
MASK64 = (1 << 64) - 1

FUNCTION_LIBRARY = r"""#!lua name=heat_s005
redis.register_function('sadd_count', function(keys, args)
  local added = redis.call('SADD', keys[1], args[1])
  if added == 1 then
    redis.call('HINCRBY', keys[2], args[2], 1)
  end
  return added
end)
redis.register_function('bloom_count', function(keys, args)
  local added = redis.call('BF.ADD', keys[1], args[1])
  if added == 1 then
    redis.call('HINCRBY', keys[2], args[2], 1)
  end
  return added
end)
"""


def run_command(args: Sequence[str], *, timeout: int = 120) -> str:
    completed = subprocess.run(
        list(args),
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return completed.stdout.strip()


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return (value ^ (value >> 31)) & MASK64


def is_oracle_torrent(torrent_id: int) -> bool:
    return splitmix64(torrent_id ^ 0xA5A5A5A5) % 100 < ORACLE_PERCENT


def pair_member(torrent_id: int, actor_id: int) -> bytes:
    return struct.pack(">QQ", torrent_id, actor_id)


@dataclass(frozen=True)
class Event:
    torrent_id: int
    actor_id: int


@dataclass
class Workload:
    events: list[Event]
    truth: dict[int, set[int]]
    oracle_truth: dict[int, set[int]]
    seed: int
    hash_count: int
    actor_count: int

    @property
    def unique_pairs(self) -> int:
        return sum(len(actors) for actors in self.truth.values())

    @property
    def oracle_unique_pairs(self) -> int:
        return sum(len(actors) for actors in self.oracle_truth.values())


def zipf_cdf(items: int, exponent: float = 1.08) -> list[float]:
    weights = [1.0 / ((rank + 1) ** exponent) for rank in range(items)]
    total = sum(weights)
    running = 0.0
    result: list[float] = []
    for weight in weights:
        running += weight / total
        result.append(running)
    result[-1] = 1.0
    return result


def generate_workload(
    event_count: int,
    hash_count: int,
    actor_count: int,
    seed: int,
) -> Workload:
    if min(event_count, hash_count, actor_count) <= 0:
        raise ValueError("event_count, hash_count and actor_count must be positive")
    rng = random.Random(seed)
    cdf = zipf_cdf(hash_count)
    events: list[Event] = []
    truth: dict[int, set[int]] = defaultdict(set)
    oracle_truth: dict[int, set[int]] = defaultdict(set)

    for sequence in range(event_count):
        torrent_id = bisect.bisect_left(cdf, rng.random()) + 1
        # Sixty-five percent of observations come from a small, torrent-local
        # repeat pool.  The rest draw from a larger shared actor population.
        # This supplies both replay pressure and a long tail without retaining
        # generator-side mutable actor pools.
        if rng.random() < 0.65:
            local_slot = rng.randrange(8 + (torrent_id % 57))
            actor_id = splitmix64((torrent_id << 24) ^ local_slot ^ seed)
        else:
            actor_id = splitmix64(rng.randrange(actor_count) ^ (seed << 1))
        event = Event(torrent_id, actor_id)
        events.append(event)
        truth[torrent_id].add(actor_id)
        if is_oracle_torrent(torrent_id):
            oracle_truth[torrent_id].add(actor_id)

    return Workload(events, dict(truth), dict(oracle_truth), seed, hash_count, actor_count)


def put_uvarint(buffer: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("uvarint cannot encode a negative number")
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value)


def read_uvarint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift <= 63:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
    raise ValueError("invalid uvarint")


def encode_daily_counts(counts: dict[int, int]) -> bytes:
    encoded = bytearray()
    previous = 0
    for torrent_id, count in sorted(counts.items()):
        put_uvarint(encoded, torrent_id - previous)
        put_uvarint(encoded, count)
        previous = torrent_id
    return bytes(encoded)


def decode_daily_counts(data: bytes) -> dict[int, int]:
    result: dict[int, int] = {}
    previous = 0
    offset = 0
    while offset < len(data):
        delta, offset = read_uvarint(data, offset)
        count, offset = read_uvarint(data, offset)
        previous += delta
        result[previous] = count
    return result


class RedisContainer:
    def __init__(self, label: str):
        token = uuid.uuid4().hex[:10]
        self.name = f"cherry-s005-{label}-{token}"
        self.volume = f"cherry-s005-data-{token}"
        self.port: int | None = None

    def __enter__(self) -> "RedisContainer":
        run_command(["docker", "volume", "create", self.volume])
        try:
            run_command(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    self.name,
                    "--cpus",
                    RESOURCE_CPUS,
                    "--memory",
                    RESOURCE_MEMORY,
                    "-p",
                    "127.0.0.1::6379",
                    "-v",
                    f"{self.volume}:/data",
                    REDIS_IMAGE,
                    "redis-server",
                    "--appendonly",
                    "yes",
                    "--appendfsync",
                    "everysec",
                    "--auto-aof-rewrite-percentage",
                    "0",
                    "--save",
                    "",
                    "--dir",
                    "/data",
                ],
                timeout=180,
            )
            mapping = run_command(["docker", "port", self.name, "6379/tcp"])
            self.port = int(mapping.rsplit(":", 1)[1])
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    client = self.client()
                    client.ping()
                    client.close()
                    return self
                except (redis.RedisError, OSError):
                    time.sleep(0.1)
            raise RuntimeError("Redis did not become ready")
        except Exception:
            self.cleanup()
            raise

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(["docker", "volume", "rm", "-f", self.volume], capture_output=True)

    def client(self) -> "redis.Redis[Any]":
        if self.port is None:
            raise RuntimeError("container has not started")
        return redis.Redis(
            host="127.0.0.1",
            port=self.port,
            decode_responses=False,
            socket_timeout=30,
            single_connection_client=True,
        )

    def aof_disk_bytes(self) -> int:
        output = run_command(
            ["docker", "exec", self.name, "sh", "-c", "du -sb /data/appendonlydir | cut -f1"]
        )
        return int(output)

    def cgroup_audit(self) -> dict[str, Any]:
        raw = run_command(
            [
                "docker",
                "inspect",
                "--format",
                "{{json .HostConfig}}",
                self.name,
            ]
        )
        host = json.loads(raw)
        return {
            "nano_cpus": host.get("NanoCpus"),
            "memory_limit_bytes": host.get("Memory"),
            "image": REDIS_IMAGE,
            "docker_image_id": run_command(
                ["docker", "inspect", "--format", "{{.Image}}", self.name]
            ),
        }

    def cgroup_memory_bytes(self, filename: str) -> int | None:
        completed = subprocess.run(
            ["docker", "exec", self.name, "cat", f"/sys/fs/cgroup/{filename}"],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            return None
        value = completed.stdout.strip()
        return int(value) if value.isdigit() else None

    def docker_memory(self) -> str:
        raw = run_command(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", self.name]
        )
        return str(json.loads(raw).get("MemUsage", ""))


def parse_module_list(raw: Any) -> list[dict[str, Any]]:
    modules: list[dict[str, Any]] = []
    for module in raw or []:
        if isinstance(module, dict):
            modules.append(
                {
                    (key.decode() if isinstance(key, bytes) else str(key)): (
                        value.decode() if isinstance(value, bytes) else value
                    )
                    for key, value in module.items()
                }
            )
        elif isinstance(module, (list, tuple)):
            decoded: dict[str, Any] = {}
            for index in range(0, len(module), 2):
                key = module[index]
                value = module[index + 1]
                decoded[key.decode() if isinstance(key, bytes) else str(key)] = (
                    value.decode() if isinstance(value, bytes) else value
                )
            modules.append(decoded)
    return modules


def capability_audit(client: "redis.Redis[Any]") -> dict[str, Any]:
    info = client.info("server")
    commands: dict[str, bool] = {}
    for command in ("BF.ADD", "FUNCTION", "FCALL", "WAITAOF", "PFADD", "HSCAN"):
        try:
            result = client.execute_command("COMMAND", "INFO", command)
            # redis-py 4.x returns a mapping while RESP/redis-py 5 may return a
            # sequence.  Both are empty/false when the command is absent.
            commands[command] = bool(result)
        except redis.RedisError:
            commands[command] = False
    modules = parse_module_list(client.execute_command("MODULE", "LIST"))
    return {
        "redis_version": info.get("redis_version"),
        "commands": commands,
        "modules": modules,
        "function_atomicity_reference": "https://redis.io/docs/latest/develop/programmability/functions-intro/",
        "waitaof_reference": "https://redis.io/docs/latest/commands/waitaof/",
        "bloom_reference": "https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/",
    }


def load_functions_and_prove_atomicity(client: "redis.Redis[Any]") -> dict[str, Any]:
    client.execute_command("FUNCTION", "LOAD", "REPLACE", FUNCTION_LIBRARY)
    client.execute_command("BF.RESERVE", "proof:bloom", 0.001, 100, "NONSCALING")
    member = pair_member(99, 101)
    first = int(client.execute_command("FCALL", "bloom_count", 2, "proof:bloom", "proof:count", member, 99))
    replay = int(client.execute_command("FCALL", "bloom_count", 2, "proof:bloom", "proof:count", member, 99))
    count = int(client.hget("proof:count", "99") or 0)
    client.delete("proof:bloom", "proof:count")
    if (first, replay, count) != (1, 0, 1):
        raise AssertionError("FCALL Bloom+HINCRBY replay proof failed")
    return {"first_add": first, "replay_add": replay, "counter": count, "passed": True}


def bloom_capacity_probe(client: "redis.Redis[Any]", capacity: int = 1_000) -> dict[str, Any]:
    """Exercise bounded and expanding filters past their nominal capacity."""
    client.execute_command("BF.RESERVE", "probe:bounded", 0.001, capacity, "NONSCALING")
    client.execute_command("BF.RESERVE", "probe:expanding", 0.001, capacity, "EXPANSION", 2)
    attempted = capacity * 3
    bounded_results: list[Any] = []
    expanding_results: list[Any] = []
    for offset in range(0, attempted, 100):
        bounded = client.pipeline(transaction=False)
        expanding = client.pipeline(transaction=False)
        for index in range(offset, min(offset + 100, attempted)):
            member = struct.pack(">Q", splitmix64(index ^ 0xB100F))
            bounded.execute_command("BF.ADD", "probe:bounded", member)
            expanding.execute_command("BF.ADD", "probe:expanding", member)
        bounded_results.extend(bounded.execute(raise_on_error=False))
        expanding_results.extend(expanding.execute(raise_on_error=False))

    def info(key: str) -> dict[str, Any]:
        raw = client.execute_command("BF.INFO", key)
        return {
            (raw[index].decode() if isinstance(raw[index], bytes) else str(raw[index])): raw[
                index + 1
            ]
            for index in range(0, len(raw), 2)
        }

    bounded_errors = [
        index for index, value in enumerate(bounded_results, start=1) if isinstance(value, Exception)
    ]
    expanding_errors = [
        index for index, value in enumerate(expanding_results, start=1) if isinstance(value, Exception)
    ]
    result = {
        "declared_capacity": capacity,
        "attempted_unique_members": attempted,
        "nonscaling": {
            "first_error_at_attempt": bounded_errors[0] if bounded_errors else None,
            "error_count": len(bounded_errors),
            "info": info("probe:bounded"),
        },
        "expansion_2": {
            "first_error_at_attempt": expanding_errors[0] if expanding_errors else None,
            "error_count": len(expanding_errors),
            "info": info("probe:expanding"),
        },
    }
    client.delete("probe:bounded", "probe:expanding")
    return result


def add_oracle(pipe: Any, event: Event) -> None:
    if is_oracle_torrent(event.torrent_id):
        pipe.sadd(
            f"heat:oracle:{event.torrent_id % ORACLE_SHARDS}",
            pair_member(event.torrent_id, event.actor_id),
        )


def enqueue_primary(pipe: Any, arm: str, event: Event) -> None:
    field = str(event.torrent_id)
    member = pair_member(event.torrent_id, event.actor_id)
    if arm == "exact":
        pipe.execute_command(
            "FCALL",
            "sadd_count",
            2,
            f"heat:exact:{event.torrent_id % EXACT_SHARDS}",
            "heat:counts",
            member,
            field,
        )
    elif arm == "bloom":
        pipe.execute_command(
            "FCALL",
            "bloom_count",
            2,
            "heat:bloom",
            "heat:counts",
            member,
            field,
        )
    elif arm == "hll":
        pipe.pfadd(f"heat:hll:{event.torrent_id}", struct.pack(">Q", event.actor_id))
    else:  # pragma: no cover - guarded by argparse and caller
        raise ValueError(f"unknown arm {arm}")
    add_oracle(pipe, event)


def ingest(
    client: "redis.Redis[Any]",
    arm: str,
    events: Sequence[Event],
    batch_size: int,
    wait_every: int,
) -> dict[str, Any]:
    pipeline_latencies: list[float] = []
    wait_latencies: list[float] = []
    written_since_wait = 0
    started = time.perf_counter()
    for offset in range(0, len(events), batch_size):
        batch = events[offset : offset + batch_size]
        pipe = client.pipeline(transaction=False)
        for event in batch:
            enqueue_primary(pipe, arm, event)
        before = time.perf_counter()
        pipe.execute()
        pipeline_latencies.append((time.perf_counter() - before) * 1000)
        written_since_wait += len(batch)
        if written_since_wait >= wait_every:
            before_wait = time.perf_counter()
            ack = client.execute_command("WAITAOF", 1, 0, 30_000)
            wait_latencies.append((time.perf_counter() - before_wait) * 1000)
            if not ack or int(ack[0]) < 1:
                raise RuntimeError(f"WAITAOF did not acknowledge local fsync: {ack!r}")
            written_since_wait = 0
    if written_since_wait:
        before_wait = time.perf_counter()
        ack = client.execute_command("WAITAOF", 1, 0, 30_000)
        wait_latencies.append((time.perf_counter() - before_wait) * 1000)
        if not ack or int(ack[0]) < 1:
            raise RuntimeError(f"final WAITAOF did not acknowledge local fsync: {ack!r}")
    elapsed = time.perf_counter() - started
    return {
        "events": len(events),
        "seconds": elapsed,
        "events_per_second": len(events) / elapsed,
        "pipeline_batch_size": batch_size,
        "pipeline_roundtrip_p50_ms": percentile(pipeline_latencies, 0.50),
        "pipeline_roundtrip_p95_ms": percentile(pipeline_latencies, 0.95),
        "pipeline_roundtrip_p99_ms": percentile(pipeline_latencies, 0.99),
        "pipeline_amortized_p95_us_per_event": percentile(pipeline_latencies, 0.95)
        * 1000
        / batch_size,
        "waitaof_every_events": wait_every,
        "waitaof_calls": len(wait_latencies),
        "waitaof_p50_ms": percentile(wait_latencies, 0.50),
        "waitaof_p95_ms": percentile(wait_latencies, 0.95),
        "waitaof_p99_ms": percentile(wait_latencies, 0.99),
    }


def hscan_counts(client: "redis.Redis[Any]", key: str) -> dict[int, int]:
    counts: dict[int, int] = {}
    cursor = 0
    while True:
        cursor, values = client.hscan(key, cursor=cursor, count=1000)
        for field, value in values.items():
            counts[int(field)] = int(value)
        if cursor == 0:
            return counts


def scan_keys(client: "redis.Redis[Any]", pattern: str) -> Iterator[bytes]:
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=1000)
        yield from keys
        if cursor == 0:
            return


def freeze_counts(client: "redis.Redis[Any]", arm: str) -> tuple[dict[int, int], dict[str, Any]]:
    started = time.perf_counter()
    if arm in ("exact", "bloom"):
        counts = hscan_counts(client, "heat:counts")
        scan_method = "HSCAN heat:counts"
    else:
        keys = list(scan_keys(client, "heat:hll:*"))
        counts: dict[int, int] = {}
        for offset in range(0, len(keys), 500):
            batch = keys[offset : offset + 500]
            pipe = client.pipeline(transaction=False)
            for key in batch:
                pipe.pfcount(key)
            values = pipe.execute()
            for key, value in zip(batch, values):
                torrent_id = int(key.rsplit(b":", 1)[1])
                counts[torrent_id] = int(value)
        scan_method = "SCAN heat:hll:* + pipelined PFCOUNT"
    redis_read_seconds = time.perf_counter() - started

    encode_started = time.perf_counter()
    raw = encode_daily_counts(counts)
    compressed = zlib.compress(raw, level=6)
    encode_seconds = time.perf_counter() - encode_started
    if decode_daily_counts(zlib.decompress(compressed)) != counts:
        raise AssertionError("delta-varint convergence round trip failed")
    return counts, {
        "method": scan_method,
        "active_hashes": len(counts),
        "redis_read_seconds": redis_read_seconds,
        "hashes_per_second": len(counts) / redis_read_seconds if redis_read_seconds else 0,
        "delta_varint_bytes": len(raw),
        "zlib_delta_varint_bytes": len(compressed),
        "encode_seconds": encode_seconds,
        "encode_hashes_per_second": len(counts) / encode_seconds if encode_seconds else 0,
    }


def compare_counts(truth: dict[int, set[int]], estimates: dict[int, int]) -> dict[str, Any]:
    undercount = 0
    overcount = 0
    hashes_under = 0
    hashes_over = 0
    exact = 0
    max_abs_error = 0
    for torrent_id, actors in truth.items():
        actual = len(actors)
        estimate = estimates.get(torrent_id, 0)
        delta = estimate - actual
        if delta < 0:
            undercount += -delta
            hashes_under += 1
        elif delta > 0:
            overcount += delta
            hashes_over += 1
        else:
            exact += 1
        max_abs_error = max(max_abs_error, abs(delta))
    logical = sum(len(actors) for actors in truth.values())
    estimated = sum(estimates.values())
    return {
        "logical_unique": logical,
        "estimated_unique": estimated,
        "net_error": estimated - logical,
        "undercount": undercount,
        "overcount": overcount,
        "undercount_fraction": undercount / logical if logical else 0,
        "overcount_fraction": overcount / logical if logical else 0,
        "hashes_exact": exact,
        "hashes_undercounted": hashes_under,
        "hashes_overcounted": hashes_over,
        "max_abs_hash_error": max_abs_error,
    }


def oracle_counts(client: "redis.Redis[Any]") -> dict[int, int]:
    result: dict[int, int] = defaultdict(int)
    for key in scan_keys(client, "heat:oracle:*"):
        cursor = 0
        while True:
            cursor, members = client.sscan(key, cursor=cursor, count=1000)
            for member in members:
                torrent_id, _actor_id = struct.unpack(">QQ", member)
                result[torrent_id] += 1
            if cursor == 0:
                break
    return dict(result)


def sum_memory_usage(client: "redis.Redis[Any]", pattern: str) -> tuple[int, int]:
    keys = list(scan_keys(client, pattern))
    total = 0
    for offset in range(0, len(keys), 500):
        batch = keys[offset : offset + 500]
        pipe = client.pipeline(transaction=False)
        for key in batch:
            pipe.memory_usage(key)
        total += sum(int(value or 0) for value in pipe.execute())
    return len(keys), total


def memory_audit(client: "redis.Redis[Any]", arm: str, container: RedisContainer) -> dict[str, Any]:
    info = client.info("memory")
    primary_pattern = {
        "exact": "heat:exact:*",
        "hll": "heat:hll:*",
        "bloom": "heat:bloom",
    }[arm]
    primary_keys, primary_bytes = sum_memory_usage(client, primary_pattern)
    count_bytes = int(client.memory_usage("heat:counts") or 0)
    oracle_keys, oracle_bytes = sum_memory_usage(client, "heat:oracle:*")
    bloom_info: dict[str, Any] | None = None
    if arm == "bloom":
        raw = client.execute_command("BF.INFO", "heat:bloom")
        bloom_info = {}
        for index in range(0, len(raw), 2):
            key = raw[index].decode() if isinstance(raw[index], bytes) else str(raw[index])
            bloom_info[key] = raw[index + 1]
    persistence = client.info("persistence")
    return {
        "redis_used_memory_bytes": int(info.get("used_memory", 0)),
        "redis_used_memory_rss_bytes": int(info.get("used_memory_rss", 0)),
        "redis_used_memory_dataset_bytes": int(info.get("used_memory_dataset", 0)),
        "allocator_frag_ratio": info.get("allocator_frag_ratio"),
        "primary_keys": primary_keys,
        "primary_memory_usage_bytes": primary_bytes,
        "counter_hash_memory_usage_bytes": count_bytes,
        "oracle_keys": oracle_keys,
        "oracle_memory_usage_bytes": oracle_bytes,
        "aof_current_size_bytes": int(persistence.get("aof_current_size", 0)),
        "aof_base_size_bytes": int(persistence.get("aof_base_size", 0)),
        "aof_disk_bytes": container.aof_disk_bytes(),
        "docker_memory_usage": container.docker_memory(),
        "bloom_info": bloom_info,
    }


def aof_rewrite_audit(client: "redis.Redis[Any]", container: RedisContainer) -> dict[str, Any]:
    """Measure a bounded manual rewrite after the day's frozen read."""
    before_persistence = client.info("persistence")
    before_disk = container.aof_disk_bytes()
    before_rss = int(client.info("memory").get("used_memory_rss", 0))
    peak_disk = before_disk
    peak_parent_rss = before_rss
    peak_cgroup_current = container.cgroup_memory_bytes("memory.current") or 0
    before_rewrites = int(before_persistence.get("aof_rewrites", 0))
    started = time.perf_counter()
    client.bgrewriteaof()
    saw_progress = False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        persistence = client.info("persistence")
        in_progress = int(persistence.get("aof_rewrite_in_progress", 0)) == 1
        saw_progress = saw_progress or in_progress
        memory = client.info("memory")
        peak_parent_rss = max(peak_parent_rss, int(memory.get("used_memory_rss", 0)))
        current = container.cgroup_memory_bytes("memory.current") or 0
        peak_cgroup_current = max(peak_cgroup_current, current)
        peak_disk = max(peak_disk, container.aof_disk_bytes())
        rewrites = int(persistence.get("aof_rewrites", before_rewrites))
        if not in_progress and (saw_progress or rewrites > before_rewrites):
            break
        time.sleep(0.01)
    else:
        raise TimeoutError("BGREWRITEAOF did not finish within 60 seconds")
    elapsed = time.perf_counter() - started
    final_persistence = client.info("persistence")
    final_disk = container.aof_disk_bytes()
    cgroup_peak = container.cgroup_memory_bytes("memory.peak")
    return {
        "seconds": elapsed,
        "pre_aof_current_size_bytes": int(before_persistence.get("aof_current_size", 0)),
        "post_aof_current_size_bytes": int(final_persistence.get("aof_current_size", 0)),
        "pre_disk_bytes": before_disk,
        "sampled_peak_disk_bytes": peak_disk,
        "post_disk_bytes": final_disk,
        "pre_parent_rss_bytes": before_rss,
        "sampled_peak_parent_rss_bytes": peak_parent_rss,
        "sampled_peak_cgroup_current_bytes": peak_cgroup_current,
        "container_lifetime_cgroup_peak_bytes": cgroup_peak,
        "last_status": final_persistence.get("aof_last_bgrewrite_status"),
        "sampled_peak_warning": (
            "Docker exec polling can miss short disk/RSS peaks; cgroup memory.peak covers the "
            "container lifetime, not only the rewrite."
        ),
    }


def run_arm(
    arm: str,
    workload: Workload,
    batch_size: int,
    wait_every: int,
) -> dict[str, Any]:
    with RedisContainer(arm) as container:
        client = container.client()
        capabilities = capability_audit(client)
        required = capabilities["commands"]
        if not all(required.values()):
            raise RuntimeError(f"missing required Redis commands: {required}")
        function_proof = load_functions_and_prove_atomicity(client)
        if arm == "bloom":
            client.execute_command(
                "BF.RESERVE",
                "heat:bloom",
                0.001,
                # Fill close to the declared capacity so the 0.1% screen is
                # exercised.  Reserving for all observations would leave this
                # replay-heavy workload half empty and hide false positives.
                max(workload.unique_pairs, 100),
                "NONSCALING",
            )
        resource_audit = container.cgroup_audit()
        before_memory = client.info("memory")
        ingest_metrics = ingest(
            client,
            arm,
            workload.events,
            batch_size=batch_size,
            wait_every=wait_every,
        )
        counts, freeze_metrics = freeze_counts(client, arm)
        estimates = compare_counts(workload.truth, counts)
        observed_oracle = oracle_counts(client)
        oracle_estimates = compare_counts(workload.oracle_truth, observed_oracle)
        oracle_primary_estimates = compare_counts(
            workload.oracle_truth,
            {torrent_id: counts.get(torrent_id, 0) for torrent_id in workload.oracle_truth},
        )
        memory = memory_audit(client, arm, container)
        rewrite = aof_rewrite_audit(client, container)
        client.close()
        return {
            "arm": arm,
            "capabilities": capabilities,
            "resource_audit": resource_audit,
            "function_atomicity_proof": function_proof,
            "before_used_memory_bytes": int(before_memory.get("used_memory", 0)),
            "ingest": ingest_metrics,
            "error": estimates,
            "oracle_storage_error": oracle_estimates,
            "oracle_primary_error": oracle_primary_estimates,
            "freeze": freeze_metrics,
            "memory": memory,
            "aof_rewrite": rewrite,
        }


def run_waitaof_probe(
    batch_sizes: Sequence[int],
    repeats: int,
    seed: int,
) -> dict[str, Any]:
    with RedisContainer("waitaof") as container:
        client = container.client()
        capabilities = capability_audit(client)
        function_proof = load_functions_and_prove_atomicity(client)
        capacity_probe = bloom_capacity_probe(client)
        capacity = sum(batch_sizes) * repeats + 100
        client.execute_command("BF.RESERVE", "heat:bloom", 0.001, capacity, "NONSCALING")
        rng = random.Random(seed)
        rows: list[dict[str, Any]] = []
        sequence = 0
        # Rotate the order per repeat so the every-second fsync phase cannot be
        # permanently associated with one batch size.
        for repeat in range(repeats):
            ordered = list(batch_sizes)
            ordered = ordered[repeat % len(ordered) :] + ordered[: repeat % len(ordered)]
            for batch_size in ordered:
                pipe = client.pipeline(transaction=False)
                for _ in range(batch_size):
                    sequence += 1
                    torrent_id = 1 + rng.randrange(10_000)
                    actor_id = splitmix64(sequence ^ seed)
                    member = pair_member(torrent_id, actor_id)
                    pipe.execute_command(
                        "FCALL",
                        "bloom_count",
                        2,
                        "heat:bloom",
                        "heat:counts",
                        member,
                        str(torrent_id),
                    )
                before = time.perf_counter()
                pipe.execute()
                pipeline_ms = (time.perf_counter() - before) * 1000
                before = time.perf_counter()
                ack = client.execute_command("WAITAOF", 1, 0, 30_000)
                wait_ms = (time.perf_counter() - before) * 1000
                if not ack or int(ack[0]) < 1:
                    raise RuntimeError(f"probe WAITAOF failed: {ack!r}")
                rows.append(
                    {
                        "repeat": repeat,
                        "batch_size": batch_size,
                        "pipeline_ms": pipeline_ms,
                        "waitaof_ms": wait_ms,
                        "total_ms": pipeline_ms + wait_ms,
                        "events_per_second_including_wait": batch_size
                        / ((pipeline_ms + wait_ms) / 1000),
                    }
                )
        summary: dict[str, Any] = {}
        for batch_size in batch_sizes:
            matching = [row for row in rows if row["batch_size"] == batch_size]
            waits = [row["waitaof_ms"] for row in matching]
            totals = [row["total_ms"] for row in matching]
            throughputs = [row["events_per_second_including_wait"] for row in matching]
            summary[str(batch_size)] = {
                "samples": len(matching),
                "waitaof_p50_ms": percentile(waits, 0.50),
                "waitaof_p95_ms": percentile(waits, 0.95),
                "total_p50_ms": percentile(totals, 0.50),
                "throughput_median_events_per_second": statistics.median(throughputs),
            }
        memory = memory_audit(client, "bloom", container)
        client.close()
        return {
            "capabilities": capabilities,
            "function_atomicity_proof": function_proof,
            "bloom_capacity_probe": capacity_probe,
            "rows": rows,
            "summary": summary,
            "memory": memory,
        }


def process_rss_bytes() -> int | None:
    try:
        import psutil

        return int(psutil.Process().memory_info().rss)
    except (ImportError, OSError):
        return None


def iter_run(path: Path) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while True:
            record = handle.read(16)
            if not record:
                return
            if len(record) != 16:
                raise ValueError(f"truncated sort run {path}")
            yield record


def run_append_log_probe(
    workload: Workload,
    frame_events: int = DEFAULT_WAIT_EVERY,
    sort_chunk_records: int = 50_000,
) -> dict[str, Any]:
    """Directionally test a compact durable log and bounded external merge.

    This is deliberately a stdlib reference, not production code.  It proves
    the byte/IO shape and exactness while exposing the extra rollover, merge,
    recovery and late-event code Redis otherwise supplies.
    """
    rss_before = process_rss_bytes()
    with tempfile.TemporaryDirectory(prefix="cherry-s005-log-") as temp:
        root = Path(temp)
        log_path = root / "heat.frames"
        fsync_latencies: list[float] = []
        raw_bytes = 0
        append_started = time.perf_counter()
        with log_path.open("wb", buffering=1024 * 1024) as handle:
            for offset in range(0, len(workload.events), frame_events):
                batch = workload.events[offset : offset + frame_events]
                raw = b"".join(pair_member(event.torrent_id, event.actor_id) for event in batch)
                compressed = zlib.compress(raw, level=1)
                handle.write(struct.pack(">II", len(batch), len(compressed)))
                handle.write(compressed)
                raw_bytes += len(raw)
                handle.flush()
                before_fsync = time.perf_counter()
                os.fsync(handle.fileno())
                fsync_latencies.append((time.perf_counter() - before_fsync) * 1000)
        append_seconds = time.perf_counter() - append_started
        compressed_log_bytes = log_path.stat().st_size
        rss_after_append = process_rss_bytes()

        run_paths: list[Path] = []
        buffered: list[bytes] = []
        frame_count = 0
        sort_started = time.perf_counter()

        def flush_run() -> None:
            if not buffered:
                return
            path = root / f"run-{len(run_paths):05d}.bin"
            with path.open("wb") as output:
                for record in sorted(set(buffered)):
                    output.write(record)
            run_paths.append(path)
            buffered.clear()

        with log_path.open("rb") as handle:
            while True:
                header = handle.read(8)
                if not header:
                    break
                if len(header) != 8:
                    raise ValueError("truncated append-log frame header")
                count, size = struct.unpack(">II", header)
                compressed = handle.read(size)
                if len(compressed) != size:
                    raise ValueError("truncated append-log frame")
                raw = zlib.decompress(compressed)
                if len(raw) != count * 16:
                    raise ValueError("append-log frame record count mismatch")
                frame_count += 1
                buffered.extend(raw[index : index + 16] for index in range(0, len(raw), 16))
                if len(buffered) >= sort_chunk_records:
                    flush_run()
        flush_run()
        run_bytes = sum(path.stat().st_size for path in run_paths)
        rss_after_runs = process_rss_bytes()

        counts: dict[int, int] = defaultdict(int)
        unique_pairs = 0
        previous: bytes | None = None
        for record in heapq.merge(*(iter_run(path) for path in run_paths)):
            if record == previous:
                continue
            previous = record
            torrent_id, _actor_id = struct.unpack(">QQ", record)
            counts[torrent_id] += 1
            unique_pairs += 1
        external_sort_seconds = time.perf_counter() - sort_started
        count_error = compare_counts(workload.truth, dict(counts))
        if count_error["undercount"] or count_error["overcount"]:
            raise AssertionError(f"append-log external merge was not exact: {count_error}")
        collapsed = encode_daily_counts(dict(counts))
        collapsed_compressed = zlib.compress(collapsed, level=6)
        rss_after_merge = process_rss_bytes()
        return {
            "record_shape": "big-endian uint64 torrent_id + uint64 actor fingerprint",
            "events": len(workload.events),
            "frame_events_and_fsync_cadence": frame_events,
            "frames": frame_count,
            "append_seconds": append_seconds,
            "append_events_per_second": len(workload.events) / append_seconds,
            "fsync_p50_ms": percentile(fsync_latencies, 0.50),
            "fsync_p95_ms": percentile(fsync_latencies, 0.95),
            "fsync_p99_ms": percentile(fsync_latencies, 0.99),
            "raw_record_bytes": raw_bytes,
            "compressed_framed_log_bytes": compressed_log_bytes,
            "compression_ratio": compressed_log_bytes / raw_bytes if raw_bytes else 0,
            "sort_chunk_records": sort_chunk_records,
            "sort_runs": len(run_paths),
            "sort_run_bytes": run_bytes,
            "sampled_peak_disk_bytes": compressed_log_bytes + run_bytes,
            "external_sort_seconds": external_sort_seconds,
            "external_sort_events_per_second": len(workload.events) / external_sort_seconds,
            "logical_unique_pairs": unique_pairs,
            "error": count_error,
            "daily_delta_varint_bytes": len(collapsed),
            "daily_zlib_delta_varint_bytes": len(collapsed_compressed),
            "process_rss_before_bytes": rss_before,
            "process_rss_after_append_bytes": rss_after_append,
            "process_rss_after_runs_bytes": rss_after_runs,
            "process_rss_after_merge_bytes": rss_after_merge,
            "scope_warning": (
                "Local host filesystem and Python reference implementation; not directly comparable "
                "to Docker Redis throughput, and not a production capacity forecast."
            ),
        }


def workload_summary(workload: Workload) -> dict[str, Any]:
    counts = sorted((len(actors) for actors in workload.truth.values()), reverse=True)
    event_bytes = b"".join(
        pair_member(event.torrent_id, event.actor_id) for event in workload.events
    )
    return {
        "seed": workload.seed,
        "events": len(workload.events),
        "configured_hashes": workload.hash_count,
        "active_hashes": len(workload.truth),
        "configured_actors": workload.actor_count,
        "logical_unique_pairs": workload.unique_pairs,
        "duplicate_fraction": 1 - workload.unique_pairs / len(workload.events),
        "oracle_active_hashes": len(workload.oracle_truth),
        "oracle_unique_pairs": workload.oracle_unique_pairs,
        "oracle_unique_fraction": workload.oracle_unique_pairs / workload.unique_pairs,
        "unique_per_active_hash_p50": percentile(counts, 0.50),
        "unique_per_active_hash_p95": percentile(counts, 0.95),
        "unique_per_active_hash_p99": percentile(counts, 0.99),
        "max_unique_per_hash": counts[0],
        "event_stream_sha256": hashlib.sha256(event_bytes).hexdigest(),
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=400_000)
    parser.add_argument("--hashes", type=int, default=20_000)
    parser.add_argument("--actors", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--wait-every", type=int, default=DEFAULT_WAIT_EVERY)
    parser.add_argument(
        "--arms",
        nargs="+",
        choices=("exact", "hll", "bloom"),
        default=("exact", "hll", "bloom"),
    )
    parser.add_argument("--skip-waitaof-probe", action="store_true")
    parser.add_argument("--skip-append-log-probe", action="store_true")
    parser.add_argument("--waitaof-repeats", type=int, default=5)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    started = datetime.now(timezone.utc)
    workload = generate_workload(args.events, args.hashes, args.actors, args.seed)
    result: dict[str, Any] = {
        "schema": "cherry.redis-heat-s005.v1",
        "started_at": started.isoformat(),
        "host": {
            "platform": sys.platform,
            "python": sys.version,
            "redis_py": getattr(redis, "__version__", "unknown"),
            "logical_cpus": os.cpu_count(),
        },
        "redis_image": {
            "reference": REDIS_IMAGE,
            "multiarch_index_digest": REDIS_INDEX_DIGEST,
            "linux_amd64_manifest_digest": REDIS_AMD64_DIGEST,
        },
        "resource_limit": {"cpus": RESOURCE_CPUS, "memory": RESOURCE_MEMORY},
        "workload": workload_summary(workload),
        "arms": [],
        "limitations": [
            "Synthetic deterministic long-tail traffic is not a production capacity forecast.",
            "The benchmark truth uses full in-process exact sets; production retains only the 1% oracle.",
            "Single-node AOF tests do not measure replica acknowledgement, failover, network RTT, or noisy-neighbour effects.",
            "End-state RSS is not a sampled peak RSS or long-running allocator-fragmentation measurement.",
            "A single UTC day is screened; expiry, late events, crash/replay recovery and 30-day PG rollups need integration tests.",
        ],
    }
    if not args.skip_waitaof_probe:
        result["waitaof_probe"] = run_waitaof_probe(
            (100, 1000, 5000), args.waitaof_repeats, args.seed ^ 0x51A0F
        )
    if not args.skip_append_log_probe:
        result["append_log_probe"] = run_append_log_probe(workload)
    for arm in args.arms:
        print(f"running {arm} arm", file=sys.stderr, flush=True)
        result["arms"].append(
            run_arm(
                arm,
                workload,
                batch_size=args.batch_size,
                wait_every=args.wait_every,
            )
        )
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    result["result_sha256_without_self"] = hashlib.sha256(canonical).hexdigest()
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
