#!/usr/bin/env python3
"""CRC-framed hash20+actor8 append-log / external-dedup S-005 screen."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
import struct
import sys
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

import sqlite_heat_s005 as base


MAGIC = b"CHH1"
VERSION = 1
HEADER = struct.Struct(">4sIIIII")  # magic, version, records, raw bytes, compressed bytes, crc32
RECORD_BYTES = 28


def authority_hashes(count: int) -> list[bytes]:
    return [hashlib.sha1(struct.pack(">Q", index + 1)).digest() for index in range(count)]


def record_for_observation(
    observation: int,
    unique_count: int,
    hash_count: int,
    seed: int,
    hashes: Sequence[bytes],
    fanout_actors: int = 0,
    fanout_hashes_per_actor: int = 0,
) -> bytes:
    torrent_id, actor = base.observation_pair(
        observation,
        unique_count,
        hash_count,
        seed,
        fanout_actors,
        fanout_hashes_per_actor,
    )
    return hashes[torrent_id - 1] + actor


def encode_frame(records: Sequence[bytes]) -> bytes:
    raw = b"".join(records)
    if any(len(record) != RECORD_BYTES for record in records):
        raise ValueError("every heat append-log record must be hash20+actor8")
    compressed = zlib.compress(raw, level=1)
    header = HEADER.pack(
        MAGIC,
        VERSION,
        len(records),
        len(raw),
        len(compressed),
        zlib.crc32(raw) & 0xFFFFFFFF,
    )
    return header + compressed


def append_frame(path: Path, records: Sequence[bytes], *, fsync: bool = True) -> float:
    frame = encode_frame(records)
    with path.open("ab", buffering=1024 * 1024) as handle:
        handle.write(frame)
        handle.flush()
        started = time.perf_counter()
        if fsync:
            os.fsync(handle.fileno())
        return (time.perf_counter() - started) * 1000


def scan_frames(path: Path, *, truncate_torn_tail: bool = False) -> Iterator[bytes]:
    last_good = 0
    torn = False
    with path.open("rb") as handle:
        while True:
            header = handle.read(HEADER.size)
            if not header:
                break
            if len(header) != HEADER.size:
                torn = True
                break
            magic, version, records, raw_size, compressed_size, crc = HEADER.unpack(header)
            if magic != MAGIC or version != VERSION or raw_size != records * RECORD_BYTES:
                torn = True
                break
            compressed = handle.read(compressed_size)
            if len(compressed) != compressed_size:
                torn = True
                break
            try:
                raw = zlib.decompress(compressed)
            except zlib.error:
                torn = True
                break
            if len(raw) != raw_size or (zlib.crc32(raw) & 0xFFFFFFFF) != crc:
                torn = True
                break
            last_good = handle.tell()
            yield raw
    if torn and truncate_torn_tail:
        with path.open("r+b") as handle:
            handle.truncate(last_good)


def iter_records(path: Path, *, truncate_torn_tail: bool = False) -> Iterator[bytes]:
    for raw in scan_frames(path, truncate_torn_tail=truncate_torn_tail):
        yield from (raw[offset : offset + RECORD_BYTES] for offset in range(0, len(raw), RECORD_BYTES))


def iter_run(path: Path) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while True:
            record = handle.read(RECORD_BYTES)
            if not record:
                return
            if len(record) != RECORD_BYTES:
                raise ValueError(f"truncated sorted run {path}")
            yield record


def write_workload_log(
    path: Path,
    observations: int,
    uniques: int,
    hash_count: int,
    seed: int,
    hashes: Sequence[bytes],
    frame_records: int,
    fanout_actors: int,
    fanout_hashes_per_actor: int,
) -> dict[str, Any]:
    path.unlink(missing_ok=True)
    fsync_ms: list[float] = []
    started = time.perf_counter()
    for offset in range(0, observations, frame_records):
        records = [
            record_for_observation(
                index,
                uniques,
                hash_count,
                seed,
                hashes,
                fanout_actors,
                fanout_hashes_per_actor,
            )
            for index in range(offset, min(offset + frame_records, observations))
        ]
        fsync_ms.append(append_frame(path, records))
    seconds = time.perf_counter() - started
    return {
        "observations": observations,
        "logical_unique_pairs": uniques,
        "frame_records": frame_records,
        "frames": len(fsync_ms),
        "append_seconds": seconds,
        "observations_per_second": observations / seconds,
        "fsync_p50_ms": base.percentile(fsync_ms, 0.50),
        "fsync_p95_ms": base.percentile(fsync_ms, 0.95),
        "fsync_p99_ms": base.percentile(fsync_ms, 0.99),
        "raw_record_bytes": observations * RECORD_BYTES,
        "framed_compressed_log_bytes": path.stat().st_size,
        "compressed_bytes_per_observation": path.stat().st_size / observations,
    }


def external_dedup(
    log_path: Path,
    work_dir: Path,
    expected_uniques: int,
    sort_chunk_records: int,
) -> dict[str, Any]:
    for path in work_dir.glob("run-*.bin"):
        path.unlink()
    buffered: list[bytes] = []
    runs: list[Path] = []
    peak_disk = log_path.stat().st_size
    started = time.perf_counter()
    input_records = 0

    def flush_run() -> None:
        nonlocal peak_disk
        if not buffered:
            return
        buffered.sort()
        path = work_dir / f"run-{len(runs):05d}.bin"
        previous: bytes | None = None
        with path.open("wb", buffering=1024 * 1024) as output:
            for record in buffered:
                if record != previous:
                    output.write(record)
                    previous = record
        buffered.clear()
        runs.append(path)
        peak_disk = max(peak_disk, log_path.stat().st_size + sum(p.stat().st_size for p in runs))

    for record in iter_records(log_path):
        input_records += 1
        buffered.append(record)
        if len(buffered) >= sort_chunk_records:
            flush_run()
    flush_run()
    run_bytes = sum(path.stat().st_size for path in runs)

    previous: bytes | None = None
    current_hash: bytes | None = None
    current_count = 0
    unique_pairs = 0
    active_hashes = 0
    daily = bytearray()

    def finish_hash() -> None:
        nonlocal active_hashes
        if current_hash is not None:
            daily.extend(current_hash)
            base.put_uvarint(daily, current_count)
            active_hashes += 1

    for record in heapq.merge(*(iter_run(path) for path in runs)):
        if record == previous:
            continue
        previous = record
        authority_hash = record[:20]
        if current_hash != authority_hash:
            finish_hash()
            current_hash = authority_hash
            current_count = 0
        current_count += 1
        unique_pairs += 1
    finish_hash()
    seconds = time.perf_counter() - started
    if unique_pairs != expected_uniques:
        raise AssertionError(f"expected {expected_uniques} unique pairs, got {unique_pairs}")
    daily_compressed = zlib.compress(bytes(daily), level=6)
    peak_disk = max(peak_disk, log_path.stat().st_size + run_bytes + len(daily_compressed))
    return {
        "sort_chunk_records": sort_chunk_records,
        "sort_runs": len(runs),
        "sort_run_bytes": run_bytes,
        "external_dedup_seconds": seconds,
        "input_records": input_records,
        "observations_per_second": input_records / seconds,
        "logical_unique_pairs": unique_pairs,
        "active_hashes": active_hashes,
        "error": 0,
        "daily_hash20_varint_bytes": len(daily),
        "daily_hash20_varint_zlib_bytes": len(daily_compressed),
        "sampled_peak_disk_bytes": peak_disk,
        "container_lifetime_memory_peak_bytes": base.cgroup_value("memory.peak"),
    }


def recovery_probe(root: Path, hashes: Sequence[bytes], seed: int) -> dict[str, Any]:
    path = root / "recovery.frames"
    path.unlink(missing_ok=True)
    expected: list[bytes] = []
    for offset in range(0, 30_000, 10_000):
        records = [
            record_for_observation(index, 40_000, len(hashes), seed, hashes)
            for index in range(offset, offset + 10_000)
        ]
        expected.extend(records)
        append_frame(path, records)
    acknowledged_size = path.stat().st_size
    # Model a power-loss/torn tail by persisting a partial, otherwise valid frame.
    next_records = [
        record_for_observation(index, 40_000, len(hashes), seed, hashes)
        for index in range(30_000, 40_000)
    ]
    frame = encode_frame(next_records)
    with path.open("ab") as handle:
        handle.write(frame[: len(frame) // 2])
        handle.flush()
        os.fsync(handle.fileno())
    recovered = list(iter_records(path, truncate_torn_tail=True))
    truncated_size = path.stat().st_size
    if recovered != expected or truncated_size != acknowledged_size:
        raise AssertionError("CRC/torn-tail recovery failed")

    # The fourth frame was never acknowledged; replay it in full. Then model
    # response loss by writing that durable frame a second time. External sort
    # must make both cases idempotent.
    append_frame(path, next_records)
    append_frame(path, next_records)
    dedup = external_dedup(path, root, 40_000, 20_000)
    return {
        "acknowledged_records_before_torn_tail": 30_000,
        "torn_tail_bytes_removed": True,
        "size_after_recovery_equals_last_acknowledged_boundary": truncated_size == acknowledged_size,
        "unacknowledged_batch_replayed": True,
        "response_lost_batch_replayed_twice": True,
        "unique_after_both_replays": dedup["logical_unique_pairs"],
        "error": dedup["error"],
        "passed": dedup["logical_unique_pairs"] == 40_000,
    }


def day_rotation_probe(root: Path, hashes: Sequence[bytes], seed: int) -> dict[str, Any]:
    current = root / "current.frames"
    previous = root / "2026-07-17.frames"
    current.unlink(missing_ok=True)
    previous.unlink(missing_ok=True)
    day_one = [
        record_for_observation(index, 10_001, len(hashes), seed, hashes)
        for index in range(10_000)
    ]
    append_frame(current, day_one)
    os.replace(current, previous)
    day_two = [
        record_for_observation(index, 5_000, len(hashes), seed ^ 1, hashes)
        for index in range(5_000)
    ]
    append_frame(current, day_two)
    late_new = record_for_observation(10_000, 10_001, len(hashes), seed, hashes)
    append_frame(previous, [day_one[5], late_new])
    finalized = external_dedup(previous, root, 10_001, 20_000)
    return {
        "same_filesystem_atomic_rename": True,
        "previous_file_remained_writable_during_grace": True,
        "late_duplicate_ignored": True,
        "late_new_pair_counted": True,
        "previous_day_unique_pairs": finalized["logical_unique_pairs"],
        "passed": finalized["logical_unique_pairs"] == 10_001,
        "production_rule": "Rename at UTC midnight; finalize only after a bounded late-event grace interval.",
    }


def run_one(
    root: Path,
    label: str,
    observations: int,
    uniques: int,
    hash_count: int,
    seed: int,
    frame_records: int,
    sort_chunk_records: int,
    fanout_actors: int,
    fanout_hashes_per_actor: int,
) -> dict[str, Any]:
    hashes = authority_hashes(hash_count)
    log_path = root / f"{label}.frames"
    append = write_workload_log(
        log_path,
        observations,
        uniques,
        hash_count,
        seed,
        hashes,
        frame_records,
        fanout_actors,
        fanout_hashes_per_actor,
    )
    dedup = external_dedup(log_path, root, uniques, sort_chunk_records)
    return {"label": label, "append": append, "dedup": dedup}


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--observations", type=int, default=8_000_000)
    parser.add_argument("--uniques", type=int, default=4_000_000)
    parser.add_argument("--hashes", type=int, default=500_000)
    parser.add_argument("--frame-records", type=int, default=10_000)
    parser.add_argument("--sort-chunk-records", type=int, default=250_000)
    parser.add_argument("--fanout-actors", type=int, default=100)
    parser.add_argument("--fanout-hashes-per-actor", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--skip-repeat-screens", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    args.data_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    formal = run_one(
        args.data_dir,
        "formal",
        args.observations,
        args.uniques,
        args.hashes,
        args.seed,
        args.frame_records,
        args.sort_chunk_records,
        args.fanout_actors,
        args.fanout_hashes_per_actor,
    )
    recovery_hashes = authority_hashes(50_000)
    result: dict[str, Any] = {
        "schema": "cherry.append-log-heat-s005.v1",
        "started_at": started.isoformat(),
        "runtime": {
            "python": sys.version,
            "python_image": base.PYTHON_IMAGE,
            "cpu_max": base.cgroup_value("cpu.max"),
            "memory_max": base.cgroup_value("memory.max"),
        },
        "record": {
            "bytes": RECORD_BYTES,
            "shape": "authority info_hash BLOB20 + daily actor fingerprint BLOB8",
            "frame_header": "magic/version/count/raw_size/compressed_size/CRC32",
            "compression": "independent zlib level-1 frames",
        },
        "formal": formal,
        "crash_and_replay": recovery_probe(args.data_dir, recovery_hashes, args.seed ^ 0xC2A5),
        "day_rotation": day_rotation_probe(args.data_dir, recovery_hashes, args.seed ^ 0xDA7E),
        "limitations": [
            "Synthetic deterministic workload and Python implementation are not a production capacity forecast.",
            "CRC32 detects torn/corrupt frames but is not cryptographic authentication; production frames need versioned checksums and host-level backup.",
            "The crash probe fsyncs a partial tail then reopens; it does not physically power-cycle the host.",
            "External merge uses bounded record chunks, but Python object overhead differs from a Go fixed-width implementation.",
            "No within-day queryable counter exists; this candidate is valid only when daily heat convergence is acceptable.",
        ],
    }
    if not args.skip_repeat_screens:
        screens = []
        for label, uniques in (("repeat90", 50_000), ("repeat50", 250_000), ("repeat10", 450_000)):
            screens.append(
                run_one(
                    args.data_dir,
                    label,
                    500_000,
                    uniques,
                    50_000,
                    args.seed,
                    args.frame_records,
                    100_000,
                    0,
                    0,
                )
            )
        result["repeat_rate_screens"] = screens
    result["container_lifetime_memory_peak_bytes"] = base.cgroup_value("memory.peak")
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    result["result_sha256_without_self"] = hashlib.sha256(canonical).hexdigest()
    rendered = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
