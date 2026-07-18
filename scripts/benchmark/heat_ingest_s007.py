#!/usr/bin/env python3
"""Reproducible CHHT/1 end-to-end ingest benchmark.

The target is a running Cherry API with heat enabled.  Each crawler stream is
strictly sequential, while independent crawler streams run concurrently so the
server can exercise its SQLite FULL group-commit path without manufacturing
out-of-order receipts that production would never send.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import hmac
import json
import math
import statistics
import struct
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


MEDIA_TYPE = "application/vnd.cherry.heat-v1"


def uvarint(value: int) -> bytes:
    if value < 0:
        raise ValueError("uvarint cannot encode a negative value")
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def deterministic_rows(crawler_index: int, batch_index: int, count: int) -> list[tuple[bytes, int]]:
    """Create long-tailed, cross-crawler-overlapping authority/actor pairs."""
    rows: list[tuple[bytes, int]] = []
    base = batch_index * count
    for offset in range(count):
        logical = base + offset
        # Half of every stream is shared across crawlers.  This makes inserted
        # counts exercise exact cross-region dedup instead of measuring only
        # append throughput.
        shared = logical if offset % 2 == 0 else logical + crawler_index * 10_000_000
        hash_rank = int(math.sqrt(shared % 4_000_000))
        info_hash = hashlib.sha1(f"s007-hash-{hash_rank}".encode()).digest()
        actor_seed = shared % 2_000_003
        actor = int.from_bytes(
            hashlib.blake2b(f"s007-actor-{actor_seed}".encode(), digest_size=8).digest(),
            "big",
        )
        rows.append((info_hash, actor))
    return rows


def encode_payload(unix_day: int, rows: list[tuple[bytes, int]]) -> tuple[bytes, int]:
    groups: dict[bytes, set[int]] = {}
    for info_hash, actor in rows:
        groups.setdefault(info_hash, set()).add(actor)
    body = bytearray(b"CHHT\x01" + struct.pack(">I", unix_day))
    body += uvarint(len(groups))
    record_count = 0
    for info_hash in sorted(groups):
        actors = sorted(groups[info_hash])
        body += info_hash
        body += uvarint(len(actors))
        for actor in actors:
            body += struct.pack(">Q", actor)
        record_count += len(actors)
    return bytes(body), record_count


@dataclass(frozen=True)
class Delivery:
    crawler: str
    day: str
    epoch: int
    start_sequence: int
    end_sequence: int
    payload_sha256: str
    signature: str
    payload: bytes
    canonical_records: int


def build_delivery(
    crawler: str,
    crawler_index: int,
    batch_index: int,
    records: int,
    unix_day: int,
    day_text: str,
    epoch: int,
    secret: bytes,
) -> Delivery:
    rows = deterministic_rows(crawler_index, batch_index, records)
    payload, canonical_records = encode_payload(unix_day, rows)
    start = batch_index * records + 1
    end = start + records - 1
    digest = hashlib.sha256(payload).hexdigest()
    prefix = f"CHHT/1\n{crawler}\n{epoch}\n{start}\n{end}\n{digest}\n".encode()
    signature = hmac.new(secret, prefix + payload, hashlib.sha256).hexdigest()
    return Delivery(crawler, day_text, epoch, start, end, digest, signature, payload, canonical_records)


def post(url: str, delivery: Delivery, timeout: float) -> tuple[dict, float]:
    request = urllib.request.Request(url, data=delivery.payload, method="POST")
    request.add_header("Content-Type", MEDIA_TYPE)
    request.add_header("X-CHHT-Crawler", delivery.crawler)
    request.add_header("X-CHHT-Epoch", str(delivery.epoch))
    request.add_header("X-CHHT-Sequence", str(delivery.start_sequence))
    request.add_header("X-CHHT-End-Sequence", str(delivery.end_sequence))
    request.add_header("X-CHHT-Payload-SHA256", delivery.payload_sha256)
    request.add_header("X-CHHT-Signature", delivery.signature)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            raw = response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"CHHT HTTP {error.code}: {error.read()[:2000]!r}") from error
    elapsed_ms = (time.perf_counter() - started) * 1000
    if status != 200:
        raise RuntimeError(f"CHHT success requires HTTP 200, received {status}")
    result = json.loads(raw)
    expected = {
        "crawler": delivery.crawler,
        "day": delivery.day,
        "epoch": delivery.epoch,
        "startSequence": delivery.start_sequence,
        "endSequence": delivery.end_sequence,
        "nextSequence": delivery.end_sequence + 1,
        "payloadSha256": delivery.payload_sha256,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise RuntimeError(f"CHHT receipt mismatch for {key}: {result.get(key)!r} != {value!r}")
    if result.get("error") is not None or result.get("code") is not None:
        raise RuntimeError(f"CHHT success receipt contained an error: {result!r}")
    return result, elapsed_ms


def percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(probability * len(ordered)) - 1))
    return ordered[index]


def run_stream(
    url: str,
    crawler: str,
    crawler_index: int,
    batches: int,
    records: int,
    unix_day: int,
    day_text: str,
    epoch: int,
    secret: bytes,
    timeout: float,
) -> tuple[list[float], int, int, Delivery, int]:
    latencies: list[float] = []
    received = inserted = 0
    first: Delivery | None = None
    first_inserted: int | None = None
    for batch_index in range(batches):
        delivery = build_delivery(
            crawler, crawler_index, batch_index, records, unix_day, day_text, epoch, secret
        )
        first = first or delivery
        result, latency = post(url, delivery, timeout)
        if first_inserted is None:
            first_inserted = int(result["inserted"])
        latencies.append(latency)
        received += int(result["received"])
        inserted += int(result["inserted"])
        if result.get("replay") is not False:
            raise RuntimeError("first delivery unexpectedly reported replay")
    assert first is not None
    assert first_inserted is not None
    return latencies, received, inserted, first, first_inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:5070/api/v1/heat/batches")
    parser.add_argument("--secret-file", type=Path, required=True)
    parser.add_argument("--crawler-ids", default="s007-sg,s007-jp")
    parser.add_argument("--batches", type=int, default=100)
    parser.add_argument("--records", type=int, default=4096)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    secret = args.secret_file.read_bytes().removesuffix(b"\n").removesuffix(b"\r")
    if len(secret) < 32:
        raise SystemExit("secret must contain at least 32 raw bytes")
    crawlers = [value.strip() for value in args.crawler_ids.split(",") if value.strip()]
    if not crawlers or any(len(value) > 64 for value in crawlers):
        raise SystemExit("crawler IDs must contain 1..64 characters")
    today = dt.datetime.now(dt.timezone.utc).date()
    unix_day = (today - dt.date(1970, 1, 1)).days
    epoch = int.from_bytes(hashlib.sha256(f"s007-{today}".encode()).digest()[:8], "big") or 1

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        futures = [
            executor.submit(
                run_stream,
                args.url,
                crawler,
                index,
                args.batches,
                args.records,
                unix_day,
                today.isoformat(),
                epoch + index,
                secret,
                args.timeout,
            )
            for index, crawler in enumerate(crawlers)
        ]
        streams = [future.result() for future in futures]
    wall = time.perf_counter() - started
    latencies = [latency for stream in streams for latency in stream[0]]
    received = sum(stream[1] for stream in streams)
    inserted = sum(stream[2] for stream in streams)

    replay_result, replay_latency = post(args.url, streams[0][3], args.timeout)
    if replay_result.get("replay") is not True or replay_result.get("inserted") != streams[0][4]:
        raise RuntimeError(f"exact replay receipt was not preserved: {replay_result!r}")

    result = {
        "schema": "cherry.heat-ingest-s007.v1",
        "recorded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "config": {
            "url": args.url,
            "crawler_ids": crawlers,
            "batches_per_crawler": args.batches,
            "source_records_per_batch": args.records,
            "unix_day": unix_day,
        },
        "results": {
            "wall_seconds": wall,
            "source_records": len(crawlers) * args.batches * args.records,
            "canonical_records_received": received,
            "exact_pairs_inserted": inserted,
            "source_records_per_second": len(crawlers) * args.batches * args.records / wall,
            "ack_ms_mean": statistics.fmean(latencies),
            "ack_ms_p50": percentile(latencies, 0.50),
            "ack_ms_p95": percentile(latencies, 0.95),
            "ack_ms_p99": percentile(latencies, 0.99),
            "replay_ack_ms": replay_latency,
            "replay_preserved_inserted_count": int(replay_result["inserted"]),
        },
        "notes": [
            "Each crawler stream is sequential; crawler streams overlap in time.",
            "Half of generated logical observations overlap across crawler streams.",
            "HTTP 200 receipts and one response-loss replay are checked field-for-field.",
        ],
    }
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
