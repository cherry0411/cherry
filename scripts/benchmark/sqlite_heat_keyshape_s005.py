#!/usr/bin/env python3
"""S-005 short SQLite id8 versus info-hash20 accumulator comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import sqlite_heat_s005 as base


def hash20(torrent_id: int) -> bytes:
    # Deterministic synthetic authority hash. Production uses the real SHA-1
    # info hash received on the DHT wire and does not compute it from an ID.
    return hashlib.sha1(torrent_id.to_bytes(8, "big")).digest()


def configure(connection: sqlite3.Connection, key_shape: str) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(f"PRAGMA cache_size=-{base.CACHE_KIB}")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    if key_shape == "local_dictionary":
        connection.execute(
            """
            CREATE TABLE hashes (
                id INTEGER PRIMARY KEY,
                info_hash BLOB NOT NULL UNIQUE CHECK(length(info_hash) = 20)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE seen (
                hash_id INTEGER NOT NULL,
                actor BLOB NOT NULL CHECK(length(actor) = 8),
                PRIMARY KEY(hash_id, actor)
            ) WITHOUT ROWID
            """
        )
        return
    if key_shape == "id8":
        key = "torrent_key INTEGER NOT NULL"
    elif key_shape == "hash20":
        key = "torrent_key BLOB NOT NULL CHECK(length(torrent_key) = 20)"
    else:
        raise ValueError(key_shape)
    connection.execute(
        f"""
        CREATE TABLE seen (
            {key},
            actor BLOB NOT NULL CHECK(length(actor) = 8),
            PRIMARY KEY(torrent_key, actor)
        ) WITHOUT ROWID
        """
    )


def file_sizes(path: Path) -> dict[str, int]:
    return base.sizes(path)


def run_shape(
    root: Path,
    key_shape: str,
    observations: int,
    uniques: int,
    hashes: int,
    batch_size: int,
    checkpoint_every: int,
    seed: int,
) -> dict[str, Any]:
    path = root / f"{key_shape}.sqlite"
    path.unlink(missing_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    configure(connection, key_shape)
    sql = (
        "INSERT OR IGNORE INTO seen(hash_id, actor) VALUES (?, ?)"
        if key_shape == "local_dictionary"
        else "INSERT OR IGNORE INTO seen(torrent_key, actor) VALUES (?, ?)"
    )
    authority_hashes = [hash20(torrent_id) for torrent_id in range(1, hashes + 1)]
    transaction_ms: list[float] = []
    checkpoint_ms: list[float] = []
    peak_wal = 0
    peak_total = 0
    started = time.perf_counter()
    for transaction, batch in enumerate(
        base.observation_batches(0, observations, batch_size, uniques, hashes, seed), start=1
    ):
        if key_shape in ("hash20", "local_dictionary"):
            hash_rows = [(authority_hashes[torrent_id - 1], actor) for torrent_id, actor in batch]
        if key_shape == "hash20":
            rows = hash_rows
        elif key_shape == "local_dictionary":
            distinct_hashes = sorted({authority_hash for authority_hash, _actor in hash_rows})
        else:
            rows = batch
        before = time.perf_counter()
        if key_shape == "local_dictionary":
            with connection:
                connection.executemany(
                    "INSERT OR IGNORE INTO hashes(info_hash) VALUES (?)",
                    ((authority_hash,) for authority_hash in distinct_hashes),
                )
                mapping: dict[bytes, int] = {}
                for offset in range(0, len(distinct_hashes), 500):
                    query_hashes = distinct_hashes[offset : offset + 500]
                    placeholders = ",".join("?" for _ in query_hashes)
                    for local_id, authority_hash in connection.execute(
                        f"SELECT id, info_hash FROM hashes WHERE info_hash IN ({placeholders})",
                        query_hashes,
                    ):
                        mapping[bytes(authority_hash)] = int(local_id)
                rows = [(mapping[authority_hash], actor) for authority_hash, actor in hash_rows]
                rows.sort()
                connection.executemany(sql, rows)
        else:
            rows.sort()
            with connection:
                connection.executemany(sql, rows)
        transaction_ms.append((time.perf_counter() - before) * 1000)
        current = file_sizes(path)
        peak_wal = max(peak_wal, current["wal"])
        peak_total = max(peak_total, sum(current.values()))
        if transaction % checkpoint_every == 0:
            before = time.perf_counter()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            checkpoint_ms.append((time.perf_counter() - before) * 1000)
    ingest_seconds = time.perf_counter() - started
    group_started = time.perf_counter()
    active = 0
    grouped = 0
    group_sql = (
        """
        SELECT h.info_hash, grouped.n
        FROM (SELECT hash_id, count(*) AS n FROM seen GROUP BY hash_id) AS grouped
        JOIN hashes AS h ON h.id = grouped.hash_id
        """
        if key_shape == "local_dictionary"
        else "SELECT torrent_key, count(*) FROM seen GROUP BY torrent_key"
    )
    for _key, count in connection.execute(group_sql):
        active += 1
        grouped += int(count)
    group_seconds = time.perf_counter() - group_started
    plan = base.query_plan(connection, group_sql)
    row_count = int(connection.execute("SELECT count(*) FROM seen").fetchone()[0])
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    connection.close()
    final = file_sizes(path)
    if row_count != uniques or grouped != uniques or integrity != "ok":
        raise AssertionError(
            f"{key_shape} exactness failed rows={row_count}, grouped={grouped}, integrity={integrity}"
        )
    return {
        "key_shape": key_shape,
        "observations": observations,
        "logical_unique_pairs": uniques,
        "active_hashes": active,
        "batch_size": batch_size,
        "wal_autocheckpoint_pages": 0,
        "truncate_checkpoint_every_transactions": checkpoint_every,
        "ingest_seconds": ingest_seconds,
        "observations_per_second": observations / ingest_seconds,
        "ack_p50_ms": base.percentile(transaction_ms, 0.50),
        "ack_p95_ms": base.percentile(transaction_ms, 0.95),
        "ack_p99_ms": base.percentile(transaction_ms, 0.99),
        "checkpoint_p50_ms": statistics.median(checkpoint_ms),
        "checkpoint_p95_ms": base.percentile(checkpoint_ms, 0.95),
        "group_seconds": group_seconds,
        "group_hashes_per_second": active / group_seconds,
        "group_query_plan": plan,
        "peak_wal_bytes": peak_wal,
        "peak_db_wal_shm_bytes": peak_total,
        "final_sizes": final,
        "final_db_bytes_per_unique": final["db"] / uniques,
        "integrity_check": integrity,
        "dictionary_hashes": (
            int(active) if key_shape == "local_dictionary" else None
        ),
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--observations", type=int, default=2_000_000)
    parser.add_argument("--uniques", type=int, default=1_000_000)
    parser.add_argument("--hashes", type=int, default=200_000)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--checkpoint-every", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260718)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    args.data_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "schema": "cherry.sqlite-heat-keyshape-s005.v1",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "python": sys.version,
            "sqlite": sqlite3.sqlite_version,
            "python_image": base.PYTHON_IMAGE,
            "cpu_max": base.cgroup_value("cpu.max"),
            "memory_max": base.cgroup_value("memory.max"),
        },
        "arms": [
            run_shape(
                args.data_dir,
                shape,
                args.observations,
                args.uniques,
                args.hashes,
                args.batch_size,
                args.checkpoint_every,
                args.seed,
            )
            for shape in ("id8", "hash20", "local_dictionary")
        ],
        "correctness_tradeoff": (
            "hash20 accepts heat before catalog insertion and removes per-event PG hash-to-ID lookups; "
            "day-end grouping performs one batched mapping and only searchable hashes are retained."
        ),
        "limitation": (
            "Synthetic short screen; SHA-1 values are derived for key width/distribution only, and absolute "
            "throughput is not a production capacity forecast."
        ),
        "container_lifetime_memory_peak_bytes": base.cgroup_value("memory.peak"),
    }
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
