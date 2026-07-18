#!/usr/bin/env python3
"""Bounded exact SQLite daily actor/hash accumulator counter-screen for S-005.

The formal invocation runs this script in a pinned 2 CPU / 2 GiB container.
SQLite is additionally restricted to a 64 MiB page cache with mmap disabled.
The default 8M-observation / 4M-unique corpus produces a database larger than
that cache.  It is deterministic and synthetic, not a capacity forecast.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence


PYTHON_IMAGE = (
    "python:3.13.11-alpine3.22@"
    "sha256:4ac787b083ff5fa9d64c6f68440088545e1b941142aed716cf9378ee348a9f1b"
)
PYTHON_INDEX_DIGEST = "sha256:2fd93799bfc6381d078a8f656a5f45d6092e5d11d16f55889b3d5cbfdc64f045"
CACHE_KIB = 65_536
MASK64 = (1 << 64) - 1


def splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return (value ^ (value >> 31)) & MASK64


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


def unique_pair(
    index: int,
    hash_count: int,
    seed: int,
    fanout_actors: int = 0,
    fanout_hashes_per_actor: int = 0,
) -> tuple[int, bytes]:
    # u**4 provides a deterministic long tail without a huge Zipf CDF.  Actor
    # is a SplitMix permutation of unique index, so every generated primary
    # pair is provably unique even if multiple actors land on the same hash.
    fanout_pairs = fanout_actors * fanout_hashes_per_actor
    if index < fanout_pairs:
        actor_number = index // fanout_hashes_per_actor
        hash_number = index % fanout_hashes_per_actor
        if fanout_hashes_per_actor > hash_count:
            raise ValueError("fanout_hashes_per_actor cannot exceed hash_count")
        return 1 + hash_number, struct.pack(">Q", (1 << 63) | actor_number)
    mixed = splitmix64(index ^ seed)
    unit = mixed / (MASK64 + 1)
    torrent_id = 1 + min(hash_count - 1, int((unit**4) * hash_count))
    # Index is an injective 63-bit actor ID in this known-exact corpus.  The
    # high half is reserved above for synthetic high-fanout actors.
    if index >= 1 << 63:
        raise ValueError("known-exact corpus exceeds the reserved actor domain")
    actor = struct.pack(">Q", index)
    return torrent_id, actor


def observation_pair(
    observation: int,
    unique_count: int,
    hash_count: int,
    seed: int,
    fanout_actors: int = 0,
    fanout_hashes_per_actor: int = 0,
) -> tuple[int, bytes]:
    if observation < unique_count:
        source = observation
    else:
        source = splitmix64(observation ^ (seed << 7)) % unique_count
    return unique_pair(
        source, hash_count, seed, fanout_actors, fanout_hashes_per_actor
    )


def observation_batches(
    start: int,
    stop: int,
    batch_size: int,
    unique_count: int,
    hash_count: int,
    seed: int,
    fanout_actors: int = 0,
    fanout_hashes_per_actor: int = 0,
) -> Iterator[list[tuple[int, bytes]]]:
    for offset in range(start, stop, batch_size):
        yield [
            observation_pair(
                index,
                unique_count,
                hash_count,
                seed,
                fanout_actors,
                fanout_hashes_per_actor,
            )
            for index in range(offset, min(offset + batch_size, stop))
        ]


def configure(
    connection: sqlite3.Connection, wal_autocheckpoint_pages: int = 1_000
) -> dict[str, Any]:
    journal_mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(f"PRAGMA cache_size=-{CACHE_KIB}")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint_pages}")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            torrent_id INTEGER NOT NULL,
            actor BLOB NOT NULL CHECK(length(actor) = 8),
            PRIMARY KEY (torrent_id, actor)
        ) WITHOUT ROWID
        """
    )
    return {
        "journal_mode": journal_mode,
        "synchronous": connection.execute("PRAGMA synchronous").fetchone()[0],
        "cache_size_pages": connection.execute("PRAGMA cache_size").fetchone()[0],
        "page_size": connection.execute("PRAGMA page_size").fetchone()[0],
        "mmap_size": connection.execute("PRAGMA mmap_size").fetchone()[0],
        "temp_store": connection.execute("PRAGMA temp_store").fetchone()[0],
        "wal_autocheckpoint_pages": connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0],
    }


def sizes(path: Path) -> dict[str, int]:
    return {
        "db": path.stat().st_size if path.exists() else 0,
        "wal": Path(f"{path}-wal").stat().st_size if Path(f"{path}-wal").exists() else 0,
        "shm": Path(f"{path}-shm").stat().st_size if Path(f"{path}-shm").exists() else 0,
    }


def cgroup_value(name: str) -> int | str | None:
    path = Path("/sys/fs/cgroup") / name
    if not path.exists():
        return None
    value = path.read_text(encoding="ascii").strip()
    return int(value) if value.isdigit() else value


def put_uvarint(buffer: bytearray, value: int) -> None:
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value)


def encode_counts(rows: Sequence[tuple[int, int]]) -> bytes:
    encoded = bytearray()
    previous = 0
    for torrent_id, count in rows:
        put_uvarint(encoded, torrent_id - previous)
        put_uvarint(encoded, count)
        previous = torrent_id
    return bytes(encoded)


def query_plan(connection: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> list[str]:
    return [str(row[3]) for row in connection.execute(f"EXPLAIN QUERY PLAN {sql}", parameters)]


def fanout_filter_audit(
    connection: sqlite3.Connection,
    main_path: Path,
    uniques: int,
    fanout_actors: int,
    fanout_hashes_per_actor: int,
) -> dict[str, Any]:
    """Price an optional actor fanout filter and the opposite PK orientation."""
    if not fanout_actors or not fanout_hashes_per_actor:
        return {
            "enabled_in_production_default": False,
            "measured": False,
            "reason": "No synthetic high-fanout actors were configured.",
        }
    # This threshold is selected only because the synthetic classes are known
    # (normal actors have fanout 1, injected actors have the configured value).
    # It is explicitly not a proposed production threshold.
    diagnostic_threshold = max(1, fanout_hashes_per_actor // 2)
    expected_removed = fanout_actors * fanout_hashes_per_actor
    memory_before = cgroup_value("memory.current")

    connection.execute("DROP TABLE IF EXISTS temp.blocked_actor")
    connection.execute(
        "CREATE TEMP TABLE blocked_actor(actor BLOB PRIMARY KEY) WITHOUT ROWID"
    )
    detect_plan = query_plan(
        connection,
        "SELECT actor FROM seen GROUP BY actor HAVING count(*) > ?",
        (diagnostic_threshold,),
    )
    detect_started = time.perf_counter()
    connection.execute(
        """
        INSERT INTO blocked_actor(actor)
        SELECT actor FROM seen GROUP BY actor HAVING count(*) > ?
        """,
        (diagnostic_threshold,),
    )
    detect_seconds = time.perf_counter() - detect_started
    blocked = int(connection.execute("SELECT count(*) FROM blocked_actor").fetchone()[0])
    temp_page_bytes = int(connection.execute("PRAGMA temp.page_count").fetchone()[0]) * int(
        connection.execute("PRAGMA temp.page_size").fetchone()[0]
    )

    filtered_sql = """
        SELECT torrent_id, count(*)
        FROM seen AS s
        WHERE NOT EXISTS (SELECT 1 FROM blocked_actor AS b WHERE b.actor = s.actor)
        GROUP BY torrent_id
    """
    torrent_filtered_plan = query_plan(connection, filtered_sql)
    filtered_started = time.perf_counter()
    retained_rows = 0
    retained_hashes = 0
    for _torrent_id, count in connection.execute(filtered_sql):
        retained_rows += int(count)
        retained_hashes += 1
    torrent_filtered_seconds = time.perf_counter() - filtered_started

    actor_path = main_path.with_name("formal-actor-first.sqlite")
    actor_path.unlink(missing_ok=True)
    connection.execute("ATTACH DATABASE ? AS actor_order", (str(actor_path),))
    connection.execute(
        """
        CREATE TABLE actor_order.seen (
            actor BLOB NOT NULL CHECK(length(actor) = 8),
            torrent_id INTEGER NOT NULL,
            PRIMARY KEY (actor, torrent_id)
        ) WITHOUT ROWID
        """
    )
    build_started = time.perf_counter()
    with connection:
        connection.execute(
            "INSERT INTO actor_order.seen(actor, torrent_id) SELECT actor, torrent_id FROM main.seen"
        )
    actor_build_seconds = time.perf_counter() - build_started
    actor_db_bytes = actor_path.stat().st_size

    actor_detect_sql = (
        "SELECT count(*) FROM (SELECT actor FROM actor_order.seen "
        "GROUP BY actor HAVING count(*) > ?)"
    )
    actor_detect_plan = query_plan(connection, actor_detect_sql, (diagnostic_threshold,))
    actor_detect_started = time.perf_counter()
    actor_blocked = int(connection.execute(actor_detect_sql, (diagnostic_threshold,)).fetchone()[0])
    actor_detect_seconds = time.perf_counter() - actor_detect_started

    actor_torrent_sql = """
        SELECT torrent_id, count(*)
        FROM actor_order.seen AS s
        WHERE NOT EXISTS (SELECT 1 FROM blocked_actor AS b WHERE b.actor = s.actor)
        GROUP BY torrent_id
    """
    actor_torrent_plan = query_plan(connection, actor_torrent_sql)
    actor_group_started = time.perf_counter()
    actor_retained_rows = 0
    actor_retained_hashes = 0
    for _torrent_id, count in connection.execute(actor_torrent_sql):
        actor_retained_rows += int(count)
        actor_retained_hashes += 1
    actor_torrent_group_seconds = time.perf_counter() - actor_group_started
    memory_after = cgroup_value("memory.current")
    lifetime_peak = cgroup_value("memory.peak")
    main_now = sizes(main_path)
    disk_with_opposite_orientation = sum(main_now.values()) + actor_db_bytes
    connection.execute("DETACH DATABASE actor_order")
    actor_path.unlink(missing_ok=True)

    if blocked != fanout_actors or actor_blocked != fanout_actors:
        raise AssertionError(
            f"fanout audit expected {fanout_actors} actors, got torrent-first={blocked}, "
            f"actor-first={actor_blocked}"
        )
    if uniques - retained_rows != expected_removed or retained_rows != actor_retained_rows:
        raise AssertionError("fanout filter retained-row proof failed")
    return {
        "enabled_in_production_default": False,
        "measured": True,
        "configuration_name": "max_distinct_hashes_per_actor",
        "diagnostic_threshold_not_a_recommendation": diagnostic_threshold,
        "why_default_is_off": (
            "A public IP/IPv6-/64 is a network actor, not a person; CGNAT, VPN and gateways can "
            "legitimately have high fanout. Enable only after trace-labelled precision/recall review."
        ),
        "synthetic_actors_detected": blocked,
        "unique_pairs_removed": expected_removed,
        "retained_unique_pairs": retained_rows,
        "retained_hashes": retained_hashes,
        "torrent_actor_pk": {
            "fanout_detection_seconds": detect_seconds,
            "fanout_detection_query_plan": detect_plan,
            "blocked_temp_table_page_bytes_after_detection": temp_page_bytes,
            "filtered_group_seconds": torrent_filtered_seconds,
            "filtered_group_query_plan": torrent_filtered_plan,
            "benefit": "Final GROUP BY torrent_id streams in primary-key order without a group temp B-tree.",
        },
        "actor_torrent_pk_counterfactual": {
            "build_seconds_not_ingest_benchmark": actor_build_seconds,
            "database_bytes": actor_db_bytes,
            "fanout_detection_seconds": actor_detect_seconds,
            "fanout_detection_query_plan": actor_detect_plan,
            "filtered_group_seconds": actor_torrent_group_seconds,
            "filtered_group_query_plan": actor_torrent_plan,
            "retained_unique_pairs": actor_retained_rows,
            "retained_hashes": actor_retained_hashes,
            "cost": "Final GROUP BY torrent_id requires a temp B-tree in this orientation.",
        },
        "sampled_resource_shape": {
            "memory_current_before_bytes": memory_before,
            "memory_current_after_bytes": memory_after,
            "container_lifetime_memory_peak_bytes": lifetime_peak,
            "main_plus_counterfactual_database_and_wal_bytes": disk_with_opposite_orientation,
            "warning": "memory.peak is container-lifetime, and ephemeral SQLite sorter files may be unlinked.",
        },
    }


def ingest_formal(
    path: Path,
    observations: int,
    uniques: int,
    hashes: int,
    batch_size: int,
    seed: int,
    fanout_actors: int,
    fanout_hashes_per_actor: int,
    wal_autocheckpoint_pages: int,
    explicit_passive_every_transactions: int,
    explicit_checkpoint_mode: str,
) -> dict[str, Any]:
    connection = sqlite3.connect(path, timeout=30)
    pragmas = configure(connection, wal_autocheckpoint_pages)
    latencies: list[float] = []
    explicit_checkpoint_latencies: list[float] = []
    peak_disk = 0
    peak_wal = 0
    started = time.perf_counter()
    sql = "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)"
    for transaction_index, batch in enumerate(observation_batches(
        0,
        observations,
        batch_size,
        uniques,
        hashes,
        seed,
        fanout_actors,
        fanout_hashes_per_actor,
    ), start=1):
        # The wire batch can be reordered because the operation is an
        # idempotent set insertion.  Sorting 5k keys before touching the B-tree
        # is the production candidate for reducing random page churn once the
        # database is larger than the page cache.
        batch.sort()
        before = time.perf_counter()
        with connection:
            connection.executemany(sql, batch)
        latencies.append((time.perf_counter() - before) * 1000)
        # Sample after the durable commit and before an explicit TRUNCATE so
        # the reported WAL/disk peak does not hide the just-acknowledged WAL.
        after_commit = sizes(path)
        peak_wal = max(peak_wal, after_commit["wal"])
        peak_disk = max(peak_disk, sum(after_commit.values()))
        if (
            explicit_passive_every_transactions > 0
            and transaction_index % explicit_passive_every_transactions == 0
        ):
            checkpoint_before = time.perf_counter()
            connection.execute(f"PRAGMA wal_checkpoint({explicit_checkpoint_mode})").fetchone()
            explicit_checkpoint_latencies.append(
                (time.perf_counter() - checkpoint_before) * 1000
            )
        current = sizes(path)
        peak_wal = max(peak_wal, current["wal"])
        peak_disk = max(peak_disk, sum(current.values()))
    ingest_seconds = time.perf_counter() - started
    pre_checkpoint = sizes(path)
    row_count = int(connection.execute("SELECT count(*) FROM seen").fetchone()[0])

    group_started = time.perf_counter()
    rows = [
        (int(torrent_id), int(count))
        for torrent_id, count in connection.execute(
            "SELECT torrent_id, count(*) FROM seen GROUP BY torrent_id ORDER BY torrent_id"
        )
    ]
    group_seconds = time.perf_counter() - group_started
    torrent_group_plan = [
        str(row[3])
        for row in connection.execute(
            "EXPLAIN QUERY PLAN SELECT torrent_id, count(*) FROM seen GROUP BY torrent_id"
        )
    ]
    collapsed = encode_counts(rows)
    collapsed_zlib = zlib.compress(collapsed, level=6)
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    fanout_audit = fanout_filter_audit(
        connection,
        path,
        uniques,
        fanout_actors,
        fanout_hashes_per_actor,
    )
    checkpoint_started = time.perf_counter()
    checkpoint = list(connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone())
    checkpoint_seconds = time.perf_counter() - checkpoint_started
    connection.close()
    post_checkpoint = sizes(path)
    if row_count != uniques or sum(count for _, count in rows) != uniques:
        raise AssertionError(
            f"expected {uniques} exact rows, got rows={row_count}, grouped={sum(c for _, c in rows)}"
        )
    if integrity != "ok":
        raise AssertionError(f"SQLite integrity_check failed: {integrity}")
    return {
        "pragmas": pragmas,
        "observations": observations,
        "logical_unique_pairs": uniques,
        "configured_hashes": hashes,
        "synthetic_fanout_actors": fanout_actors,
        "synthetic_distinct_hashes_per_fanout_actor": fanout_hashes_per_actor,
        "active_hashes": len(rows),
        "batch_size": batch_size,
        "input_batch_sorted_by_primary_key": True,
        "transactions": len(latencies),
        "explicit_checkpoint_every_transactions": explicit_passive_every_transactions,
        "explicit_checkpoint_mode": explicit_checkpoint_mode,
        "explicit_checkpoint_calls": len(explicit_checkpoint_latencies),
        "explicit_checkpoint_p50_ms": percentile(explicit_checkpoint_latencies, 0.50),
        "explicit_checkpoint_p95_ms": percentile(explicit_checkpoint_latencies, 0.95),
        "ack_semantics": (
            "synchronous=FULL commit is the durable ACK boundary; the explicit checkpoint "
            "runs after that ACK boundary and before this single-thread driver accepts the next batch."
        ),
        "ingest_seconds": ingest_seconds,
        "observations_per_second": observations / ingest_seconds,
        "transaction_p50_ms": percentile(latencies, 0.50),
        "transaction_p95_ms": percentile(latencies, 0.95),
        "transaction_p99_ms": percentile(latencies, 0.99),
        "group_seconds": group_seconds,
        "group_hashes_per_second": len(rows) / group_seconds,
        "torrent_group_query_plan": torrent_group_plan,
        "integrity_check": integrity,
        "daily_delta_varint_bytes": len(collapsed),
        "daily_zlib_delta_varint_bytes": len(collapsed_zlib),
        "pre_checkpoint_sizes": pre_checkpoint,
        "post_checkpoint_sizes": post_checkpoint,
        "peak_wal_bytes_sampled_per_transaction": peak_wal,
        "peak_total_bytes_sampled_per_transaction": peak_disk,
        "checkpoint_result": checkpoint,
        "checkpoint_seconds": checkpoint_seconds,
        "post_checkpoint_db_bytes_per_unique": post_checkpoint["db"] / uniques,
        "database_exceeded_64mib_cache": post_checkpoint["db"] > CACHE_KIB * 1024,
        "fanout_filter_audit": fanout_audit,
    }


def write_ack(path: Path, value: int) -> None:
    with path.open("w", encoding="ascii") as handle:
        handle.write(str(value))
        handle.flush()
        os.fsync(handle.fileno())


def crash_writer(args: argparse.Namespace) -> int:
    path = Path(args.crash_db)
    ack = Path(args.crash_ack)
    connection = sqlite3.connect(path, timeout=30)
    configure(connection)
    sql = "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)"
    for batch_index, batch in enumerate(
        observation_batches(0, 20_000, 5_000, 25_000, 5_000, args.seed)
    ):
        with connection:
            connection.executemany(sql, batch)
        write_ack(ack, (batch_index + 1) * 5_000)
    connection.execute("BEGIN IMMEDIATE")
    connection.executemany(
        sql,
        next(observation_batches(20_000, 22_500, 2_500, 25_000, 5_000, args.seed)),
    )
    os._exit(91)


def crash_replay_probe(root: Path, seed: int) -> dict[str, Any]:
    path = root / "crash.sqlite"
    ack = root / "crash.ack"
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--crash-writer",
            "--crash-db",
            str(path),
            "--crash-ack",
            str(ack),
            "--seed",
            str(seed),
        ],
        capture_output=True,
    )
    if completed.returncode != 91:
        raise RuntimeError(f"crash writer returned {completed.returncode}: {completed.stderr!r}")
    acknowledged = int(ack.read_text(encoding="ascii"))
    connection = sqlite3.connect(path, timeout=30)
    configure(connection)
    integrity_after_crash = connection.execute("PRAGMA integrity_check").fetchone()[0]
    recovered_rows = int(connection.execute("SELECT count(*) FROM seen").fetchone()[0])
    replay = next(observation_batches(20_000, 25_000, 5_000, 25_000, 5_000, seed))
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)", replay
        )
    replayed_rows = int(connection.execute("SELECT count(*) FROM seen").fetchone()[0])
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.close()
    passed = (
        acknowledged == 20_000
        and recovered_rows == 20_000
        and replayed_rows == 25_000
        and integrity_after_crash == "ok"
    )
    if not passed:
        raise AssertionError("crash/replay proof failed")
    return {
        "forced_exit_code": completed.returncode,
        "acknowledged_observations": acknowledged,
        "recovered_unique_rows": recovered_rows,
        "rows_after_full_unacked_batch_replay": replayed_rows,
        "integrity_after_crash": integrity_after_crash,
        "passed": passed,
        "guarantee": "FULL commit precedes fsynced batch ack; replay uses INSERT OR IGNORE",
    }


def insert_unique_range(path: Path, start: int, stop: int, seed: int) -> None:
    connection = sqlite3.connect(path, timeout=30)
    configure(connection)
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)",
            (unique_pair(index, 5_000, seed) for index in range(start, stop)),
        )
    connection.close()


def day_rotation_probe(root: Path, seed: int) -> dict[str, Any]:
    current = root / "current.sqlite"
    previous = root / "2026-07-17.sqlite"
    insert_unique_range(current, 0, 10_000, seed)
    os.replace(current, previous)
    insert_unique_range(current, 20_000, 25_000, seed)

    # During a bounded grace interval, route a late event to yesterday.  One
    # replay is ignored and one genuinely late pair is accepted.
    connection = sqlite3.connect(previous, timeout=30)
    configure(connection)
    duplicate = unique_pair(5, 5_000, seed)
    late = unique_pair(10_001, 5_000, seed)
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO seen(torrent_id, actor) VALUES (?, ?)",
            (duplicate, late),
        )
    previous_count = int(connection.execute("SELECT count(*) FROM seen").fetchone()[0])
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.close()
    current_connection = sqlite3.connect(current)
    current_count = int(current_connection.execute("SELECT count(*) FROM seen").fetchone()[0])
    current_connection.close()
    passed = previous_count == 10_001 and current_count == 5_000
    if not passed:
        raise AssertionError("day rotation/late-event proof failed")
    return {
        "atomic_same_filesystem_rename": True,
        "previous_day_rows_after_duplicate_and_new_late_event": previous_count,
        "current_day_rows": current_count,
        "passed": passed,
        "production_rule": (
            "Rename at UTC day boundary, keep the previous file writable for a bounded grace "
            "interval, and only then freeze/group/checkpoint/delete actor rows."
        ),
    }


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--observations", type=int, default=8_000_000)
    parser.add_argument("--uniques", type=int, default=4_000_000)
    parser.add_argument("--hashes", type=int, default=500_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--fanout-actors", type=int, default=100)
    parser.add_argument("--fanout-hashes-per-actor", type=int, default=1_000)
    parser.add_argument("--wal-autocheckpoint-pages", type=int, default=0)
    parser.add_argument("--explicit-passive-every-transactions", type=int, default=100)
    parser.add_argument(
        "--explicit-checkpoint-mode", choices=("PASSIVE", "TRUNCATE"), default="TRUNCATE"
    )
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--crash-writer", action="store_true")
    parser.add_argument("--crash-db")
    parser.add_argument("--crash-ack")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.crash_writer:
        return crash_writer(args)
    if args.uniques > args.observations:
        raise ValueError("uniques cannot exceed observations")
    args.data_dir.mkdir(parents=True, exist_ok=True)
    formal_path = args.data_dir / "formal.sqlite"
    for suffix in ("", "-wal", "-shm"):
        Path(f"{formal_path}{suffix}").unlink(missing_ok=True)
    started = datetime.now(timezone.utc)
    result: dict[str, Any] = {
        "schema": "cherry.sqlite-heat-s005.v1",
        "started_at": started.isoformat(),
        "runtime": {
            "python": sys.version,
            "sqlite": sqlite3.sqlite_version,
            "python_image": PYTHON_IMAGE,
            "python_multiarch_index_digest": PYTHON_INDEX_DIGEST,
            "cpu_max": cgroup_value("cpu.max"),
            "memory_max": cgroup_value("memory.max"),
        },
        "formal": ingest_formal(
            formal_path,
            args.observations,
            args.uniques,
            args.hashes,
            args.batch_size,
            args.seed,
            args.fanout_actors,
            args.fanout_hashes_per_actor,
            args.wal_autocheckpoint_pages,
            args.explicit_passive_every_transactions,
            args.explicit_checkpoint_mode,
        ),
        "crash_replay": crash_replay_probe(args.data_dir, args.seed ^ 0xC2A5),
        "day_rotation": day_rotation_probe(args.data_dir, args.seed ^ 0xDA7E),
        "container_lifetime_memory_peak_bytes": cgroup_value("memory.peak"),
        "limitations": [
            "Synthetic known-unique workload is not a production capacity or latency forecast.",
            "The deterministic u^4 hash distribution is long-tailed but not a fitted DHT trace.",
            "One local Docker run does not cover noisy neighbours, disk exhaustion, bit rot or host loss.",
            "The crash proof kills an uncommitted writer process, not the Docker daemon or physical host.",
            "Cross-host replication and backup are outside this single-file daily accumulator screen.",
        ],
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
