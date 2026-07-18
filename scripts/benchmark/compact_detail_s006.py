#!/usr/bin/env python3
"""Reproducible PostgreSQL 17 benchmark for compact torrent detail v1.

The harness compares the same deterministic retained-detail corpus as either
per-file/per-extension rows or one LZ4-TOASTed bytea payload per torrent. It
uses two cold table rebuilds per arm in AB/BA order, then pgbench random reads
and client-side v1 decoding. This is a directional storage-shape experiment,
not a production capacity forecast.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


POSTGRES_IMAGE = (
    "postgres@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
)
HARNESS_VERSION = "s006-compact-detail-v1"
MAX_PATH_BYTES = 16 * 1024
MAX_EXTENSION_BYTES = 32
MAX_FILES = 10_000
MAX_EXTENSIONS = 128
MAX_PAYLOAD_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class FileEntry:
    path: str
    length: int


@dataclass(frozen=True)
class ExtensionEntry:
    name: str
    files: int
    length: int


@dataclass(frozen=True)
class TorrentDetail:
    torrent_id: int
    declared_file_count: int
    files: tuple[FileEntry, ...]
    extensions: tuple[ExtensionEntry, ...]


def uvarint(value: int) -> bytes:
    if value < 0 or value > (1 << 63) - 1:
        raise ValueError("v1 values must fit non-negative Int64")
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        result.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(result)


def _bounded_utf8(value: str, maximum: int, field: str) -> bytes:
    if not value:
        raise ValueError(f"{field} is empty")
    if "\x00" in value:
        raise ValueError(f"{field} contains NUL")
    encoded = value.encode("utf-8", "strict")
    if len(encoded) > maximum:
        raise ValueError(f"{field} exceeds {maximum} bytes")
    return encoded


def encode_detail(
    files: Sequence[FileEntry], extensions: Sequence[ExtensionEntry]
) -> bytes:
    if len(files) > MAX_FILES or len(extensions) > MAX_EXTENSIONS:
        raise ValueError("detail entry count exceeds v1 bounds")
    ordered_files = sorted(
        ((_bounded_utf8(item.path, MAX_PATH_BYTES, "path"), item.length) for item in files),
        key=lambda item: (item[0], item[1]),
    )
    ordered_extensions = sorted(
        (
            (_bounded_utf8(item.name, MAX_EXTENSION_BYTES, "extension"), item.files, item.length)
            for item in extensions
        ),
        key=lambda item: item[0],
    )
    if any(left[0] == right[0] for left, right in zip(ordered_extensions, ordered_extensions[1:])):
        raise ValueError("duplicate exact extension name")
    output = bytearray((1,))
    output.extend(uvarint(len(ordered_files)))
    previous = b""
    for path, length in ordered_files:
        prefix = 0
        while prefix < min(len(previous), len(path)) and previous[prefix] == path[prefix]:
            prefix += 1
        output.extend(uvarint(prefix))
        output.extend(uvarint(len(path) - prefix))
        output.extend(path[prefix:])
        output.extend(uvarint(length))
        previous = path
    output.extend(uvarint(len(ordered_extensions)))
    for name, files_count, length in ordered_extensions:
        if files_count <= 0:
            raise ValueError("extension count must be positive")
        output.extend(uvarint(len(name)))
        output.extend(name)
        output.extend(uvarint(files_count))
        output.extend(uvarint(length))
    if len(output) > MAX_PAYLOAD_BYTES:
        raise ValueError("detail payload exceeds 64 MiB")
    return bytes(output)


def decode_detail(payload: bytes) -> tuple[list[FileEntry], list[ExtensionEntry]]:
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError("detail payload exceeds 64 MiB")
    position = 0

    def read_byte() -> int:
        nonlocal position
        if position >= len(payload):
            raise ValueError("truncated payload")
        value = payload[position]
        position += 1
        return value

    def read_varint() -> int:
        start = position
        value = 0
        for index in range(10):
            byte = read_byte()
            if index == 9 and byte & 0x7F > 1:
                raise ValueError("overflowing varint")
            value |= (byte & 0x7F) << (index * 7)
            if not byte & 0x80:
                if len(uvarint(value)) != position - start:
                    raise ValueError("non-canonical varint")
                return value
        raise ValueError("unterminated varint")

    if read_byte() != 1:
        raise ValueError("unsupported version")
    file_count = read_varint()
    if file_count > MAX_FILES:
        raise ValueError("too many files")
    files: list[FileEntry] = []
    previous = b""
    previous_length = 0
    for index in range(file_count):
        prefix = read_varint()
        suffix_length = read_varint()
        if prefix > len(previous) or prefix + suffix_length > MAX_PATH_BYTES:
            raise ValueError("invalid path bounds")
        if suffix_length > len(payload) - position:
            raise ValueError("truncated path")
        current = previous[:prefix] + payload[position : position + suffix_length]
        position += suffix_length
        actual_prefix = 0
        while (
            actual_prefix < min(len(previous), len(current))
            and previous[actual_prefix] == current[actual_prefix]
        ):
            actual_prefix += 1
        if actual_prefix != prefix:
            raise ValueError("non-canonical prefix")
        length = read_varint()
        if length > (1 << 63) - 1:
            raise ValueError("length overflow")
        if index and (current < previous or (current == previous and length < previous_length)):
            raise ValueError("non-canonical file order")
        path = current.decode("utf-8", "strict")
        if "\x00" in path:
            raise ValueError("NUL path")
        files.append(FileEntry(path, length))
        previous, previous_length = current, length

    extension_count = read_varint()
    if extension_count > MAX_EXTENSIONS:
        raise ValueError("too many extensions")
    extensions: list[ExtensionEntry] = []
    previous_extension: bytes | None = None
    for _ in range(extension_count):
        name_length = read_varint()
        if name_length > MAX_EXTENSION_BYTES or name_length > len(payload) - position:
            raise ValueError("invalid extension bounds")
        name_bytes = payload[position : position + name_length]
        position += name_length
        if previous_extension is not None and name_bytes <= previous_extension:
            raise ValueError("non-canonical extension order")
        name = name_bytes.decode("utf-8", "strict")
        if "\x00" in name:
            raise ValueError("NUL extension")
        files_count, length = read_varint(), read_varint()
        if not 0 < files_count <= (1 << 31) - 1 or length > (1 << 63) - 1:
            raise ValueError("invalid extension aggregate")
        extensions.append(ExtensionEntry(name, files_count, length))
        previous_extension = name_bytes
    if position != len(payload):
        raise ValueError("trailing bytes")
    return files, extensions


def build_corpus(torrent_count: int, seed: int) -> list[TorrentDetail]:
    rng = random.Random(seed)
    corpus: list[TorrentDetail] = []
    extensions = ("mkv", "mp4", "flac", "jpg", "srt", "txt", "zip", "pdf")
    for torrent_id in range(1, torrent_count + 1):
        bucket = torrent_id % 100
        if bucket < 50:
            retained, declared, summary = 1, 1, False
        elif bucket < 75:
            retained, declared, summary = 8, 8, False
        elif bucket < 90:
            retained, declared, summary = 32, 32, False
        elif bucket < 98:
            retained, declared, summary = 128, 128, False
        else:
            retained, declared, summary = 64, 2_000 + torrent_id % 1001, True

        prefix = f"资源/{torrent_id:07d}/第{torrent_id % 24 + 1:02d}季"
        files = tuple(
            FileEntry(
                f"{prefix}/第{index:04d}集-{rng.randrange(1_000_000):06d}.{extensions[index % len(extensions)]}",
                rng.randrange(0, 8 * 1024 * 1024 * 1024),
            )
            for index in range(retained)
        )
        aggregates: tuple[ExtensionEntry, ...] = ()
        if summary:
            parts = []
            remaining_files, remaining_bytes = declared, sum(item.length for item in files) * 20
            for index, extension in enumerate(extensions):
                count = max(1, remaining_files // (len(extensions) - index))
                length = max(0, remaining_bytes // (len(extensions) - index))
                parts.append(ExtensionEntry(extension, count, length))
                remaining_files -= count
                remaining_bytes -= length
            aggregates = tuple(parts)
        corpus.append(TorrentDetail(torrent_id, declared, files, aggregates))
    return corpus


def run(command: Sequence[str], *, input_text: str | None = None, check: bool = True) -> str:
    completed = subprocess.run(
        command,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if check and completed.returncode:
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(command)}\n{completed.stdout}")
    return completed.stdout


class PostgresHarness:
    def __init__(self, name: str, keep: bool):
        self.name, self.keep = name, keep

    def start(self) -> None:
        run(
            [
                "docker", "run", "--name", self.name, "--cpus=2", "--memory=2g",
                "--tmpfs", "/var/lib/postgresql/data:rw,size=1400m",
                "-e", "POSTGRES_PASSWORD=cherry", "-e", "POSTGRES_DB=cherry",
                "-d", POSTGRES_IMAGE,
            ]
        )
        for _ in range(120):
            probe = subprocess.run(
                ["docker", "exec", self.name, "psql", "-U", "postgres", "-d", "cherry", "-Atc", "SELECT 1"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )
            if probe.returncode == 0 and probe.stdout.strip() == "1":
                return
            time.sleep(0.25)
        raise RuntimeError("PostgreSQL did not become ready")

    def stop(self) -> None:
        if not self.keep:
            run(["docker", "rm", "-f", self.name], check=False)

    def psql(self, sql: str, *, tuples: bool = False) -> str:
        args = ["docker", "exec", "-i", self.name, "psql", "-U", "postgres", "-d", "cherry", "-v", "ON_ERROR_STOP=1"]
        if tuples:
            args.append("-At")
        return run(args, input_text=sql)

    def copy_lines(self, prefix: str, lines: Iterable[str]) -> float:
        command = [
            "docker", "exec", "-i", self.name, "psql", "-U", "postgres", "-d", "cherry",
            "-v", "ON_ERROR_STOP=1", "-q",
        ]
        started = time.perf_counter()
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
        )
        assert process.stdin is not None
        process.stdin.write(prefix.rstrip(";") + ";\n")
        for line in lines:
            process.stdin.write(line)
        process.stdin.write("\\.\n")
        process.stdin.close()
        output = process.stdout.read() if process.stdout else ""
        return_code = process.wait()
        elapsed = time.perf_counter() - started
        if return_code:
            raise RuntimeError(f"COPY failed ({return_code}): {output}")
        return elapsed


LEGACY_DDL = """
DROP TABLE IF EXISTS legacy_files, legacy_extensions;
CREATE TABLE legacy_files(torrent_id bigint NOT NULL, path_text text NOT NULL, length bigint NOT NULL);
CREATE INDEX legacy_files_torrent_id ON legacy_files(torrent_id);
CREATE TABLE legacy_extensions(torrent_id bigint NOT NULL, extension varchar(32) NOT NULL, file_count integer NOT NULL, total_length bigint NOT NULL, PRIMARY KEY(torrent_id,extension));
"""
DETAIL_DDL = """
DROP TABLE IF EXISTS compact_details;
CREATE TABLE compact_details(torrent_id bigint PRIMARY KEY, payload bytea COMPRESSION lz4 NOT NULL);
"""


def load_legacy(pg: PostgresHarness, corpus: Sequence[TorrentDetail]) -> float:
    pg.psql(LEGACY_DDL)
    elapsed = pg.copy_lines(
        "COPY legacy_files(torrent_id,path_text,length) FROM STDIN",
        (
            f"{item.torrent_id}\t{file.path.replace(chr(9), ' ')}\t{file.length}\n"
            for item in corpus for file in item.files
        ),
    )
    elapsed += pg.copy_lines(
        "COPY legacy_extensions(torrent_id,extension,file_count,total_length) FROM STDIN",
        (
            f"{item.torrent_id}\t{extension.name}\t{extension.files}\t{extension.length}\n"
            for item in corpus for extension in item.extensions
        ),
    )
    pg.psql("VACUUM (ANALYZE) legacy_files; VACUUM (ANALYZE) legacy_extensions;")
    return elapsed


def load_detail(pg: PostgresHarness, encoded: Sequence[tuple[int, bytes]]) -> float:
    pg.psql(DETAIL_DDL)
    elapsed = pg.copy_lines(
        "COPY compact_details(torrent_id,payload) FROM STDIN",
        # COPY text consumes one escaping layer before bytea input sees \xHEX.
        (f"{torrent_id}\t\\\\x{payload.hex()}\n" for torrent_id, payload in encoded),
    )
    pg.psql("VACUUM (ANALYZE) compact_details;")
    return elapsed


def relation_bytes(pg: PostgresHarness, relations: Sequence[str]) -> int:
    values = ",".join(f"'{name}'" for name in relations)
    return int(pg.psql(
        f"SELECT COALESCE(SUM(pg_total_relation_size(name::regclass)),0) FROM (VALUES {','.join(f'({value})' for value in values.split(','))}) AS relation(name);",
        tuples=True,
    ).strip())


def pgbench(pg: PostgresHarness, label: str, script: str, maximum_id: int, seconds: int) -> dict:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8") as handle:
        handle.write(script)
        local_path = handle.name
    remote_path = f"/tmp/{label}.sql"
    try:
        run(["docker", "cp", local_path, f"{pg.name}:{remote_path}"])
        prefix = f"/tmp/{label}_latency"
        output = run(
            [
                "docker", "exec", pg.name, "pgbench", "-U", "postgres", "-d", "cherry",
                "-n", "-c", "4", "-j", "2", "-T", str(seconds),
                "--random-seed=424242", "-D", f"maximum_id={maximum_id}",
                "-l", f"--log-prefix={prefix}", "-f", remote_path,
            ]
        )
        logs = run(["docker", "exec", pg.name, "sh", "-c", f"cat {prefix}.*"])
        latency_us = [int(line.split()[2]) for line in logs.splitlines() if line and not line.startswith("#")]
        tps_match = re.search(r"tps = ([0-9.]+)", output)
        return {
            "tps": float(tps_match.group(1)) if tps_match else None,
            "transactions": len(latency_us),
            "latency_ms": {
                "p50": percentile(latency_us, 50) / 1000,
                "p95": percentile(latency_us, 95) / 1000,
                "p99": percentile(latency_us, 99) / 1000,
            },
        }
    finally:
        Path(local_path).unlink(missing_ok=True)


def percentile(values: Sequence[int], percentile_value: int) -> float:
    ordered = sorted(values)
    if not ordered:
        return math.nan
    position = (len(ordered) - 1) * percentile_value / 100
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--torrents", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20260718)
    parser.add_argument("--read-seconds", type=int, default=10)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep-container", action="store_true")
    args = parser.parse_args()
    if args.torrents <= 0:
        parser.error("--torrents must be positive")

    corpus = build_corpus(args.torrents, args.seed)
    encoded = [(item.torrent_id, encode_detail(item.files, item.extensions)) for item in corpus]
    for item, (_, payload) in zip(corpus, encoded):
        files, extensions = decode_detail(payload)
        expected_files = sorted(item.files, key=lambda entry: (entry.path.encode(), entry.length))
        expected_extensions = sorted(item.extensions, key=lambda entry: entry.name.encode())
        if files != expected_files or extensions != expected_extensions:
            raise AssertionError("codec roundtrip mismatch")

    container = f"cherry-detail-s006-{os.getpid()}"
    pg = PostgresHarness(container, args.keep_container)
    try:
        pg.start()
        version = pg.psql("SHOW server_version;", tuples=True).strip()
        write_runs: dict[str, list[float]] = {"legacy": [], "compact": []}
        storage_runs: dict[str, list[int]] = {"legacy": [], "compact": []}
        for arm in ("legacy", "compact", "compact", "legacy"):
            pg.psql("CHECKPOINT;")
            if arm == "legacy":
                write_runs[arm].append(load_legacy(pg, corpus))
                storage_runs[arm].append(relation_bytes(pg, ["legacy_files", "legacy_extensions"]))
            else:
                write_runs[arm].append(load_detail(pg, encoded))
                storage_runs[arm].append(relation_bytes(pg, ["compact_details"]))

        # Rebuild both arms concurrently for identical random-read residency opportunity.
        load_legacy(pg, corpus)
        load_detail(pg, encoded)
        read_runs: dict[str, list[dict]] = {"legacy": [], "compact": []}
        scripts = {
            "legacy": "\\set torrent_id random(1, :maximum_id)\nSELECT path_text,length FROM legacy_files WHERE torrent_id=:torrent_id;\nSELECT extension,file_count,total_length FROM legacy_extensions WHERE torrent_id=:torrent_id;\n",
            "compact": "\\set torrent_id random(1, :maximum_id)\nSELECT payload FROM compact_details WHERE torrent_id=:torrent_id;\n",
        }
        for arm in ("legacy", "compact", "compact", "legacy"):
            read_runs[arm].append(pgbench(pg, f"{arm}_{len(read_runs[arm])}", scripts[arm], args.torrents, args.read_seconds))

        fetch_started = time.perf_counter()
        payload_lines = pg.psql(
            "COPY (SELECT encode(payload,'hex') FROM compact_details ORDER BY torrent_id) TO STDOUT;"
        ).splitlines()
        fetch_seconds = time.perf_counter() - fetch_started
        decode_started = time.perf_counter()
        decoded_files = 0
        for line in payload_lines:
            files, _ = decode_detail(bytes.fromhex(line))
            decoded_files += len(files)
        decode_seconds = time.perf_counter() - decode_started

        legacy_bytes = round(statistics.median(storage_runs["legacy"]))
        compact_bytes = round(statistics.median(storage_runs["compact"]))
        result = {
            "harness_version": HARNESS_VERSION,
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "postgres_image": POSTGRES_IMAGE,
            "postgres_version": version,
            "limits": {"cpus": 2, "memory": "2g", "data_tmpfs": "1400m"},
            "corpus": {
                "seed": args.seed,
                "torrents": len(corpus),
                "retained_file_rows": sum(len(item.files) for item in corpus),
                "extension_rows": sum(len(item.extensions) for item in corpus),
                "summary_torrents": sum(bool(item.extensions) for item in corpus),
                "logical_sha256": hashlib.sha256(
                    b"".join(torrent_id.to_bytes(8, "big") + payload for torrent_id, payload in encoded)
                ).hexdigest(),
                "encoded_payload_bytes": sum(len(payload) for _, payload in encoded),
            },
            "write_seconds_abba": write_runs,
            "write_seconds_median": {arm: statistics.median(values) for arm, values in write_runs.items()},
            "relation_bytes_runs": storage_runs,
            "relation_bytes_median": {"legacy": legacy_bytes, "compact": compact_bytes},
            "storage_reduction_fraction": 1 - compact_bytes / legacy_bytes,
            "random_read_runs_abba": read_runs,
            "random_read_median_tps": {
                arm: statistics.median(run["tps"] for run in runs) for arm, runs in read_runs.items()
            },
            "client_decode": {
                "payload_fetch_seconds": fetch_seconds,
                "decode_seconds": decode_seconds,
                "torrents_per_second": len(corpus) / decode_seconds,
                "files_per_second": decoded_files / decode_seconds,
            },
            "limitations": [
                "synthetic prefix-friendly retained paths, not a measured production corpus",
                "tmpfs excludes physical disk and long-run WAL/checkpoint/TOAST churn",
                "pgbench measures PostgreSQL retrieval; .NET API allocation and JSON serialization are excluded",
                "single short process with ABBA order is directional, not a storage-host sizing run",
            ],
        }
        canonical = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        result["result_sha256_without_self"] = hashlib.sha256(canonical.encode()).hexdigest()
        output = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(output + "\n", encoding="utf-8")
        print(output)
        return 0
    finally:
        pg.stop()


if __name__ == "__main__":
    raise SystemExit(main())
