#!/usr/bin/env python3
"""S-003 real PostgreSQL/Meilisearch diagnostic benchmark.

The harness owns isolated Docker containers and a deterministic zero-raw
corpus.  It measures two independent mechanisms:

* A/B relevance ordering with the production document shape and query sort.
* Outbox response for batch sizes 100/500/1000 using the same claim/load/
  Meilisearch-task/ack protocol as the application.

Synthetic judgments are directional evidence only.  The script never connects
to production services and never changes the production `torrents` index.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import re
import socket
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg
import requests


HARNESS_VERSION = "s003-harness-v2"
POSTGRES_IMAGE = (
    "postgres@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
)
MEILI_IMAGE = (
    "getmeili/meilisearch:v1.45.1@sha256:ac40212f9e5a7526d8007586e3e46fb0441d29dd36c7b02fa2341d2c9a1f6493"
)
POSTGRES_PASSWORD = "cherry-s003-local-only"
POSTGRES_DATABASE = "cherry_s003"
INDEX_UID = "torrents"
FORBIDDEN_RAW_KEYS = {"raw", "rawbytes", "raw_bytes", "bencode", "pieces", "metadata_info"}

COMMON_SETTINGS: dict[str, Any] = {
    "searchableAttributes": ["name"],
    "sortableAttributes": ["createdAt", "fileCount", "peerCount", "totalLength"],
    "filterableAttributes": ["fileCount", "totalLength", "isPrivate", "peerCount"],
    "typoTolerance": {
        "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 8},
        "disableOnWords": [],
        "disableOnAttributes": [],
    },
}

RANKING_ARMS = {
    "A_current": ["sort", "createdAt:desc", "words", "exactness"],
    "B_relevance_first": ["words", "exactness", "sort", "createdAt:desc"],
}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def checksum(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def info_hash(corpus_id: str, key: str) -> str:
    return hashlib.sha1(f"{corpus_id}:{key}".encode("utf-8")).hexdigest()


def percentile(values: Iterable[float], p: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * p
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def latency_summary(values: Iterable[float]) -> dict[str, float | int | None]:
    samples = [float(value) for value in values]
    return {
        "samples": len(samples),
        "p50_ms": percentile(samples, 0.50),
        "p95_ms": percentile(samples, 0.95),
        "max_ms": max(samples) if samples else None,
    }


def run(command: list[str], *, check: bool = True) -> str:
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result.stdout.strip()


def docker(*args: str, check: bool = True) -> str:
    return run(["docker", *args], check=check)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def parse_size(value: str) -> int:
    match = re.match(r"^\s*([0-9.]+)\s*([kmgt]?i?b)\s*$", value, re.IGNORECASE)
    if not match:
        raise ValueError(f"cannot parse Docker size: {value!r}")
    number = float(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    return int(number * factors[unit])


def container_rss_bytes(name: str) -> int:
    usage = docker("stats", "--no-stream", "--format", "{{.MemUsage}}", name)
    return parse_size(usage.split("/", 1)[0].strip())


def assert_zero_raw(value: Any, path: str = "root") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).replace("-", "_").lower()
            if normalized in FORBIDDEN_RAW_KEYS:
                raise AssertionError(f"forbidden raw field {path}.{key}")
            assert_zero_raw(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            assert_zero_raw(child, f"{path}[{index}]")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raise AssertionError(f"binary value forbidden at {path}")


@dataclass(frozen=True)
class Corpus:
    corpus_id: str
    documents: list[dict[str, Any]]
    queries: list[dict[str, Any]]
    manifest: dict[str, Any]


def load_corpus(spec_path: Path) -> Corpus:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != 1:
        raise ValueError("unsupported corpus schema")
    corpus_id = str(spec["corpus_id"])
    total_documents = int(spec["total_documents"])
    rng = random.Random(int(spec["noise_seed"]))
    documents: list[dict[str, Any]] = []
    queries: list[dict[str, Any]] = []
    base_created = datetime(2020, 1, 1, tzinfo=timezone.utc)
    recent_created = datetime(2026, 7, 18, tzinfo=timezone.utc)

    def add_document(
        key: str,
        name: str,
        aliases: list[str],
        peer_count: int,
        created: datetime,
        file_count: int = 1,
    ) -> str:
        digest = info_hash(corpus_id, key)
        documents.append(
            {
                "infoHash": digest,
                "name": name,
                "aliases": list(aliases),
                "totalLength": 700_000_000 + len(documents) * 4096,
                "fileCount": file_count,
                "isPrivate": False,
                "peerCount": peer_count,
                "createdAt": int(created.timestamp() * 1000),
            }
        )
        return digest

    for group_index, group in enumerate(spec["groups"]):
        group_id = str(group["id"])
        target_hash = add_document(
            f"{group_id}:target",
            str(group["target_name"]),
            list(group.get("aliases", [])),
            peer_count=1,
            created=base_created + timedelta(days=group_index),
            file_count=max(1, len(group.get("aliases", []))),
        )
        judgments = {target_hash: 3}
        secondary_name = group.get("secondary_name")
        if secondary_name:
            secondary_hash = add_document(
                f"{group_id}:secondary",
                str(secondary_name),
                [],
                peer_count=2,
                created=base_created + timedelta(days=100 + group_index),
            )
            judgments[secondary_hash] = 2

        distractor_count = int(group.get("distractor_count", 0))
        template = group.get("distractor_template")
        for index in range(distractor_count):
            add_document(
                f"{group_id}:distractor:{index}",
                str(template).format(n=index + 1),
                [],
                peer_count=1_000_000 - group_index * 1000 - index,
                created=recent_created - timedelta(minutes=index),
            )

        alias_only = "alias" in str(group["class"]) or "filename" in str(group["class"])
        queries.append(
            {
                "id": group_id,
                "class": str(group["class"]),
                "query": str(group["query"]),
                "matchingStrategy": str(group["matching_strategy"]),
                "judgments": judgments,
                "aliasOnly": alias_only,
                "modelRecallCeiling": 0.0 if alias_only else 1.0,
            }
        )

    noise_adjectives = [
        "Amber", "Quiet", "Marble", "Silver", "Cedar", "Winter", "Copper", "Velvet"
    ]
    noise_nouns = [
        "Archive", "Snapshot", "Collection", "Dataset", "Bundle", "Notebook", "Package", "Backup"
    ]
    while len(documents) < total_documents:
        index = len(documents)
        name = (
            f"{rng.choice(noise_adjectives)} {rng.choice(noise_nouns)} "
            f"{rng.randrange(100000, 999999)}"
        )
        aliases = [f"asset-{index:05d}-{rng.randrange(1000, 9999)}.bin"]
        add_document(
            f"noise:{index}",
            name,
            aliases,
            peer_count=rng.randrange(0, 50_000),
            created=base_created + timedelta(minutes=index),
        )

    if len(documents) != total_documents:
        raise AssertionError("corpus generator exceeded total_documents")
    if len({document["infoHash"] for document in documents}) != len(documents):
        raise AssertionError("corpus contains duplicate info hashes")

    assert_zero_raw({"documents": documents, "queries": queries})
    corpus_sha = checksum({"documents": documents, "queries": queries})
    spec_sha = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": 1,
        "corpus_id": corpus_id,
        "corpus_sha256": corpus_sha,
        "spec_sha256": spec_sha,
        "document_count": len(documents),
        "query_count": len(queries),
        "alias_only_query_count": sum(bool(query["aliasOnly"]) for query in queries),
        "permanent_raw_retention_percent": 0,
    }
    return Corpus(corpus_id, documents, queries, manifest)


def search_documents(corpus: Corpus) -> list[dict[str, Any]]:
    allowed = {
        "infoHash", "name", "totalLength", "fileCount", "isPrivate", "peerCount", "createdAt"
    }
    documents = [{key: value for key, value in document.items() if key in allowed} for document in corpus.documents]
    assert all("aliases" not in document for document in documents)
    assert_zero_raw(documents)
    return documents


class Meili:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(
            method,
            self.base_url + path,
            timeout=60,
            **kwargs,
        )
        if not response.ok:
            raise RuntimeError(
                f"Meilisearch {method} {path} returned {response.status_code}: {response.text[:2000]}"
            )
        return response

    def json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        return self.request(method, path, **kwargs).json()

    def wait_task(self, task_uid: int, timeout_seconds: float = 180.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            task = self.json("GET", f"/tasks/{task_uid}")
            status = task.get("status")
            if status == "succeeded":
                return task
            if status in {"failed", "canceled"}:
                raise RuntimeError(f"Meilisearch task {task_uid} {status}: {task.get('error')}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Meilisearch task {task_uid} timed out")
            time.sleep(0.05)

    def task_request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        accepted = self.json(method, path, **kwargs)
        uid = accepted.get("taskUid")
        if not isinstance(uid, int):
            raise RuntimeError(f"Meilisearch response omitted numeric taskUid: {accepted}")
        return self.wait_task(uid)

    def create_index(self, settings: dict[str, Any]) -> None:
        self.task_request(
            "POST", "/indexes", json={"uid": INDEX_UID, "primaryKey": "infoHash"}
        )
        self.task_request("PATCH", f"/indexes/{INDEX_UID}/settings", json=settings)

    def submit_documents(self, documents: list[dict[str, Any]]) -> dict[str, Any]:
        return self.task_request("POST", f"/indexes/{INDEX_UID}/documents", json=documents)

    def search(self, query: dict[str, Any]) -> tuple[list[str], float]:
        body = {
            "q": query["query"],
            "offset": 0,
            "limit": 20,
            "sort": ["peerCount:desc"],
            "attributesToRetrieve": ["infoHash"],
            "matchingStrategy": query["matchingStrategy"],
        }
        started = time.perf_counter()
        result = self.json("POST", f"/indexes/{INDEX_UID}/search", json=body)
        elapsed_ms = (time.perf_counter() - started) * 1000
        return [str(hit["infoHash"]) for hit in result.get("hits", [])], elapsed_ms


@dataclass
class ContainerRegistry:
    keep: bool
    names: list[str]

    def add(self, name: str) -> None:
        self.names.append(name)

    def stop(self, name: str) -> None:
        docker("stop", "-t", "5", name, check=False)
        if not self.keep:
            docker("rm", "-f", name, check=False)

    def cleanup(self) -> None:
        for name in reversed(self.names):
            self.stop(name)


def wait_meili(base_url: str, timeout_seconds: float = 60.0) -> Meili:
    client = Meili(base_url)
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            if client.json("GET", "/health").get("status") == "available":
                return client
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Meilisearch did not become healthy at {base_url}")
        time.sleep(0.2)


def start_meili(registry: ContainerRegistry, run_id: str, label: str) -> tuple[str, str, Meili]:
    name = f"cherry-s003-meili-{run_id}-{label}".lower().replace("_", "-")
    port = free_port()
    container_id = docker(
        "run", "-d", "--name", name,
        "--cpus", "2", "--memory", "1g",
        "--tmpfs", "/meili_data:rw,size=1073741824",
        "-e", "MEILI_ENV=development",
        "-e", "MEILI_NO_ANALYTICS=true",
        "-p", f"127.0.0.1:{port}:7700",
        MEILI_IMAGE,
    )
    registry.add(name)
    client = wait_meili(f"http://127.0.0.1:{port}")
    return name, container_id, client


def start_postgres(
    registry: ContainerRegistry, run_id: str, label: str = ""
) -> tuple[str, str, int, str]:
    suffix = f"-{label}" if label else ""
    name = f"cherry-s003-pg-{run_id}{suffix}".lower().replace("_", "-")
    port = free_port()
    container_id = docker(
        "run", "-d", "--name", name,
        "--cpus", "2", "--memory", "1g",
        "--tmpfs", "/var/lib/postgresql/data:rw,size=1073741824",
        "-e", f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
        "-e", f"POSTGRES_DB={POSTGRES_DATABASE}",
        "-p", f"127.0.0.1:{port}:5432",
        POSTGRES_IMAGE,
    )
    registry.add(name)
    dsn = (
        f"host=127.0.0.1 port={port} dbname={POSTGRES_DATABASE} "
        f"user=postgres password={POSTGRES_PASSWORD}"
    )
    deadline = time.monotonic() + 60
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=1) as connection:
                connection.execute("SELECT 1")
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise TimeoutError("PostgreSQL did not become ready")
            time.sleep(0.2)
    return name, container_id, port, dsn


def prepare_postgres(dsn: str, corpus: Corpus) -> dict[str, Any]:
    started = time.perf_counter()
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS s003 CASCADE")
            cursor.execute("CREATE SCHEMA s003")
            cursor.execute(
                """
                CREATE TABLE s003.torrents (
                    info_hash varchar(40) PRIMARY KEY,
                    name text NOT NULL,
                    total_length bigint NOT NULL,
                    file_count integer NOT NULL,
                    is_private boolean NOT NULL,
                    peer_count integer NOT NULL,
                    created_at timestamptz NOT NULL)
                """
            )
            cursor.execute(
                """
                CREATE TABLE s003.aliases (
                    info_hash varchar(40) NOT NULL REFERENCES s003.torrents(info_hash),
                    path_text text NOT NULL)
                """
            )
            cursor.execute("CREATE INDEX aliases_info_hash_idx ON s003.aliases(info_hash)")
            cursor.execute(
                """
                CREATE TABLE s003.search_outbox (
                    info_hash varchar(40) PRIMARY KEY REFERENCES s003.torrents(info_hash),
                    generation bigint NOT NULL DEFAULT 1,
                    enqueued_at timestamptz NOT NULL,
                    available_at timestamptz NOT NULL,
                    lease_owner uuid NULL,
                    lease_until timestamptz NULL,
                    attempt_count integer NOT NULL DEFAULT 0,
                    last_error varchar(1024) NULL,
                    updated_at timestamptz NOT NULL)
                """
            )
            cursor.execute(
                "CREATE INDEX outbox_available_idx ON s003.search_outbox(available_at, info_hash)"
            )
            with cursor.copy(
                "COPY s003.torrents "
                "(info_hash,name,total_length,file_count,is_private,peer_count,created_at) FROM STDIN"
            ) as copy:
                for document in corpus.documents:
                    copy.write_row(
                        (
                            document["infoHash"],
                            document["name"],
                            document["totalLength"],
                            document["fileCount"],
                            document["isPrivate"],
                            document["peerCount"],
                            datetime.fromtimestamp(document["createdAt"] / 1000, tz=timezone.utc),
                        )
                    )
            with cursor.copy("COPY s003.aliases (info_hash,path_text) FROM STDIN") as copy:
                for document in corpus.documents:
                    for alias in document["aliases"]:
                        copy.write_row((document["infoHash"], alias))
            cursor.execute("ANALYZE s003.torrents")
            cursor.execute("ANALYZE s003.aliases")
        connection.commit()
        version = connection.execute("SHOW server_version").fetchone()[0]
        torrent_bytes, alias_bytes = connection.execute(
            """
            SELECT pg_total_relation_size('s003.torrents'),
                   pg_total_relation_size('s003.aliases')
            """
        ).fetchone()
        raw_columns = connection.execute(
            """
            SELECT COUNT(*)
              FROM information_schema.columns
             WHERE table_schema='s003'
               AND lower(column_name) IN ('raw','raw_bytes','bencode','pieces','metadata_info')
            """
        ).fetchone()[0]
    if raw_columns != 0:
        raise AssertionError("benchmark schema unexpectedly contains raw-byte columns")
    return {
        "server_version": version,
        "seed_seconds": time.perf_counter() - started,
        "torrent_relation_bytes": torrent_bytes,
        "alias_relation_bytes": alias_bytes,
        "raw_columns": raw_columns,
    }


def audit_production_schema(dsn: str, port: int) -> dict[str, Any]:
    """Apply the repository's real EF migrations and return a stable schema audit."""
    with psycopg.connect(dsn) as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        connection.commit()

    repo_root = Path(__file__).resolve().parents[2]
    infrastructure = repo_root / "backend" / "src" / "Cherry.Infrastructure"
    startup = repo_root / "backend" / "src" / "Cherry.Api"
    connection_string = (
        f"Host=127.0.0.1;Port={port};Database={POSTGRES_DATABASE};"
        f"Username=postgres;Password={POSTGRES_PASSWORD}"
    )
    started = time.perf_counter()
    run(
        [
            "dotnet", "ef", "database", "update",
            "--project", str(infrastructure),
            "--startup-project", str(startup),
            "--connection", connection_string,
        ]
    )
    migration_seconds = time.perf_counter() - started

    with psycopg.connect(dsn) as connection:
        migrations = [
            str(row[0])
            for row in connection.execute(
                'SELECT "MigrationId" FROM "__EFMigrationsHistory" ORDER BY "MigrationId"'
            ).fetchall()
        ]
        columns = [
            {
                "table": str(row[0]),
                "column": str(row[1]),
                "type": str(row[2]),
                "nullable": str(row[3]) == "YES",
            }
            for row in connection.execute(
                """
                SELECT table_name,column_name,
                       CASE WHEN character_maximum_length IS NULL THEN data_type
                            ELSE data_type || '(' || character_maximum_length || ')' END,
                       is_nullable
                  FROM information_schema.columns
                 WHERE table_schema='public'
                   AND table_name <> '__EFMigrationsHistory'
                 ORDER BY table_name,ordinal_position
                """
            ).fetchall()
        ]
        indexes = [
            {"table": str(row[0]), "index": str(row[1]), "definition": str(row[2])}
            for row in connection.execute(
                """
                SELECT tablename,indexname,indexdef
                  FROM pg_indexes
                 WHERE schemaname='public'
                 ORDER BY tablename,indexname
                """
            ).fetchall()
        ]
        relation_bytes = {
            str(row[0]): int(row[1])
            for row in connection.execute(
                """
                SELECT c.relname,pg_total_relation_size(c.oid)
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE n.nspname='public' AND c.relkind='r'
                 ORDER BY c.relname
                """
            ).fetchall()
        }
    schema = {"migrations": migrations, "columns": columns, "indexes": indexes}
    assert_zero_raw(schema)
    return {
        **schema,
        "schema_sha256": checksum(schema),
        "empty_relation_bytes": relation_bytes,
        "migration_seconds": migration_seconds,
    }


def settings_for(ranking_rules: list[str]) -> dict[str, Any]:
    settings = dict(COMMON_SETTINGS)
    settings["rankingRules"] = list(ranking_rules)
    return settings


def dcg(grades: list[int]) -> float:
    return sum((2**grade - 1) / math.log2(index + 2) for index, grade in enumerate(grades))


def evaluate_query(query: dict[str, Any], hits: list[str]) -> dict[str, Any]:
    judgments = {str(key): int(value) for key, value in query["judgments"].items()}
    relevant = set(judgments)
    retrieved = [hit for hit in hits[:20] if hit in relevant]
    grades = [judgments.get(hit, 0) for hit in hits[:10]]
    ideal = sorted(judgments.values(), reverse=True)[:10]
    first_rank = next((index + 1 for index, hit in enumerate(hits) if hit in relevant), None)
    return {
        "recall_at_20": len(set(retrieved)) / len(relevant) if relevant else 1.0,
        "ndcg_at_10": dcg(grades) / dcg(ideal) if ideal else 1.0,
        "mrr": 1.0 / first_rank if first_rank else 0.0,
        "zero_result": 1 if not hits else 0,
        "first_relevant_rank": first_rank,
        "hits": hits,
    }


def aggregate_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def mean(key: str) -> float:
        return statistics.fmean(float(row[key]) for row in rows) if rows else 0.0

    return {
        "queries": len(rows),
        "recall_at_20": mean("recall_at_20"),
        "ndcg_at_10": mean("ndcg_at_10"),
        "mrr": mean("mrr"),
        "zero_result_rate": mean("zero_result"),
    }


def quality_arm(
    registry: ContainerRegistry,
    run_id: str,
    arm: str,
    corpus: Corpus,
) -> dict[str, Any]:
    container_name, container_id, client = start_meili(registry, run_id, f"quality-{arm}")
    try:
        version = client.json("GET", "/version")
        before_stats = client.json("GET", "/stats")
        settings = settings_for(RANKING_ARMS[arm])
        client.create_index(settings)
        index_task_started = time.perf_counter()
        index_task = client.submit_documents(search_documents(corpus))
        index_wall_ms = (time.perf_counter() - index_task_started) * 1000

        rows: list[dict[str, Any]] = []
        cold_latencies: list[float] = []
        for query in corpus.queries:
            hits, latency_ms = client.search(query)
            cold_latencies.append(latency_ms)
            evaluated = evaluate_query(query, hits)
            rows.append(
                {
                    "id": query["id"],
                    "class": query["class"],
                    "alias_only": query["aliasOnly"],
                    "model_recall_ceiling": query["modelRecallCeiling"],
                    **evaluated,
                }
            )

        warm_latencies: list[float] = []
        for _ in range(10):
            for query in corpus.queries:
                _, latency_ms = client.search(query)
                warm_latencies.append(latency_ms)

        classes: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            classes.setdefault(str(row["class"]), []).append(row)
        after_stats = client.json("GET", "/stats")
        index_stats = client.json("GET", f"/indexes/{INDEX_UID}/stats")
        effective_settings = client.json("GET", f"/indexes/{INDEX_UID}/settings")
        database_delta = max(
            0, int(after_stats["databaseSize"]) - int(before_stats["databaseSize"])
        )
        return {
            "arm": arm,
            "container": container_name,
            "container_id": container_id,
            "version": version,
            "ranking_rules": RANKING_ARMS[arm],
            "submitted_settings": settings,
            "settings_sha256": checksum(settings),
            "effective_settings": effective_settings,
            "effective_settings_sha256": checksum(effective_settings),
            "quality": aggregate_quality(rows),
            "quality_by_class": {
                name: aggregate_quality(class_rows) for name, class_rows in classes.items()
            },
            "per_query": rows,
            "cold_first_pass": latency_summary(cold_latencies),
            "warm": latency_summary(warm_latencies),
            "index_task_wall_ms": index_wall_ms,
            "index_task": index_task,
            "database_bytes_increment": database_delta,
            "index_bytes_per_document": database_delta / len(corpus.documents),
            "raw_document_db_bytes": int(index_stats.get("rawDocumentDbSize", 0)),
            "raw_document_bytes_per_document": (
                int(index_stats.get("rawDocumentDbSize", 0)) / len(corpus.documents)
            ),
            "document_count": int(index_stats["numberOfDocuments"]),
            "rss_bytes": container_rss_bytes(container_name),
        }
    finally:
        registry.stop(container_name)


def reset_outbox(dsn: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute("TRUNCATE s003.search_outbox")
        connection.execute(
            """
            INSERT INTO s003.search_outbox (
                info_hash,generation,enqueued_at,available_at,lease_owner,
                lease_until,attempt_count,last_error,updated_at)
            SELECT info_hash,1,NOW(),NOW(),NULL,NULL,0,NULL,NOW()
              FROM s003.torrents
            """
        )
        connection.commit()


def claim_batch(connection: psycopg.Connection[Any], batch_size: int) -> tuple[uuid.UUID, list[str]]:
    owner = uuid.uuid4()
    rows = connection.execute(
        """
        WITH candidates AS (
            SELECT info_hash
              FROM s003.search_outbox
             WHERE available_at <= NOW()
               AND (lease_until IS NULL OR lease_until <= NOW())
             ORDER BY available_at, info_hash
             FOR UPDATE SKIP LOCKED
             LIMIT %s)
        UPDATE s003.search_outbox AS item
           SET lease_owner=%s,
               lease_until=NOW() + interval '5 minutes',
               updated_at=NOW()
          FROM candidates
         WHERE item.info_hash=candidates.info_hash
        RETURNING item.info_hash
        """,
        (batch_size, owner),
    ).fetchall()
    connection.commit()
    return owner, [str(row[0]) for row in rows]


def load_batch(connection: psycopg.Connection[Any], hashes: list[str]) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT info_hash,name,total_length,file_count,is_private,peer_count,
               (extract(epoch FROM created_at) * 1000)::bigint
          FROM s003.torrents
         WHERE info_hash = ANY(%s)
        """,
        (hashes,),
    ).fetchall()
    documents = [
        {
            "infoHash": row[0],
            "name": row[1],
            "totalLength": row[2],
            "fileCount": row[3],
            "isPrivate": row[4],
            "peerCount": row[5],
            "createdAt": row[6],
        }
        for row in rows
    ]
    assert_zero_raw(documents)
    return documents


def acknowledge_batch(
    connection: psycopg.Connection[Any], owner: uuid.UUID, hashes: list[str]
) -> int:
    cursor = connection.execute(
        """
        DELETE FROM s003.search_outbox
         WHERE lease_owner=%s
           AND generation=1
           AND info_hash=ANY(%s)
        """,
        (owner, hashes),
    )
    deleted = cursor.rowcount
    connection.commit()
    return deleted


def drain_outbox(
    dsn: str,
    client: Meili,
    batch_size: int,
    max_batches: int | None = None,
) -> dict[str, Any]:
    task_rows: list[dict[str, Any]] = []
    total_documents = 0
    started = time.perf_counter()
    with psycopg.connect(dsn) as connection:
        while max_batches is None or len(task_rows) < max_batches:
            batch_started = time.perf_counter()
            owner, hashes = claim_batch(connection, batch_size)
            claim_ms = (time.perf_counter() - batch_started) * 1000
            if not hashes:
                break
            load_started = time.perf_counter()
            documents = load_batch(connection, hashes)
            load_ms = (time.perf_counter() - load_started) * 1000
            meili_started = time.perf_counter()
            task = client.submit_documents(documents)
            meili_ms = (time.perf_counter() - meili_started) * 1000
            ack_started = time.perf_counter()
            acknowledged = acknowledge_batch(connection, owner, hashes)
            ack_ms = (time.perf_counter() - ack_started) * 1000
            if acknowledged != len(hashes) or len(documents) != len(hashes):
                raise AssertionError(
                    f"batch mismatch: claim={len(hashes)} load={len(documents)} ack={acknowledged}"
                )
            total_documents += len(hashes)
            task_rows.append(
                {
                    "documents": len(hashes),
                    "claim_ms": claim_ms,
                    "load_ms": load_ms,
                    "meili_task_ms": meili_ms,
                    "ack_ms": ack_ms,
                    "pipeline_ms": (time.perf_counter() - batch_started) * 1000,
                    "task_uid": task.get("uid"),
                    "task_duration": task.get("duration"),
                }
            )
    elapsed = time.perf_counter() - started
    return {
        "documents": total_documents,
        "elapsed_seconds": elapsed,
        "documents_per_second": total_documents / elapsed if elapsed else 0,
        "tasks": task_rows,
    }


def outbox_arm(
    registry: ContainerRegistry,
    run_id: str,
    corpus: Corpus,
    batch_size: int,
) -> dict[str, Any]:
    settings = settings_for(RANKING_ARMS["A_current"])

    # Each arm owns a fresh PostgreSQL process as well as fresh Meilisearch
    # processes. Reusing one PostgreSQL process would make shared buffers,
    # allocator high-water marks, and relation churn depend on arm order,
    # violating the single-variable comparison.
    postgres_container, postgres_id, _, dsn = start_postgres(
        registry, run_id, f"batch-{batch_size}"
    )
    postgres = prepare_postgres(dsn, corpus)

    # One unreported warm-up batch on a disposable real Meili process. This
    # warms PostgreSQL pages and Python/HTTP paths without contaminating the
    # fresh measurement index or its database-size delta.
    reset_outbox(dsn)
    warm_name, _, warm_client = start_meili(registry, run_id, f"batch-{batch_size}-warm")
    try:
        warm_client.create_index(settings)
        warm = drain_outbox(dsn, warm_client, batch_size, max_batches=1)
        if warm["documents"] != batch_size:
            raise AssertionError("warm-up did not process one full batch")
    finally:
        registry.stop(warm_name)

    reset_outbox(dsn)
    name, container_id, client = start_meili(registry, run_id, f"batch-{batch_size}")
    try:
        version = client.json("GET", "/version")
        before_stats = client.json("GET", "/stats")
        client.create_index(settings)
        measured = drain_outbox(dsn, client, batch_size)
        after_stats = client.json("GET", "/stats")
        index_stats = client.json("GET", f"/indexes/{INDEX_UID}/stats")
        with psycopg.connect(dsn) as connection:
            remaining = int(
                connection.execute("SELECT COUNT(*) FROM s003.search_outbox").fetchone()[0]
            )
        if measured["documents"] != len(corpus.documents):
            raise AssertionError("outbox arm did not process the fixed corpus")
        if remaining != 0 or int(index_stats["numberOfDocuments"]) != len(corpus.documents):
            raise AssertionError("outbox/index document count mismatch")
        database_delta = max(
            0, int(after_stats["databaseSize"]) - int(before_stats["databaseSize"])
        )
        task_pipeline = [row["pipeline_ms"] for row in measured["tasks"]]
        meili_tasks = [row["meili_task_ms"] for row in measured["tasks"]]
        return {
            "batch_size": batch_size,
            "container": name,
            "container_id": container_id,
            "version": version,
            "postgres_version": postgres["server_version"],
            "postgres_container": postgres_container,
            "postgres_container_id": postgres_id,
            "settings_sha256": checksum(settings),
            "warmup_documents": warm["documents"],
            "documents": measured["documents"],
            "elapsed_seconds": measured["elapsed_seconds"],
            "documents_per_second": measured["documents_per_second"],
            "task_count": len(measured["tasks"]),
            "pipeline_task_latency": latency_summary(task_pipeline),
            "meili_task_latency": latency_summary(meili_tasks),
            "tasks": measured["tasks"],
            "final_outbox_depth": remaining,
            "indexed_documents": int(index_stats["numberOfDocuments"]),
            "database_bytes_increment": database_delta,
            "index_bytes_per_document": database_delta / len(corpus.documents),
            "raw_document_db_bytes": int(index_stats.get("rawDocumentDbSize", 0)),
            "postgres_rss_bytes": container_rss_bytes(postgres_container),
            "meili_rss_bytes": container_rss_bytes(name),
        }
    finally:
        registry.stop(name)
        registry.stop(postgres_container)


def markdown_summary(result: dict[str, Any]) -> str:
    quality = result["quality_arms"]
    lines = [
        "# S-003 local diagnostic result",
        "",
        f"- Run: `{result['run_id']}`",
        f"- Corpus: `{result['corpus']['corpus_id']}` / `{result['corpus']['corpus_sha256']}`",
        f"- Harness: `{result['harness_version']}` / `{result['harness_sha256']}`",
        f"- Documents / queries: {result['corpus']['document_count']} / {result['corpus']['query_count']}",
        f"- Production-schema checksum: `{result['production_schema_audit']['schema_sha256']}`",
        "- Permanent raw retention: 0%",
        "",
        "## Ranking",
        "",
        "| Arm | Recall@20 | nDCG@10 | MRR | Zero-result | Warm p50 ms | Warm p95 ms | bytes/doc | RSS MiB |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm in ("A_current", "B_relevance_first"):
        row = quality[arm]
        metrics = row["quality"]
        lines.append(
            f"| {arm} | {metrics['recall_at_20']:.4f} | {metrics['ndcg_at_10']:.4f} | "
            f"{metrics['mrr']:.4f} | {metrics['zero_result_rate']:.4f} | "
            f"{row['warm']['p50_ms']:.3f} | {row['warm']['p95_ms']:.3f} | "
            f"{row['index_bytes_per_document']:.1f} | {row['rss_bytes']/1048576:.1f} |"
        )

    lines.extend(
        [
            "",
            "Alias-only queries are included above. The production name-only document omits the",
            "PostgreSQL alias/file rows, so their model-level Recall@20 ceiling is 0.",
            "",
            "## Outbox batch screen",
            "",
            "| Batch | docs/s | tasks | pipeline p50 ms | pipeline p95 ms | Meili p95 ms | bytes/doc | PG RSS MiB | Meili RSS MiB |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["outbox_arms"]:
        lines.append(
            f"| {row['batch_size']} | {row['documents_per_second']:.1f} | {row['task_count']} | "
            f"{row['pipeline_task_latency']['p50_ms']:.2f} | "
            f"{row['pipeline_task_latency']['p95_ms']:.2f} | "
            f"{row['meili_task_latency']['p95_ms']:.2f} | "
            f"{row['index_bytes_per_document']:.1f} | "
            f"{row['postgres_rss_bytes']/1048576:.1f} | {row['meili_rss_bytes']/1048576:.1f} |"
        )
    lines.extend(
        [
            "",
            "Synthetic measurements are directional diagnostics, not production settings or",
            "storage-server sizing claims.",
            "The outbox driver executes the production-equivalent PostgreSQL claim/load/ack and",
            "real Meilisearch task protocol, but does not include .NET/EF/host-network overhead.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path(__file__).with_name("storage_search_s003_corpus.json"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "cherry-picker" / ".bench" / "storage-search",
    )
    parser.add_argument(
        "--keep-containers",
        action="store_true",
        help="stop but retain exact benchmark containers for controller inspection",
    )
    parser.add_argument(
        "--batch-sizes",
        default="100,500,1000",
        help="comma-separated isolated batch sizes",
    )
    args = parser.parse_args()

    batch_sizes = [int(value) for value in args.batch_sizes.split(",")]
    if batch_sizes != [100, 500, 1000]:
        raise ValueError("S-003 pre-registration fixes batch sizes to 100,500,1000")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output_root / f"s003-{run_id}"
    output.mkdir(parents=True, exist_ok=False)
    corpus = load_corpus(args.corpus)
    (output / "corpus-manifest.json").write_text(
        json.dumps(corpus.manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    registry = ContainerRegistry(args.keep_containers, [])
    postgres_name = ""
    try:
        postgres_name, postgres_id, postgres_port, dsn = start_postgres(registry, run_id.lower())
        postgres = prepare_postgres(dsn, corpus)
        production_schema = audit_production_schema(dsn, postgres_port)
        quality: dict[str, Any] = {}
        for arm in ("A_current", "B_relevance_first"):
            quality[arm] = quality_arm(registry, run_id.lower(), arm, corpus)
        outbox = [
            outbox_arm(
                registry, run_id.lower(), corpus, batch_size
            )
            for batch_size in batch_sizes
        ]
        result = {
            "schema_version": 1,
            "harness_version": HARNESS_VERSION,
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "corpus": corpus.manifest,
            "environment": {
                "platform": platform.platform(),
                "python": sys.version,
                "logical_cpu_count": os.cpu_count(),
                "docker_server_version": docker("version", "--format", "{{.Server.Version}}"),
                "postgres_image": POSTGRES_IMAGE,
                "meili_image": MEILI_IMAGE,
                "postgres_container": postgres_name,
                "postgres_container_id": postgres_id,
                "postgres_host_port": postgres_port,
                "container_cpu_limit": 2,
                "container_memory_limit_bytes": 1024**3,
                "reproduction_command": "python scripts/benchmark/storage_search_s003.py",
            },
            "postgres": postgres,
            "production_schema_audit": production_schema,
            "quality_arms": quality,
            "outbox_arms": outbox,
            "limitations": [
                "Synthetic judgments diagnose direction and cannot authorize production ranking.",
                "Cold latency is a first-query-pass proxy on a fresh index, not a controlled host OS page-cache flush.",
                "The outbox driver mirrors PostgreSQL SQL and real Meilisearch tasks but excludes .NET/EF and production network overhead.",
                "The 10k-document outbox screen is too short for long-run compaction or allocator sizing.",
            ],
            "retention": {"permanent_raw_percent": 0},
            "container_names": registry.names,
        }
        assert_zero_raw(result)
        (output / "result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (output / "summary.md").write_text(markdown_summary(result), encoding="utf-8")
        print(output)
        return 0
    except Exception as exception:
        (output / "failure.txt").write_text(str(exception) + "\n", encoding="utf-8")
        raise
    finally:
        registry.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
