#!/usr/bin/env python3
"""Create an immutable, run-specific crawler config from a tracked template."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def set_path(config: dict, path: str, value) -> None:
    parts = path.split(".")
    current = config
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child
    current[parts[-1]] = value


def route_benchmark_sink(config: dict, sink_url: str) -> str:
    """Route only the scientific oracle when durable production is enabled.

    Legacy benchmark configs have no spool_dir and keep their historical
    single-endpoint behavior. A durable config preserves http_endpoint as the
    production storage destination and points oracle_endpoint at the frozen
    run-local sink.
    """
    exporter = config.setdefault("exporter", {})
    if str(exporter.get("spool_dir", "")).strip():
        if not str(exporter.get("http_endpoint", "")).strip():
            raise ValueError("durable benchmark config requires production exporter.http_endpoint")
        exporter["oracle_endpoint"] = sink_url
        return "dual"
    exporter["http_endpoint"] = sink_url
    return "legacy"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-id-dir", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--sink-url", required=True)
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="PATH=JSON_VALUE",
        help="override any JSON field, for example discovery.lookup_rate=350",
    )
    args = parser.parse_args()

    config = json.loads(args.input.read_text(encoding="utf-8"))
    config["instance_id"] = args.run_id
    config["listen_addr"] = f":{args.port}"
    set_path(config, "discovery.node_id_dir", args.node_id_dir)
    set_path(config, "exporter.kind", "http")

    for item in args.set:
        if "=" not in item:
            parser.error(f"--set requires PATH=VALUE, got {item!r}")
        path, raw = item.split("=", 1)
        set_path(config, path, parse_value(raw))

    try:
        route_benchmark_sink(config, args.sink_url)
    except ValueError as error:
        parser.error(str(error))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
