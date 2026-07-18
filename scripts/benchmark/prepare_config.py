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
    set_path(config, "exporter.http_endpoint", args.sink_url)

    for item in args.set:
        if "=" not in item:
            parser.error(f"--set requires PATH=VALUE, got {item!r}")
        path, raw = item.split("=", 1)
        set_path(config, path, parse_value(raw))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
