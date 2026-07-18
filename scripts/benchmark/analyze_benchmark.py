#!/usr/bin/env python3
"""Reduce one crawler benchmark into a machine-readable scientific record."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from pathlib import Path

RUNTIME = re.compile(r"runtime 30s:\s+(.*)$")


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def parse_runtime(path: Path, first_line: int) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for number, line in enumerate(handle, start=1):
            if number <= first_line:
                continue
            match = RUNTIME.search(line)
            if not match:
                continue
            row: dict[str, float] = {}
            for token in match.group(1).split():
                if "=" not in token:
                    continue
                key, value = token.split("=", 1)
                if value in {"true", "false"}:
                    row[key] = 1.0 if value == "true" else 0.0
                    continue
                try:
                    row[key] = float(value)
                except ValueError:
                    pass
            rows.append(row)
    return rows


def parse_host_metrics(path: Path, warmup: float, total: float) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            for raw in csv.DictReader(handle):
                try:
                    elapsed = float(raw["elapsed_s"])
                except (KeyError, TypeError, ValueError):
                    continue
                if elapsed < warmup or elapsed > total + 60:
                    continue
                row: dict[str, float] = {"elapsed_s": elapsed}
                for key in ("cpu_pct", "rss_kb", "threads", "rx_bytes", "tx_bytes", "oracle_unique"):
                    try:
                        row[key] = float(raw.get(key, ""))
                    except (TypeError, ValueError):
                        row[key] = math.nan
                rows.append(row)
    except OSError:
        pass
    return rows


def total(rows: list[dict[str, float]], key: str) -> int:
    return int(sum(row.get(key, 0) for row in rows))


def finite(values):
    return [value for value in values if isinstance(value, (int, float)) and math.isfinite(value)]


def mean(values) -> float | None:
    values = finite(values)
    return statistics.fmean(values) if values else None


def maximum(values) -> float | None:
    values = finite(values)
    return max(values) if values else None


def slope(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 3:
        return None
    xbar = statistics.fmean(x for x, _ in points)
    ybar = statistics.fmean(y for _, y in points)
    denominator = sum((x - xbar) ** 2 for x, _ in points)
    if denominator == 0:
        return None
    return sum((x - xbar) * (y - ybar) for x, y in points) / denominator


def oracle_rate_slope(metrics: list[dict[str, float]], warmup: float) -> float | None:
    points: list[tuple[float, float]] = []
    for previous, current in zip(metrics, metrics[1:]):
        dt = current["elapsed_s"] - previous["elapsed_s"]
        a = previous.get("oracle_unique", math.nan)
        b = current.get("oracle_unique", math.nan)
        if dt <= 0 or not math.isfinite(a) or not math.isfinite(b) or b < a:
            continue
        uptime_hours = ((previous["elapsed_s"] + current["elapsed_s"]) / 2 - warmup) / 3600
        unique_per_hour = (b - a) * 3600 / dt
        points.append((uptime_hours, unique_per_hour))
    # Unit: change in unique/hour for each additional uptime hour.
    return slope(points)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--from-line", required=True, type=int)
    parser.add_argument("--host-metrics", required=True, type=Path)
    parser.add_argument("--sink-before", required=True, type=Path)
    parser.add_argument("--sink-after", required=True, type=Path)
    parser.add_argument("--warmup-seconds", required=True, type=float)
    parser.add_argument("--measure-seconds", required=True, type=float)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    runtime_rows = parse_runtime(args.log, args.from_line)
    metrics = parse_host_metrics(
        args.host_metrics,
        args.warmup_seconds,
        args.warmup_seconds + args.measure_seconds,
    )
    before = read_json(args.sink_before)
    after = read_json(args.sink_after)
    unique_delta = max(0, int(after.get("metadata_unique", 0)) - int(before.get("metadata_unique", 0)))
    duplicate_delta = max(
        0,
        int(after.get("metadata_duplicates", 0)) - int(before.get("metadata_duplicates", 0)),
    )

    local_windows = [row.get("meta_sent", row.get("wire_ok", 0)) for row in runtime_rows]
    nodes = [row.get("nodes", math.nan) for row in runtime_rows]
    result = {
        "schema_version": 1,
        "run_id": args.run_id,
        "measurement_seconds": args.measure_seconds,
        "runtime_windows": len(runtime_rows),
        "primary": {
            "global_unique_metadata": unique_delta,
            "global_unique_per_second": unique_delta / args.measure_seconds,
            "global_unique_per_hour": unique_delta * 3600 / args.measure_seconds,
            "oracle_duplicate_metadata": duplicate_delta,
            "decay_slope_unique_per_hour_per_uptime_hour": oracle_rate_slope(metrics, args.warmup_seconds),
        },
        "local_funnel": {
            "metadata_sent": total(runtime_rows, "meta_sent"),
            "wire_download_ok": total(runtime_rows, "wire_ok"),
            "wire_queue_dropped": total(runtime_rows, "wire_q_drop"),
            "wire_dial_attempts": total(runtime_rows, "wire_dial"),
            "wire_handshake_ok": total(runtime_rows, "wire_hs"),
            "metadata_per_second": total(runtime_rows, "meta_sent") / args.measure_seconds,
            "first_window_metadata": local_windows[0] if local_windows else None,
            "last_window_metadata": local_windows[-1] if local_windows else None,
            "peak_window_metadata": max(local_windows) if local_windows else None,
            "median_window_metadata": statistics.median(local_windows) if local_windows else None,
        },
        "discovery": {
            "dht_packets_received": total(runtime_rows, "dht_recv"),
            "dht_packets_dropped": total(runtime_rows, "dropped"),
            "active_lookup_dropped": total(runtime_rows, "lookup_drop"),
            "active_lookup_sent": total(runtime_rows, "lookup_sent"),
            "followups_sent": total(runtime_rows, "follow_sent"),
            "refresh_queries": total(runtime_rows, "refresh_q"),
            "nodes_inserted": total(runtime_rows, "node_add"),
            "nodes_removed": total(runtime_rows, "node_rm"),
            "routing_nodes_mean": mean(nodes),
            "routing_nodes_max": maximum(nodes),
        },
        "resources": {
            "cpu_pct_mean": mean(row.get("cpu_pct") for row in metrics),
            "rss_mb_mean": (mean(row.get("rss_kb") for row in metrics) or 0) / 1024,
            "rss_mb_max": (maximum(row.get("rss_kb") for row in metrics) or 0) / 1024,
            "threads_max": maximum(row.get("threads") for row in metrics),
        },
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "RESULT"
        f" run_id={args.run_id}"
        f" global_unique={unique_delta}"
        f" global_unique_s={unique_delta / args.measure_seconds:.3f}"
        f" local_meta_s={result['local_funnel']['metadata_per_second']:.3f}"
        f" rss_max_mb={result['resources']['rss_mb_max']:.1f}"
        f" windows={len(runtime_rows)}"
    )


if __name__ == "__main__":
    main()
