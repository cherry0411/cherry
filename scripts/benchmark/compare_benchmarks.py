#!/usr/bin/env python3
"""Compare sequential A/B crawler benchmark records without extra packages."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load_records(path: Path) -> list[dict]:
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "manifest" in record and "result" in record:
            records.append(record)
    return records


def metric(record: dict, section: str, name: str):
    return record.get("result", {}).get(section, {}).get(name)


def summarize(records: list[dict]) -> dict:
    rates = [metric(r, "primary", "global_unique_per_second") for r in records]
    rates = [float(v) for v in rates if isinstance(v, (int, float))]
    rss = [metric(r, "resources", "rss_mb_max") for r in records]
    rss = [float(v) for v in rss if isinstance(v, (int, float))]
    cpu = [metric(r, "resources", "cpu_pct_mean") for r in records]
    cpu = [float(v) for v in cpu if isinstance(v, (int, float))]
    return {
        "runs": len(records),
        "global_unique_per_second_mean": statistics.fmean(rates) if rates else None,
        "global_unique_per_second_median": statistics.median(rates) if rates else None,
        "global_unique_per_second_min": min(rates) if rates else None,
        "global_unique_per_second_max": max(rates) if rates else None,
        "rss_mb_max_mean": statistics.fmean(rss) if rss else None,
        "cpu_pct_mean": statistics.fmean(cpu) if cpu else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", required=True, type=Path)
    parser.add_argument("--label-contains", default="")
    parser.add_argument("--warm-only", action="store_true")
    parser.add_argument("--arm-a", default="A")
    parser.add_argument("--arm-b", default="B")
    args = parser.parse_args()

    records = load_records(args.index)
    if args.label_contains:
        records = [r for r in records if args.label_contains in r.get("manifest", {}).get("label", "")]
    if args.warm_only:
        records = [r for r in records if int(r.get("manifest", {}).get("node_ids_before", 0)) > 0]
    records.sort(key=lambda r: r.get("result", {}).get("run_id", ""))

    groups: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        groups[record.get("manifest", {}).get("variant", "unknown")].append(record)

    pairs = []
    last_a = None
    for record in records:
        arm = record.get("manifest", {}).get("variant")
        if arm == args.arm_a:
            last_a = record
        elif arm == args.arm_b and last_a is not None:
            a_rate = metric(last_a, "primary", "global_unique_per_second")
            b_rate = metric(record, "primary", "global_unique_per_second")
            if isinstance(a_rate, (int, float)) and isinstance(b_rate, (int, float)):
                pairs.append({
                    "a_run": last_a["result"]["run_id"],
                    "b_run": record["result"]["run_id"],
                    "a_rate": a_rate,
                    "b_rate": b_rate,
                    "delta_per_second": b_rate - a_rate,
                    "delta_percent": ((b_rate / a_rate) - 1) * 100 if a_rate else None,
                })
            last_a = None

    pair_deltas = [p["delta_percent"] for p in pairs if p["delta_percent"] is not None]
    output = {
        "arms": {arm: summarize(items) for arm, items in sorted(groups.items())},
        "pairs": pairs,
        "paired_delta_percent_mean": statistics.fmean(pair_deltas) if pair_deltas else None,
        "enough_pairs_for_durable_claim": len(pairs) >= 3,
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
