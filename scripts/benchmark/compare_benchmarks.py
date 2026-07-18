#!/usr/bin/env python3
"""Compare sequential crawler experiments with order and health controls."""

from __future__ import annotations

import argparse
import json
import math
import random
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


def number(value) -> float | None:
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return None


def divide(numerator, denominator) -> float | None:
    numerator, denominator = number(numerator), number(denominator)
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def rate(record: dict) -> float | None:
    return number(metric(record, "primary", "global_unique_per_second"))


def run_id(record: dict) -> str:
    return str(record.get("result", {}).get("run_id", ""))


def variant(record: dict) -> str:
    return str(record.get("manifest", {}).get("variant", "unknown"))


def mechanical_efficiency(record: dict) -> dict[str, float | None]:
    result = record.get("result", {})
    local = result.get("local_funnel", {})
    discovery = result.get("discovery", {})
    resources = result.get("resources", {})
    sent = discovery.get("active_lookup_sent")
    dropped = discovery.get("active_lookup_dropped")
    lookup_total = None
    if number(sent) is not None and number(dropped) is not None:
        lookup_total = float(sent) + float(dropped)
    return {
        "wire_ok_per_dial": divide(local.get("wire_download_ok"), local.get("wire_dial_attempts")),
        "metadata_per_dht_packet": divide(local.get("metadata_sent"), discovery.get("dht_packets_received")),
        "lookup_admission_rate": divide(sent, lookup_total),
        "metadata_per_cpu_pct": divide(local.get("metadata_per_second"), resources.get("cpu_pct_mean")),
        "refresh_queries_per_metadata": divide(discovery.get("refresh_queries"), local.get("metadata_sent")),
    }


def summarize(records: list[dict]) -> dict:
    rates = [value for record in records if (value := rate(record)) is not None]
    rss = [value for record in records if (value := number(metric(record, "resources", "rss_mb_max"))) is not None]
    cpu = [value for record in records if (value := number(metric(record, "resources", "cpu_pct_mean"))) is not None]
    return {
        "runs": len(records),
        "global_unique_per_second_mean": statistics.fmean(rates) if rates else None,
        "global_unique_per_second_median": statistics.median(rates) if rates else None,
        "global_unique_per_second_min": min(rates) if rates else None,
        "global_unique_per_second_max": max(rates) if rates else None,
        "rss_mb_max_mean": statistics.fmean(rss) if rss else None,
        "cpu_pct_mean": statistics.fmean(cpu) if cpu else None,
    }


def validate_run(
    record: dict, rss_limit_mb: float, max_udp_drops: int, require_warm: bool = False
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    manifest = record.get("manifest", {})
    result = record.get("result", {})
    measure = number(manifest.get("measure_seconds")) or number(result.get("measurement_seconds"))
    windows = number(result.get("runtime_windows"))
    if measure is None or windows is None:
        errors.append("missing measurement duration/runtime windows")
    else:
        expected = max(1, round(measure / 30))
        if windows < expected * 0.9:
            errors.append(f"runtime window coverage {windows / expected:.1%} < 90%")

    if rate(record) is None:
        errors.append("missing primary global unique rate")
    if require_warm and int(manifest.get("node_ids_before", 0)) <= 0:
        errors.append("cold identity excluded by --warm-only")
    rss = number(metric(record, "resources", "rss_mb_max"))
    if rss is None:
        warnings.append("missing RSS peak")
    elif rss > rss_limit_mb:
        errors.append(f"RSS {rss:.1f} MiB > {rss_limit_mb:.1f} MiB guardrail")

    for name in ("udp_rcvbuf_errors", "udp_sndbuf_errors"):
        value = number(metric(record, "resources", name))
        if value is None:
            warnings.append(f"missing {name}")
        elif value > max_udp_drops:
            errors.append(f"{name}={int(value)} > {max_udp_drops}")

    missing = number(metric(record, "health", "oracle_sample_missing_rate"))
    if missing is not None and missing > 0.1:
        errors.append(f"oracle monitor missing rate {missing:.1%} > 10%")
    return errors, warnings


COMPATIBILITY_FIELDS = ("cohort", "mode", "warmup_seconds", "measure_seconds", "port")


def validate_pair(first: dict, second: dict) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    left, right = first.get("manifest", {}), second.get("manifest", {})
    for field in COMPATIBILITY_FIELDS:
        if str(left.get(field, "")) != str(right.get(field, "")):
            errors.append(f"incompatible {field}: {left.get(field)!r} != {right.get(field)!r}")
    left_oracle_mode = str(left.get("oracle_mode", "shared"))
    right_oracle_mode = str(right.get("oracle_mode", "shared"))
    if left_oracle_mode != right_oracle_mode:
        errors.append(f"incompatible oracle_mode: {left_oracle_mode!r} != {right_oracle_mode!r}")
    elif left_oracle_mode == "isolated":
        left_baseline = str(left.get("oracle_baseline_sha", ""))
        right_baseline = str(right.get("oracle_baseline_sha", ""))
        if not left_baseline or not right_baseline:
            errors.append("isolated pair lacks oracle_baseline_sha")
        elif left_baseline != right_baseline:
            errors.append("isolated pair used different oracle baselines")
        left_overlay = str(left.get("oracle_overlay", ""))
        right_overlay = str(right.get("oracle_overlay", ""))
        if not left_overlay or not right_overlay:
            errors.append("isolated pair lacks writable overlay paths")
        elif left_overlay == right_overlay:
            errors.append("isolated pair reused the same writable overlay")
    a, b = left.get("template_config_sha"), right.get("template_config_sha")
    if not a or not b:
        warnings.append("legacy manifest lacks template_config_sha")
    elif a != b:
        if variant(first) == variant(second):
            errors.append("same-arm control changed template_config_sha")
        else:
            warnings.append("template_config_sha differs across arms (must be the intended treatment)")
    return errors, warnings


def pair_delta(first: dict, second: dict, arm_a: str, arm_b: str) -> dict | None:
    first_arm, second_arm = variant(first), variant(second)
    first_rate, second_rate = rate(first), rate(second)
    if first_rate is None or second_rate is None:
        return None

    if {first_arm, second_arm} == {arm_a, arm_b}:
        a_record = first if first_arm == arm_a else second
        b_record = first if first_arm == arm_b else second
        a_rate, b_rate = rate(a_record), rate(b_record)
        assert a_rate is not None and b_rate is not None
        efficiency_a = mechanical_efficiency(a_record)
        efficiency_b = mechanical_efficiency(b_record)
        efficiency_delta = {
            key: (efficiency_b[key] - efficiency_a[key])
            if efficiency_a[key] is not None and efficiency_b[key] is not None else None
            for key in efficiency_a
        }
        return {
            "kind": "treatment",
            "order": f"{first_arm}->{second_arm}",
            "first_run": run_id(first),
            "second_run": run_id(second),
            "a_run": run_id(a_record),
            "b_run": run_id(b_record),
            "a_rate": a_rate,
            "b_rate": b_rate,
            "delta_per_second": b_rate - a_rate,
            "delta_percent": ((b_rate / a_rate) - 1) * 100 if a_rate > 0 else None,
            "log_ratio": math.log(b_rate / a_rate) if a_rate > 0 and b_rate > 0 else None,
            "mechanical_efficiency_delta_b_minus_a": efficiency_delta,
        }

    if first_arm == second_arm and first_arm in {arm_a, arm_b}:
        return {
            "kind": "negative_control",
            "order": f"{first_arm}->{second_arm}",
            "first_run": run_id(first),
            "second_run": run_id(second),
            "arm": first_arm,
            "first_rate": first_rate,
            "second_rate": second_rate,
            "delta_per_second": second_rate - first_rate,
            "delta_percent": ((second_rate / first_rate) - 1) * 100 if first_rate > 0 else None,
            "log_ratio": math.log(second_rate / first_rate) if first_rate > 0 and second_rate > 0 else None,
        }
    return None


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def bootstrap_median_ci(values: list[float], samples: int = 5000) -> list[float] | None:
    if not values:
        return None
    rng = random.Random(0xC0FFEE)
    medians = [statistics.median(rng.choices(values, k=len(values))) for _ in range(samples)]
    return [percentile(medians, 0.025), percentile(medians, 0.975)]


def sign_test(values: list[float]) -> dict:
    positives = sum(value > 0 for value in values)
    negatives = sum(value < 0 for value in values)
    n = positives + negatives
    if n == 0:
        return {"nonzero": 0, "positive": 0, "negative": 0, "two_sided_p": None, "one_sided_improvement_p": None}
    tail = sum(math.comb(n, i) for i in range(0, min(positives, negatives) + 1)) / (2 ** n)
    improve = sum(math.comb(n, i) for i in range(positives, n + 1)) / (2 ** n)
    return {
        "nonzero": n,
        "positive": positives,
        "negative": negatives,
        "two_sided_p": min(1.0, 2 * tail),
        "one_sided_improvement_p": improve,
    }


def delta_summary(pairs: list[dict]) -> dict:
    deltas = [float(pair["delta_per_second"]) for pair in pairs]
    logs = [float(pair["log_ratio"]) for pair in pairs if pair.get("log_ratio") is not None]
    return {
        "pairs": len(pairs),
        "delta_per_second_mean": statistics.fmean(deltas) if deltas else None,
        "delta_per_second_median": statistics.median(deltas) if deltas else None,
        "delta_per_second_bootstrap_median_ci95": bootstrap_median_ci(deltas),
        "log_ratio_median": statistics.median(logs) if logs else None,
        "median_effect_percent_from_log_ratio": (math.exp(statistics.median(logs)) - 1) * 100 if logs else None,
        "sign_test": sign_test(deltas),
    }


def treatment_signature(record: dict) -> tuple:
    manifest = record.get("manifest", {})
    return (
        manifest.get("binary_sha"),
        manifest.get("template_config_sha"),
        manifest.get("treatment_sha"),
        tuple(sorted(manifest.get("overrides", []))),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", required=True, type=Path)
    parser.add_argument("--label-contains", default="")
    parser.add_argument("--experiment", default="")
    parser.add_argument("--start-run-id", default="")
    parser.add_argument("--warm-only", action="store_true")
    parser.add_argument("--arm-a", default="A")
    parser.add_argument("--arm-b", default="B")
    parser.add_argument("--rss-limit-mb", type=float, default=3800)
    parser.add_argument("--max-udp-drops", type=int, default=0)
    args = parser.parse_args()

    records = load_records(args.index)
    if args.label_contains:
        records = [record for record in records if args.label_contains in record.get("manifest", {}).get("label", "")]
    if args.experiment:
        records = [record for record in records if record.get("manifest", {}).get("experiment") == args.experiment]
    records.sort(key=run_id)
    if args.start_run_id:
        records = [record for record in records if run_id(record) >= args.start_run_id]

    groups: dict[str, list[dict]] = defaultdict(list)
    health: dict[str, dict] = {}
    for record in records:
        groups[variant(record)].append(record)
        errors, warnings = validate_run(
            record, args.rss_limit_mb, args.max_udp_drops, require_warm=args.warm_only
        )
        health[run_id(record)] = {"valid": not errors, "errors": errors, "warnings": warnings}

    signatures = {
        arm: sorted({treatment_signature(record) for record in items}, key=repr)
        for arm, items in groups.items()
    }
    arm_treatments_consistent = all(len(items) <= 1 for items in signatures.values())

    treatment_pairs: list[dict] = []
    controls: list[dict] = []
    rejected_pairs: list[dict] = []
    # Blocks are strict, adjacent, and non-overlapping.  Dropping a bad run must
    # not shift every later block and accidentally invent new pairings.
    for index in range(0, len(records) - 1, 2):
        first, second = records[index], records[index + 1]
        errors, warnings = validate_pair(first, second)
        for record in (first, second):
            errors.extend(f"{run_id(record)}: {error}" for error in health[run_id(record)]["errors"])
            warnings.extend(f"{run_id(record)}: {warning}" for warning in health[run_id(record)]["warnings"])
        pair = pair_delta(first, second, args.arm_a, args.arm_b)
        if pair is None:
            errors.append(f"unsupported arm pair {variant(first)}->{variant(second)}")
        if errors:
            rejected_pairs.append({
                "first_run": run_id(first), "second_run": run_id(second),
                "errors": errors, "warnings": warnings,
            })
            continue
        pair["warnings"] = warnings
        (treatment_pairs if pair["kind"] == "treatment" else controls).append(pair)

    by_order: dict[str, list[dict]] = defaultdict(list)
    for pair in treatment_pairs:
        by_order[pair["order"]].append(pair)
    control_noise = [abs(float(pair["delta_per_second"])) for pair in controls]
    noise_p95 = percentile(control_noise, 0.95)
    summary = delta_summary(treatment_pairs)
    ci = summary["delta_per_second_bootstrap_median_ci95"]
    signs = summary["sign_test"]
    median_delta = summary["delta_per_second_median"]
    exceeds_noise = (
        noise_p95 is not None and median_delta is not None and abs(median_delta) > noise_p95
    )
    durable_win = bool(
        len(treatment_pairs) >= 6
        and signs["positive"] >= 5
        and ci is not None and ci[0] is not None and ci[0] > 0
        and len(controls) >= 3 and exceeds_noise and arm_treatments_consistent
    )

    percentages = [float(pair["delta_percent"]) for pair in treatment_pairs if pair.get("delta_percent") is not None]

    output = {
        "arms": {arm: summarize(items) for arm, items in sorted(groups.items())},
        "run_health": health,
        "treatment_pairs": treatment_pairs,
        "pairs": treatment_pairs,
        "negative_control_pairs": controls,
        "rejected_pairs": rejected_pairs,
        "effect": summary,
        "effect_by_order": {order: delta_summary(items) for order, items in sorted(by_order.items())},
        "negative_control_noise_abs_delta_p95": noise_p95,
        "effect_exceeds_negative_control_noise": exceeds_noise,
        "durable_candidate_win": durable_win,
        "enough_pairs_for_durable_claim": durable_win,
        "paired_delta_percent_mean": statistics.fmean(percentages) if percentages else None,
        "arm_treatment_signatures": {arm: [list(item) for item in items] for arm, items in signatures.items()},
        "arm_treatments_consistent": arm_treatments_consistent,
        "durability_requirements": {
            "minimum_valid_treatment_pairs": 6,
            "minimum_positive_pairs": 5,
            "minimum_negative_control_pairs": 3,
            "bootstrap_median_ci_excludes_zero": bool(ci and ci[0] is not None and ci[0] > 0),
            "negative_control_required": True,
        },
        "unpaired_run": run_id(records[-1]) if len(records) % 2 else None,
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
