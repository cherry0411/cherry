#!/usr/bin/env python3
"""Reduce one crawler benchmark into a machine-readable scientific record."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

RUNTIME = re.compile(r"runtime 30s:\s+(.*)$")
RUNTIME_TIMESTAMP = re.compile(
    r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
)
RUNTIME_WINDOW_SECONDS = 30.0


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def read_event_time(path: Path) -> float | None:
    """Read an RFC3339 UTC marker as a Unix timestamp."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except (OSError, ValueError):
        return None


def runtime_event_time(line: str) -> float | None:
    """Return the UTC event time from a benchmark crawler log line.

    The benchmark runner launches the crawler with TZ=UTC. Keeping the event
    timestamp on every runtime row lets the reducer make window ownership
    independent of the race between ``wc -l`` and the crawler's 30-second
    ticker.
    """
    match = RUNTIME_TIMESTAMP.search(line)
    if not match:
        return None
    try:
        base = datetime.strptime(match.group("timestamp"), "%Y/%m/%d %H:%M:%S")
    except ValueError:
        return None
    fraction = match.group("fraction") or ""
    fractional_seconds = float(f"0.{fraction}") if fraction else 0.0
    return base.replace(tzinfo=timezone.utc).timestamp() + fractional_seconds


def parse_runtime(
    path: Path,
    first_line: int,
    measure_start: float | None = None,
    measure_end: float | None = None,
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for number, line in enumerate(handle, start=1):
            if number <= first_line:
                continue
            match = RUNTIME.search(line)
            if not match:
                continue
            if measure_start is not None or measure_end is not None:
                event_time = runtime_event_time(line)
                # A row without a trustworthy event timestamp cannot be
                # assigned to a measurement window safely.
                if event_time is None:
                    continue
                # Windows are right-closed: a row emitted exactly at the
                # measure-start marker belongs to warmup, while the final row
                # at measure-end belongs to measurement.
                if measure_start is not None and event_time <= measure_start:
                    continue
                if measure_end is not None and event_time > measure_end:
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


def expected_runtime_windows(measure_seconds: float) -> int:
    """Count complete 30-second windows in a measurement duration."""
    return max(1, math.floor(measure_seconds / RUNTIME_WINDOW_SECONDS))


def runtime_window_coverage(runtime_rows: list[dict[str, float]], measure_seconds: float) -> float:
    return len(runtime_rows) / expected_runtime_windows(measure_seconds)


def parse_host_metrics(
    path: Path,
    warmup: float,
    total: float,
    measure_start: float | None = None,
    measure_end: float | None = None,
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            for raw in csv.DictReader(handle):
                try:
                    elapsed = float(raw["elapsed_s"])
                except (KeyError, TypeError, ValueError):
                    continue
                if measure_start is not None or measure_end is not None:
                    try:
                        event_time = datetime.fromisoformat(
                            raw["utc"].replace("Z", "+00:00")
                        ).timestamp()
                    except (KeyError, TypeError, ValueError):
                        continue
                    if measure_start is not None and event_time <= measure_start:
                        continue
                    if measure_end is not None and event_time > measure_end:
                        continue
                elif elapsed < warmup or elapsed > total + 60:
                    continue
                row: dict[str, float] = {"elapsed_s": elapsed}
                for key in (
                    "cpu_pct", "rss_kb", "threads", "rx_bytes", "tx_bytes",
                    "udp_rcvbuf_errors", "udp_sndbuf_errors", "oracle_unique",
                    "tx_qdisc_drops",
                ):
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


def counter_delta(rows: list[dict[str, float]], key: str) -> int | None:
    values = finite(row.get(key, math.nan) for row in rows)
    if len(values) < 2:
        return None
    return max(0, int(values[-1] - values[0]))


def json_counter_delta(before: dict, after: dict, key: str) -> int | None:
    a, b = before.get(key), after.get(key)
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return None
    return max(0, int(b - a))


def slope(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 3:
        return None
    xbar = statistics.fmean(x for x, _ in points)
    ybar = statistics.fmean(y for _, y in points)
    denominator = sum((x - xbar) ** 2 for x, _ in points)
    if denominator == 0:
        return None
    return sum((x - xbar) * (y - ybar) for x, y in points) / denominator


def clean_oracle_samples(metrics: list[dict[str, float]]) -> tuple[list[tuple[float, float]], int]:
    """Return monotonic oracle samples and the number of rejected observations.

    A failed monitor request used to be recorded as zero.  Zipping raw rows then
    interpreted the following healthy sample as an enormous 30-second jump.  By
    retaining the last valid sample (and its original timestamp), a temporary
    gap becomes one longer, correctly averaged interval instead.
    """
    samples: list[tuple[float, float]] = []
    rejected = 0
    has_positive_sample = any(
        math.isfinite(row.get("oracle_unique", math.nan)) and row.get("oracle_unique", 0) > 0
        for row in metrics
    )
    for row in metrics:
        elapsed = row.get("elapsed_s", math.nan)
        value = row.get("oracle_unique", math.nan)
        if not math.isfinite(elapsed) or not math.isfinite(value):
            rejected += 1
            continue
        # A zero between positive samples is the legacy curl-failure sentinel.
        if (value == 0 and has_positive_sample) or (samples and value < samples[-1][1]):
            rejected += 1
            continue
        samples.append((elapsed, value))
    return samples, rejected


def oracle_rate_windows(metrics: list[dict[str, float]], warmup: float) -> tuple[list[tuple[float, float]], int]:
    samples, rejected = clean_oracle_samples(metrics)
    points: list[tuple[float, float]] = []
    for previous, current in zip(samples, samples[1:]):
        dt = current[0] - previous[0]
        if dt <= 0:
            continue
        uptime_hours = ((previous[0] + current[0]) / 2 - warmup) / 3600
        unique_per_hour = (current[1] - previous[1]) * 3600 / dt
        points.append((uptime_hours, unique_per_hour))
    return points, rejected


def oracle_rate_slope(metrics: list[dict[str, float]], warmup: float) -> float | None:
    points, _ = oracle_rate_windows(metrics, warmup)
    # Unit: change in unique/hour for each additional uptime hour.
    return slope(points)


def split_oracle_rates(
    metrics: list[dict[str, float]], warmup: float, measure: float
) -> tuple[float | None, float | None, int]:
    points, rejected = oracle_rate_windows(metrics, warmup)
    midpoint_hours = measure / 7200
    first = [rate / 3600 for uptime, rate in points if uptime < midpoint_hours]
    second = [rate / 3600 for uptime, rate in points if uptime >= midpoint_hours]
    return mean(first), mean(second), rejected


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def peer_source_funnel(runtime_rows: list[dict[str, float]]) -> dict[str, float | None]:
    """Split the dial->connect->download funnel by peer source.

    announce_* counters come from announce_peer senders (open NAT pinhole,
    provably alive); gp_* counters come from get_peers "values" (third-party
    reported, possibly stale). Missing keys total to 0 for legacy logs, so this
    is backward compatible and simply reports zeros/None then.
    """
    ann_queued = total(runtime_rows, "ann_q")
    ann_bl = total(runtime_rows, "ann_bl")
    ann_inflight = total(runtime_rows, "ann_inflight")
    ann_dial = total(runtime_rows, "ann_dial")
    ann_conn = total(runtime_rows, "ann_conn")
    ann_ok = total(runtime_rows, "ann_ok")
    gp_queued = total(runtime_rows, "gp_q")
    gp_bl = total(runtime_rows, "gp_bl")
    gp_inflight = total(runtime_rows, "gp_inflight")
    gp_dial = total(runtime_rows, "gp_dial")
    gp_conn = total(runtime_rows, "gp_conn")
    gp_ok = total(runtime_rows, "gp_ok")
    ann_connect_rate = _safe_ratio(ann_conn, ann_dial)
    gp_connect_rate = _safe_ratio(gp_conn, gp_dial)
    connect_rate_advantage = (
        ann_connect_rate / gp_connect_rate
        if ann_connect_rate is not None and gp_connect_rate else None
    )
    return {
        # Pre-dial supply accounting: queued splits into blacklisted /
        # inflight-deduped / dialed. A funnel that starves with a low dial rate
        # but high blacklisted or inflight share means supply is being discarded,
        # not merely unreachable.
        "announce_queued": ann_queued,
        "announce_blacklisted": ann_bl,
        "announce_inflight_deduped": ann_inflight,
        "announce_blacklisted_rate": _safe_ratio(ann_bl, ann_queued),
        "announce_inflight_rate": _safe_ratio(ann_inflight, ann_queued),
        "announce_dial": ann_dial,
        "announce_connect": ann_conn,
        "announce_download_ok": ann_ok,
        "announce_connect_rate": ann_connect_rate,
        "announce_download_rate": _safe_ratio(ann_ok, ann_dial),
        "getpeers_queued": gp_queued,
        "getpeers_blacklisted": gp_bl,
        "getpeers_inflight_deduped": gp_inflight,
        "getpeers_blacklisted_rate": _safe_ratio(gp_bl, gp_queued),
        "getpeers_inflight_rate": _safe_ratio(gp_inflight, gp_queued),
        "getpeers_dial": gp_dial,
        "getpeers_connect": gp_conn,
        "getpeers_download_ok": gp_ok,
        "getpeers_connect_rate": gp_connect_rate,
        "getpeers_download_rate": _safe_ratio(gp_ok, gp_dial),
        # >1 means announce peers connect better than get_peers values, i.e.
        # evidence for prioritizing announce-sourced peers (hypothesis B8).
        "announce_connect_rate_advantage": connect_rate_advantage,
    }


def blacklist_health(runtime_rows: list[dict[str, float]]) -> dict[str, float | None]:
    """Report blacklist saturation. size/max are gauges (last observed);
    rejected/expired accumulate. A near-full blacklist that keeps rejecting
    inserts is a diagnostic blind spot: it silently stops protecting workers.
    Missing keys yield None/0 for legacy logs.
    """
    sizes = [row["bl_size"] for row in runtime_rows if "bl_size" in row]
    maxes = [row["bl_max"] for row in runtime_rows if "bl_max" in row]
    last_size = sizes[-1] if sizes else None
    last_max = maxes[-1] if maxes else None
    return {
        "size": last_size,
        "max": last_max,
        "fill_ratio": _safe_ratio(last_size, last_max)
        if last_size is not None and last_max else None,
        "insert_rejected": total(runtime_rows, "bl_reject"),
        "expired_evicted": total(runtime_rows, "bl_expired"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--from-line", required=True, type=int)
    parser.add_argument("--measure-start", type=Path)
    parser.add_argument("--measure-end", type=Path)
    parser.add_argument("--host-metrics", required=True, type=Path)
    parser.add_argument("--sink-before", required=True, type=Path)
    parser.add_argument("--sink-after", required=True, type=Path)
    parser.add_argument("--warmup-seconds", required=True, type=float)
    parser.add_argument("--measure-seconds", required=True, type=float)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    measure_start = read_event_time(args.measure_start) if args.measure_start else None
    measure_end = read_event_time(args.measure_end) if args.measure_end else None
    if args.measure_start and measure_start is None:
        parser.error(f"invalid measurement start marker: {args.measure_start}")
    if args.measure_end and measure_end is None:
        parser.error(f"invalid measurement end marker: {args.measure_end}")
    if measure_start is not None and measure_end is not None and measure_end <= measure_start:
        parser.error("measurement end must be after measurement start")
    runtime_rows = parse_runtime(
        args.log,
        args.from_line,
        measure_start=measure_start,
        measure_end=measure_end,
    )
    metrics = parse_host_metrics(
        args.host_metrics,
        args.warmup_seconds,
        args.warmup_seconds + args.measure_seconds,
        measure_start=measure_start,
        measure_end=measure_end,
    )
    before = read_json(args.sink_before)
    after = read_json(args.sink_after)
    unique_delta = max(0, int(after.get("metadata_unique", 0)) - int(before.get("metadata_unique", 0)))
    duplicate_delta = max(
        0,
        int(after.get("metadata_duplicates", 0)) - int(before.get("metadata_duplicates", 0)),
    )
    check_hash_delta = json_counter_delta(before, after, "check_hashes")
    check_found_delta = json_counter_delta(before, after, "check_found")
    check_found_ratio = (
        check_found_delta / check_hash_delta
        if check_hash_delta and check_found_delta is not None else None
    )

    local_windows = [row.get("meta_sent", row.get("wire_ok", 0)) for row in runtime_rows]
    nodes = [row.get("nodes", math.nan) for row in runtime_rows]
    first_half_rate, second_half_rate, rejected_oracle_samples = split_oracle_rates(
        metrics, args.warmup_seconds, args.measure_seconds
    )
    runtime_windows_expected = expected_runtime_windows(args.measure_seconds)
    oracle_sample_total = len(metrics)
    oracle_samples_valid = oracle_sample_total - rejected_oracle_samples
    oracle_sample_coverage = min(1.0, oracle_samples_valid / runtime_windows_expected)
    oracle_sample_missing_rate = max(
        rejected_oracle_samples / oracle_sample_total if oracle_sample_total else 1.0,
        1.0 - oracle_sample_coverage,
    )
    transient_slope = oracle_rate_slope(metrics, args.warmup_seconds)
    locale_classified = total(runtime_rows, "meta_locale_n")
    locale_han = total(runtime_rows, "meta_han")
    locale_kana = total(runtime_rows, "meta_kana")
    locale_hangul = total(runtime_rows, "meta_hangul")
    locale_chinese_proxy = total(runtime_rows, "meta_zh_proxy")
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
            "oracle_check_hashes": check_hash_delta,
            "oracle_check_found": check_found_delta,
            "oracle_check_found_ratio": check_found_ratio,
            # Compatibility alias; short runs measure a post-start transient,
            # not a proven long-run decay process.
            "decay_slope_unique_per_hour_per_uptime_hour": transient_slope,
            "transient_slope_unique_per_hour_per_uptime_hour": transient_slope,
            "first_half_unique_per_second": first_half_rate,
            "second_half_unique_per_second": second_half_rate,
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
        # Peer-source funnel: tests whether announce_peer peers (which just
        # contacted us, so their NAT pinhole is open and they are provably
        # alive) out-connect third-party get_peers "values". If
        # announce_connect_rate is materially higher than getpeers_connect_rate,
        # prioritizing announce-sourced peers should lift the connect-rate half
        # of the observed sustained-rate decay. connect_rate = dial_ok/dial.
        "peer_source_funnel": peer_source_funnel(runtime_rows),
        # Blacklist saturation health: a near-full blacklist silently drops new
        # inserts (Len>=max no-op), so bad peers keep consuming dial workers.
        # fill_ratio near 1 with rising insert_rejected explains a decaying
        # connect rate that is not caused by peer supply.
        "blacklist_health": blacklist_health(runtime_rows),
        # Script-level signals are regional comparison proxies, not language
        # detection. In particular, Han-only Japanese/Korean names can satisfy
        # chinese_proxy; Kana and Hangul are reported separately so that bias is
        # visible instead of hidden in one headline number.
        "metadata_locale": {
            "classified": locale_classified,
            "name_path_han": locale_han,
            "name_path_kana": locale_kana,
            "name_path_hangul": locale_hangul,
            "chinese_proxy": locale_chinese_proxy,
            "han_ratio": locale_han / locale_classified if locale_classified else None,
            "kana_ratio": locale_kana / locale_classified if locale_classified else None,
            "hangul_ratio": locale_hangul / locale_classified if locale_classified else None,
            "chinese_proxy_ratio": (
                locale_chinese_proxy / locale_classified if locale_classified else None
            ),
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
            "host_rx_bytes": counter_delta(metrics, "rx_bytes"),
            "host_tx_bytes": counter_delta(metrics, "tx_bytes"),
            "udp_rcvbuf_errors": counter_delta(metrics, "udp_rcvbuf_errors"),
            "udp_sndbuf_errors": counter_delta(metrics, "udp_sndbuf_errors"),
            "tx_qdisc_drops": counter_delta(metrics, "tx_qdisc_drops"),
        },
        "health": {
            "runtime_windows_expected": runtime_windows_expected,
            "runtime_window_coverage": runtime_window_coverage(
                runtime_rows, args.measure_seconds
            ),
            "monitor_samples": oracle_sample_total,
            "oracle_samples_valid": oracle_samples_valid,
            "oracle_samples_rejected": rejected_oracle_samples,
            "oracle_sample_coverage": oracle_sample_coverage,
            "oracle_sample_missing_rate": oracle_sample_missing_rate,
        },
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "RESULT"
        f" run_id={args.run_id}"
        f" global_unique={unique_delta}"
        f" global_unique_s={unique_delta / args.measure_seconds:.3f}"
        f" local_meta_s={result['local_funnel']['metadata_per_second']:.3f}"
        f" zh_proxy={locale_chinese_proxy}"
        f" rss_max_mb={result['resources']['rss_mb_max']:.1f}"
        f" windows={len(runtime_rows)}"
    )


if __name__ == "__main__":
    main()
