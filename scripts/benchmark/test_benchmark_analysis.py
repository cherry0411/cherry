#!/usr/bin/env python3
"""Standard-library regression tests for the benchmark reducers."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


analyze = load_module("analyze_benchmark", "analyze_benchmark.py")
compare = load_module("compare_benchmarks", "compare_benchmarks.py")


def record(arm: str, run: str, unique_rate: float, *, udp: int = 0, cohort: str = "test") -> dict:
    return {
        "manifest": {
            "variant": arm, "cohort": cohort, "mode": "steady",
            "warmup_seconds": "300", "measure_seconds": "1200", "port": "21000",
            "template_config_sha": "template",
        },
        "result": {
            "run_id": run, "measurement_seconds": 1200, "runtime_windows": 40,
            "primary": {"global_unique_per_second": unique_rate},
            "local_funnel": {
                "metadata_sent": 12000, "metadata_per_second": 10,
                "wire_download_ok": 15000, "wire_dial_attempts": 300000,
            },
            "discovery": {
                "dht_packets_received": 10_000_000, "active_lookup_sent": 600_000,
                "active_lookup_dropped": 300_000, "refresh_queries": 12_000_000,
            },
            "resources": {
                "rss_mb_max": 1800, "cpu_pct_mean": 100,
                "udp_rcvbuf_errors": udp, "udp_sndbuf_errors": 0,
            },
            "health": {"oracle_sample_missing_rate": 0},
        },
    }


class AnalyzerTests(unittest.TestCase):
    def test_runtime_parser_keeps_locale_proxy_counters(self):
        with tempfile.TemporaryDirectory() as raw:
            log = Path(raw) / "crawler.log"
            log.write_text(
                "prefix runtime 30s: meta_locale_n=20 meta_han=8 "
                "meta_kana=2 meta_hangul=1 meta_zh_proxy=6 paused=false\n",
                encoding="utf-8",
            )
            rows = analyze.parse_runtime(log, 0)
            self.assertEqual(rows[0]["meta_locale_n"], 20)
            self.assertEqual(rows[0]["meta_zh_proxy"], 6)

    def test_json_counter_delta_supports_new_sink_counters(self):
        self.assertEqual(analyze.json_counter_delta({"check_found": 10}, {"check_found": 17}, "check_found"), 7)
        self.assertIsNone(analyze.json_counter_delta({}, {"check_found": 17}, "check_found"))

    def test_oracle_gap_is_averaged_not_spiked(self):
        metrics = [
            {"elapsed_s": 300.0, "oracle_unique": 1000.0},
            {"elapsed_s": 330.0, "oracle_unique": 1030.0},
            {"elapsed_s": 360.0, "oracle_unique": 0.0},
            {"elapsed_s": 390.0, "oracle_unique": 1090.0},
        ]
        points, rejected = analyze.oracle_rate_windows(metrics, 300)
        self.assertEqual(rejected, 1)
        self.assertEqual(len(points), 2)
        self.assertAlmostEqual(points[0][1], 3600)
        self.assertAlmostEqual(points[1][1], 3600)

    def test_non_monotonic_oracle_sample_is_rejected(self):
        samples, rejected = analyze.clean_oracle_samples([
            {"elapsed_s": 1.0, "oracle_unique": 10.0},
            {"elapsed_s": 2.0, "oracle_unique": 9.0},
            {"elapsed_s": 3.0, "oracle_unique": 12.0},
        ])
        self.assertEqual(samples, [(1.0, 10.0), (3.0, 12.0)])
        self.assertEqual(rejected, 1)

    def test_leading_legacy_zero_is_rejected_when_later_positive(self):
        samples, rejected = analyze.clean_oracle_samples([
            {"elapsed_s": 1.0, "oracle_unique": 0.0},
            {"elapsed_s": 2.0, "oracle_unique": 12.0},
        ])
        self.assertEqual(samples, [(2.0, 12.0)])
        self.assertEqual(rejected, 1)

    def test_peer_source_funnel_computes_connect_rate_advantage(self):
        # announce peers connect at 50% (500/1000), get_peers values at 20%
        # (200/1000): advantage should be 2.5x.
        rows = [{
            "ann_dial": 1000, "ann_conn": 500, "ann_ok": 100,
            "gp_dial": 1000, "gp_conn": 200, "gp_ok": 20,
        }]
        funnel = analyze.peer_source_funnel(rows)
        self.assertAlmostEqual(funnel["announce_connect_rate"], 0.5)
        self.assertAlmostEqual(funnel["getpeers_connect_rate"], 0.2)
        self.assertAlmostEqual(funnel["announce_connect_rate_advantage"], 2.5)
        self.assertAlmostEqual(funnel["announce_download_rate"], 0.1)

    def test_peer_source_funnel_is_backward_compatible_with_legacy_logs(self):
        # Legacy logs lack ann_/gp_ tokens: totals are 0 and rates are None,
        # never a ZeroDivisionError.
        rows = [{"meta_sent": 5}]
        funnel = analyze.peer_source_funnel(rows)
        self.assertEqual(funnel["announce_dial"], 0)
        self.assertIsNone(funnel["announce_connect_rate"])
        self.assertIsNone(funnel["announce_connect_rate_advantage"])

    def test_runtime_parser_keeps_peer_source_funnel_counters(self):
        with tempfile.TemporaryDirectory() as raw:
            log = Path(raw) / "crawler.log"
            log.write_text(
                "prefix runtime 30s: wire_ok=10 ann_dial=100 ann_conn=40 "
                "ann_ok=8 gp_dial=200 gp_conn=30 gp_ok=5 paused=false\n",
                encoding="utf-8",
            )
            rows = analyze.parse_runtime(log, 0)
            funnel = analyze.peer_source_funnel(rows)
            self.assertEqual(funnel["announce_dial"], 100)
            self.assertEqual(funnel["getpeers_connect"], 30)
            self.assertAlmostEqual(funnel["announce_connect_rate"], 0.4)

    def test_peer_source_funnel_reports_predial_supply_loss(self):
        # 100 queued announce peers: 30 blacklisted, 10 inflight-deduped, 60
        # dialed. Pre-dial loss must be visible and attributed per source.
        rows = [{
            "ann_q": 100, "ann_bl": 30, "ann_inflight": 10, "ann_dial": 60,
            "ann_conn": 24, "ann_ok": 6,
            "gp_q": 0, "gp_bl": 0, "gp_inflight": 0, "gp_dial": 0,
        }]
        funnel = analyze.peer_source_funnel(rows)
        self.assertEqual(funnel["announce_queued"], 100)
        self.assertAlmostEqual(funnel["announce_blacklisted_rate"], 0.3)
        self.assertAlmostEqual(funnel["announce_inflight_rate"], 0.1)
        # Legacy get_peers with zero queued must not divide by zero.
        self.assertIsNone(funnel["getpeers_blacklisted_rate"])

    def test_blacklist_health_reports_fill_and_rejects(self):
        # size/max are gauges (last row wins); reject/expired accumulate.
        rows = [
            {"bl_size": 1000, "bl_max": 2000, "bl_reject": 0, "bl_expired": 5},
            {"bl_size": 2000, "bl_max": 2000, "bl_reject": 40, "bl_expired": 7},
        ]
        health = analyze.blacklist_health(rows)
        self.assertEqual(health["size"], 2000)
        self.assertEqual(health["max"], 2000)
        self.assertAlmostEqual(health["fill_ratio"], 1.0)
        self.assertEqual(health["insert_rejected"], 40)
        self.assertEqual(health["expired_evicted"], 12)

    def test_blacklist_health_is_backward_compatible(self):
        health = analyze.blacklist_health([{"meta_sent": 3}])
        self.assertIsNone(health["size"])
        self.assertIsNone(health["fill_ratio"])
        self.assertEqual(health["insert_rejected"], 0)


class ComparatorTests(unittest.TestCase):
    def test_ba_order_always_computes_b_minus_a(self):
        pair = compare.pair_delta(record("B", "01", 12), record("A", "02", 10), "A", "B")
        self.assertIsNotNone(pair)
        self.assertEqual(pair["order"], "B->A")
        self.assertEqual(pair["delta_per_second"], 2)

    def test_same_arm_pair_is_negative_control(self):
        pair = compare.pair_delta(record("A", "01", 10), record("A", "02", 9), "A", "B")
        self.assertEqual(pair["kind"], "negative_control")
        self.assertEqual(pair["delta_per_second"], -1)

    def test_udp_drop_fails_health_gate(self):
        errors, _ = compare.validate_run(record("A", "01", 10, udp=1), 3800, 0)
        self.assertTrue(any("udp_rcvbuf_errors" in error for error in errors))

    def test_incompatible_cohort_rejects_pair(self):
        errors, _ = compare.validate_pair(
            record("A", "01", 10, cohort="one"), record("B", "02", 12, cohort="two")
        )
        self.assertTrue(any("cohort" in error for error in errors))

    def test_isolated_pair_requires_same_baseline_and_distinct_overlays(self):
        first = record("A", "01", 10)
        second = record("B", "02", 12)
        first["manifest"].update({
            "oracle_mode": "isolated", "oracle_baseline_sha": "frozen", "oracle_overlay": "one.bin",
        })
        second["manifest"].update({
            "oracle_mode": "isolated", "oracle_baseline_sha": "frozen", "oracle_overlay": "two.bin",
        })
        errors, _ = compare.validate_pair(first, second)
        self.assertEqual(errors, [])
        second["manifest"]["oracle_overlay"] = "one.bin"
        errors, _ = compare.validate_pair(first, second)
        self.assertTrue(any("reused" in error for error in errors))

    def test_isolated_pair_rejects_different_baselines(self):
        first = record("A", "01", 10)
        second = record("B", "02", 12)
        first["manifest"].update({
            "oracle_mode": "isolated", "oracle_baseline_sha": "one", "oracle_overlay": "one.bin",
        })
        second["manifest"].update({
            "oracle_mode": "isolated", "oracle_baseline_sha": "two", "oracle_overlay": "two.bin",
        })
        errors, _ = compare.validate_pair(first, second)
        self.assertTrue(any("different oracle baselines" in error for error in errors))

    def test_sign_test_is_exact(self):
        result = compare.sign_test([1, 1, 1, 1, 1])
        self.assertEqual(result["one_sided_improvement_p"], 1 / 32)
        self.assertEqual(result["two_sided_p"], 1 / 16)


if __name__ == "__main__":
    unittest.main()
