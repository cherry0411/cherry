#!/usr/bin/env python3
"""Standard-library regression tests for the benchmark reducers."""

from __future__ import annotations

import importlib.util
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

    def test_sign_test_is_exact(self):
        result = compare.sign_test([1, 1, 1, 1, 1])
        self.assertEqual(result["one_sided_improvement_p"], 1 / 32)
        self.assertEqual(result["two_sided_p"], 1 / 16)


if __name__ == "__main__":
    unittest.main()
