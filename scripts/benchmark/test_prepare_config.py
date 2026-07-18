#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("prepare_config", HERE / "prepare_config.py")
assert SPEC and SPEC.loader
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)


class PrepareConfigTests(unittest.TestCase):
    def test_legacy_config_routes_export_to_sink(self):
        config = {"exporter": {"http_endpoint": "http://old"}}
        self.assertEqual(prepare.route_benchmark_sink(config, "http://oracle"), "legacy")
        self.assertEqual(config["exporter"]["http_endpoint"], "http://oracle")
        self.assertNotIn("oracle_endpoint", config["exporter"])

    def test_durable_config_preserves_production_and_routes_oracle(self):
        config = {"exporter": {
            "http_endpoint": "https://storage/api/v1/torrents/batch",
            "spool_dir": "/var/lib/cherry/spool",
        }}
        self.assertEqual(prepare.route_benchmark_sink(config, "http://127.0.0.1:5070"), "dual")
        self.assertEqual(
            config["exporter"]["http_endpoint"],
            "https://storage/api/v1/torrents/batch",
        )
        self.assertEqual(config["exporter"]["oracle_endpoint"], "http://127.0.0.1:5070")

    def test_durable_config_requires_production_endpoint(self):
        with self.assertRaisesRegex(ValueError, "production"):
            prepare.route_benchmark_sink(
                {"exporter": {"spool_dir": "/var/lib/cherry/spool"}}, "http://oracle"
            )


if __name__ == "__main__":
    unittest.main()
