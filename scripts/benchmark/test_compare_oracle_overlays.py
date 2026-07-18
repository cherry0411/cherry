#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "compare_oracle_overlays", HERE / "compare_oracle_overlays.py"
)
assert SPEC and SPEC.loader
overlays = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(overlays)


def write_records(path: Path, rows: list[tuple[bytes, bytes]]) -> None:
    path.write_bytes(b"".join(kind + value for kind, value in rows))


class OverlayComparisonTests(unittest.TestCase):
    def test_exact_metadata_union_and_marginal_contribution(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            left, right = root / "left.bin", root / "right.bin"
            a, b, c = bytes([1]) * 20, bytes([2]) * 20, bytes([3]) * 20
            write_records(left, [(b"M", a), (b"M", b)])
            write_records(right, [(b"M", b), (b"M", c)])

            result = overlays.compare(overlays.read_overlay(left), overlays.read_overlay(right))

            self.assertEqual(result["metadata"]["intersection"], 1)
            self.assertEqual(result["metadata"]["union"], 3)
            self.assertEqual(result["metadata"]["right_exclusive"], 1)
            self.assertEqual(result["metadata"]["jaccard"], 1 / 3)
            self.assertEqual(result["metadata"]["right_marginal_over_left"], 0.5)

    def test_rejects_trailing_partial_record(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "broken.bin"
            path.write_bytes(b"M" + bytes(19))
            with self.assertRaisesRegex(ValueError, "trailing partial"):
                overlays.read_overlay(path)

    def test_cross_kind_history_uses_information_priority(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "invalid.bin"
            value = bytes([9]) * 20
            write_records(path, [(b"M", value), (b"R", value)])
            result = overlays.read_overlay(path)
            self.assertEqual(result["full"], {value})
            self.assertEqual(result["rejected"], set())

    def test_typed_actions_and_legacy_metadata_are_backward_compatible(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "typed.bin"
            a, b, c, d = (bytes([n]) * 20 for n in range(1, 5))
            write_records(path, [(b"M", a), (b"S", b), (b"H", c), (b"R", d)])
            result = overlays.read_overlay(path)
            self.assertEqual(result["full"], {a})
            self.assertEqual(result["summary"], {b})
            self.assertEqual(result["hash_only"], {c})
            self.assertEqual(result["rejected"], {d})
            self.assertEqual(result["metadata"], {a, b})


if __name__ == "__main__":
    unittest.main()
