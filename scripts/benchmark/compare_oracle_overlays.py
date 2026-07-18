#!/usr/bin/env python3
"""Compare independent benchmark-sink overlays without attribution bias.

Each sink overlay is a sequence of 21-byte records: one closed action byte
(``F``/``S``/``H``/``R``) followed by the raw 20-byte info-hash. Legacy ``M``
(searchable metadata) is read as ``F``. Regional crawlers deliberately write
separate overlays so the same hash can be credited to both regions. This
reducer computes exact intersection, union, and marginal contribution.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


RECORD_SIZE = 21
VALID_KINDS = {
    b"M": "full", b"F": "full", b"S": "summary",
    b"H": "hash_only", b"R": "rejected",
}
PRIORITY = {"rejected": 1, "hash_only": 2, "summary": 3, "full": 4}


def read_overlay(path: Path) -> dict[str, set[bytes]]:
    classifications: dict[bytes, str] = {}
    with path.open("rb") as handle:
        offset = 0
        while chunk := handle.read(RECORD_SIZE):
            if len(chunk) != RECORD_SIZE:
                raise ValueError(
                    f"{path}: trailing partial record at byte {offset} "
                    f"({len(chunk)}/{RECORD_SIZE} bytes)"
                )
            kind = VALID_KINDS.get(chunk[:1])
            if kind is None:
                raise ValueError(f"{path}: invalid record type {chunk[:1]!r} at byte {offset}")
            value = chunk[1:]
            previous = classifications.get(value)
            if previous is None or PRIORITY[kind] > PRIORITY[previous]:
                classifications[value] = kind
            offset += RECORD_SIZE
    records = {name: set() for name in PRIORITY}
    for value, kind in classifications.items():
        records[kind].add(value)
    records["searchable"] = records["full"] | records["summary"]
    # Backward-compatible alias used by older union automation.
    records["metadata"] = records["searchable"]
    return records


def compare(left: dict[str, set[bytes]], right: dict[str, set[bytes]]) -> dict:
    result: dict[str, object] = {"schema_version": 2}
    for kind in ("searchable", "full", "summary", "hash_only", "rejected"):
        a = left[kind]
        b = right[kind]
        intersection = a & b
        union = a | b
        smaller = min(len(a), len(b))
        result[kind] = {
            "left": len(a),
            "right": len(b),
            "intersection": len(intersection),
            "union": len(union),
            "left_exclusive": len(a - b),
            "right_exclusive": len(b - a),
            "jaccard": len(intersection) / len(union) if union else None,
            "overlap_of_smaller": len(intersection) / smaller if smaller else None,
            "right_marginal_over_left": len(b - a) / len(b) if b else None,
            "left_marginal_over_right": len(a - b) / len(a) if a else None,
        }
    result["metadata"] = result["searchable"]
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--left", required=True, type=Path)
    parser.add_argument("--right", required=True, type=Path)
    parser.add_argument("--left-label", default="left")
    parser.add_argument("--right-label", default="right")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = compare(read_overlay(args.left), read_overlay(args.right))
    result["left"] = {"label": args.left_label, "path": str(args.left)}
    result["right"] = {"label": args.right_label, "path": str(args.right)}
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
