#!/usr/bin/env python3
"""Small, shared boundary calculations for the benchmark runner."""

from __future__ import annotations

import argparse


RUNTIME_WINDOW_SECONDS = 30


def warmup_runtime_rows(warmup_seconds: int) -> int:
    """Return the absolute runtime-row target for a fresh run log."""
    if warmup_seconds < 0:
        raise ValueError("warmup seconds must be non-negative")
    return (warmup_seconds + RUNTIME_WINDOW_SECONDS - 1) // RUNTIME_WINDOW_SECONDS


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("warmup_seconds", type=int)
    args = parser.parse_args()
    try:
        print(warmup_runtime_rows(args.warmup_seconds))
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
