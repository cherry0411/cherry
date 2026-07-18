#!/usr/bin/env python3
"""Entry point for fail-closed preregistration preflight/submission."""

from pathlib import Path

from scientific_benchmark import start_main


if __name__ == "__main__":
    raise SystemExit(start_main(Path(__file__)))
