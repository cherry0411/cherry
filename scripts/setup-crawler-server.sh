#!/usr/bin/env bash
set -euo pipefail

# Backward-compatible entry point. The old version unconditionally enlarged
# conntrack to two million entries and installed redundant NOTRACK rules.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/tune-crawler-os.sh" "$@"
