#!/usr/bin/env bash
set -euo pipefail

# Sequential ABAB controller. It never co-runs variants on the same 2C/4G host.
# Binaries/configs may differ, while the identity cohort and global uniqueness
# oracle stay shared across all blocks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
label=""
blocks=4
warmup=10m
measure=60m
cohort=primary
config_a=""; config_b=""; binary_a=""; binary_b=""

while (($#)); do
  case "$1" in
    --label) label="$2"; shift 2 ;;
    --blocks) blocks="$2"; shift 2 ;;
    --warmup) warmup="$2"; shift 2 ;;
    --measure) measure="$2"; shift 2 ;;
    --cohort) cohort="$2"; shift 2 ;;
    --config-a) config_a="$2"; shift 2 ;;
    --config-b) config_b="$2"; shift 2 ;;
    --binary-a) binary_a="$2"; shift 2 ;;
    --binary-b) binary_b="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -n "${label}" && -n "${config_a}" && -n "${config_b}" && -n "${binary_a}" && -n "${binary_b}" ]] || {
  echo "--label, --config-a/b, and --binary-a/b are required" >&2; exit 2;
}
[[ "${blocks}" =~ ^[0-9]+$ && "${blocks}" -ge 2 ]] || { echo "--blocks must be >=2" >&2; exit 2; }

# Randomize the first arm, but retain strict alternation and record the order.
if (( $(date +%s) % 2 )); then first=A; else first=B; fi
echo "EXPERIMENT label=${label} blocks=${blocks} first=${first} warmup=${warmup} measure=${measure}"

for ((block=1; block<=blocks; block++)); do
  if ((block % 2 == 1)); then arm="${first}"; else [[ "${first}" == A ]] && arm=B || arm=A; fi
  if [[ "${arm}" == A ]]; then config="${config_a}"; binary="${binary_a}"; else config="${config_b}"; binary="${binary_b}"; fi
  "${SCRIPT_DIR}/run-crawler-benchmark.sh" \
    --label "${label}-block${block}" --variant "${arm}" --mode steady --cohort "${cohort}" \
    --warmup "${warmup}" --measure "${measure}" --config "${config}" --binary "${binary}"
done
