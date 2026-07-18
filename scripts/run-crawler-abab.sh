#!/usr/bin/env bash
set -euo pipefail

# Sequential ABAB controller. It never co-runs variants on the same 2C/4G host.
# Binaries/configs may differ, while the identity cohort and global uniqueness
# oracle stay shared across all blocks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
label=""
experiment=""
blocks=4
design=balanced
seed=""
dry_run=false
warmup=10m
measure=60m
cohort=primary
config_a=""; config_b=""; binary_a=""; binary_b=""

while (($#)); do
  case "$1" in
    --label) label="$2"; shift 2 ;;
    --experiment) experiment="$2"; shift 2 ;;
    --blocks) blocks="$2"; shift 2 ;;
    --design) design="$2"; shift 2 ;;
    --seed) seed="$2"; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
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
experiment="${experiment:-${label}}"
[[ "${blocks}" =~ ^[0-9]+$ && "${blocks}" -ge 2 ]] || { echo "--blocks must be >=2" >&2; exit 2; }
[[ "${design}" =~ ^(balanced|aa|bb)$ ]] || { echo "--design must be balanced, aa, or bb" >&2; exit 2; }
if [[ "${design}" == balanced && $((blocks % 2)) -ne 0 ]]; then
  echo "--blocks must be even for the balanced adjacent-pair design" >&2; exit 2
fi

# Randomize the first pair, then alternate AB and BA pairs. This balances the
# shared-oracle depletion/order bias while keeping every analysis block adjacent.
seed="${seed:-$(date +%s)}"
[[ "${seed}" =~ ^[0-9]+$ ]] || { echo "--seed must be an integer" >&2; exit 2; }
if (( seed % 2 )); then first_pair=AB; else first_pair=BA; fi
plan=""
for ((block=1; block<=blocks; block++)); do
  if [[ "${design}" == aa ]]; then arm=A
  elif [[ "${design}" == bb ]]; then arm=B
  else
    pair=$(((block - 1) / 2))
    if ((pair % 2 == 0)); then order="${first_pair}"; else [[ "${first_pair}" == AB ]] && order=BA || order=AB; fi
    if ((block % 2 == 1)); then arm="${order:0:1}"; else arm="${order:1:1}"; fi
  fi
  plan+="${arm}"
done
echo "EXPERIMENT label=${label} experiment=${experiment} blocks=${blocks} design=${design} seed=${seed} plan=${plan} warmup=${warmup} measure=${measure}"
${dry_run} && exit 0

for ((block=1; block<=blocks; block++)); do
  arm="${plan:block-1:1}"
  if [[ "${arm}" == A ]]; then config="${config_a}"; binary="${binary_a}"; else config="${config_b}"; binary="${binary_b}"; fi
  "${SCRIPT_DIR}/run-crawler-benchmark.sh" \
    --label "${label}-block${block}" --experiment "${experiment}" --variant "${arm}" --mode steady --cohort "${cohort}" \
    --warmup "${warmup}" --measure "${measure}" --config "${config}" --binary "${binary}"
done
