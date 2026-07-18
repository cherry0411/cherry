#!/usr/bin/env bash
set -euo pipefail

# Sequential paired controller. It never co-runs variants on the same 2C/4G
# host. Legacy shared-oracle behavior remains available; isolated mode freezes
# production once and gives every block a fresh writable overlay.

if [[ -z "${CHERRY_ABAB_SCRIPT_SNAPSHOT:-}" ]]; then
  original_script="$(readlink -f "${BASH_SOURCE[0]}")"
  snapshot="${TMPDIR:-/tmp}/cherry-abab-$$-$(date +%s).sh"
  cp "${original_script}" "${snapshot}"
  chmod 700 "${snapshot}"
  export CHERRY_ABAB_SCRIPT_SNAPSHOT="${snapshot}"
  export CHERRY_ABAB_SCRIPT_DIR="$(cd "$(dirname "${original_script}")" && pwd)"
  exec "${snapshot}" "$@"
fi

SCRIPT_DIR="${CHERRY_ABAB_SCRIPT_DIR}"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/benchmark/oracle_control.sh"
RUNTIME_ROOT="${CHERRY_BENCH_ROOT:-/home/ubuntu/cherry}"
[[ -d "${RUNTIME_ROOT}" ]] || RUNTIME_ROOT="${REPO_ROOT}/.benchmark"
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
sink_binary="${RUNTIME_ROOT}/bin/benchmark-sink"
sink_url="http://127.0.0.1:5070/api/v1/torrents/batch"
oracle_mode="shared"
production_oracle=""
oracle_experiment_dir=""
finalize_oracle=false
declare -a overrides_a=() overrides_b=()

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
    --sink-binary) sink_binary="$2"; shift 2 ;;
    --sink-url) sink_url="$2"; shift 2 ;;
    --oracle-mode) oracle_mode="$2"; shift 2 ;;
    --production-oracle) production_oracle="$2"; shift 2 ;;
    --oracle-experiment-dir) oracle_experiment_dir="$2"; shift 2 ;;
    --finalize-oracle) finalize_oracle=true; shift ;;
    --set-a) overrides_a+=("$2"); shift 2 ;;
    --set-b) overrides_b+=("$2"); shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -n "${label}" && -n "${config_a}" && -n "${config_b}" && -n "${binary_a}" && -n "${binary_b}" ]] || {
  echo "--label, --config-a/b, and --binary-a/b are required" >&2; exit 2;
}
experiment="${experiment:-${label}}"
[[ "${blocks}" =~ ^[0-9]+$ && "${blocks}" -ge 2 ]] || { echo "--blocks must be >=2" >&2; exit 2; }
[[ "${design}" =~ ^(balanced|aa|bb)$ ]] || { echo "--design must be balanced, aa, or bb" >&2; exit 2; }
[[ "${oracle_mode}" =~ ^(shared|isolated)$ ]] || { echo "--oracle-mode must be shared or isolated" >&2; exit 2; }
if [[ "${oracle_mode}" != isolated && "${finalize_oracle}" == true ]]; then
  echo "--finalize-oracle requires --oracle-mode isolated" >&2; exit 2
fi
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
echo "EXPERIMENT label=${label} experiment=${experiment} blocks=${blocks} design=${design} seed=${seed} plan=${plan} warmup=${warmup} measure=${measure} oracle=${oracle_mode} finalize=${finalize_oracle}"
if ${dry_run}; then
  rm -f "${CHERRY_ABAB_SCRIPT_SNAPSHOT}"
  exit 0
fi

sink_base="${sink_url%%/api/*}"
global_sink_pidfile="${RUNTIME_ROOT}/run/benchmark-sink.pid"
active_runner_pid=""
production_sink_was_running=false
isolation_owns_endpoint=false
controller_complete=false
experiment_manifest=""
baseline_path=""
baseline_sha=""
declare -a overlay_paths=()

update_experiment_status() {
  local status="$1" finalized="$2" production_sha="${3:-}"
  [[ -n "${experiment_manifest}" && -f "${experiment_manifest}" ]] || return 0
  python3 - "${experiment_manifest}" "${status}" "${finalized}" "${production_sha}" <<'PY'
import datetime, json, os, sys
path, status, finalized, production_sha = sys.argv[1:]
with open(path, encoding="utf-8") as source:
    manifest = json.load(source)
manifest["status"] = status
manifest["finalized"] = finalized == "true"
manifest["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
if production_sha:
    manifest["production_oracle_sha"] = production_sha
temp = path + ".tmp"
with open(temp, "w", encoding="utf-8") as target:
    json.dump(manifest, target, indent=2, sort_keys=True)
    target.write("\n")
os.replace(temp, path)
PY
  manifest_sha="$(sha256sum "${experiment_manifest}" | awk '{print $1}')"
  printf '%s  %s\n' "${manifest_sha}" "$(basename "${experiment_manifest}")" > "${experiment_manifest}.sha256.tmp"
  mv "${experiment_manifest}.sha256.tmp" "${experiment_manifest}.sha256"
}

controller_cleanup() {
  local status=$?
  if oracle_pid_is_running "${active_runner_pid}"; then
    kill -TERM "${active_runner_pid}" 2>/dev/null || true
    wait "${active_runner_pid}" 2>/dev/null || true
  fi
  if [[ "${oracle_mode}" == isolated && "${isolation_owns_endpoint}" == true ]]; then
    oracle_stop_managed_sink "${global_sink_pidfile}" "${sink_binary}" "${sink_base}" || true
    if [[ "${controller_complete}" != true ]]; then
      update_experiment_status "interrupted" false || true
    fi
    if [[ "${production_sink_was_running}" == true ]] && ! oracle_health_is_up "${sink_base}"; then
      oracle_start_sink "${sink_binary}" "${sink_base}" "${production_oracle}" "" \
        "${RUNTIME_ROOT}/state/oracle/sink.log" "${global_sink_pidfile}" true || true
    fi
  fi
  rm -f "${CHERRY_ABAB_SCRIPT_SNAPSHOT}" 2>/dev/null || true
  return "${status}"
}
trap controller_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "${oracle_mode}" == isolated ]]; then
  [[ -x "${sink_binary}" ]] || { echo "sink binary is not executable: ${sink_binary}" >&2; exit 2; }
  production_oracle="${production_oracle:-${RUNTIME_ROOT}/state/oracle/hashes.bin}"
  mkdir -p "${RUNTIME_ROOT}/run" "$(dirname "${production_oracle}")" "${RUNTIME_ROOT}/bench/oracle-experiments"
  [[ -f "${production_oracle}" ]] || touch "${production_oracle}"
  oracle_validate_record_file "${production_oracle}"

  crawler_pid="$(cat "${RUNTIME_ROOT}/run/crawler.pid" 2>/dev/null || true)"
  if oracle_pid_is_running "${crawler_pid}"; then
    echo "a crawler is already running (PID ${crawler_pid}); refusing to freeze a moving oracle" >&2
    exit 1
  fi
  rm -f "${RUNTIME_ROOT}/run/crawler.pid"

  managed_pid="$(cat "${global_sink_pidfile}" 2>/dev/null || true)"
  if oracle_health_is_up "${sink_base}" || oracle_pid_is_running "${managed_pid}"; then
    production_sink_was_running=true
    if ! oracle_pid_matches_binary "${managed_pid}" "${sink_binary}"; then
      echo "the active oracle is not owned by ${sink_binary}; refusing to freeze" >&2
      exit 1
    fi
    if ! oracle_pid_has_data_path "${managed_pid}" "${production_oracle}"; then
      echo "the active oracle does not use production data ${production_oracle}; refusing to freeze" >&2
      exit 1
    fi
    oracle_stop_managed_sink "${global_sink_pidfile}" "${sink_binary}" "${sink_base}"
    isolation_owns_endpoint=true
  else
    isolation_owns_endpoint=true
  fi

  utc="$(date -u +%Y%m%dT%H%M%SZ)"
  safe_label="$(printf '%s' "${label}" | tr -cs 'A-Za-z0-9._-' '-')"
  oracle_experiment_dir="${oracle_experiment_dir:-${RUNTIME_ROOT}/bench/oracle-experiments/${utc}_${safe_label}_${seed}}"
  mkdir -p "$(dirname "${oracle_experiment_dir}")"
  mkdir "${oracle_experiment_dir}"
  baseline_path="${oracle_experiment_dir}/baseline.bin"
  oracle_freeze_baseline "${production_oracle}" "${baseline_path}"
  baseline_sha="$(sha256sum "${baseline_path}" | awk '{print $1}')"
  baseline_bytes="$(stat -c %s "${baseline_path}")"
  production_sha="$(sha256sum "${production_oracle}" | awk '{print $1}')"
  [[ "${baseline_sha}" == "${production_sha}" ]] || { echo "frozen oracle digest mismatch" >&2; exit 1; }
  experiment_manifest="${oracle_experiment_dir}/manifest.json"
  python3 - "${experiment_manifest}" "${label}" "${experiment}" "${design}" "${seed}" "${plan}" \
    "${baseline_path}" "${baseline_sha}" "${baseline_bytes}" "${production_oracle}" "${production_sha}" <<'PY'
import datetime, json, sys
(path, label, experiment, design, seed, plan, baseline, baseline_sha, baseline_bytes,
 production, production_sha) = sys.argv[1:]
manifest = {
    "schema_version": 1,
    "label": label,
    "experiment": experiment,
    "design": design,
    "seed": int(seed),
    "plan": plan,
    "oracle_mode": "isolated",
    "baseline": baseline,
    "baseline_sha": baseline_sha,
    "baseline_bytes": int(baseline_bytes),
    "production_oracle": production,
    "production_oracle_sha_at_freeze": production_sha,
    "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "status": "running",
    "finalized": False,
    "blocks": [],
}
with open(path, "w", encoding="utf-8") as target:
    json.dump(manifest, target, indent=2, sort_keys=True)
    target.write("\n")
PY
  update_experiment_status running false "${production_sha}"
  echo "ORACLE frozen=${baseline_path} sha256=${baseline_sha} production=${production_oracle}"
fi

for ((block=1; block<=blocks; block++)); do
  arm="${plan:block-1:1}"
  if [[ "${arm}" == A ]]; then config="${config_a}"; binary="${binary_a}"; else config="${config_b}"; binary="${binary_b}"; fi
  command=("${SCRIPT_DIR}/run-crawler-benchmark.sh" \
    --label "${label}-block${block}" --experiment "${experiment}" --variant "${arm}" --mode steady --cohort "${cohort}" \
    --warmup "${warmup}" --measure "${measure}" --config "${config}" --binary "${binary}" \
    --sink-binary "${sink_binary}" --sink-url "${sink_url}")
  if [[ "${oracle_mode}" == isolated ]]; then
    overlay_path="${oracle_experiment_dir}/block-$(printf '%03d' "${block}")-${arm}.bin"
    overlay_paths+=("${overlay_path}")
    command+=(--oracle-mode isolated --oracle-baseline "${baseline_path}" --oracle-baseline-sha "${baseline_sha}" \
      --oracle-overlay "${overlay_path}" --oracle-production-data "${production_oracle}")
  fi
  if [[ "${arm}" == A ]]; then selected_overrides=("${overrides_a[@]}"); else selected_overrides=("${overrides_b[@]}"); fi
  for override in "${selected_overrides[@]}"; do command+=(--set "${override}"); done
  if [[ "${oracle_mode}" == isolated ]]; then
    block_log="${oracle_experiment_dir}/block-$(printf '%03d' "${block}").controller.log"
    "${command[@]}" > >(tee "${block_log}") 2>&1 &
    active_runner_pid=$!
    if wait "${active_runner_pid}"; then
      block_status=0
    else
      block_status=$?
    fi
    active_runner_pid=""
    if ((block_status != 0)); then
      echo "block ${block} (${arm}) failed with status ${block_status}" >&2
      exit "${block_status}"
    fi
    run_id="$(awk '/^DONE run_id=/{for(i=1;i<=NF;i++) if($i ~ /^run_id=/){sub(/^run_id=/,"",$i); print $i}}' "${block_log}" | tail -n 1)"
    [[ -n "${run_id}" ]] || { echo "block ${block} completed without a DONE run_id" >&2; exit 1; }
    oracle_validate_record_file "${overlay_path}"
    overlay_sha="$(sha256sum "${overlay_path}" | awk '{print $1}')"
    overlay_bytes="$(stat -c %s "${overlay_path}")"
    python3 - "${experiment_manifest}" "${block}" "${arm}" "${run_id}" "${overlay_path}" "${overlay_sha}" "${overlay_bytes}" <<'PY'
import datetime, json, os, sys
path, block, arm, run_id, overlay, overlay_sha, overlay_bytes = sys.argv[1:]
with open(path, encoding="utf-8") as source:
    manifest = json.load(source)
manifest["blocks"].append({
    "block": int(block), "arm": arm, "run_id": run_id, "overlay": overlay,
    "overlay_sha": overlay_sha, "overlay_bytes": int(overlay_bytes),
    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
})
manifest["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
temp = path + ".tmp"
with open(temp, "w", encoding="utf-8") as target:
    json.dump(manifest, target, indent=2, sort_keys=True)
    target.write("\n")
os.replace(temp, path)
PY
    update_experiment_status running false "${baseline_sha}"
  else
    "${command[@]}"
  fi
done

if [[ "${oracle_mode}" == isolated ]]; then
  current_production_sha="$(sha256sum "${production_oracle}" | awk '{print $1}')"
  if [[ "${current_production_sha}" != "${baseline_sha}" ]]; then
    echo "production oracle changed while the isolated experiment ran; refusing finalization" >&2
    exit 1
  fi
  finalized=false
  if [[ "${finalize_oracle}" == true ]]; then
    finalize_command=("${sink_binary}" -finalize-production "${production_oracle}")
    for overlay_path in "${overlay_paths[@]}"; do finalize_command+=(-merge-overlay "${overlay_path}"); done
    "${finalize_command[@]}"
    current_production_sha="$(sha256sum "${production_oracle}" | awk '{print $1}')"
    finalized=true
  fi
  update_experiment_status completed "${finalized}" "${current_production_sha}"
  controller_complete=true
  if [[ "${finalized}" == true ]]; then
    echo "ORACLE finalized=true production=${production_oracle} sha256=${current_production_sha}"
  else
    echo "ORACLE finalized=false overlays_preserved=${#overlay_paths[@]} manifest=${experiment_manifest}"
  fi
fi
