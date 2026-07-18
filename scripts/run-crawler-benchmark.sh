#!/usr/bin/env bash
set -euo pipefail

# Reproducible single-host crawler benchmark. Each run gets an immutable
# directory; steady/cold identity semantics are explicit; only post-warmup
# windows count toward the result.

# Bash may read a script lazily. Execute an immutable snapshot so deploying a
# newer controller in place cannot corrupt a run that is already measuring.
if [[ -z "${CHERRY_BENCH_SCRIPT_SNAPSHOT:-}" ]]; then
  original_script="$(readlink -f "${BASH_SOURCE[0]}")"
  snapshot="${TMPDIR:-/tmp}/cherry-benchmark-$$-$(date +%s).sh"
  cp "${original_script}" "${snapshot}"
  chmod 700 "${snapshot}"
  export CHERRY_BENCH_SCRIPT_SNAPSHOT="${snapshot}"
  export CHERRY_BENCH_SCRIPT_DIR="$(cd "$(dirname "${original_script}")" && pwd)"
  exec "${snapshot}" "$@"
fi

SCRIPT_DIR="${CHERRY_BENCH_SCRIPT_DIR}"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNTIME_ROOT="${CHERRY_BENCH_ROOT:-/home/ubuntu/cherry}"
[[ -d "${RUNTIME_ROOT}" ]] || RUNTIME_ROOT="${REPO_ROOT}/.benchmark"

label=""
experiment=""
variant="candidate"
mode="steady"
cohort="primary"
warmup="10m"
measure="60m"
port=21000
config="${REPO_ROOT}/cherry-picker/configs/metadata-2c4g.json"
binary="${RUNTIME_ROOT}/bin/cherry-picker"
sink_binary="${RUNTIME_ROOT}/bin/benchmark-sink"
sink_url="http://127.0.0.1:5070/api/v1/torrents/batch"
declare -a overrides=()

usage() {
  cat <<'EOF'
Usage: run-crawler-benchmark.sh --label NAME [options]
  --experiment NAME       experiment family used for safe pairing
  --variant NAME          A/B variant label (default: candidate)
  --mode MODE             steady, warm-restart, or cold (default: steady)
  --cohort NAME           stable node-ID cohort (default: primary)
  --warmup DURATION       integer s/m/h duration (default: 10m)
  --measure DURATION      integer s/m/h duration (default: 60m)
  --config PATH           tracked config template
  --binary PATH           crawler binary
  --sink-binary PATH      benchmark uniqueness oracle
  --sink-url URL          crawler batch endpoint
  --port PORT             first of the consecutive DHT ports
  --set PATH=VALUE        JSON config override; repeatable
EOF
}

while (($#)); do
  case "$1" in
    --label) label="$2"; shift 2 ;;
    --experiment) experiment="$2"; shift 2 ;;
    --variant) variant="$2"; shift 2 ;;
    --mode) mode="$2"; shift 2 ;;
    --cohort) cohort="$2"; shift 2 ;;
    --warmup) warmup="$2"; shift 2 ;;
    --measure) measure="$2"; shift 2 ;;
    --config) config="$2"; shift 2 ;;
    --binary) binary="$2"; shift 2 ;;
    --sink-binary) sink_binary="$2"; shift 2 ;;
    --sink-url) sink_url="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    --set) overrides+=("$2"); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "${label}" ]] || { echo "--label is required" >&2; exit 2; }
experiment="${experiment:-${label}}"
[[ "${mode}" =~ ^(steady|warm-restart|cold)$ ]] || { echo "invalid --mode: ${mode}" >&2; exit 2; }
[[ -x "${binary}" ]] || { echo "crawler binary is not executable: ${binary}" >&2; exit 2; }
[[ -x "${sink_binary}" ]] || { echo "sink binary is not executable: ${sink_binary}" >&2; exit 2; }
[[ -f "${config}" ]] || { echo "config does not exist: ${config}" >&2; exit 2; }
template_config_sha="$(sha256sum "${config}" | awk '{print $1}')"

duration_seconds() {
  local raw="$1" number unit
  if [[ "${raw}" =~ ^([0-9]+)([smh])$ ]]; then
    number="${BASH_REMATCH[1]}"; unit="${BASH_REMATCH[2]}"
  elif [[ "${raw}" =~ ^[0-9]+$ ]]; then
    number="${raw}"; unit=s
  else
    echo "invalid duration: ${raw}" >&2; return 2
  fi
  case "${unit}" in s) echo "${number}" ;; m) echo $((number * 60)) ;; h) echo $((number * 3600)) ;; esac
}

warmup_seconds="$(duration_seconds "${warmup}")"
measure_seconds="$(duration_seconds "${measure}")"
utc="$(date -u +%Y%m%dT%H%M%SZ)"
binary_sha="$(sha256sum "${binary}" | awk '{print $1}')"
short_sha="${binary_sha:0:12}"
safe_label="$(printf '%s' "${label}" | tr -cs 'A-Za-z0-9._-' '-')"
safe_variant="$(printf '%s' "${variant}" | tr -cs 'A-Za-z0-9._-' '-')"
run_id="${utc}_${safe_label}_${safe_variant}_${short_sha}"
run_dir="${RUNTIME_ROOT}/bench/runs/${run_id}"
mkdir -p "${run_dir}" "${RUNTIME_ROOT}/run" "${RUNTIME_ROOT}/state/oracle" "${RUNTIME_ROOT}/state/nodes"
"${binary}" --version > "${run_dir}/build.json"

case "${mode}" in
  cold) node_dir="${run_dir}/nodes" ;;
  steady|warm-restart) node_dir="${RUNTIME_ROOT}/state/nodes/${cohort}" ;;
esac
mkdir -p "${node_dir}"
node_ids_before="$(find "${node_dir}" -maxdepth 1 -type f -name 'node_*' 2>/dev/null | wc -l)"

effective_config="${run_dir}/config.json"
prepare_args=(
  --input "${config}" --output "${effective_config}" --run-id "${run_id}"
  --node-id-dir "${node_dir}" --port "${port}" --sink-url "${sink_url}"
)
for override in "${overrides[@]}"; do prepare_args+=(--set "${override}"); done
python3 "${SCRIPT_DIR}/benchmark/prepare_config.py" "${prepare_args[@]}"
config_sha="$(sha256sum "${effective_config}" | awk '{print $1}')"
if ((${#overrides[@]})); then
  treatment_input="$(printf '%s\n' "${overrides[@]}" | sort)"
else
  treatment_input=""
fi
treatment_sha="$(printf '%s\n%s' "${template_config_sha}" "${treatment_input}" | sha256sum | awk '{print $1}')"

sink_base="${sink_url%%/api/*}"
if ! curl -fsS "${sink_base}/health" >/dev/null 2>&1; then
  sink_log="${RUNTIME_ROOT}/state/oracle/sink.log"
  nohup env GOMAXPROCS=1 GOMEMLIMIT=384MiB "${sink_binary}" \
    -listen "${sink_base#http://}" -data "${RUNTIME_ROOT}/state/oracle/hashes.bin" \
    >>"${sink_log}" 2>&1 &
  echo $! > "${RUNTIME_ROOT}/run/benchmark-sink.pid"
  for _ in $(seq 1 50); do
    curl -fsS "${sink_base}/health" >/dev/null 2>&1 && break
    sleep 0.2
  done
fi
curl -fsS "${sink_base}/health" >/dev/null

stop_pidfile_process() {
  local pidfile="$1" pid
  [[ -f "${pidfile}" ]] || return 0
  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  [[ "${pid}" =~ ^[0-9]+$ ]] || return 0
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    for _ in $(seq 1 100); do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 0.1
    done
  fi
}
stop_pidfile_process "${RUNTIME_ROOT}/run/crawler.pid"

uname -a > "${run_dir}/environment.txt"
{
  echo "cpus=$(nproc)"
  echo "memory_bytes=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo)"
  echo "kernel=$(uname -r)"
  echo "binary_sha256=${binary_sha}"
  echo "config_file_sha256=${config_sha}"
  sysctl net.core.rmem_max net.core.wmem_max net.core.netdev_max_backlog net.ipv4.ip_local_port_range 2>/dev/null || true
  git -C "${REPO_ROOT}" status --short 2>/dev/null || true
  git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || true
} >> "${run_dir}/environment.txt"

export RUN_ID="${run_id}" LABEL="${label}" EXPERIMENT="${experiment}" VARIANT="${variant}" MODE="${mode}" COHORT="${cohort}"
export WARMUP_SECONDS="${warmup_seconds}" MEASURE_SECONDS="${measure_seconds}" PORT="${port}"
export BINARY="${binary}" BINARY_SHA="${binary_sha}" CONFIG="${effective_config}" CONFIG_SHA="${config_sha}"
export TEMPLATE_CONFIG_SHA="${template_config_sha}" TREATMENT_SHA="${treatment_sha}"
export NODE_DIR="${node_dir}" SINK_URL="${sink_url}"
export NODE_IDS_BEFORE="${node_ids_before}"
if ((${#overrides[@]})); then
  OVERRIDES="$(printf '%s\n' "${overrides[@]}")"
else
  OVERRIDES=""
fi
export OVERRIDES
export BUILD_JSON="${run_dir}/build.json"
python3 - "${run_dir}/manifest.json" <<'PY'
import json, os, sys
keys = ["RUN_ID", "LABEL", "EXPERIMENT", "VARIANT", "MODE", "COHORT", "WARMUP_SECONDS", "MEASURE_SECONDS",
        "PORT", "BINARY", "BINARY_SHA", "CONFIG", "CONFIG_SHA", "TEMPLATE_CONFIG_SHA", "TREATMENT_SHA",
        "NODE_DIR", "NODE_IDS_BEFORE", "SINK_URL"]
manifest = {key.lower(): os.environ[key] for key in keys}
manifest["schema_version"] = 1
manifest["overrides"] = os.environ.get("OVERRIDES", "").splitlines() if os.environ.get("OVERRIDES") else []
try:
    manifest["build"] = json.load(open(os.environ["BUILD_JSON"], encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    manifest["build"] = {}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

log_file="${run_dir}/crawler.log"
metrics_file="${run_dir}/host-metrics.csv"
netdev="$(ip route show default 2>/dev/null | awk 'NR == 1 {print $5}')"
echo "utc,elapsed_s,cpu_pct,rss_kb,threads,rx_bytes,tx_bytes,udp_rcvbuf_errors,udp_sndbuf_errors,tx_qdisc_drops,oracle_unique" > "${metrics_file}"
start_epoch="$(date +%s)"

env GOMAXPROCS=2 CHERRY_PICKER_MEM_LIMIT_MB=3072 CHERRY_PICKER_CONFIG="${effective_config}" \
  "${binary}" >>"${log_file}" 2>&1 &
crawler_pid=$!
echo "${crawler_pid}" > "${RUNTIME_ROOT}/run/crawler.pid"

monitor() {
  while kill -0 "${crawler_pid}" 2>/dev/null; do
    local now elapsed cpu rss threads rx tx udp_rcv udp_snd oracle stats
    now="$(date +%s)"; elapsed=$((now - start_epoch))
    cpu="$(ps -p "${crawler_pid}" -o %cpu= 2>/dev/null | tr -d ' ' || true)"; cpu="${cpu:-0}"
    rss="$(awk '/VmRSS:/ {print $2}' "/proc/${crawler_pid}/status" 2>/dev/null || echo 0)"
    threads="$(awk '/Threads:/ {print $2}' "/proc/${crawler_pid}/status" 2>/dev/null || echo 0)"
    read -r rx tx < <(awk -F'[: ]+' '$1 != "lo" && NF > 10 {rx += $3; tx += $11} END {print rx+0, tx+0}' /proc/net/dev)
    read -r udp_rcv udp_snd < <(awk '$1 == "Udp:" { if (++row == 1) { for (i=2; i<=NF; i++) col[$i]=i } else { print $(col["RcvbufErrors"]), $(col["SndbufErrors"]) } }' /proc/net/snmp)
    qdisc_drops="$(tc -s qdisc show dev "${netdev}" 2>/dev/null | awk '/^qdisc .* root/{root=1; next} root && /Sent / {for (i=1; i<=NF; i++) if ($i == "(dropped") {gsub(/,/, "", $(i+1)); print $(i+1); exit}}' || true)"
    stats="$(curl -fsS "${sink_base}/stats" 2>/dev/null || true)"
    oracle="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata_unique"])' <<<"${stats}" 2>/dev/null || true)"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$(date -u +%FT%TZ)" "${elapsed}" "${cpu}" "${rss}" "${threads}" "${rx}" "${tx}" "${udp_rcv}" "${udp_snd}" "${qdisc_drops}" "${oracle}" >> "${metrics_file}"
    sleep 30
  done
}
monitor & monitor_pid=$!

cleanup() {
  if kill -0 "${crawler_pid}" 2>/dev/null; then kill -TERM "${crawler_pid}" 2>/dev/null || true; fi
  kill "${monitor_pid}" 2>/dev/null || true
  rm -f "${CHERRY_BENCH_SCRIPT_SNAPSHOT}" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

wait_phase() {
  local seconds="$1" phase="$2" deadline
  deadline=$(( $(date +%s) + seconds ))
  while (( $(date +%s) < deadline )); do
    if ! kill -0 "${crawler_pid}" 2>/dev/null; then
      echo "crawler exited during ${phase}" >&2
      tail -n 40 "${log_file}" >&2 || true
      return 1
    fi
    sleep 5
  done
}

echo "RUN run_id=${run_id} mode=${mode} warmup=${warmup} measure=${measure} dir=${run_dir}"
wait_phase "${warmup_seconds}" warmup
from_line="$(wc -l < "${log_file}")"
curl -fsS "${sink_base}/stats" > "${run_dir}/sink-before.json"
date -u +%FT%TZ > "${run_dir}/measurement-start.txt"
echo "MEASURE run_id=${run_id} from_line=${from_line}"
wait_phase "${measure_seconds}" measurement

kill -TERM "${crawler_pid}" 2>/dev/null || true
for _ in $(seq 1 200); do
  kill -0 "${crawler_pid}" 2>/dev/null || break
  sleep 0.1
done
wait "${crawler_pid}" 2>/dev/null || true
kill "${monitor_pid}" 2>/dev/null || true
wait "${monitor_pid}" 2>/dev/null || true
sleep 1
curl -fsS "${sink_base}/stats" > "${run_dir}/sink-after.json"
date -u +%FT%TZ > "${run_dir}/measurement-end.txt"

python3 "${SCRIPT_DIR}/benchmark/analyze_benchmark.py" \
  --run-id "${run_id}" --log "${log_file}" --from-line "${from_line}" \
  --host-metrics "${metrics_file}" --sink-before "${run_dir}/sink-before.json" \
  --sink-after "${run_dir}/sink-after.json" --warmup-seconds "${warmup_seconds}" \
  --measure-seconds "${measure_seconds}" --output "${run_dir}/result.json" | tee "${run_dir}/result.txt"

python3 - "${run_dir}/manifest.json" "${run_dir}/result.json" "${RUNTIME_ROOT}/bench/index.jsonl" <<'PY'
import json, sys
record = {"manifest": json.load(open(sys.argv[1], encoding="utf-8")),
          "result": json.load(open(sys.argv[2], encoding="utf-8"))}
with open(sys.argv[3], "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record, sort_keys=True) + "\n")
PY

rm -f "${RUNTIME_ROOT}/run/crawler.pid"
trap - INT TERM EXIT
rm -f "${CHERRY_BENCH_SCRIPT_SNAPSHOT}" 2>/dev/null || true
echo "DONE run_id=${run_id} result=${run_dir}/result.json"
