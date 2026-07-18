#!/usr/bin/env bash
set -euo pipefail

# Fail-closed CHHT v2 deployment gate. Unknown or v1 data is never deleted.
# Operators may explicitly archive a verified all-v1 directory.

usage="usage: preflight-heat-spool-v2.sh SPOOL_DIR [--archive-v1] | --config JSON [--archive-v1]"
[[ $# -ge 1 ]] || { echo "${usage}" >&2; exit 2; }
if [[ "$1" == "--config" ]]; then
  [[ $# -ge 2 && $# -le 3 ]] || { echo "${usage}" >&2; exit 2; }
  config_path="$2"
  mode="${3:---check}"
  [[ -f "${config_path}" ]] || {
    echo "configured CHERRY_PICKER_CONFIG is not a file: ${config_path}" >&2
    exit 1
  }
  # A JSON config completely replaces the environment configuration in the Go
  # loader. Resolve heat.spool_dir from that same authority; checking the
  # systemd environment's fallback directory would prove the wrong spool safe.
  json_python=""
  for candidate in python3 python; do
    if command -v "${candidate}" >/dev/null 2>&1 &&
       "${candidate}" -c 'import json' >/dev/null 2>&1; then
      json_python="${candidate}"
      break
    fi
  done
  [[ -n "${json_python}" ]] || {
    echo "python3 with the standard json module is required for config preflight" >&2
    exit 1
  }
  spool_dir="$("${json_python}" - "${config_path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as source:
    value = json.load(source).get("heat", {}).get("spool_dir", "")
if value is None:
    value = ""
if not isinstance(value, str):
    raise SystemExit("heat.spool_dir must be a JSON string")
print(value.strip())
PY
)" || { echo "could not resolve heat.spool_dir from ${config_path}" >&2; exit 1; }
  if [[ -z "${spool_dir}" ]]; then
    [[ "${mode}" == "--check" ]] || {
      echo "no heat.spool_dir is configured in ${config_path}" >&2
      exit 2
    }
    echo "heat spool preflight: no spool configured in ${config_path}"
    exit 0
  fi
else
  [[ $# -le 2 ]] || { echo "${usage}" >&2; exit 2; }
  spool_dir="$1"
  mode="${2:---check}"
fi
[[ "${mode}" == "--check" || "${mode}" == "--archive-v1" ]] || {
  echo "mode must be --check or --archive-v1" >&2
  exit 2
}

if [[ ! -e "${spool_dir}" ]]; then
  if [[ "${mode}" == "--archive-v1" ]]; then
    echo "no heat spool exists to archive: ${spool_dir}" >&2
    exit 2
  fi
  echo "heat spool preflight: absent (safe for empty CHHT v2 creation)"
  exit 0
fi
[[ -d "${spool_dir}" ]] || { echo "heat spool path is not a directory: ${spool_dir}" >&2; exit 1; }

command -v flock >/dev/null 2>&1 || {
  echo "flock is required for heat spool preflight" >&2
  exit 1
}
exec 9>"${spool_dir}/heat.lock"
flock -n 9 || { echo "heat spool is active; stop cherry-picker before preflight" >&2; exit 1; }

shopt -s nullglob
segments=("${spool_dir}"/heat-*.spool)
cursor="${spool_dir}/heat.cursor"
versions=()
for path in "${segments[@]}"; do
  [[ -f "${path}" ]] || { echo "unexpected non-file spool entry: ${path}" >&2; exit 1; }
  magic="$(od -An -tx1 -N4 "${path}" | tr -d ' \n')"
  version="$(od -An -tu1 -j4 -N1 "${path}" | tr -d ' \n')"
  [[ "${magic}" == "43484853" && "${version}" =~ ^[0-9]+$ ]] || {
    echo "unknown heat spool header: ${path}; refusing deployment" >&2
    exit 1
  }
  versions+=("${version}")
done
if [[ -e "${cursor}" ]]; then
  [[ -f "${cursor}" ]] || { echo "unexpected non-file cursor: ${cursor}" >&2; exit 1; }
  magic="$(od -An -tx1 -N4 "${cursor}" | tr -d ' \n')"
  version="$(od -An -tu1 -j4 -N1 "${cursor}" | tr -d ' \n')"
  [[ "${magic}" == "43484843" && "${version}" =~ ^[0-9]+$ ]] || {
    echo "unknown heat cursor header: ${cursor}; refusing deployment" >&2
    exit 1
  }
  versions+=("${version}")
fi

if (( ${#versions[@]} == 0 )); then
  echo "heat spool preflight: empty (safe for CHHT v2 creation)"
  exit 0
fi
for version in "${versions[@]}"; do
  [[ "${version}" == "1" || "${version}" == "2" ]] || {
    echo "unsupported heat spool version ${version}; refusing deployment" >&2
    exit 1
  }
done
if printf '%s\n' "${versions[@]}" | grep -qx 1 &&
   printf '%s\n' "${versions[@]}" | grep -qx 2; then
  echo "mixed CHHT v1/v2 spool identities; manual forensic recovery required" >&2
  exit 1
fi
if [[ "${versions[0]}" == "2" ]]; then
  [[ "${mode}" == "--check" ]] || { echo "refusing to archive an active-format CHHT v2 spool" >&2; exit 1; }
  echo "heat spool preflight: CHHT v2"
  exit 0
fi

if [[ "${mode}" == "--check" ]]; then
  echo "CHHT v1 heat spool found at ${spool_dir}; deployment stopped without modifying data" >&2
  echo "after review, run: $0 '${spool_dir}' --archive-v1" >&2
  exit 1
fi

archive="${spool_dir}.chht-v1-archive-$(date -u +%Y%m%dT%H%M%SZ)"
[[ ! -e "${archive}" ]] || { echo "archive target already exists: ${archive}" >&2; exit 1; }
parent="$(dirname "${spool_dir}")"
permissions="$(stat -c %a "${spool_dir}")"
owner="$(stat -c %u "${spool_dir}")"
group="$(stat -c %g "${spool_dir}")"
exec 9>&-
mv -- "${spool_dir}" "${archive}"
install -d -m "${permissions}" -o "${owner}" -g "${group}" "${spool_dir}"
[[ "$(stat -c %u "${spool_dir}")" == "${owner}" &&
   "$(stat -c %g "${spool_dir}")" == "${group}" ]] || {
  echo "replacement spool ownership does not match archived directory" >&2
  exit 1
}
sync -d "${parent}" 2>/dev/null || sync
echo "archived CHHT v1 spool without deletion: ${archive}"
echo "created empty directory for CHHT v2: ${spool_dir}"
