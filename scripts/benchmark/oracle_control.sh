#!/usr/bin/env bash

# Shared lifecycle helpers for the benchmark uniqueness oracle. This file is
# sourced by controllers that already enable `set -euo pipefail`.

oracle_health_is_up() {
  local base="$1"
  curl -fsS --max-time 2 "${base}/health" >/dev/null 2>&1
}

oracle_pid_is_running() {
  local pid="${1:-}"
  [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null
}

oracle_pid_matches_binary() {
  local pid="$1" binary="$2" actual expected
  oracle_pid_is_running "${pid}" || return 1
  actual="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
  expected="$(readlink -f "${binary}" 2>/dev/null || true)"
  [[ -n "${actual}" && -n "${expected}" && "${actual}" == "${expected}" ]]
}

oracle_pid_has_data_path() {
  local pid="$1" expected="$2" argument previous=""
  expected="$(readlink -m "${expected}")"
  while IFS= read -r -d '' argument; do
    if [[ "${previous}" == "-data" && "$(readlink -m "${argument}")" == "${expected}" ]]; then
      return 0
    fi
    previous="${argument}"
  done < "/proc/${pid}/cmdline"
  return 1
}

oracle_stop_managed_sink() {
  local pidfile="$1" binary="$2" base="$3" pid=""
  if [[ -f "${pidfile}" ]]; then
    pid="$(cat "${pidfile}" 2>/dev/null || true)"
  fi
  if ! oracle_pid_is_running "${pid}"; then
    rm -f "${pidfile}"
    if oracle_health_is_up "${base}"; then
      echo "oracle endpoint is healthy but ${pidfile} has no live owner; refusing to stop an unmanaged service" >&2
      return 1
    fi
    return 0
  fi
  if ! oracle_pid_matches_binary "${pid}" "${binary}"; then
    echo "PID ${pid} from ${pidfile} is not ${binary}; refusing to signal a reused or foreign PID" >&2
    return 1
  fi
  kill -TERM "${pid}" 2>/dev/null || true
  # Reap the process when it is our child; for a detached process wait returns
  # immediately and the bounded polling below remains authoritative.
  wait "${pid}" 2>/dev/null || true
  for _ in $(seq 1 100); do
    oracle_pid_is_running "${pid}" || break
    sleep 0.1
  done
  if oracle_pid_is_running "${pid}"; then
    kill -KILL "${pid}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      oracle_pid_is_running "${pid}" || break
      sleep 0.1
    done
  fi
  if oracle_pid_is_running "${pid}"; then
    echo "oracle PID ${pid} did not stop" >&2
    return 1
  fi
  rm -f "${pidfile}"
  for _ in $(seq 1 20); do
    oracle_health_is_up "${base}" || return 0
    sleep 0.1
  done
  echo "oracle endpoint ${base} remained healthy after managed PID ${pid} stopped" >&2
  return 1
}

oracle_start_sink() {
  local binary="$1" base="$2" data="$3" baseline="$4" log="$5" pidfile="$6" detached="${7:-false}"
  local listen pid
  [[ "${base}" == http://* ]] || { echo "managed oracle requires an http:// sink URL: ${base}" >&2; return 2; }
  oracle_health_is_up "${base}" && { echo "oracle endpoint is already in use: ${base}" >&2; return 1; }
  listen="${base#http://}"
  mkdir -p "$(dirname "${data}")" "$(dirname "${log}")" "$(dirname "${pidfile}")"
  local -a command=(env GOMAXPROCS=1 GOMEMLIMIT=384MiB "${binary}" -listen "${listen}" -data "${data}")
  [[ -n "${baseline}" ]] && command+=(-baseline "${baseline}")
  if [[ "${detached}" == true ]]; then
    nohup "${command[@]}" >>"${log}" 2>&1 &
  else
    "${command[@]}" >>"${log}" 2>&1 &
  fi
  pid=$!
  printf '%s\n' "${pid}" > "${pidfile}"
  ORACLE_STARTED_PID="${pid}"
  for _ in $(seq 1 50); do
    if oracle_health_is_up "${base}"; then
      return 0
    fi
    if ! oracle_pid_is_running "${pid}"; then
      echo "oracle exited before becoming healthy; see ${log}" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "oracle did not become healthy; see ${log}" >&2
  return 1
}

oracle_validate_record_file() {
  local path="$1" bytes
  [[ -f "${path}" ]] || { echo "oracle file does not exist: ${path}" >&2; return 1; }
  bytes="$(stat -c %s "${path}")"
  if ((bytes % 21 != 0)); then
    echo "oracle file has a trailing partial record: ${path} (${bytes} bytes)" >&2
    return 1
  fi
}

oracle_freeze_baseline() {
  local production="$1" baseline="$2" temp
  oracle_validate_record_file "${production}"
  mkdir -p "$(dirname "${baseline}")"
  temp="${baseline}.tmp.$$"
  rm -f "${temp}"
  cp --reflink=auto -- "${production}" "${temp}"
  oracle_validate_record_file "${temp}"
  chmod 0444 "${temp}"
  mv -- "${temp}" "${baseline}"
}
