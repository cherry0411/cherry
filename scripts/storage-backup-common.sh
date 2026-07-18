#!/usr/bin/env bash
set -euo pipefail

load_cherry_backup_config() {
  local config_dump remote_name
  : "${CHERRY_BACKUP_CONFIG:=/etc/cherry-backup.env}"
  if [[ ! -r "${CHERRY_BACKUP_CONFIG}" ]]; then
    echo "missing ${CHERRY_BACKUP_CONFIG}" >&2
    return 1
  fi
  # shellcheck disable=SC1090
  source "${CHERRY_BACKUP_CONFIG}"
  : "${CHERRY_BACKUP_REMOTE:?set CHERRY_BACKUP_REMOTE in ${CHERRY_BACKUP_CONFIG}}"
  : "${CHERRY_RCLONE_CONFIG:=/etc/rclone/cherry.conf}"
  : "${CHERRY_RCLONE_ESCROW_MARKER:=/etc/cherry-rclone-key-escrowed}"
  : "${CHERRY_BACKUP_HOST_ID:=$(hostname -s)}"
  : "${CHERRY_BACKUP_KEEP_BASES:=6}"
  : "${CHERRY_BACKUP_WAL_RETENTION_DAYS:=50}"
  : "${CHERRY_BACKUP_LOCAL_WAL_HOURS:=24}"
  : "${CHERRY_BACKUP_PG_MAX_RATE:=64M}"

  [[ "${CHERRY_BACKUP_REMOTE}" == *:* ]] || {
    echo "CHERRY_BACKUP_REMOTE must be an rclone remote:path" >&2
    return 1
  }
  [[ "${CHERRY_BACKUP_HOST_ID}" =~ ^[A-Za-z0-9._-]+$ ]] || {
    echo "invalid CHERRY_BACKUP_HOST_ID" >&2
    return 1
  }
  [[ "${CHERRY_BACKUP_KEEP_BASES}" =~ ^[0-9]+$ && "${CHERRY_BACKUP_KEEP_BASES}" -ge 2 ]] || {
    echo "CHERRY_BACKUP_KEEP_BASES must be at least 2" >&2
    return 1
  }
  [[ "${CHERRY_BACKUP_WAL_RETENTION_DAYS}" =~ ^[0-9]+$ &&
     "${CHERRY_BACKUP_WAL_RETENTION_DAYS}" -ge $((CHERRY_BACKUP_KEEP_BASES * 7 + 2)) ]] || {
    echo "WAL retention must exceed the retained weekly base span" >&2
    return 1
  }
  [[ -r "${CHERRY_RCLONE_CONFIG}" ]] || {
    echo "missing rclone config ${CHERRY_RCLONE_CONFIG}" >&2
    return 1
  }
  [[ -r "${CHERRY_RCLONE_ESCROW_MARKER}" ]] || {
    echo "missing crypt-key escrow marker ${CHERRY_RCLONE_ESCROW_MARKER}" >&2
    return 1
  }
  expected_config_sha="$(tr -d '[:space:]' <"${CHERRY_RCLONE_ESCROW_MARKER}")"
  actual_config_sha="$(sha256sum "${CHERRY_RCLONE_CONFIG}" | awk '{print $1}')"
  [[ "${expected_config_sha}" =~ ^[0-9a-f]{64}$ &&
     "${expected_config_sha}" == "${actual_config_sha}" ]] || {
    echo "rclone config changed or has not been independently escrowed" >&2
    return 1
  }

  remote_name="${CHERRY_BACKUP_REMOTE%%:*}"
  config_dump="$(rclone --config "${CHERRY_RCLONE_CONFIG}" config dump)"
  jq -e --arg name "${remote_name}" '.[$name].type == "crypt"' \
    <<<"${config_dump}" >/dev/null || {
      echo "${remote_name}: must be an rclone crypt remote" >&2
      return 1
    }

  CHERRY_BACKUP_REMOTE="${CHERRY_BACKUP_REMOTE%/}"
  CHERRY_BACKUP_REMOTE_ROOT="${CHERRY_BACKUP_REMOTE}/${CHERRY_BACKUP_HOST_ID}"
  CHERRY_RCLONE=(rclone --config "${CHERRY_RCLONE_CONFIG}")
  export CHERRY_BACKUP_REMOTE_ROOT
}

check_cherry_backup_remote() {
  "${CHERRY_RCLONE[@]}" mkdir "${CHERRY_BACKUP_REMOTE_ROOT}/base"
  "${CHERRY_RCLONE[@]}" mkdir "${CHERRY_BACKUP_REMOTE_ROOT}/wal"
  "${CHERRY_RCLONE[@]}" lsd "${CHERRY_BACKUP_REMOTE_ROOT}" >/dev/null
}
