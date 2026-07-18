#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=storage-backup-common.sh
source /usr/local/sbin/storage-backup-common.sh
load_cherry_backup_config
check_cherry_backup_remote

REPO_DIR="${CHERRY_REPO_DIR:-${REPO_DIR:-/opt/cherry}}"
ENV_FILE="${REPO_DIR}/deploy/storage/.env"
READY_FILE=/var/lib/cherry-backup/offsite-ready
LOCK_FILE=/run/lock/cherry-storage-backup.lock
POSTGRES_IMAGE='postgres:17.10-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193'
COMPOSE=(docker compose --env-file "${ENV_FILE}" -f "${REPO_DIR}/deploy/storage/compose.yml")

exec 9>"${LOCK_FILE}"
flock -n 9 || { echo "storage backup already running" >&2; exit 1; }

if [[ "${1:-}" == "--gate" && -s "${READY_FILE}" ]]; then
  ready_remote="$(sed -n 's/^remote=//p' "${READY_FILE}")"
  ready_id="$(sed -n 's/^backup_id=//p' "${READY_FILE}")"
  ready_epoch="$(sed -n 's/^verified_epoch=//p' "${READY_FILE}")"
  latest_id="$("${CHERRY_RCLONE[@]}" cat "${CHERRY_BACKUP_REMOTE_ROOT}/base/LATEST" 2>/dev/null || true)"
  if [[ "${ready_remote}" == "${CHERRY_BACKUP_REMOTE_ROOT}" &&
        "${ready_id}" == "${latest_id}" &&
        "${latest_id}" =~ ^[A-Za-z0-9._-]+$ &&
        "${ready_epoch}" =~ ^[0-9]+$ ]] &&
     (( $(date -u +%s) >= ready_epoch && $(date -u +%s) - ready_epoch < 8 * 86400 )) &&
     "${CHERRY_RCLONE[@]}" cat "${CHERRY_BACKUP_REMOTE_ROOT}/base/${latest_id}/SHA256SUMS" >/dev/null 2>&1; then
    echo "existing off-host backup gate is valid: ${latest_id}"
    exit 0
  fi
fi

[[ -r "${ENV_FILE}" ]] || { echo "missing ${ENV_FILE}" >&2; exit 1; }
# shellcheck disable=SC1090
source "${ENV_FILE}"
: "${POSTGRES_USER:?missing POSTGRES_USER}"
: "${POSTGRES_DB:?missing POSTGRES_DB}"
DATA_ROOT="${CHERRY_DATA_ROOT:-/srv/cherry}"

git_id="$(git -C "${REPO_DIR}" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)"
backup_id="$(date -u +%Y%m%dT%H%M%SZ)-${git_id}-$$"
stage="${DATA_ROOT}/backups/staging/${backup_id}"
failed="${DATA_ROOT}/backups/failed/${backup_id}"
pg_plain="${stage}/postgres"
manifest_tmp="${stage}.SHA256SUMS.tmp"
api_was_running=0
success=0

cleanup() {
  rm -f -- "${manifest_tmp}"
  if [[ "${api_was_running}" -eq 1 ]]; then
    "${COMPOSE[@]}" start api >/dev/null || true
  fi
  if [[ "${success}" -eq 1 ]]; then
    rm -rf -- "${stage}"
  elif [[ -d "${stage}" ]]; then
    mkdir -p -- "$(dirname "${failed}")"
    mv -- "${stage}" "${failed}" || true
    echo "failed backup retained at ${failed}" >&2
  fi
}
trap cleanup EXIT

install -d -m 0700 "${stage}" "${pg_plain}"
pg_bytes="$(du -sb "${DATA_ROOT}/postgres" | awk '{print $1}')"
heat_bytes="$(du -sb "${DATA_ROOT}/api/heat" | awk '{print $1}')"
available_bytes="$(df --output=avail -B1 "${DATA_ROOT}/backups" | tail -n 1 | tr -d ' ')"
required_bytes=$((pg_bytes * 2 + heat_bytes * 2 + 1073741824))
if (( available_bytes < required_bytes )); then
  echo "insufficient staging space: need ${required_bytes}, have ${available_bytes}" >&2
  exit 1
fi

# The heat snapshot MUST precede the PostgreSQL base backup. If a day seals
# between them, recovery has either its PG frame or the earlier SQLite source.
if [[ -n "$("${COMPOSE[@]}" ps --status running -q api)" ]]; then
  api_was_running=1
  "${COMPOSE[@]}" stop -t 30 api
fi
# The rolling database contains a stable 64-bit actor token for at most 24h.
# It is deliberately disposable and must never become long-lived through a
# backup. Daily files contain only storage-side day-repseudonymized actors.
ZSTD_CLEVEL=1 tar --zstd -C "${DATA_ROOT}/api" \
  --exclude='heat/heat-rolling-24h.sqlite3' \
  --exclude='heat/heat-rolling-24h.sqlite3-*' \
  -cpf "${stage}/heat.tar.zst" heat
# Do not use grep -q under pipefail here: an early match can SIGPIPE tar and
# turn the true branch into status 141, silently bypassing this privacy gate.
if tar --zstd -tf "${stage}/heat.tar.zst" | grep 'heat-rolling-24h\.sqlite3' >/dev/null; then
  echo "privacy gate failed: rolling actor database entered heat backup" >&2
  exit 1
fi
if [[ "${api_was_running}" -eq 1 ]]; then
  "${COMPOSE[@]}" start api >/dev/null
  api_was_running=0
fi
heat_snapshot_at="$(date -u +%FT%TZ)"

"${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" -Atc 'SELECT pg_switch_wal()' >/dev/null
"${COMPOSE[@]}" exec -T -u root postgres sh -c \
  "mkdir -p '/var/lib/postgresql/backups/staging/${backup_id}/postgres' && chown -R postgres:postgres '/var/lib/postgresql/backups/staging/${backup_id}/postgres'"
"${COMPOSE[@]}" exec -T -u postgres postgres pg_basebackup \
  -h /var/run/postgresql -U "${POSTGRES_USER}" -D "/var/lib/postgresql/backups/staging/${backup_id}/postgres" \
  --format=plain --wal-method=stream --checkpoint=spread --manifest-checksums=SHA256 \
  --label="cherry-${backup_id}" --max-rate="${CHERRY_BACKUP_PG_MAX_RATE}" --progress

ZSTD_CLEVEL=1 tar --zstd -C "${pg_plain}" -cpf "${stage}/postgres.tar.zst" .
rm -rf -- "${pg_plain}"

# Restore credentials remain encrypted off-host. Archive an explicit allowlist:
# storage .env includes the daily re-HMAC key needed to continue an in-progress
# daily SQLite source without double counting, but not the crawler-only actor
# master. Without that master or rolling DB, daily pseudonyms cannot be linked
# back to stable rolling actors or source IPs.
ZSTD_CLEVEL=1 tar --zstd -cpf "${stage}/secrets.tar.zst" \
  -C "${REPO_DIR}/deploy/storage" .env -C /etc cherry-backup.env
tar --zstd -tf "${stage}/secrets.tar.zst" | \
  /usr/local/sbin/cherry-storage-secret-privacy-gate
pg_lsn="$("${COMPOSE[@]}" exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Atc 'SELECT pg_current_wal_lsn()')"
cat >"${stage}/backup.meta" <<EOF
format=cherry-storage-backup-v1
backup_id=${backup_id}
host_id=${CHERRY_BACKUP_HOST_ID}
git_commit=${git_id}
heat_snapshot_at=${heat_snapshot_at}
completed_at=$(date -u +%FT%TZ)
postgres_lsn=${pg_lsn}
postgres_image=${POSTGRES_IMAGE}
EOF
(
  cd "${stage}"
  find . -type f -print0 | sort -z | xargs -0 sha256sum >"${manifest_tmp}"
)
mv -- "${manifest_tmp}" "${stage}/SHA256SUMS"
(cd "${stage}" && sha256sum -c SHA256SUMS >/dev/null)

remote_stage="${CHERRY_BACKUP_REMOTE_ROOT}/base/${backup_id}"
"${CHERRY_RCLONE[@]}" copy "${stage}" "${remote_stage}" \
  --immutable --transfers 1 --checkers 2 --contimeout 15s --timeout 10m
remote_manifest="$(mktemp)"
trap 'rm -f -- "${remote_manifest}"; cleanup' EXIT
"${CHERRY_RCLONE[@]}" cat "${remote_stage}/SHA256SUMS" >"${remote_manifest}"
cmp -s "${stage}/SHA256SUMS" "${remote_manifest}" || {
  echo "remote manifest readback mismatch" >&2
  exit 1
}
printf '%s\n' "${backup_id}" | "${CHERRY_RCLONE[@]}" rcat "${CHERRY_BACKUP_REMOTE_ROOT}/base/LATEST"

mapfile -t expired_bases < <(
  "${CHERRY_RCLONE[@]}" lsf "${CHERRY_BACKUP_REMOTE_ROOT}/base" --dirs-only |
    sed 's:/$::' | grep -E '^[A-Za-z0-9._-]+$' | sort -r | tail -n "+$((CHERRY_BACKUP_KEEP_BASES + 1))"
)
for expired_id in "${expired_bases[@]}"; do
  "${CHERRY_RCLONE[@]}" purge "${CHERRY_BACKUP_REMOTE_ROOT}/base/${expired_id}"
done

install -d -m 0700 "$(dirname "${READY_FILE}")"
cat >"${READY_FILE}" <<EOF
remote=${CHERRY_BACKUP_REMOTE_ROOT}
backup_id=${backup_id}
verified_epoch=$(date -u +%s)
verified_at=$(date -u +%FT%TZ)
EOF
chmod 0600 "${READY_FILE}"
success=1
rm -f -- "${remote_manifest}"
echo "off-host backup committed and manifest read back: ${backup_id}"
