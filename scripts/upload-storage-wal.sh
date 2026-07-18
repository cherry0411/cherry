#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=storage-backup-common.sh
source /usr/local/sbin/storage-backup-common.sh
load_cherry_backup_config
check_cherry_backup_remote

DATA_ROOT="${CHERRY_DATA_ROOT:-/srv/cherry}"
WAL_DIR="${DATA_ROOT}/backups/wal"
READY_FILE=/var/lib/cherry-backup/offsite-ready
exec 9>/run/lock/cherry-storage-wal-upload.lock
flock -n 9 || exit 0

find "${WAL_DIR}" -maxdepth 1 -type f -name '*.gz' -print0 | while IFS= read -r -d '' wal; do
  digest="${wal}.sha256"
  base="$(basename "${wal}")"
  if [[ -e "${digest}" ]]; then
    (cd "${WAL_DIR}" && sha256sum -c "$(basename "${digest}")" >/dev/null)
  else
    printf '%s  %s\n' "$(sha256sum "${wal}" | awk '{print $1}')" "${base}" >"${digest}.tmp"
    mv -- "${digest}.tmp" "${digest}"
  fi
done

"${CHERRY_RCLONE[@]}" copy "${WAL_DIR}" "${CHERRY_BACKUP_REMOTE_ROOT}/wal" \
  --include '*.gz' --include '*.gz.sha256' --immutable --transfers 1 --checkers 2 \
  --contimeout 15s --timeout 5m
"${CHERRY_RCLONE[@]}" check "${WAL_DIR}" "${CHERRY_BACKUP_REMOTE_ROOT}/wal" \
  --include '*.gz' --include '*.gz.sha256' --one-way --size-only --checkers 2

find "${WAL_DIR}" -maxdepth 1 -type f \( -name '*.gz' -o -name '*.gz.sha256' \) \
  -mmin "+$((CHERRY_BACKUP_LOCAL_WAL_HOURS * 60))" -delete

# Never age out remote WAL when fresh base backups have stopped succeeding.
if [[ -s "${READY_FILE}" ]]; then
  verified_epoch="$(sed -n 's/^verified_epoch=//p' "${READY_FILE}")"
  if [[ "${verified_epoch}" =~ ^[0-9]+$ ]] &&
     (( $(date -u +%s) - verified_epoch < 8 * 86400 )); then
    "${CHERRY_RCLONE[@]}" delete "${CHERRY_BACKUP_REMOTE_ROOT}/wal" \
      --min-age "${CHERRY_BACKUP_WAL_RETENTION_DAYS}d" \
      --include '*.gz' --include '*.gz.sha256' --rmdirs
  fi
fi
